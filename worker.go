package jobmanager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type WorkerConfig struct {
	PollInterval              time.Duration
	PollIntervalRandomization time.Duration
	MaxPollInterval           time.Duration
	TaskDeadline              time.Duration
	HeartbeatInterval         time.Duration
	Retrier                   Retrier
}

// TODO(low): implement subscription mechanism for job updates between workers - if worker received/saved job, other workers should update their state to reduce races. Can be done via storage wrapper.

const (
	defaultPollInterval              = 200 * time.Millisecond
	defaultPollIntervalRandomization = 50 * time.Millisecond
	defaultMaxPollInterval           = 2 * time.Second
	defaultTaskDeadline              = 30 * time.Second
	defaultHeartbeatInterval         = 5 * time.Second
)

// Worker - iterates through jobs and executes tasks from jobs. Can execute only one task at a time. Task processing concurrency is controlled by number of workers.
type worker struct {
	id       string
	registry jobRegistry
	storage  Storage
	config   WorkerConfig
	retrier  Retrier
	lgr      Logger
	metrics  Metrics

	// Cache to minimize S3 requests
	jobCache map[JobCode]jobCacheEntry
}

type jobCacheEntry struct {
	version   string
	nextPoll  time.Time
	exhausted bool // true if job reached maxIterations
}

func newWorker(registry jobRegistry, storage Storage, config WorkerConfig, logger Logger, metrics Metrics) *worker {
	if config.PollInterval <= 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.PollIntervalRandomization <= 0 {
		config.PollInterval = defaultPollIntervalRandomization
	}
	if config.MaxPollInterval <= 0 {
		config.MaxPollInterval = defaultMaxPollInterval
	}
	if config.TaskDeadline <= 0 {
		config.TaskDeadline = defaultTaskDeadline
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = defaultHeartbeatInterval
	}

	if config.Retrier == nil {
		config.Retrier = NewRetrier(RetrierConfig{}, logger)
	}

	return &worker{
		id:       uuid.New().String(),
		registry: registry,
		storage:  storage,
		config:   config,
		retrier:  config.Retrier,
		jobCache: make(map[JobCode]jobCacheEntry),
		lgr:      logger.With(slog.String("component", "worker")),
		metrics:  metrics,
	}
}

func (w *worker) start(ctx context.Context) error {
	ctx = w.lgr.CtxWithLogAttrs(ctx, slog.String("worker", w.id))
	w.lgr.InfoContext(ctx, "Starting worker")

	pollInterval := w.config.PollInterval

	for {
		select {
		case <-ctx.Done():
			w.lgr.InfoContext(ctx, "Stopping worker")
			return ctx.Err()
		default:
			// Adaptive polling: if no work, increase interval
			if !w.processJobs(ctx) {
				pollInterval = minDuration(pollInterval*2, w.config.MaxPollInterval)
			} else {
				pollInterval = w.config.PollInterval
			}

			time.Sleep(pollInterval + time.Duration(rand.Int63n(int64(w.config.PollIntervalRandomization)))) // Reduce strong concurrency between workers
		}
	}
}

func (w *worker) processJobs(ctx context.Context) bool {
	jobCodes := w.registry.listJobs()
	workDone := false

	for _, jobCode := range jobCodes {
		if w.processJob(ctx, jobCode) {
			workDone = true
		}
	}

	return workDone
}

func (w *worker) processJob(ctx context.Context, jobCode JobCode) bool {
	ctx = w.lgr.CtxWithLogAttrs(ctx, slog.String("job_code", jobCode.String()))

	if !w.shouldPollJob(jobCode) {
		return false
	}

	job, err := w.storage.GetJob(ctx, jobCode)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			job, err = w.createNewJob(ctx, jobCode, nil)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return false
				}
				w.lgr.ErrorContext(ctx, fmt.Errorf("failed to store new job %s: %w", jobCode, err).Error())
				return false
			}
			// go to process job
		} else if errors.Is(err, context.Canceled) {
			return false
		} else {
			w.lgr.ErrorContext(ctx, fmt.Errorf("failed to get job '%s': %w", jobCode, err).Error())
			return false
		}
	}

	w.updateCache(jobCode, job.Version(), false)

	return w.tryProcessJob(ctx, job)
}

func (w *worker) shouldPollJob(jobCode JobCode) bool {
	entry, exists := w.jobCache[jobCode]
	if !exists {
		return true
	}

	// Don't poll jobs that exhausted iteration limit
	if entry.exhausted {
		return false
	}

	return time.Now().After(entry.nextPoll)
}

// createNewJob creates a new job instance if it doesn't exist.
func (w *worker) createNewJob(ctx context.Context, code JobCode, metadata map[string]any) (*Job, error) {
	// Jobs can only be created from code, creation outside code makes no sense since there won't be task handlers in code.
	ctx = w.lgr.CtxWithLogAttrs(ctx, slog.String("action", "create_new_job"))

	jobDef, err := w.registry.getJob(code)
	if err != nil {
		return nil, fmt.Errorf("job %s not registered: %w", code, err)
	}

	job := NewJob(code, w.createTasksFromJobDef(jobDef), metadata, w.id)

	err = w.saveJobState(ctx, job, func(savedJob *Job) (updatedJob *Job, needRetry bool, err error) {
		// TODO(low): in saveJobState on ErrConcurrentModification we re-read job, which is unnecessary in this case.
		w.lgr.DebugContext(ctx, fmt.Sprintf("Job has concurrent modification when creating - skip"))
		return savedJob, false, nil // someone beat us to it
	})
	if err != nil {
		return nil, err
	}

	w.lgr.InfoContext(ctx, fmt.Sprintf("New job '%s' created (id: %s) by worker '%s'", code, job.ID(), job.updatedByWorkerID))

	return job, nil
}

func (w *worker) tryProcessJob(ctx context.Context, job *Job) bool {
	if job.IsReadyToNextIteration() {
		var iterErr error
		job, iterErr = w.startNewJobIteration(ctx, job)
		if iterErr != nil {
			w.lgr.ErrorContext(ctx, fmt.Errorf("failed to start job iteration: %w", iterErr).Error())
			return false
		}
	} else if job.IsProcessed() && job.IsIterationLimitReached() {
		// job completed and reached iteration limit - don't process anymore
		w.updateCache(job.Code(), job.Version(), true)
		return false
	}
	ctx = w.lgr.CtxWithLogAttrs(ctx, slog.String("job_id", job.ID()))

	if job.IsReadyForProcessing() {
		wasAttemptProcessing, err := w.pickAndExecuteTask(ctx, job)
		if err != nil {
			if !errors.Is(err, ErrTaskWorkerMismatch) {
				w.lgr.ErrorContext(ctx, fmt.Errorf("failed to execute task: %w", err).Error())
			} else if !errors.Is(err, context.Canceled) {
				return false
			} else {
				w.lgr.DebugContext(ctx, fmt.Errorf("failed to start job iteration: %w", err).Error())
			}
		}

		return wasAttemptProcessing
	}

	return false
}

func (w *worker) startNewJobIteration(ctx context.Context, job *Job) (*Job, error) {
	// This can be either first job run or next iteration of job run.
	ctx = w.lgr.CtxWithLogAttrs(ctx, slog.String("action", "start_new_job_iteration"))

	jobDef, err := w.registry.getJob(job.Code())
	if err != nil {
		return nil, fmt.Errorf("failed to get job definition %s: %w", job.Code(), err)
	}

	err = job.NextIteration(w.createTasksFromJobDef(jobDef), w.id)
	if err != nil {
		return nil, err
	}

	err = w.saveJobState(ctx, job, func(savedJob *Job) (updatedJob *Job, needRetry bool, err error) {
		// TODO(low): in saveJobState on ErrConcurrentModification we re-read job, which is unnecessary in this case.
		w.lgr.DebugContext(ctx, fmt.Sprintf("Job has concurrent modification when starting new iteration - skip"))
		return savedJob, false, nil // someone beat us to it - exit with updated job
	})
	if err != nil {
		return nil, err
	}

	w.lgr.InfoContext(ctx, fmt.Sprintf("New job '%s' iteration started (id: %s) by worker '%s'", job.Code(), job.ID(), job.updatedByWorkerID))

	return job, nil
}

func (w *worker) pickAndExecuteTask(ctx context.Context, job *Job) (wasAttemptProcessing bool, err error) {
	ctx = w.lgr.CtxWithLogAttrs(ctx, slog.String("action", "pick_and_execute_task"))

	tsk, err := job.PickTaskToExecute(w.id)
	if err != nil {
		// TODO(low): think about what to do here, likely we have invalid job state
		return false, err
	}
	if tsk == nil {
		w.lgr.DebugContext(ctx, fmt.Sprintf("Tasks for job %s not found", job.Code()))
		return false, nil
	}
	ctx = w.lgr.CtxWithLogAttrs(ctx, slog.String("task_code", tsk.Code().String()), slog.String("task_id", tsk.ID()))

	err = job.StartTask(tsk.ID(), w.id, w.config.TaskDeadline)
	if err != nil {
		return true, err
	}

	w.lgr.DebugContext(ctx, fmt.Sprintf("Task '%s' started", tsk.Code()))
	err = w.saveJobState(ctx, job, func(savedJob *Job) (updatedJob *Job, needRetry bool, err error) {
		err = savedJob.StartTask(tsk.ID(), w.id, w.config.TaskDeadline)
		if err != nil {
			w.lgr.DebugContext(ctx, "Job has concurrent modification when picking task - skip")
			return savedJob, false, err // task taken by another worker or something went wrong - don't retry
		}
		w.lgr.DebugContext(ctx, "Job has concurrent modification when picking task - retry")
		return savedJob, true, nil
	})
	if err != nil {
		if errors.Is(err, ErrTaskWorkerMismatch) { // task taken by another worker
			w.lgr.InfoContext(ctx, err.Error())
			return true, nil
		}
		return true, fmt.Errorf("save job failed after task started: %w", err)
	}

	w.lgr.InfoContext(ctx, fmt.Sprintf("Started processing task '%s' (code: %s)", tsk.ID(), tsk.Code()))

	return true, w.executeTask(ctx, job, tsk)
}

func (w *worker) executeTask(ctx context.Context, job *Job, tsk *task) error {
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()

	go w.startHeartbeat(heartbeatCtx, job.Clone(), tsk.Clone())

	executor, err := w.registry.getTaskExecutor(job.Code(), tsk.Code())
	if err != nil {
		return err
	}

	err = executor(ctx, tsk, newJobManager(job, w.storage, w.id))
	if err != nil {
		w.lgr.InfoContext(ctx, fmt.Errorf("task execution failed: %w", err).Error())
		if err := job.FailTask(tsk.ID(), err.Error()); err != nil {
			return err
		}
		saveErr := w.saveProcessedTask(ctx, job, tsk.ID(), func(savedJob *Job) (updatedJob *Job, needRetry bool, err error) {
			w.lgr.DebugContext(ctx, fmt.Sprintf("Job '%s' has concurrent modification after task execute failed - retry (worker job status: '%s', saved status: '%s')", job.Code(), job.Status(), savedJob.Status()))
			err = savedJob.MergeWithWorkerTasks(job, w.id)
			if err != nil {
				return savedJob, false, err // task taken by another worker or something went wrong - don't retry
			}
			return savedJob, true, nil
		})
		if saveErr != nil {
			return fmt.Errorf("saving job (status: %s) with fail task fail after execution: %w", job.Status(), saveErr)
		}

		return nil
	}

	w.lgr.InfoContext(ctx, fmt.Sprintf("Task '%s' handled successful ('%s')", tsk.Code(), tsk.Status()))

	w.tryCompleteJob(ctx, job)

	saveErr := w.saveProcessedTask(ctx, job, tsk.ID(), func(savedJob *Job) (updatedJob *Job, needRetry bool, err error) {
		w.lgr.DebugContext(ctx, fmt.Sprintf("Job '%s' has concurrent modification after task execute success - retry (worker job status: '%s', saved status: '%s')", job.Code(), job.Status(), savedJob.Status()))
		err = savedJob.MergeWithWorkerTasks(job, w.id)
		if err != nil {
			return savedJob, false, err // task taken by another worker or something went wrong - don't retry
		}
		w.tryCompleteJob(ctx, savedJob) // conditions for job completion might have been met (another worker completed task)
		return savedJob, true, nil
	})
	if saveErr != nil {
		return fmt.Errorf("saving job (status: %s) with complete task fail after execution: %w", job.Status(), saveErr)
	}

	if job.IsProcessed() {
		w.jobCompleted(ctx, job)
	}

	return nil
}

func (w *worker) startHeartbeat(ctx context.Context, job *Job, task *task) {
	ctx = w.lgr.CtxWithLogAttrs(ctx, slog.String("source", "heartbeat"))
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := job.UpdateTaskHeartbeat(task.ID(), w.id, w.config.TaskDeadline)
			if err != nil {
				w.lgr.ErrorContext(ctx, fmt.Errorf("failed to update heartbeat for task %s: %w", task.Code(), err).Error())
				return
			}
			w.lgr.DebugContext(ctx, fmt.Sprintf("Heartbeat updating for task %s", task.Code()))
			err = w.saveJobState(ctx, job, func(savedJob *Job) (updatedJob *Job, needRetry bool, err error) {
				if ctx.Err() != nil {
					return savedJob, false, err
				}
				err = job.UpdateTaskHeartbeat(task.ID(), w.id, w.config.TaskDeadline)
				if err != nil {
					return savedJob, false, err
				}
				return savedJob, true, nil
			})
			if err != nil {
				if errors.Is(err, ErrTaskWorkerMismatch) {
					w.lgr.DebugContext(ctx, fmt.Sprintf("Heartbeat cannot update task - worker changed %s", task.Code()))
					return
				}
				if errors.Is(err, context.Canceled) {
					w.lgr.DebugContext(ctx, fmt.Sprintf("Heartbeat cannot update task - context cancelled %s", task.Code()))
					return
				}
				w.lgr.ErrorContext(ctx, fmt.Errorf("failed to update heartbeat for task %s: %w", task.Code(), err).Error())
				return
			}
			w.lgr.DebugContext(ctx, fmt.Sprintf("Heartbeat updated for task %s", task.Code()))
		}
	}
}

// *job is automatically updated when using concurrentModificationHandler
func (w *worker) saveJobState(ctx context.Context, job *Job, concurrentModificationHandler func(savedJob *Job) (updatedJob *Job, needRetry bool, err error)) error {
	err := w.retrier.Retry(
		ctx, func() (needRetry bool, err error) {
			err = w.storage.SaveJob(ctx, job)
			if err == nil {
				return false, nil
			}

			if errors.Is(err, ErrConcurrentModification) {
				savedJob, err := w.storage.GetJob(ctx, job.Code())
				if err != nil { // TODO(low): need nested retry to update job on errors without restarting entire flow
					return true, err
				}

				updatedJob, needRetry, err := concurrentModificationHandler(savedJob)
				*job = *updatedJob
				return needRetry, err
			}

			return true, err
		},
	)

	return err
}

func (w *worker) updateCache(jobCode JobCode, version string, exhausted bool) {
	w.jobCache[jobCode] = jobCacheEntry{
		version:   version,
		nextPoll:  time.Now().Add(w.config.PollInterval),
		exhausted: exhausted,
	}
}

func (w *worker) createTasksFromJobDef(jobDef JobDefinition) []*task {
	var tasks []*task
	for _, taskDef := range jobDef.InitialTasks() {
		tasks = append(tasks, newTask(taskDef.Code(), w.id, taskDef.Input()))
	}

	return tasks
}

func (w *worker) saveProcessedTask(ctx context.Context, job *Job, taskID string, concurrentModificationHandler func(savedJob *Job) (updatedJob *Job, needRetry bool, err error)) error {
	err := w.saveJobState(ctx, job, concurrentModificationHandler)
	if err != nil {
		return err
	}

	w.lgr.DebugContext(ctx, fmt.Sprintf("Job '%s' saved with processed task (version: %s; tasks: %s)", job.Code(), job.Version(), job.tasksAsString()))

	tsk := job.GetTask(taskID)
	w.metrics.RecordTaskProcessed(ctx, job.Code(), tsk.Code(), tsk.Status(), tsk.CompletedAt().Sub(tsk.StartedAt()))
	return nil
}

func (w *worker) tryCompleteJob(ctx context.Context, job *Job) {
	_, err := job.TryToComplete(w.id)
	if err != nil {
		w.lgr.ErrorContext(ctx, fmt.Errorf("job %s completed error: %w", job.Code(), err).Error())
	}
}

func (w *worker) jobCompleted(ctx context.Context, job *Job) {
	w.lgr.InfoContext(ctx, fmt.Sprintf("Job %s completed", job.Code()))
	w.metrics.RecordJobIterationComplete(ctx, job.Code(), JobCompleted, job.CompletedAt().Sub(job.StartedAt()))
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
