package jobmanager

import (
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
)

var ErrTaskNotFound = errors.New("task not found")

// Job represents a unit of work consisting of multiple tasks
type JobDefinition struct {
	// TODO(med): add schedule time - after successful completion of a job, set `next_start_at` field and worker should remember the next start time to avoid reading this job unnecessarily (only read at next scheduled start).
	code          JobCode
	initialTasks  func() []TaskDefinition
	taskExecutors map[TaskCode]TaskExecutor
	maxIterations uint64 // 0 = unlimited
}

// JobDefinitionOption - function to configure JobDefinition
type JobDefinitionOption func(*JobDefinition)

// WithMaxIterations sets the maximum number of iterations for the job
// 0 means unlimited number of iterations
func WithMaxIterations(maxIterations uint64) JobDefinitionOption {
	return func(jd *JobDefinition) {
		jd.maxIterations = maxIterations
	}
}

// NewJobDefinition creates a new job definition
func NewJobDefinition(code JobCode, initialTasks []TaskDefinition, taskExecutors map[TaskCode]TaskExecutor, opts ...JobDefinitionOption) (JobDefinition, error) {
	if len(initialTasks) == 0 {
		return JobDefinition{}, fmt.Errorf("initial tasks cannot be empty")
	}
	if len(taskExecutors) == 0 {
		return JobDefinition{}, fmt.Errorf("task executors cannot be empty")
	}
	initialTasksCodes := make(map[TaskCode]struct{}, len(initialTasks))
	for _, initialTask := range initialTasks {
		if _, ok := initialTasksCodes[initialTask.Code()]; ok {
			return JobDefinition{}, fmt.Errorf("task with code %s has already been initialized", initialTask.Code())
		}
		if _, ok := taskExecutors[initialTask.Code()]; !ok {
			return JobDefinition{}, fmt.Errorf("cannot find task executor for initial task %s", initialTask.Code())
		}
		initialTasksCodes[initialTask.Code()] = struct{}{}
	}

	jd := JobDefinition{
		code:          code,
		initialTasks:  func() []TaskDefinition { return initialTasks },
		taskExecutors: taskExecutors,
		maxIterations: 0, // unlimited by default
	}

	// Apply options
	for _, opt := range opts {
		opt(&jd)
	}

	return jd, nil
}

func (jd JobDefinition) Code() JobCode {
	return jd.code
}

func (jd JobDefinition) InitialTasks() []TaskDefinition {
	if jd.initialTasks == nil {
		return nil
	}
	return jd.initialTasks()
}

func (jd JobDefinition) TaskExecutors() map[TaskCode]TaskExecutor {
	return jd.taskExecutors
}

func (jd JobDefinition) MaxIterations() uint64 {
	return jd.maxIterations
}

type JobCode string

func (jd JobCode) String() string {
	return string(jd)
}

// validJobStatusTransitions defines allowed transitions between job statuses
var validJobStatusTransitions = map[JobStatus][]JobStatus{
	JobStarted: {
		JobRunning, // tasks started execution
		JobFailed,  // error before execution started
	},
	JobRunning: {
		JobCompleted, // all tasks completed
		JobFailed,    // task failed
	},
	JobCompleted: {
		JobStarted, // new iteration
	},
	JobFailed: {
		JobStarted, // retry
	},
}

type JobStatus string

const (
	JobStarted   JobStatus = "started"   // New job created or new iteration started - tasks can be picked up for work.
	JobRunning   JobStatus = "running"   // Job is in progress: tasks are executing
	JobCompleted JobStatus = "completed" // Job completed successfully
	JobFailed    JobStatus = "failed"
	// TODO(low): add a status for jobs that should not be repeated. For example, a job should only run N times (or only once).
)

func (s JobStatus) String() string {
	return string(s)
}

// To checks if the transition to the new status is allowed and performs it
func (s *JobStatus) To(new JobStatus) error {
	// If status doesn't change - it's ok
	if *s == new {
		return nil
	}

	// Check if transition to new status is allowed
	for _, allowed := range validJobStatusTransitions[*s] {
		if allowed == new {
			*s = new
			return nil
		}
	}

	return fmt.Errorf("job status change to '%s' is not allowed (old: %s)", new, s)
}

type Job struct {
	// IMPORTANT: when modifying fields don't forget about Clone
	id                string
	code              JobCode
	iterNum           uint64
	status            JobStatus
	tasksById         map[string]*task // A single task type (TaskCode) can have multiple tasks.
	updatedByWorkerID string
	startedAt         time.Time
	runningAt         time.Time
	completedAt       time.Time
	nextStartAt       time.Time
	metadata          map[string]any
	version           string
	maxIterations     uint64 // 0 = unlimited, from JobDefinition
}

func NewJob(code JobCode, tasks []*task, metadata map[string]any, workerID string) *Job {
	j := &Job{
		id:                uuid.New().String(),
		code:              code,
		iterNum:           1,
		status:            JobStarted,
		tasksById:         make(map[string]*task),
		metadata:          metadata,
		startedAt:         time.Now(),
		updatedByWorkerID: workerID,
	}

	for _, tsk := range tasks {
		_ = j.AddTask(tsk)
	}

	return j
}

// NextIteration prepares the job for the next iteration
func (j *Job) NextIteration(tasks []*task, workerID string) error {
	if err := j.status.To(JobStarted); err != nil {
		return err
	}

	old := *j
	*j = *(NewJob(j.code, tasks, j.metadata, workerID))
	j.id = old.id
	// TODO(low): in the future, a mechanism for restarting the sequence is needed (currently the maximum sequence is 10^20). Sequential uuid will not work, as there may be a race when creating a new job by different workers.
	j.iterNum = old.iterNum + 1
	j.startedAt = time.Now()

	return nil
}

func (j *Job) ID() string {
	return j.id
}

func (j *Job) Code() JobCode {
	return j.code
}

// IterationNum returns the current job iteration number
func (j *Job) IterationNum() uint64 {
	return j.iterNum
}

func (j *Job) Status() JobStatus {
	return j.status
}

func (j *Job) IsStarted() bool {
	return j.status == JobStarted
}

func (j *Job) IsProcessed() bool {
	return j.status == JobCompleted || j.status == JobFailed
}

func (j *Job) IsReadyForProcessing() bool {
	return j.status == JobStarted || j.status == JobRunning
}

func (j *Job) IsReadyToNextIteration() bool {
	if j.status != JobCompleted && j.status != JobFailed {
		return false
	}

	// Job is exhausted if it's processed and reached iteration limit
	if j.IsIterationLimitReached() {
		return false
	}

	if j.nextStartAt.IsZero() {
		return true
	}

	return time.Now().After(j.nextStartAt)
}

func (j *Job) IsIterationLimitReached() bool {
	// TODO(low): add a special status that we no longer run the job and remove this check at the complete stage
	if j.maxIterations > 0 && j.iterNum >= j.maxIterations {
		return true
	}

	return false
}

func (j *Job) StartedAt() time.Time {
	return j.startedAt
}

func (j *Job) CompletedAt() time.Time {
	return j.completedAt
}

func (j *Job) NextStartAt() time.Time {
	return j.nextStartAt
}

func (j *Job) Metadata() map[string]any {
	return j.metadata
}

func (j *Job) Version() string {
	return j.version
}

func (j *Job) UpdateVersion(version string) {
	j.version = version
}

// ErrTaskNotFound, ErrTaskWorkerMismatch
// StartTask marks task as started by worker
func (j *Job) StartTask(taskID string, workerID string, deadline time.Duration) error {
	tsk := j.tasksById[taskID]
	if tsk == nil {
		return fmt.Errorf("cannot find task with id %s: %w", taskID, ErrTaskNotFound)
	}

	err := tsk.start(workerID, deadline)
	if err != nil {
		return err
	}

	j.updatedByWorkerID = workerID

	return nil
}

// AddTask adds new task to job
func (j *Job) AddTask(task *task) error {
	tsk := j.tasksById[task.ID()]
	if tsk != nil {
		return fmt.Errorf("task with id %s already registered in job", task.ID())
	}

	j.tasksById[task.ID()] = task
	return nil
}

// CompleteTask marks task as completed
func (j *Job) CompleteTask(taskID string, output []byte) error {
	tsk := j.tasksById[taskID]
	if tsk == nil {
		return fmt.Errorf("cannot find task with id %s", taskID)
	}
	return tsk.complete(output)
}

// FailTask marks task as failed
func (j *Job) FailTask(taskID string, errMsg string) error {
	tsk := j.tasksById[taskID]
	if tsk == nil {
		return fmt.Errorf("cannot find task with id %s", taskID)
	}
	return tsk.fail(errMsg)
}

// UpdateTaskHeartbeat updates task heartbeat timestamp
func (j *Job) UpdateTaskHeartbeat(taskID string, workerID string, deadline time.Duration) error {
	tsk := j.tasksById[taskID]
	if tsk == nil {
		return fmt.Errorf("cannot find task with id %s", taskID)
	}

	return tsk.updateHeartbeat(workerID, deadline)
}

// PickTaskToExecute selects a random pending task (to reduce competition between workers)
func (j *Job) PickTaskToExecute(workerID string) (*task, error) {
	if j.Status() != JobRunning {
		if err := j.Work(workerID); err != nil { // change status to Running
			return nil, fmt.Errorf("task cannot be picked up cause invalid job %s state", j.Code())
		}
	}

	// TODO(low): with a large number of tasks in the job, iteration can add overhead. Solution: pending tasks can be cached.
	for _, tsk := range j.tasksById { // Since map iteration is randomized, no additional randomization is needed.
		if tsk.canBePickedUp() {
			return tsk, nil
		}
	}

	if j.allTasksCompleted() {
		return nil, fmt.Errorf("wrong running job %s state - all tasks complete", j.Code())
	}

	return nil, nil
}

// MergeWithWorkerTasks merges worker's task changes into saved job state
func (j *Job) MergeWithWorkerTasks(workerJob *Job, workerID string) error {
	if j.ID() != workerJob.ID() {
		return fmt.Errorf("merge job '%s' failed - IDs is different", j.Code())
	}
	if err := j.status.To(workerJob.Status()); err != nil {
		// TODO(low): its may be ok when job was already saved as completed, but current worker is late with failed task (current worker job status is running)
		return fmt.Errorf("merge job '%s' status failed: %w", j.Code(), err)
	}

	for _, tsk := range workerJob.tasksById {
		// A task can be created by an executor or a worker can update a task.
		// It seems there is no point in checking the validity of task status changes, as a task is processed by one worker and there can be no races.
		if (tsk.createdByWorker == workerID && tsk.ProcessedByWorker() == "") || tsk.ProcessedByWorker() == workerID {
			j.tasksById[tsk.ID()] = tsk
		}
	}

	j.updatedByWorkerID = workerID
	if !workerJob.CompletedAt().IsZero() {
		j.completedAt = workerJob.CompletedAt()
	}
	if !workerJob.runningAt.IsZero() {
		j.completedAt = workerJob.runningAt
	}

	return nil
}

func (j *Job) allTasksCompleted() bool {
	for _, tsk := range j.tasksById {
		if !tsk.IsCompleted() {
			return false
		}
	}
	return len(j.tasksById) > 0
}

func (j *Job) Work(workerID string) error {
	if err := j.status.To(JobRunning); err != nil {
		return err
	}
	j.updatedByWorkerID = workerID
	j.runningAt = time.Now()
	return nil
}

func (j *Job) TryToComplete(workerID string) (bool, error) {
	if !j.allTasksCompleted() {
		return false, nil
	}
	if err := j.status.To(JobCompleted); err != nil {
		return false, err
	}
	j.updatedByWorkerID = workerID
	j.completedAt = time.Now()

	return true, nil
}

func (j *Job) Fail(workerID string) error {
	if err := j.status.To(JobFailed); err != nil {
		return err
	}
	j.updatedByWorkerID = workerID
	j.completedAt = time.Now()
	return nil
}

// ShouldPoll checks if job should be polled
func (j *Job) ShouldPoll() bool {
	if j.nextStartAt.IsZero() {
		return true
	}
	return time.Now().After(j.nextStartAt)
}

func (j *Job) GetTask(id string) *task {
	return j.tasksById[id]
}

func (j *Job) GetTasksByCode(code TaskCode) []*task {
	var result []*task
	for _, task := range j.tasksById {
		if task.Code() == code {
			result = append(result, task)
		}
	}
	return result
}

// Clone creates a deep copy of job
func (j *Job) Clone() *Job {
	clone := *j

	clone.tasksById = make(map[string]*task, len(j.tasksById))
	for i, t := range j.tasksById {
		clone.tasksById[i] = t.Clone()
	}

	clone.metadata = maps.Clone(clone.metadata)

	return &clone
}

func (j *Job) setNextNextStartAt(nextStart time.Time) {
	j.nextStartAt = nextStart
}

// tasksAsString - represent tasks as string for debug
func (j *Job) tasksAsString() string {
	var str []byte
	count := 0
	for id, tsk := range j.tasksById {
		str = append(str, []byte(fmt.Sprintf("id: %s; code: %s; status: %s; ", id, tsk.Code(), tsk.Status()))...)
		count++
		if count > 3 {
			str = append(str, []byte("...")...)
			break
		}
	}

	return string(str)
}
