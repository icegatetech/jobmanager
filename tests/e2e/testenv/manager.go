package testenv

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/icegatetech/jobmanager"
)

type ManagerEnv struct {
	t       *testing.T
	manager *jobmanager.JobsManager
	storage jobmanager.Storage
	logger  jobmanager.Logger
	jobDefs jobmanager.JobDefinitions
}

func NewManagerEnv(
	t *testing.T,
	storage jobmanager.Storage,
	logger jobmanager.Logger,
	jobs jobmanager.JobDefinitions,
	config jobmanager.JobsManagerConfig,
) (*ManagerEnv, error) {
	if config.WorkerCount == 0 {
		config.WorkerCount = 1
	}
	if config.WorkerConfig.PollInterval == 0 {
		config.WorkerConfig.PollInterval = 100 * time.Millisecond
	}
	if config.WorkerConfig.PollIntervalRandomization == 0 {
		config.WorkerConfig.PollIntervalRandomization = 10 * time.Millisecond
	}
	if config.WorkerConfig.TaskDeadline == 0 {
		config.WorkerConfig.TaskDeadline = 5 * time.Second
	}
	if config.WorkerConfig.HeartbeatInterval == 0 {
		config.WorkerConfig.HeartbeatInterval = 1 * time.Second
	}

	manager, err := jobmanager.NewJobsManager(storage, config, logger, jobs)
	if err != nil {
		return nil, fmt.Errorf("failed to create JobsManager: %w", err)
	}

	return &ManagerEnv{
		t:       t,
		manager: manager,
		storage: storage,
		logger:  logger,
		jobDefs: jobs,
	}, nil
}

func (e *ManagerEnv) Start(ctx context.Context) error {
	return e.manager.Start(ctx)
}

func (e *ManagerEnv) Wait() {
	e.manager.Wait()
}

// WaitForAllJobsCompletion waits for completion of all jobs with maxIterations.
// After return, calling code should cancel context to stop workers.
func (e *ManagerEnv) WaitForAllJobsCompletion(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for jobs to complete")
		case <-ticker.C:
			allDone, err := e.checkAllJobsCompleted(ctx)
			if err != nil {
				continue
			}
			if allDone {
				return nil
			}
		}
	}
}

func (e *ManagerEnv) checkAllJobsCompleted(ctx context.Context) (bool, error) {
	jobsWithLimit := 0
	jobsCompleted := 0

	for jobCode, jobDef := range e.jobDefs {
		// Ignore jobs without iteration limit
		if jobDef.MaxIterations() == 0 {
			continue
		}
		jobsWithLimit++

		job, err := e.storage.GetJob(ctx, jobCode)
		if err != nil {
			if errors.Is(err, jobmanager.ErrJobNotFound) {
				continue // job not yet created
			}
			if errors.Is(err, jobmanager.ErrConcurrentModification) {
				continue
			}
			return false, err
		}

		// job is considered completed if:
		// 1. It's in completed/failed status
		// 2. Its iterNum >= maxIterations
		if job.IsProcessed() && job.IterationNum() >= jobDef.MaxIterations() {
			jobsCompleted++
		}
	}

	// If no jobs with limit - consider everything ready
	if jobsWithLimit == 0 {
		return true, nil
	}

	return jobsCompleted == jobsWithLimit, nil
}

// Storage returns the storage instance
func (e *ManagerEnv) Storage() jobmanager.Storage {
	return e.storage
}
