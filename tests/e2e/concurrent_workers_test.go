package e2e

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/icegatetech/jobmanager"
	"github.com/icegatetech/jobmanager/tests/e2e/testenv"
)

// TestConcurrentWorkers verifies that multiple workers can process tasks from the same job concurrently
func TestConcurrentWorkers(t *testing.T) {
	ctx := context.Background()

	taskCount := 10
	var maxIterations uint64 = 1
	workersCnt := 10

	minioEnv, err := testenv.NewMinIOEnv(ctx, t)
	require.NoError(t, err)
	defer func() {
		if err := minioEnv.Close(); err != nil {
			t.Logf("failed to close MinIO: %v", err)
		}
	}()

	var executedTasks sync.Map // map[taskID]bool
	var executionCount atomic.Int32

	workExecutor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		// Simulate some work
		time.Sleep(20 * time.Millisecond)

		// Track execution
		executionCount.Add(1)
		executedTasks.Store(task.ID(), true)

		return jm.CompleteTask(task.ID(), []byte("done"))
	}

	initTaskDef, err := jobmanager.NewTaskDefinition("init_task", nil)
	require.NoError(t, err)

	initExecutor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		// Create multiple work tasks
		for i := 0; i < taskCount; i++ {
			workTaskDef, err := jobmanager.NewTaskDefinition("work_task", []byte{byte(i)})
			if err != nil {
				return err
			}
			if err := jm.AddTask(workTaskDef); err != nil {
				return err
			}
		}
		return jm.CompleteTask(task.ID(), nil)
	}

	jobDef, err := jobmanager.NewJobDefinition(
		"test_concurrent_job",
		[]jobmanager.TaskDefinition{initTaskDef},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"init_task": initExecutor,
			"work_task": workExecutor,
		},
		jobmanager.WithMaxIterations(maxIterations),
	)
	require.NoError(t, err)

	jobDefs, err := jobmanager.NewJobDefinitions(jobDef)
	require.NoError(t, err)

	logger := testenv.NewTestLogger(t)
	storage, err := jobmanager.NewS3Storage(
		ctx,
		jobmanager.S3StorageConfig{
			Endpoint:        minioEnv.Endpoint(),
			AccessKeyID:     minioEnv.Username(),
			SecretAccessKey: minioEnv.Password(),
			BucketName:      "test-jobs",
			UseSSL:          false,
			BucketPrefix:    "jobs/",
		},
		logger,
		jobmanager.NewDisabledMetrics(),
		jobDefs,
		jobmanager.NewRetrier(jobmanager.RetrierConfig{}, logger),
	)
	require.NoError(t, err)

	managerEnv, err := testenv.NewManagerEnv(
		t,
		storage,
		logger,
		jobDefs,
		jobmanager.JobsManagerConfig{
			WorkerCount: workersCnt,
		},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = managerEnv.Start(ctx)
	require.NoError(t, err)

	err = managerEnv.WaitForAllJobsCompletion(ctx, 30*time.Second)
	require.NoError(t, err)
	cancel()
	managerEnv.Wait()

	// Verify all tasks were executed exactly once
	assert.Equal(t, int32(taskCount), executionCount.Load(), "all tasks should be executed")

	// Verify each task was executed exactly once
	executedCount := 0
	executedTasks.Range(func(key, value interface{}) bool {
		executedCount++
		return true
	})
	assert.Equal(t, taskCount, executedCount, "no task should be executed twice")

	// Verify final job state
	job, err := storage.GetJob(context.Background(), "test_concurrent_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, job.Status())
	assert.Equal(t, maxIterations, job.IterationNum())
}
