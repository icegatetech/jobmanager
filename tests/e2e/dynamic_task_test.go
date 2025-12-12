package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/icegatetech/jobmanager"
	"github.com/icegatetech/jobmanager/tests/e2e/testenv"
)

// TestDynamicTaskCreation verifies that an executor can create new tasks dynamically.
func TestDynamicTaskCreation(t *testing.T) {
	ctx := context.Background()

	// 1. Start MinIO
	minioEnv, err := testenv.NewMinIOEnv(ctx, t)
	require.NoError(t, err)
	defer func() {
		if err := minioEnv.Close(); err != nil {
			t.Logf("failed to close MinIO: %v", err)
		}
	}()

	// 2. Track task execution
	dynamicTaskCount := 5
	var initTaskExecuted atomic.Bool
	var dynamicTasksExecuted atomic.Int32

	initTaskDef, err := jobmanager.NewTaskDefinition("init_task", nil)
	require.NoError(t, err)

	// Initial task creates multiple dynamic tasks
	initExecutor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		initTaskExecuted.Store(true)

		// Create multiple dynamic tasks
		for i := 0; i < dynamicTaskCount; i++ {
			dynamicTaskDef, err := jobmanager.NewTaskDefinition("dynamic_task", []byte{byte(i)})
			if err != nil {
				return err
			}
			if err := jm.AddTask(dynamicTaskDef); err != nil {
				return err
			}
		}

		return jm.CompleteTask(task.ID(), nil)
	}

	// Dynamic task executor
	dynamicExecutor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		dynamicTasksExecuted.Add(1)
		return jm.CompleteTask(task.ID(), []byte("done"))
	}

	jobDef, err := jobmanager.NewJobDefinition(
		"test_dynamic_job",
		[]jobmanager.TaskDefinition{initTaskDef},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"init_task":    initExecutor,
			"dynamic_task": dynamicExecutor,
		},
		jobmanager.WithMaxIterations(1),
	)
	require.NoError(t, err)

	jobDefs, err := jobmanager.NewJobDefinitions(jobDef)
	require.NoError(t, err)

	// 3. Create storage
	logger := testenv.NewTestLogger(t)
	storage, err := jobmanager.NewS3Storage(
		ctx,
		jobmanager.S3StorageConfig{
			Endpoint:        minioEnv.Endpoint(),
			AccessKeyID:     minioEnv.Username(),
			SecretAccessKey: minioEnv.Password(),
			BucketName:      "test-jobs",
			UseSSL:          false,
			BucketPrefix:    "jobs",
		},
		logger,
		jobmanager.NewDisabledMetrics(),
		jobDefs,
		jobmanager.NewRetrier(jobmanager.RetrierConfig{}, logger),
	)
	require.NoError(t, err)

	// 4. Start manager
	managerEnv, err := testenv.NewManagerEnv(
		t,
		storage,
		logger,
		jobDefs,
		jobmanager.JobsManagerConfig{
			WorkerCount: 1,
		},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	managerEnv.GoStart(ctx)

	// 5. Wait for job completion
	err = managerEnv.WaitForAllJobsCompletion(ctx, 15*time.Second)
	require.NoError(t, err)
	cancel()

	// 6. Verify all tasks executed
	assert.True(t, initTaskExecuted.Load(), "init task should be executed")
	assert.Equal(t, int32(dynamicTaskCount), dynamicTasksExecuted.Load(), "all dynamic tasks should be executed")

	// Verify final job state
	job, err := storage.GetJob(context.Background(), "test_dynamic_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, job.Status())

	// Verify task count: 1 init + N dynamic
	tasks := job.GetTasksByCode("dynamic_task")
	assert.Len(t, tasks, dynamicTaskCount, "should have correct number of dynamic tasks")
}
