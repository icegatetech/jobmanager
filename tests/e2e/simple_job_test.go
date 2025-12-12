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

// TestSimpleJobExecution verifies basic job execution with one task
func TestSimpleJobExecution(t *testing.T) {
	ctx := context.Background()

	var maxIterations uint64 = 1

	// 1. Start MinIO
	minioEnv, err := testenv.NewMinIOEnv(ctx, t)
	require.NoError(t, err, "failed to start MinIO")
	defer func() {
		if err := minioEnv.Close(); err != nil {
			t.Logf("failed to close MinIO: %v", err)
		}
	}()

	// 2. Define job with single task
	var executed atomic.Bool
	var taskInput []byte

	taskDef, err := jobmanager.NewTaskDefinition("simple_task", []byte("test-input"))
	require.NoError(t, err)

	executor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		taskInput = task.GetInput()
		executed.Store(true)
		return jm.CompleteTask(task.ID(), []byte("result"))
	}

	jobDef, err := jobmanager.NewJobDefinition(
		"test_simple_job",
		[]jobmanager.TaskDefinition{taskDef},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"simple_task": executor,
		},
		jobmanager.WithMaxIterations(maxIterations),
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
	require.NoError(t, err, "failed to create storage")

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

	// 5. Wait for all jobs with maxIterations to complete, then stop workers
	err = managerEnv.WaitForAllJobsCompletion(ctx, 10*time.Second)
	require.NoError(t, err)
	cancel()

	// 6. Verify
	assert.True(t, executed.Load(), "task should be executed")
	assert.Equal(t, []byte("test-input"), taskInput, "task should receive correct input")

	// Verify job state in storage
	job, err := storage.GetJob(context.Background(), "test_simple_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, job.Status())
	assert.Equal(t, maxIterations, job.IterationNum())
}

// TestMultiTaskSequence verifies a job with sequential tasks
func TestMultiTaskSequence(t *testing.T) {
	ctx := context.Background()

	var maxIterations uint64 = 1

	// 1. Start MinIO
	minioEnv, err := testenv.NewMinIOEnv(ctx, t)
	require.NoError(t, err)
	defer func() {
		if err := minioEnv.Close(); err != nil {
			t.Logf("failed to close MinIO: %v", err)
		}
	}()

	// 2. Define job with two sequential tasks
	var firstTaskExecuted, secondTaskExecuted atomic.Bool

	firstTaskDef, err := jobmanager.NewTaskDefinition("first_task", nil)
	require.NoError(t, err)

	firstTaskExecutor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		firstTaskExecuted.Store(true)

		// Create second task
		secondTaskDef, err := jobmanager.NewTaskDefinition("second_task", []byte("from-first"))
		if err != nil {
			return err
		}

		if err := jm.AddTask(secondTaskDef); err != nil {
			return err
		}

		return jm.CompleteTask(task.ID(), []byte("first-done"))
	}

	secondTaskExecutor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		secondTaskExecuted.Store(true)
		assert.Equal(t, []byte("from-first"), task.GetInput())
		return jm.CompleteTask(task.ID(), []byte("second-done"))
	}

	jobDef, err := jobmanager.NewJobDefinition(
		"test_sequence_job",
		[]jobmanager.TaskDefinition{firstTaskDef},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"first_task":  firstTaskExecutor,
			"second_task": secondTaskExecutor,
		},
		jobmanager.WithMaxIterations(maxIterations),
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

	err = managerEnv.WaitForAllJobsCompletion(ctx, 15*time.Second)
	require.NoError(t, err)
	cancel()

	assert.True(t, firstTaskExecuted.Load(), "first task should execute")
	assert.True(t, secondTaskExecuted.Load(), "second task should execute")

	job, err := storage.GetJob(context.Background(), "test_sequence_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, job.Status())
	assert.Equal(t, maxIterations, job.IterationNum())
}
