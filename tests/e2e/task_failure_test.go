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

// TestTaskFailureAndRetry verifies that failed tasks are retried
func TestTaskFailureAndRetry(t *testing.T) {
	ctx := context.Background()

	// 1. Start MinIO
	minioEnv, err := testenv.NewMinIOEnv(ctx, t)
	require.NoError(t, err)
	defer func() {
		if err := minioEnv.Close(); err != nil {
			t.Logf("failed to close MinIO: %v", err)
		}
	}()

	// 2. Track attempts
	var attemptCount atomic.Int32

	taskDef, err := jobmanager.NewTaskDefinition("flaky_task", nil)
	require.NoError(t, err)

	executor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		attempt := attemptCount.Add(1)

		// Fail on first attempt, succeed on second
		if attempt == 1 {
			return assert.AnError // Simulate failure
		}

		return jm.CompleteTask(task.ID(), []byte("success"))
	}

	jobDef, err := jobmanager.NewJobDefinition(
		"test_retry_job",
		[]jobmanager.TaskDefinition{taskDef},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"flaky_task": executor,
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
			BucketPrefix:    "jobs/",
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

	err = managerEnv.Start(ctx)
	require.NoError(t, err)

	// 5. Wait for all jobs with maxIterations to complete, then stop workers
	err = managerEnv.WaitForAllJobsCompletion(ctx, 15*time.Second)
	require.NoError(t, err)
	cancel()
	managerEnv.Wait()

	// 6. Verify task was attempted multiple times
	assert.GreaterOrEqual(t, attemptCount.Load(), int32(2), "task should be retried after failure")

	// Verify final job state
	job, err := storage.GetJob(context.Background(), "test_retry_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, job.Status())
}
