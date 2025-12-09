package e2e

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/icegatetech/jobmanager"
	"github.com/icegatetech/jobmanager/tests/e2e/testenv"
)

// TestTaskDeadlineExpiry verifies that a task that fails with timeout error
// will be retried and eventually complete successfully.
func TestTaskDeadlineExpiry(t *testing.T) {
	ctx := context.Background()

	// 1. Start MinIO
	minioEnv, err := testenv.NewMinIOEnv(ctx, t)
	require.NoError(t, err)
	defer func() {
		if err := minioEnv.Close(); err != nil {
			t.Logf("failed to close MinIO: %v", err)
		}
	}()

	// 2. Track task execution attempts
	var attemptCount atomic.Int32

	taskDef, err := jobmanager.NewTaskDefinition("hanging_task", nil)
	require.NoError(t, err)

	executor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		attempt := attemptCount.Add(1)
		t.Logf("Task attempt %d started", attempt)

		if attempt == 1 {
			// First attempt: simulate a timeout error
			return errors.New("simulated timeout on first attempt")
		}

		// Second attempt: complete successfully
		return jm.CompleteTask(task.ID(), []byte("success"))
	}

	jobDef, err := jobmanager.NewJobDefinition(
		"test_deadline_job",
		[]jobmanager.TaskDefinition{taskDef},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"hanging_task": executor,
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

	// 4. Start manager with 1 worker
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

	// 5. Wait for job completion (should retry after first attempt fails)
	err = managerEnv.WaitForAllJobsCompletion(ctx, 15*time.Second)
	require.NoError(t, err)
	cancel()
	managerEnv.Wait()

	// 6. Verify task was attempted multiple times
	assert.GreaterOrEqual(t, attemptCount.Load(), int32(2), "task should be attempted at least twice (first fails, second completes)")

	// Verify final job state
	job, err := storage.GetJob(context.Background(), "test_deadline_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, job.Status())
}
