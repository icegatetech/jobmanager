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

// TestJobIterations verifies that a job can complete and restart for multiple iterations.
func TestJobIterations(t *testing.T) {
	ctx := context.Background()

	// 1. Start MinIO
	minioEnv, err := testenv.NewMinIOEnv(ctx, t)
	require.NoError(t, err)
	defer func() {
		if err := minioEnv.Close(); err != nil {
			t.Logf("failed to close MinIO: %v", err)
		}
	}()

	// 2. Track iterations
	expectedIterations := uint64(3)
	var iterationCount atomic.Uint64

	taskDef, err := jobmanager.NewTaskDefinition("iteration_task", nil)
	require.NoError(t, err)

	executor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		current := iterationCount.Add(1)
		t.Logf("Executing iteration %d", current)

		// Complete the task - job will automatically restart for next iteration
		return jm.CompleteTask(task.ID(), []byte("done"))
	}

	jobDef, err := jobmanager.NewJobDefinition(
		"test_iterations_job",
		[]jobmanager.TaskDefinition{taskDef},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"iteration_task": executor,
		},
		jobmanager.WithMaxIterations(expectedIterations),
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

	// 5. Wait for all iterations to complete
	err = managerEnv.WaitForAllJobsCompletion(ctx, 15*time.Second)
	require.NoError(t, err)
	cancel()
	managerEnv.Wait()

	// 6. Verify correct number of iterations
	assert.Equal(t, expectedIterations, iterationCount.Load(), "should have completed all iterations")

	// Verify final job state
	job, err := storage.GetJob(context.Background(), "test_iterations_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, job.Status())
	assert.Equal(t, expectedIterations, job.IterationNum(), "job should be at final iteration")
}
