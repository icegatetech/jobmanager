package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/icegatetech/jobmanager"
	"github.com/icegatetech/jobmanager/tests/e2e/testenv"
)

// TestCacheInvalidation verifies that CachedStorage correctly reflects updates.
func TestCacheInvalidation(t *testing.T) {
	ctx := context.Background()

	// 1. Start MinIO
	minioEnv, err := testenv.NewMinIOEnv(ctx, t)
	require.NoError(t, err)
	defer func() {
		if err := minioEnv.Close(); err != nil {
			t.Logf("failed to close MinIO: %v", err)
		}
	}()

	// 2. Define a simple job
	taskDef, err := jobmanager.NewTaskDefinition("cache_task", nil)
	require.NoError(t, err)

	executor := func(ctx context.Context, task jobmanager.ImmutableTask, jm jobmanager.JobManager) error {
		return jm.CompleteTask(task.ID(), []byte("done"))
	}

	jobDef, err := jobmanager.NewJobDefinition(
		"test_cache_job",
		[]jobmanager.TaskDefinition{taskDef},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"cache_task": executor,
		},
		jobmanager.WithMaxIterations(1),
	)
	require.NoError(t, err)

	jobDefs, err := jobmanager.NewJobDefinitions(jobDef)
	require.NoError(t, err)

	// 3. Create S3Storage wrapped with CachedStorage
	logger := testenv.NewTestLogger(t)
	s3Storage, err := jobmanager.NewS3Storage(
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

	cachedStorage := jobmanager.NewCachedStorage(s3Storage, logger, jobmanager.NewDisabledMetrics())

	// 4. Start manager with CachedStorage
	managerEnv, err := testenv.NewManagerEnv(
		t,
		cachedStorage,
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

	// 5. Wait for job completion
	err = managerEnv.WaitForAllJobsCompletion(ctx, 10*time.Second)
	require.NoError(t, err)
	cancel()
	managerEnv.Wait()

	// 6. Verify job completed and cache is consistent
	// Get from cached storage - should return completed job
	jobFromCache, err := cachedStorage.GetJob(context.Background(), "test_cache_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, jobFromCache.Status(), "cached job should be completed")

	// Get from underlying S3 storage directly - should match
	jobFromS3, err := s3Storage.GetJob(context.Background(), "test_cache_job")
	require.NoError(t, err)
	assert.Equal(t, jobmanager.JobCompleted, jobFromS3.Status(), "S3 job should be completed")

	// Verify both have the same version (cache is up-to-date)
	assert.Equal(t, jobFromS3.Version(), jobFromCache.Version(), "cache should have same version as S3")
}
