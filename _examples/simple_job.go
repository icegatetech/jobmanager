package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/icegatetech/jobmanager"
)

// This minimal example demonstrates how to configure and run a simple job with a single task.
// It sets up the necessary components: logger, storage (S3), job definition, and the job manager itself.

func main() {
	if err := runSimpleJob(); err != nil {
		log.Fatal(err)
	}
}

func runSimpleJob() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	lgr := jobmanager.NewSlogLogger(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)

	// 1. Define the initial task
	taskDef, err := jobmanager.NewTaskDefinition("my task code", nil)
	if err != nil {
		return fmt.Errorf("failed to create task executor: %w", err)
	}
	// 2. Map executor for task
	taskExecutors := map[jobmanager.TaskCode]jobmanager.TaskExecutor{"my task code": taskExecutor}
	// 3. Define the job
	jobDef, err := jobmanager.NewJobDefinition(
		"my job code",
		[]jobmanager.TaskDefinition{taskDef},
		taskExecutors,
	)
	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	// 4. Get the jobs together
	jobDefs, err := jobmanager.NewJobDefinitions(jobDef)
	if err != nil {
		return fmt.Errorf("failed to create jobs: %w", err)
	}

	// 5. Initialize S3 Storage (MinIO)
	storage, err := jobmanager.NewS3Storage(
		ctx,
		jobmanager.S3StorageConfig{
			Endpoint:        "localhost:9000",
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
			BucketName:      "jobs",
			UseSSL:          false,
			BucketPrefix:    "jobs",
		},
		lgr,
		jobmanager.NewDisabledMetrics(),
		jobDefs,
		jobmanager.NewRetrier(jobmanager.RetrierConfig{}, lgr),
	)
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	// 6. Initialize JobsManager to manage all of them
	manager, err := jobmanager.NewJobsManager(storage, jobmanager.JobsManagerConfig{}, lgr, jobDefs)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}

	// 7. Start (blocking)
	if err := manager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start manager: %w", err)
	}

	return nil
}

func taskExecutor(ctx context.Context, task jobmanager.ImmutableTask, manager jobmanager.JobManager) error {
	// Simulate work
	time.Sleep(100 * time.Millisecond)

	return manager.CompleteTask(task.ID(), []byte("done"))
}
