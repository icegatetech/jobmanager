package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/icegatetech/jobmanager"
)

// TODO(med): example with storing json models in tasks as import/export
// TODO(low): example with reading/writing to SQLite

// This example demonstrates a job with two sequential tasks.
// The first task 'first_step' simulates intermittent failures.
// Upon success, it creates the 'second_step' task.

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

	// 1. Define the 'Simple Job'
	firstStepTask, err := jobmanager.NewTaskDefinition("first_step", nil)
	if err != nil {
		return fmt.Errorf("failed to create first step task: %w", err)
	}
	extr := executor{lgr: lgr}
	jobDef, err := jobmanager.NewJobDefinition(
		"simple_sequence",
		[]jobmanager.TaskDefinition{firstStepTask},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"first_step":  extr.firstStepExecutor,
			"second_step": extr.secondStepExecutor,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	jobDefs, err := jobmanager.NewJobDefinitions(jobDef)
	if err != nil {
		return fmt.Errorf("failed to create jobs: %w", err)
	}

	// 2. Initialize S3 Storage (MinIO)
	storage, err := jobmanager.NewS3Storage(
		ctx,
		jobmanager.S3StorageConfig{
			Endpoint:        "localhost:9000",
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
			BucketName:      "jobs",
			UseSSL:          false,
			BucketPrefix:    "jobs/",
		},
		lgr,
		jobmanager.NewDisabledMetrics(),
		jobDefs,
		jobmanager.NewRetrier(
			jobmanager.RetrierConfig{
				Delays: []time.Duration{10 * time.Millisecond, 50 * time.Millisecond},
			},
			lgr,
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}
	storage = jobmanager.NewCachedStorage(storage, lgr, jobmanager.NewDisabledMetrics())

	// 3. Initialize JobsManager
	manager, err := jobmanager.NewJobsManager(
		storage,
		jobmanager.JobsManagerConfig{
			WorkerCount: 5,
			WorkerConfig: jobmanager.WorkerConfig{
				PollInterval:              500 * time.Millisecond,
				PollIntervalRandomization: 50 * time.Millisecond,
				TaskDeadline:              2 * time.Second,
				HeartbeatInterval:         500 * time.Millisecond,
				Retrier: jobmanager.NewRetrier(
					jobmanager.RetrierConfig{
						Delays: []time.Duration{50 * time.Millisecond, 100 * time.Millisecond},
					},
					lgr,
				),
			},
		},
		lgr,
		jobDefs,
	)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}

	// 4. Create the job (emulating an external trigger)
	lgr.Info("Starting manager...")
	if err := manager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start manager: %w", err)
	}

	lgr.Info("JobsManager started. Press Ctrl+C to exit.")
	manager.Wait()

	return nil
}

type executor struct {
	lgr jobmanager.Logger
}

func (e executor) firstStepExecutor(ctx context.Context, task jobmanager.ImmutableTask, manager jobmanager.JobManager) error {
	e.lgr.Info("[FirstStep] Started", slog.String("task_id", task.ID()))

	// Simulate work
	time.Sleep(500 * time.Millisecond)

	// Simulate flaky failure (30% chance)
	if rand.Float32() < 0.3 {
		return errors.New("random simulated failure")
	}

	e.lgr.Info("[FirstStep] Work completed successfully. Scheduling next step.")

	// Create the second task
	nextTask, err := jobmanager.NewTaskDefinition("second_step", []byte("data from step 1"))
	if err != nil {
		return fmt.Errorf("failed to create second step task: %w", err)
	}

	err = manager.AddTask(nextTask)
	if err != nil {
		return fmt.Errorf("failed to add second step task: %w", err)
	}

	return manager.CompleteTask(task.ID(), []byte("done"))
}

func (e executor) secondStepExecutor(ctx context.Context, task jobmanager.ImmutableTask, manager jobmanager.JobManager) error {
	input := string(task.GetInput())
	e.lgr.Info("[SecondStep] Started", slog.String("input", input))

	// Simulate work
	time.Sleep(200 * time.Millisecond)

	e.lgr.Info("[SecondStep] Finished.")

	return manager.CompleteTask(task.ID(), nil)
}
