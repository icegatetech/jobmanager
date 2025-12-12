package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/icegatetech/jobmanager"
)

// This example demonstrates how to pass structured data (JSON) between sequential tasks.
// The 'json_first_step' task creates a 'TaskData' struct, marshals it to JSON, and passes it to 'json_second_step'.
// The 'json_second_step' task receives the JSON as input, unmarshals it, and processes the data.

// TaskData represents the data model passed between tasks
type TaskData struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Value     int       `json:"value"`
}

func main() {
	if err := runJsonModelJob(); err != nil {
		log.Fatal(err)
	}
}

func runJsonModelJob() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	lgr := jobmanager.NewSlogLogger(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)

	// 1. Define the tasks
	firstStepTask, err := jobmanager.NewTaskDefinition("json_first_step", nil)
	if err != nil {
		return fmt.Errorf("failed to create first step task: %w", err)
	}

	extr := jsonExecutor{lgr: lgr}
	jobDef, err := jobmanager.NewJobDefinition(
		"json_model_job",
		[]jobmanager.TaskDefinition{firstStepTask},
		map[jobmanager.TaskCode]jobmanager.TaskExecutor{
			"json_first_step":  extr.firstStepExecutor,
			"json_second_step": extr.secondStepExecutor,
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
			BucketPrefix:    "jobs",
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

	return nil
}

type jsonExecutor struct {
	lgr jobmanager.Logger
}

func (e jsonExecutor) firstStepExecutor(ctx context.Context, task jobmanager.ImmutableTask, manager jobmanager.JobManager) error {
	e.lgr.Info("[FirstStep] Started", slog.String("task_id", task.ID()))

	// Simulate work
	time.Sleep(200 * time.Millisecond)

	// Create data for the next step
	data := TaskData{
		ID:        task.ID(),
		Timestamp: time.Now(),
		Message:   "Hello from first step!",
		Value:     42,
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal struct: %w", err)
	}

	e.lgr.Info("[FirstStep] Work completed. Scheduling next step with data.", slog.String("data", string(jsonData)))

	// Create the second task with JSON data
	nextTask, err := jobmanager.NewTaskDefinition("json_second_step", jsonData)
	if err != nil {
		return fmt.Errorf("failed to create second step task: %w", err)
	}

	err = manager.AddTask(nextTask)
	if err != nil {
		return fmt.Errorf("failed to add second step task: %w", err)
	}

	return manager.CompleteTask(task.ID(), nil)
}

func (e jsonExecutor) secondStepExecutor(ctx context.Context, task jobmanager.ImmutableTask, manager jobmanager.JobManager) error {
	// Deserialize from JSON
	var data TaskData
	if err := json.Unmarshal(task.GetInput(), &data); err != nil {
		return fmt.Errorf("failed to unmarshal struct: %w", err)
	}

	e.lgr.Info("[SecondStep] Started",
		slog.String("received_id", data.ID),
		slog.String("received_msg", data.Message),
		slog.Int("received_value", data.Value),
		slog.Time("received_ts", data.Timestamp),
	)

	// Simulate work
	time.Sleep(200 * time.Millisecond)

	e.lgr.Info("[SecondStep] Finished.")

	return manager.CompleteTask(task.ID(), nil)
}
