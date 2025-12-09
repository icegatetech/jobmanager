package jobmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/metric"
)

type JobsManagerConfig struct {
	WorkerCount  int
	WorkerConfig WorkerConfig
}

type JobDefinitions map[JobCode]JobDefinition

func NewJobDefinitions(jobs ...JobDefinition) (JobDefinitions, error) {
	defs := make(map[JobCode]JobDefinition, len(jobs))
	for _, job := range jobs {
		if _, exists := defs[job.Code()]; exists {
			return nil, fmt.Errorf("job %s has duplicate", job.Code())
		}
		if job.Code() == "" {
			return nil, fmt.Errorf("job code cannot be empty")
		}
		if job.InitialTasks() == nil {
			return nil, fmt.Errorf("initial tasks function cannot be nil")
		}
		defs[job.Code()] = job
	}

	return defs, nil
}

func (d JobDefinitions) GetJob(code JobCode) (JobDefinition, error) {
	def, exists := d[code]
	if !exists {
		return JobDefinition{}, fmt.Errorf("job definition %s not found", code)
	}

	return def, nil
}

// Option defines a functional option for JobsManager
type Option func(*JobsManager)

// WithMeterProvider sets the MeterProvider for metrics
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(m *JobsManager) {
		m.meterProvider = mp
	}
}

// JobsManager manages workers and provides API for job operations
type JobsManager struct {
	registry jobRegistry
	storage  Storage
	cfg      JobsManagerConfig
	lgr      Logger

	meterProvider metric.MeterProvider
	metrics       Metrics

	wg sync.WaitGroup
}

func NewJobsManager(storage Storage, config JobsManagerConfig, logger Logger, jobs JobDefinitions, opts ...Option) (*JobsManager, error) {
	// TODO(low): consider reducing the number of parameters for manager startup

	if len(jobs) == 0 {
		return nil, fmt.Errorf("jobs cannot be empty")
	}
	if config.WorkerCount <= 0 {
		config.WorkerCount = 1
	}

	registry := newJobRegistry()
	for _, job := range jobs {
		err := registry.register(job)
		if err != nil {
			return nil, err
		}
	}

	mgr := &JobsManager{
		registry: registry,
		storage:  storage,
		cfg:      config,
		lgr:      logger,
	}

	for _, opt := range opts {
		opt(mgr)
	}

	metrics, err := NewMetrics(mgr.meterProvider)
	if err != nil {
		return nil, err
	}
	mgr.metrics = metrics

	return mgr, nil
}

func (m *JobsManager) Start(ctx context.Context) error {
	m.lgr.Info(fmt.Sprintf("Starting %d workers", m.cfg.WorkerCount))

	// TODO(med): dynamic worker count - reduce workers when there's little work to minimize storage requests
	for i := 0; i < m.cfg.WorkerCount; i++ {
		w := newWorker(m.registry, m.storage, m.cfg.WorkerConfig, m.lgr, m.metrics)

		// TODO(high): add panic recover
		m.wg.Add(1)
		go func(w *worker) {
			defer m.wg.Done()
			if err := w.start(ctx); err != nil && errors.Is(err, context.Canceled) {
				m.lgr.Info(fmt.Sprintf("Worker %s stopped with error: %v", w.id, err))
			}
		}(w)
	}

	return nil
}

func (m *JobsManager) Wait() {
	m.wg.Wait()
}
