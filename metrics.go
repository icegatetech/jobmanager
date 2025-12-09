package jobmanager

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	instrumentationName = "github.com/icegatetech/jobmanager"
)

type Metrics struct {
	enabled      bool
	jobDuration  metric.Float64Histogram
	taskDuration metric.Float64Histogram
	s3Latency    metric.Float64Histogram
	cacheHits    metric.Int64Counter
	cacheMisses  metric.Int64Counter
}

func NewDisabledMetrics() Metrics {
	return Metrics{enabled: false}
}

func NewMetrics(mp metric.MeterProvider) (Metrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	meter := mp.Meter(instrumentationName)

	jobDuration, err := meter.Float64Histogram(
		"jobmanager_job_duration_seconds",
		metric.WithDescription("Duration of job execution from start to finish"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return Metrics{}, fmt.Errorf("failed to create job duration histogram: %w", err)
	}

	taskDuration, err := meter.Float64Histogram(
		"jobmanager_task_duration_seconds",
		metric.WithDescription("Duration of individual task execution"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return Metrics{}, fmt.Errorf("failed to create task duration histogram: %w", err)
	}

	s3Latency, err := meter.Float64Histogram(
		"jobmanager_storage_s3_latency_seconds",
		metric.WithDescription("Latency of S3 GET and PUT operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return Metrics{}, fmt.Errorf("failed to create s3 latency histogram: %w", err)
	}

	cacheHits, err := meter.Int64Counter(
		"jobmanager_storage_cache_hits_total",
		metric.WithDescription("Total number of cache hits"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return Metrics{}, fmt.Errorf("failed to create cache hits counter: %w", err)
	}

	cacheMisses, err := meter.Int64Counter(
		"jobmanager_storage_cache_misses_total",
		metric.WithDescription("Total number of cache misses"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return Metrics{}, fmt.Errorf("failed to create cache misses counter: %w", err)
	}

	return Metrics{
		enabled:      true,
		jobDuration:  jobDuration,
		taskDuration: taskDuration,
		s3Latency:    s3Latency,
		cacheHits:    cacheHits,
		cacheMisses:  cacheMisses,
	}, nil
}

func (m Metrics) RecordJobIterationComplete(ctx context.Context, code JobCode, status JobStatus, duration time.Duration) {
	if !m.enabled {
		return
	}
	m.jobDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("code", code.String()),
		attribute.String("status", status.String()),
	))
}

func (m Metrics) RecordTaskProcessed(ctx context.Context, jobCode JobCode, taskCode TaskCode, status TaskStatus, duration time.Duration) {
	if !m.enabled {
		return
	}
	m.taskDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("job_code", jobCode.String()),
		attribute.String("task_code", taskCode.String()),
		attribute.String("status", status.String()),
	))
}

func (m Metrics) RecordS3Operation(ctx context.Context, operation string, status string, duration time.Duration) {
	if !m.enabled {
		return
	}
	m.s3Latency.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("operation", operation),
		attribute.String("http_status", status),
	))
}

func (m Metrics) RecordCacheHit(ctx context.Context, method string) {
	if !m.enabled {
		return
	}
	m.cacheHits.Add(ctx, 1, metric.WithAttributes(
		attribute.String("method", method),
	))
}

func (m Metrics) RecordCacheMiss(ctx context.Context, method string) {
	if !m.enabled {
		return
	}
	m.cacheMisses.Add(ctx, 1, metric.WithAttributes(
		attribute.String("method", method),
	))
}
