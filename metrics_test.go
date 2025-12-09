package jobmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetrics_Recording(t *testing.T) {
	ctx := context.Background()
	jobCode := JobCode("test_job")
	taskCode := TaskCode("test_task")

	// Setup SDK
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	metrics, err := NewMetrics(provider)
	assert.NoError(t, err)

	metrics.RecordJobIterationComplete(ctx, jobCode, JobCompleted, 1*time.Second)
	metrics.RecordTaskProcessed(ctx, jobCode, taskCode, TaskCompleted, 500*time.Millisecond)

	// Collect metrics
	var resourceMetrics metricdata.ResourceMetrics
	err = reader.Collect(ctx, &resourceMetrics)
	assert.NoError(t, err)

	// Verify
	foundJobDuration := false
	foundTaskDuration := false

	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, m := range scopeMetrics.Metrics {
			switch m.Name {
			case "jobmanager_job_duration_seconds":
				foundJobDuration = true
				data := m.Data.(metricdata.Histogram[float64])
				assert.NotEmpty(t, data.DataPoints)
				assert.Equal(t, uint64(1), data.DataPoints[0].Count)
				assert.Equal(t, 1.0, data.DataPoints[0].Sum)
			case "jobmanager_task_duration_seconds":
				foundTaskDuration = true
				data := m.Data.(metricdata.Histogram[float64])
				assert.NotEmpty(t, data.DataPoints)
				assert.Equal(t, uint64(1), data.DataPoints[0].Count)
				assert.Equal(t, 0.5, data.DataPoints[0].Sum)
			}
		}
	}

	assert.True(t, foundJobDuration, "jobmanager_job_duration_seconds metric not found")
	assert.True(t, foundTaskDuration, "jobmanager_task_duration_seconds metric not found")
}
