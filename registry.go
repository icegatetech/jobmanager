package jobmanager

import (
	"context"
	"fmt"
)

// TaskExecutor executes a specific task
type TaskExecutor func(ctx context.Context, task ImmutableTask, mgr JobManager) error

type jobRegistry struct {
	jobsByCode                 map[JobCode]JobDefinition
	taskExecutorsByJobTaskCode map[string]TaskExecutor
}

func newJobRegistry() jobRegistry {
	return jobRegistry{
		jobsByCode:                 make(map[JobCode]JobDefinition),
		taskExecutorsByJobTaskCode: make(map[string]TaskExecutor),
	}
}

func (r *jobRegistry) register(job JobDefinition) error {
	r.jobsByCode[job.Code()] = job

	for taskCode, executor := range job.taskExecutors {
		r.taskExecutorsByJobTaskCode[r.taskExecutorKey(job.Code(), taskCode)] = executor
	}

	return nil
}

func (r *jobRegistry) getJob(code JobCode) (JobDefinition, error) {
	def, exists := r.jobsByCode[code]
	if !exists {
		return JobDefinition{}, fmt.Errorf("job %s not registered", code)
	}

	return def, nil
}

func (r *jobRegistry) getTaskExecutor(jCode JobCode, tCode TaskCode) (TaskExecutor, error) {
	e, ok := r.taskExecutorsByJobTaskCode[r.taskExecutorKey(jCode, tCode)]
	if !ok {
		return nil, fmt.Errorf("executor for task %s and job %s not exist", tCode, jCode)
	}

	return e, nil
}

// TODO(low): add iterator
// listJobs - return job codes in random order
func (r *jobRegistry) listJobs() []JobCode {
	codes := make([]JobCode, 0, len(r.jobsByCode))
	for c := range r.jobsByCode {
		codes = append(codes, c)
	}

	return codes
}

func (r *jobRegistry) taskExecutorKey(jCode JobCode, tCode TaskCode) string {
	return fmt.Sprintf("%s:%s", jCode, tCode)
}
