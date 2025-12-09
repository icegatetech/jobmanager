package jobmanager

// TODO(med): add method to complete all job iterations
// TODO(med): add method to complete current job iteration

// JobManager client context for task execution
type JobManager interface {
	AddTask(task TaskDefinition) error
	CompleteTask(taskID string, output []byte) error
	FailTask(taskID string, errMsg string) error
	GetTask(id string) ImmutableTask
	GetTasksByCode(code TaskCode) []ImmutableTask
}

type jobManager struct {
	job      *Job
	storage  Storage
	workerID string
}

func newJobManager(job *Job, storage Storage, workerID string) JobManager {
	return &jobManager{
		job:      job,
		storage:  storage,
		workerID: workerID,
	}
}

func (m *jobManager) AddTask(task TaskDefinition) error {
	return m.job.AddTask(newTask(task.Code(), m.workerID, task.Input()))
}

func (m *jobManager) CompleteTask(taskID string, output []byte) error {
	return m.job.CompleteTask(taskID, output)
}

func (m *jobManager) FailTask(taskID string, errMsg string) error {
	return m.job.FailTask(taskID, errMsg)
}

func (m *jobManager) GetTask(id string) ImmutableTask {
	tsk := m.job.GetTask(id)
	if tsk == nil {
		return nil
	}
	return tsk
}

func (m *jobManager) GetTasksByCode(code TaskCode) []ImmutableTask {
	tasks := m.job.GetTasksByCode(code)
	readers := make([]ImmutableTask, len(tasks))
	for i, task := range tasks {
		readers[i] = task
	}
	return readers
}
