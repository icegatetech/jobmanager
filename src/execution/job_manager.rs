use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::{Error, ImmutableTask, Job, TaskCode, TaskDefinition};

// TODO(med): add method to complete all job iterations
// TODO(med): add method to complete current job iteration

/// Client interface for task executors: the only way an executor observes or mutates the job
/// it is running in.
///
/// An instance is scoped to a single worker's in-memory copy of one job and is only valid for
/// the duration of the executor call it was passed to. Mutations are applied to that in-memory
/// copy immediately but are only persisted to storage once the executor returns; a task
/// executor must not assume a call here is durable until then.
pub trait JobManager: Send + Sync {
    /// Registers a new task in the current job iteration and returns its generated id.
    ///
    /// Use this to fan out follow-up work from within an executor (e.g. chaining a task onto
    /// the completion of another). The task is only picked up by a worker once the job holding
    /// it is persisted.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_def`'s input exceeds the configured size limit, if one of its
    /// declared dependencies does not exist in the job, or if a task with the same id is
    /// already registered.
    fn add_task(&self, task_def: TaskDefinition) -> Result<Uuid, Error>;

    /// Marks the task as completed and stores `output` as its result.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_id` is not part of the job, if `output` exceeds the
    /// configured size limit, or if the task is not in a state that can transition to
    /// completed.
    fn complete_task(&self, task_id: &Uuid, output: Vec<u8>) -> Result<(), Error>;

    /// Marks the task as failed, recording `error_msg` as the failure reason.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_id` is not part of the job or if the task is not in a state
    /// that can transition to failed.
    fn fail_task(&self, task_id: &Uuid, error_msg: &str) -> Result<(), Error>;

    /// Sets the earliest time at which the job's next iteration is allowed to start.
    ///
    /// Takes precedence over the job's iteration interval in both directions: it can hold the
    /// next iteration past the interval or release it earlier. Consulted once the current
    /// iteration finishes and only while the iteration limit is not reached. If never called,
    /// eligibility falls back to the configured iteration interval.
    fn set_next_start_at(&self, next_start_at: DateTime<Utc>) -> Result<(), Error>;

    /// Returns the current state of the task with the given id.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_id` is not part of the job.
    fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, Error>;

    /// Returns every task in the job whose code matches `code`, in unspecified order.
    ///
    /// Returns an empty vector if no task matches.
    fn get_tasks_by_code(&self, code: &TaskCode) -> Result<Vec<Arc<dyn ImmutableTask>>, Error>;
}

// JobManagerImpl - internal implementation
pub(crate) struct JobManagerImpl<'a> {
    job: &'a RwLock<Job>,
    worker_id: Uuid,
}

impl<'a> JobManagerImpl<'a> {
    pub(crate) const fn new(job: &'a RwLock<Job>, worker_id: Uuid) -> Self {
        Self { job, worker_id }
    }
}

impl JobManager for JobManagerImpl<'_> {
    fn add_task(&self, task_def: TaskDefinition) -> Result<Uuid, Error> {
        self.job
            .write()
            .add_task(&task_def, self.worker_id)
            .map_err(|e| Error::Other(e.to_string()))
    }

    fn complete_task(&self, task_id: &Uuid, output: Vec<u8>) -> Result<(), Error> {
        self.job
            .write()
            .complete_task(task_id, output)
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(())
    }

    fn fail_task(&self, task_id: &Uuid, error_msg: &str) -> Result<(), Error> {
        self.job
            .write()
            .fail_task(task_id, error_msg)
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(())
    }

    fn set_next_start_at(&self, next_start_at: DateTime<Utc>) -> Result<(), Error> {
        self.job.write().set_next_start_at(next_start_at);
        Ok(())
    }

    fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, Error> {
        self.job.read().get_task(task_id).map_err(|e| Error::Other(e.to_string()))
    }

    fn get_tasks_by_code(&self, code: &TaskCode) -> Result<Vec<Arc<dyn ImmutableTask>>, Error> {
        Ok(self.job.read().get_tasks_by_code(code))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Duration;

    use super::*;
    use crate::{JobCode, TaskLimits};

    #[test]
    fn set_next_start_at_updates_job_under_lock() {
        let task_def = TaskDefinition::new(TaskCode::new("task"), Vec::new(), Duration::seconds(5)).unwrap();
        let job = Job::new(
            JobCode::new("job"),
            vec![task_def],
            HashMap::new(),
            Uuid::from_u128(1),
            None,
            None,
            TaskLimits::default(),
        )
        .unwrap();
        let job = RwLock::new(job);
        let manager = JobManagerImpl::new(&job, Uuid::from_u128(2));

        let next_start_at = Utc::now() + Duration::seconds(30);
        manager.set_next_start_at(next_start_at).unwrap();

        assert_eq!(job.read().next_start_at(), Some(next_start_at));
    }
}
