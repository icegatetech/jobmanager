use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Job, JobCode, JobStatus, Task, TaskCode, TaskLimits, TaskStatus};

/// One task as every backend persists it.
///
/// This is the whole of what survives a save: a field the domain keeps but this structure does not
/// carry is gone the moment the state is stored, under every backend alike. `Task::created_by_task`
/// is such a field - it names an execution that is still running, and no execution outlives the save
/// of its own result.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StoredTask {
    id: Uuid,
    code: String,
    status: TaskStatus,
    created_by_worker: Uuid,
    timeout_ms: i64,
    max_lifetime_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    processing_by: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deadline_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    lifetime_deadline_at: Option<DateTime<Utc>>,
    attempt: u32,
    max_attempts: u32,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    input: Vec<u8>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    output: Vec<u8>,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    error: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    depends_on: Vec<Uuid>,
}

impl StoredTask {
    fn from_task(task: &Task) -> Self {
        Self {
            id: *task.id(),
            code: task.code().to_string(),
            status: task.status().clone(),
            timeout_ms: task.timeout().num_milliseconds(),
            max_lifetime_ms: task.max_lifetime().num_milliseconds(),
            created_by_worker: task.created_by_worker(),
            processing_by: task.processing_by_worker(),
            started_at: task.started_at(),
            completed_at: task.completed_at(),
            deadline_at: task.deadline_at(),
            lifetime_deadline_at: task.lifetime_deadline_at(),
            attempt: task.attempt(),
            max_attempts: task.max_attempts(),
            input: task.input().to_vec(),
            output: task.output().to_vec(),
            error: task.error_msg().to_string(),
            depends_on: task.depends_on().to_vec(),
        }
    }

    fn into_task(self) -> Task {
        Task::restore(
            self.id,
            TaskCode::new(self.code),
            self.status,
            self.processing_by,
            self.created_by_worker,
            Duration::milliseconds(self.timeout_ms),
            Duration::milliseconds(self.max_lifetime_ms),
            self.started_at,
            self.completed_at,
            self.deadline_at,
            self.lifetime_deadline_at,
            self.attempt,
            self.max_attempts,
            self.input,
            self.output,
            self.error,
            self.depends_on,
        )
    }
}

/// One job iteration as every backend persists it.
///
/// The settings a job runs by are deliberately absent: they are re-read from the job's description
/// on every load, so changing one in code also applies to iterations already in storage.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StoredJob {
    id: Uuid,
    code: String,
    iter_num: u64,
    status: JobStatus,
    tasks: Vec<StoredTask>,
    updated_by: Uuid,
    started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    running_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_start_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<std::collections::HashMap<String, serde_json::Value>>,
}

impl StoredJob {
    /// Code of the job the state belongs to, which a backend needs before the job itself exists -
    /// the settings it is rebuilt with come from that job's description.
    pub(crate) fn code(&self) -> &str {
        &self.code
    }

    pub(crate) fn from_job(job: &Job) -> Self {
        Self {
            id: *job.id(),
            code: job.code().to_string(),
            iter_num: job.iter_num(),
            status: job.status().clone(),
            tasks: job.tasks_as_iter().map(StoredTask::from_task).collect(),
            updated_by: job.updated_by_worker_id(),
            started_at: job.started_at(),
            running_at: job.running_at(),
            completed_at: job.completed_at(),
            next_start_at: job.next_start_at(),
            metadata: if job.metadata().is_empty() {
                None
            } else {
                Some(job.metadata().clone())
            },
        }
    }

    /// Rebuilds the job, taking the settings from its current description and `version` from the
    /// backend that read it.
    pub(crate) fn into_job(
        self,
        max_iterations: Option<u64>,
        iteration_interval: Option<std::time::Duration>,
        task_limits: TaskLimits,
        version: &str,
    ) -> Job {
        Job::restore(
            self.id,
            JobCode::new(self.code),
            version.to_string(),
            self.iter_num,
            self.status,
            self.tasks.into_iter().map(StoredTask::into_task).collect(),
            self.updated_by,
            self.started_at,
            self.running_at,
            self.completed_at,
            self.next_start_at,
            self.metadata.unwrap_or_default(),
            max_iterations,
            iteration_interval,
            task_limits,
        )
    }
}

/// Copy of `job` holding only what this backend keeps, for one that stores the domain state itself
/// instead of a serialized form.
///
/// Dropping a field is the backend's own business - the domain state is written as if all of it
/// survives - so a backend that skips the serialization still has to hand back what a reader of a
/// serialized one would get, and the mapping above is the single description of that.
pub(crate) fn copy_persisted_state(job: &Job) -> Job {
    StoredJob::from_job(job).into_job(
        job.max_iterations(),
        job.iteration_interval(),
        job.task_limits(),
        job.version(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{DEFAULT_MAX_ATTEMPTS, TaskDefinition};

    const TIMEOUT: Duration = Duration::seconds(5);

    /// Version every fixture below is restored with, which a copy of the state has to carry over -
    /// a backend that lost it would save the job as a new object instead of a conditional update.
    const FIXTURE_VERSION: &str = "version";

    /// State whose only task carries `task_bounds` - the timeout and the maximum lifetime - as
    /// written, so that a state missing either can be handed to the parser exactly as the tests
    /// below need it.
    fn stored_job_json(task_bounds: &str) -> String {
        format!(
            r#"{{
                "id": "00000000-0000-0000-0000-000000000001",
                "code": "job",
                "iter_num": 1,
                "status": "started",
                "tasks": [{{
                    "id": "00000000-0000-0000-0000-000000000002",
                    "code": "task",
                    "status": "todo",
                    "created_by_worker": "00000000-0000-0000-0000-000000000003",
                    "attempt": 0,
                    "max_attempts": 5{task_bounds}
                }}],
                "updated_by": "00000000-0000-0000-0000-000000000003",
                "started_at": "2026-01-01T00:00:00Z"
            }}"#
        )
    }

    /// The state a save writes, whose lifetime is deliberately not a multiple of the timeout: a
    /// value recomputed from anything rather than read out of the object would not match it.
    fn stored_job_json_with_bounds() -> String {
        stored_job_json(r#", "timeout_ms": 5000, "max_lifetime_ms": 7000"#)
    }

    fn restore_job_from(json: &str) -> Job {
        serde_json::from_str::<StoredJob>(json)
            .expect("the stored state must parse")
            .into_job(None, None, TaskLimits::default(), FIXTURE_VERSION)
    }

    #[test]
    fn stored_task_bounds_come_back_as_written() {
        let job = restore_job_from(&stored_job_json_with_bounds());

        let task = job.tasks_as_iter().next().expect("the state declares one task");
        assert_eq!(task.timeout(), TIMEOUT);
        assert_eq!(task.max_lifetime(), Duration::seconds(7));
    }

    /// The lifetime is part of what a task is, not an optional extra: state without it is refused
    /// rather than read as a zero, which would fail the task at its first start, or as a value
    /// invented here, which would be a limit nobody declared.
    #[test]
    fn state_without_a_maximum_lifetime_is_refused() {
        let error = serde_json::from_str::<StoredJob>(&stored_job_json(r#", "timeout_ms": 5000"#))
            .expect_err("state without the lifetime must not parse");

        assert!(error.to_string().contains("max_lifetime_ms"), "got: {error}");
    }

    /// The timeout is refused on the same grounds as the lifetime, and a zero would cost as much:
    /// the deadline a start hands out is already in the past, so the task is taken over on every
    /// pass it is picked on until its lifetime runs out.
    #[test]
    fn state_without_a_timeout_is_refused() {
        let error = serde_json::from_str::<StoredJob>(&stored_job_json(r#", "max_lifetime_ms": 7000"#))
            .expect_err("state without the timeout must not parse");

        assert!(error.to_string().contains("timeout_ms"), "got: {error}");
    }

    /// The parentage of a running execution is what a save is not allowed to keep: kept, a later
    /// failure of that task would drop work the iteration already owns.
    #[test]
    fn a_persisted_copy_drops_the_execution_that_created_a_task() {
        let (job, child_id) = job_with_a_task_created_by_its_execution();

        let stored = copy_persisted_state(&job);

        assert_eq!(
            stored
                .find_task(&child_id)
                .expect("the created task must survive the save")
                .created_by_task(),
            None
        );
    }

    /// A job holding the one thing a save drops: a task an execution created, alongside the started
    /// task it belongs to.
    fn job_with_a_task_created_by_its_execution() -> (Job, Uuid) {
        let mut job = job_with_started_task();
        let parent_id = *job.tasks_as_iter().next().expect("the description declares one task").id();
        let child_id = job
            .add_task(
                &TaskDefinition::new(TaskCode::new("child"), TIMEOUT.to_std().expect("a positive timeout")),
                Uuid::from_u128(3),
                Some(parent_id),
            )
            .expect("a running execution may create a task");
        assert_eq!(
            job.find_task(&child_id)
                .expect("the created task must be in the job")
                .created_by_task(),
            Some(parent_id),
            "the fixture must have attributed the task to its execution"
        );

        (job, child_id)
    }

    /// A save keeps the result of the work, so the round trip has to carry the fields a worker
    /// decides by - not only drop the ones it must.
    #[test]
    fn a_persisted_copy_keeps_the_state_a_worker_decides_by() {
        let job = job_with_started_task();

        let stored = copy_persisted_state(&job);

        let task = job.tasks_as_iter().next().expect("the description declares one task");
        let stored_task = stored.find_task(task.id()).expect("the task must survive the save");
        assert_eq!(stored_task.status(), task.status());
        assert_eq!(stored_task.attempt(), 1);
        assert_eq!(stored_task.max_attempts(), DEFAULT_MAX_ATTEMPTS);
        assert_eq!(stored_task.deadline_at(), task.deadline_at());
        assert_eq!(stored_task.lifetime_deadline_at(), task.lifetime_deadline_at());
        assert_eq!(stored_task.processing_by_worker(), Some(Uuid::from_u128(3)));
        assert_eq!(stored.iter_num(), job.iter_num());
        assert_eq!(stored.version(), job.version());
    }

    /// A job holding one task a worker has already started, which is the state every field above is
    /// observable in.
    fn job_with_started_task() -> Job {
        let mut job = restore_job_from(&stored_job_json_with_bounds());
        let task_id = *job.tasks_as_iter().next().expect("the state declares one task").id();
        job.start_task(&task_id, Uuid::from_u128(3)).expect("a todo task must start");
        job
    }

    /// Metadata is stored only when there is any, so the empty map has to survive as an empty map
    /// rather than come back as something a worker sees differently.
    #[test]
    fn a_persisted_copy_keeps_an_empty_metadata_map() {
        let job = restore_job_from(&stored_job_json_with_bounds());

        assert_eq!(*copy_persisted_state(&job).metadata(), HashMap::new());
    }
}
