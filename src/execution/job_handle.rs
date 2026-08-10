use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::{Error, ImmutableTask, Job, JobError, TaskCode, TaskDefinition, TaskRef};

// TODO(med): add method to complete all job iterations
// TODO(med): add method to complete current job iteration

/// Access an executor has to the job its task belongs to: the only way it observes or mutates that
/// job.
///
/// An instance is scoped to a single worker's in-memory copy of one job. Mutations are applied to
/// that copy immediately but are only persisted once the executor returns; a task executor must
/// not assume a call here is durable until then.
///
/// The handle stops working the moment the executor returns. A handle that escaped into a
/// background task keeps compiling but every method on it fails with [`Error::Other`], because the
/// worker is by then persisting the job and a late mutation would be silently lost.
pub trait JobHandle: Send + Sync {
    /// Registers a new task in the current job iteration and returns a reference to it.
    ///
    /// Use this to fan out follow-up work from within an executor (e.g. chaining a task onto
    /// the completion of another). The returned reference is what
    /// [`TaskDefinition::with_dependencies`] takes, so a further task can be made to wait for this
    /// one. The task is only picked up by a worker once the job holding it is persisted.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_def` does not pass the job's limits, if one of its declared
    /// dependencies does not exist in the job, or if the handle is no longer valid.
    fn add_task(&self, task_def: TaskDefinition) -> Result<TaskRef, Error>;

    /// Marks the task as completed and stores `output` as its result.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_id` is not part of the job, if `output` exceeds the
    /// configured size limit, if the task is not in a state that can transition to completed, or
    /// if the handle is no longer valid.
    fn complete_task(&self, task_id: &Uuid, output: Vec<u8>) -> Result<(), Error>;

    /// Marks the task as failed, recording `error_msg` as the failure reason.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_id` is not part of the job, if the task is not in a state
    /// that can transition to failed, or if the handle is no longer valid.
    fn fail_task(&self, task_id: &Uuid, error_msg: &str) -> Result<(), Error>;

    /// Sets the earliest time at which the job's next iteration is allowed to start.
    ///
    /// Takes precedence over the job's iteration interval in both directions: it can hold the
    /// next iteration past the interval or release it earlier. Consulted once the current
    /// iteration finishes and only while the iteration limit is not reached. If never called,
    /// eligibility falls back to the configured iteration interval.
    ///
    /// # Errors
    ///
    /// Returns an error if the handle is no longer valid.
    fn set_next_start_at(&self, next_start_at: DateTime<Utc>) -> Result<(), Error>;

    /// Returns the current state of the task with the given id.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_id` is not part of the job or if the handle is no longer valid.
    fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, Error>;

    /// Returns every task in the job whose code matches `code`, in unspecified order.
    ///
    /// Returns an empty vector if no task matches.
    ///
    /// # Errors
    ///
    /// Returns an error if the handle is no longer valid.
    fn get_tasks_by_code(&self, code: &TaskCode) -> Result<Vec<Arc<dyn ImmutableTask>>, Error>;
}

/// The job an executor works on through its handle, together with whether that handle is still
/// open.
///
/// The two share one lock because they answer one question: whether this call may still reach the
/// job. Kept apart, a call admitted by the open-check could arrive at the job after the worker
/// closed the handle and took the state away, and its result would be lost with no error anywhere.
/// The worker takes the job back out once [`JobHandleImpl::close`] returned.
pub(crate) struct JobHandleState {
    job: Job,
    is_open: bool,
}

impl JobHandleState {
    /// Wraps the worker's copy of the job for the duration of one task execution, open for it.
    pub(crate) const fn new(job: Job) -> Self {
        Self { job, is_open: true }
    }

    /// Borrows the job for a worker that could not reclaim the state, because the executor still
    /// holds a handle to it.
    pub(crate) const fn job(&self) -> &Job {
        &self.job
    }

    /// Hands the job back to the worker that opened the state.
    pub(crate) fn into_job(self) -> Job {
        self.job
    }
}

// JobHandleImpl - internal implementation
pub(crate) struct JobHandleImpl {
    state: Arc<RwLock<JobHandleState>>,
    worker_id: Uuid,
}

impl JobHandleImpl {
    pub(crate) const fn new(state: Arc<RwLock<JobHandleState>>, worker_id: Uuid) -> Self {
        Self { state, worker_id }
    }

    /// Invalidates the handle. Called by the worker once the executor returned, so a handle that
    /// escaped into a background task cannot mutate a job the worker is about to persist.
    ///
    /// Takes the same lock every call on the handle takes, so a call already inside one finishes
    /// before this returns, and a call starting afterwards is refused.
    pub(crate) fn close(&self) {
        self.state.write().is_open = false;
    }

    /// Applies `mutation` to the job, and only while the handle is open.
    fn write_job<T>(&self, writer: impl FnOnce(&mut Job) -> Result<T, JobError>) -> Result<T, Error> {
        let mut state = self.state.write();
        Self::check_open(&state)?;
        writer(&mut state.job).map_err(|e| Error::Other(e.to_string()))
    }

    /// Reads the job, and only while the handle is open.
    fn read_job<T>(&self, reader: impl FnOnce(&Job) -> Result<T, JobError>) -> Result<T, Error> {
        let state = self.state.read();
        Self::check_open(&state)?;
        reader(&state.job).map_err(|e| Error::Other(e.to_string()))
    }

    /// Refuses the call when the handle was closed. Callers hold the lock, which is what makes the
    /// verdict still true by the time the job is touched.
    fn check_open(state: &JobHandleState) -> Result<(), Error> {
        if state.is_open {
            return Ok(());
        }
        Err(Error::Other(
            "job handle is no longer valid: the task execution it belongs to has finished".into(),
        ))
    }
}

impl JobHandle for JobHandleImpl {
    fn add_task(&self, task_def: TaskDefinition) -> Result<TaskRef, Error> {
        self.write_job(|job| job.add_task(&task_def, self.worker_id).map(TaskRef::created))
    }

    fn complete_task(&self, task_id: &Uuid, output: Vec<u8>) -> Result<(), Error> {
        self.write_job(|job| job.complete_task(task_id, output))
    }

    fn fail_task(&self, task_id: &Uuid, error_msg: &str) -> Result<(), Error> {
        self.write_job(|job| job.fail_task(task_id, error_msg))
    }

    fn set_next_start_at(&self, next_start_at: DateTime<Utc>) -> Result<(), Error> {
        self.write_job(|job| {
            job.set_next_start_at(next_start_at);
            Ok(())
        })
    }

    fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, Error> {
        self.read_job(|job| job.get_task(task_id))
    }

    fn get_tasks_by_code(&self, code: &TaskCode) -> Result<Vec<Arc<dyn ImmutableTask>>, Error> {
        self.read_job(|job| Ok(job.get_tasks_by_code(code)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::thread;

    use chrono::Duration;
    use parking_lot::Mutex;

    use super::*;
    use crate::{JobCode, JobDefinition, JobDefinitionId, TaskLimits, TaskOutcome, task_fn};

    /// Longest a test waits for a thread it expects to make progress. A wait that runs out is a
    /// failure with a diagnostic, never a silently passing test.
    const PROGRESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    /// Which of the two threads of [`close_waits_for_a_call_already_touching_the_job`] recorded
    /// itself as finished.
    #[derive(Debug, PartialEq, Eq)]
    enum Finish {
        Mutation,
        Close,
    }

    fn test_state() -> Arc<RwLock<JobHandleState>> {
        let task_def = TaskDefinition::new(TaskCode::new("task"), std::time::Duration::from_secs(5));
        let executor = task_fn(|_ctx| async { Ok(TaskOutcome::empty()) });
        let job_def = JobDefinition::new(
            JobDefinitionId::new(),
            JobCode::new("job"),
            vec![(task_def, executor)],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )
        .unwrap();
        let job = Job::new(&job_def, HashMap::new(), Uuid::from_u128(1)).unwrap();
        Arc::new(RwLock::new(JobHandleState::new(job)))
    }

    fn only_task_id(state: &Arc<RwLock<JobHandleState>>) -> Uuid {
        *state
            .read()
            .job()
            .get_tasks_by_code(&TaskCode::new("task"))
            .first()
            .unwrap()
            .id()
    }

    #[test]
    fn set_next_start_at_updates_job_under_lock() {
        let state = test_state();
        let handle = JobHandleImpl::new(Arc::clone(&state), Uuid::from_u128(2));

        let next_start_at = Utc::now() + Duration::seconds(30);
        handle.set_next_start_at(next_start_at).unwrap();

        assert_eq!(state.read().job().next_start_at(), Some(next_start_at));
    }

    /// Completing is only legal from `Started`, so the task is started first - through the job
    /// itself, since starting is the worker's move, not the executor's.
    #[test]
    fn open_handle_mutates_the_job() {
        let state = test_state();
        let handle = JobHandleImpl::new(Arc::clone(&state), Uuid::from_u128(2));
        let task_id = only_task_id(&state);

        state.write().job.start_task(&task_id, Uuid::from_u128(2)).unwrap();
        handle.complete_task(&task_id, b"done".to_vec()).unwrap();

        assert!(state.read().job().get_task(&task_id).unwrap().is_completed());
    }

    /// The runtime replacement for the compile-time guarantee the borrowed handle used to give:
    /// after the worker closes it, a handle that outlived its executor can neither read nor write.
    #[test]
    fn closed_handle_rejects_every_call() {
        let state = test_state();
        let handle = JobHandleImpl::new(Arc::clone(&state), Uuid::from_u128(2));
        let task_id = only_task_id(&state);
        state.write().job.start_task(&task_id, Uuid::from_u128(2)).unwrap();

        handle.close();

        let failures = [
            handle.complete_task(&task_id, Vec::new()).err(),
            handle.fail_task(&task_id, "late").err(),
            handle.set_next_start_at(Utc::now()).err(),
            handle.get_task(&task_id).err(),
            handle.get_tasks_by_code(&TaskCode::new("task")).err(),
            handle
                .add_task(TaskDefinition::new(
                    TaskCode::new("task"),
                    std::time::Duration::from_secs(5),
                ))
                .err(),
        ];
        for failure in failures {
            let message = failure.expect("a closed handle must reject the call").to_string();
            assert!(message.contains("no longer valid"), "got: {message}");
        }

        assert!(
            !state.read().job().get_task(&task_id).unwrap().is_completed(),
            "a rejected call must not have reached the job"
        );
    }

    /// The worker reads the job the moment `close` returns, so a call that is already touching the
    /// job has to finish first - otherwise its result lands after the snapshot and is lost.
    ///
    /// A real mutation stands in for such a call: it reports itself from inside the job and stays
    /// there until the test releases it, so the interleaving is reached by message rather than by
    /// waiting out a sleep. What is asserted is the order the two threads finish in - with the
    /// open-check and the mutation split apart, `close` finishes while the mutation is still inside
    /// the job. The one thing left to timing is the closing thread stalling between its report and
    /// the call itself, which costs the test its coverage for that run but never fails it wrongly.
    #[test]
    fn close_waits_for_a_call_already_touching_the_job() {
        let state = test_state();
        let handle = Arc::new(JobHandleImpl::new(Arc::clone(&state), Uuid::from_u128(2)));
        let finish_order = Arc::new(Mutex::new(Vec::new()));
        let (entered_sender, entered_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();

        let mutating = thread::spawn({
            let handle = Arc::clone(&handle);
            let finish_order = Arc::clone(&finish_order);
            move || {
                handle.write_job(|_job| {
                    entered_sender.send(()).expect("the test must be waiting for the report");
                    release_receiver.recv().expect("the test must release the mutation");
                    finish_order.lock().push(Finish::Mutation);
                    Ok(())
                })
            }
        });
        recv_before_deadline(&entered_receiver, "the mutation must reach the job");

        let (closing_sender, closing_receiver) = mpsc::channel();
        let closing = thread::spawn({
            let handle = Arc::clone(&handle);
            let finish_order = Arc::clone(&finish_order);
            move || {
                closing_sender.send(()).expect("the test must be waiting for the report");
                handle.close();
                finish_order.lock().push(Finish::Close);
            }
        });
        recv_before_deadline(&closing_receiver, "the closing thread must reach close");

        release_sender.send(()).expect("the mutation must still be inside the job");
        wait_for_thread(mutating).expect("the mutation must be applied");
        wait_for_thread(closing);

        assert_eq!(
            *finish_order.lock(),
            vec![Finish::Mutation, Finish::Close],
            "close must not finish while a call is inside the job"
        );
    }

    /// The other order of the same race: a call that was still waiting for the lock when `close`
    /// took it is refused, and the job the worker then snapshots is untouched by it.
    #[test]
    fn a_call_waiting_for_the_lock_is_refused_once_close_wins_it() {
        let state = test_state();
        let handle = Arc::new(JobHandleImpl::new(Arc::clone(&state), Uuid::from_u128(2)));
        let task_id = only_task_id(&state);
        state.write().job.start_task(&task_id, Uuid::from_u128(2)).unwrap();

        handle.close();
        let completing = thread::spawn({
            let handle = Arc::clone(&handle);
            move || handle.complete_task(&task_id, b"late".to_vec())
        });

        let error = wait_for_thread(completing).expect_err("a call losing the race to close must be refused");
        assert!(error.to_string().contains("no longer valid"), "got: {error}");
        assert!(
            !state.read().job().get_task(&task_id).unwrap().is_completed(),
            "a refused call must leave the job as the worker will snapshot it"
        );
    }

    /// Waits for a report the test expects to arrive, failing with `expectation` rather than
    /// hanging the suite if it never does.
    fn recv_before_deadline(reports: &mpsc::Receiver<()>, expectation: &str) {
        reports.recv_timeout(PROGRESS_TIMEOUT).expect(expectation);
    }

    /// Joins `thread`, failing the test rather than hanging the suite if it never finishes.
    fn wait_for_thread<T: Send + 'static>(thread: thread::JoinHandle<T>) -> T {
        let deadline = std::time::Instant::now() + PROGRESS_TIMEOUT;
        while !thread.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "the thread did not finish in time"
            );
            std::thread::yield_now();
        }
        thread.join().expect("the thread must not panic")
    }
}
