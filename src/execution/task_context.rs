use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{ImmutableTask, JobHandle, TaskCode};

/// Everything one task execution is given: the task itself, the job it runs in, and the
/// cancellation signal of the worker pool.
///
/// Owned rather than borrowed, so an executor can be a plain async closure. The job handle inside
/// stops working the moment the executor returns; see [`JobHandle`].
pub struct TaskContext {
    task: Arc<dyn ImmutableTask>,
    job: Arc<dyn JobHandle>,
    cancel_token: CancellationToken,
}

impl TaskContext {
    pub(crate) const fn new(
        task: Arc<dyn ImmutableTask>,
        job: Arc<dyn JobHandle>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            task,
            job,
            cancel_token,
        }
    }

    /// Identifier of the task being executed.
    pub fn id(&self) -> &Uuid {
        self.task.id()
    }

    /// Code the task was defined with.
    pub fn code(&self) -> &TaskCode {
        self.task.code()
    }

    /// Input payload the task was created with. Empty if none was supplied.
    pub fn input(&self) -> &[u8] {
        self.task.get_input()
    }

    /// Cancellation signal of the worker pool; select on it in long-running work.
    pub const fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// Whether a shutdown has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// Access to the job this task runs in: creating follow-up tasks, reading siblings,
    /// rescheduling the next iteration.
    pub fn job(&self) -> &dyn JobHandle {
        self.job.as_ref()
    }

    /// Read-only view of the task being executed, for the fields not surfaced directly here.
    ///
    /// It is a snapshot taken when the task was picked up, so it does not reflect a change the
    /// executor itself makes through [`Self::job`].
    pub const fn task(&self) -> &Arc<dyn ImmutableTask> {
        &self.task
    }
}
