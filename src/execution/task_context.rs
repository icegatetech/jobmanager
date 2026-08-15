use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{ImmutableTask, JobHandle, TaskCode};

/// Everything one task execution is given: the task itself, the job it runs in, and the
/// cancellation signal of this execution.
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

    /// Cancellation signal of this execution; select on it in long-running work.
    ///
    /// Three events cancel it: a shutdown of the worker pool, the task's own deadline passing, and
    /// the executor cancelling the token itself. The first two are cooperative - an executor that
    /// ignores the token keeps running, which is why a deadline lets another worker take the task
    /// over rather than stopping the work.
    ///
    /// The token is this execution's own, so cancelling it stops the work waiting on it and nothing
    /// beyond: it neither cancels the pool nor makes the worker treat the outcome as a deadline. An
    /// executor that cancels itself and then returns
    /// [`TaskOutcome::Cancelled`](crate::TaskOutcome::Cancelled) has refused the task, and it is
    /// failed as such.
    pub const fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// Whether this execution has been cancelled; see [`Self::cancel_token`] for what cancels it.
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
