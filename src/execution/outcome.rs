//! What a worker's work on a job says about when its pool polls the job again.
//!
//! The worker reports what it did in its own terms - [`TaskStart`] and [`ExecutionEnd`] - and the
//! pool reads them here as the [`JobProgress`] its schedule takes, so the worker knows nothing about
//! polling and nothing about the pool it runs in.
//! IMPORTANT: this module is intended solely for DTOs and mapping; it contains no business logic.

use chrono::{DateTime, Utc};

use crate::core::job::NextIterationStatus;
use crate::execution::worker::{ExecutionEnd, TaskStart};

/// What the last state a worker read or wrote for a job says about when polling the job again can
/// change anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JobProgress {
    /// The job spent its iteration budget: nothing a later read finds can change anything.
    IterationsEnded,
    /// The iteration finished; the next one is not due before the given moment.
    NextIterationDueAt(DateTime<Utc>),
    /// The state holds a task another worker may start at once, or the iteration closed and the
    /// next one may start at once.
    TaskPickable,
    /// The state holds no task another worker may start at once, and a failed one may be started
    /// again from the given moment.
    RetryDueAt(DateTime<Utc>),
    /// The worker moved the job, or tried to, and saw no task another worker may start at once: none
    /// is left, another worker got the task or the verdict first, or the execution ended in an
    /// error.
    NothingPickable,
    /// The worker changed nothing, or failed.
    Stalled,
}

impl JobProgress {
    pub(crate) fn from_task_start(task_start: &TaskStart) -> Self {
        match *task_start {
            TaskStart::Started {
                has_pickable_task_left, ..
            } => Self::from_tasks_left(has_pickable_task_left),
            // The state names the next poll, so the read after it is not spent finding that out.
            TaskStart::IterationClosed(next_iteration_status) => match next_iteration_status {
                NextIterationStatus::BudgetSpent => Self::IterationsEnded,
                NextIterationStatus::DueAt(due_at) => Self::NextIterationDueAt(due_at),
                NextIterationStatus::Ready => Self::NextIterationDueAt(Utc::now()),
                // Nothing to wait for and nothing terminal about the state, so the job falls back to
                // the ordinary backoff rather than to a moment nobody can compute.
                NextIterationStatus::Unscheduled => Self::Stalled,
            },
            TaskStart::LostToAnotherWorker => Self::NothingPickable,
            TaskStart::NoTaskAvailable {
                retry_at: Some(retry_at),
            } => Self::RetryDueAt(retry_at),
            TaskStart::NoTaskAvailable { retry_at: None } | TaskStart::Stalled => Self::Stalled,
        }
    }

    /// What an execution end says; `None` is an execution that was cancelled or failed before it
    /// ended with a state.
    pub(crate) const fn from_job_execution(execution_end: Option<ExecutionEnd>) -> Self {
        match execution_end {
            None => Self::NothingPickable,
            Some(ExecutionEnd::TaskEnded { has_pickable_task_left }) => Self::from_tasks_left(has_pickable_task_left),
            Some(ExecutionEnd::IterationClosed(next_iteration_status)) => match next_iteration_status {
                NextIterationStatus::BudgetSpent => Self::IterationsEnded,
                NextIterationStatus::DueAt(due_at) => Self::NextIterationDueAt(due_at),
                // Due at once, as the `NextIterationDueAt(now)` of a closed iteration read by a task
                // start is.
                NextIterationStatus::Ready => Self::TaskPickable,
                // `Unscheduled` names no moment, and an execution end has no backoff to answer with
                // as a task start does with `Stalled`: the poll already scheduled stays unless the
                // base interval is sooner.
                NextIterationStatus::Unscheduled => Self::NothingPickable,
            },
        }
    }

    const fn from_tasks_left(has_pickable_task_left: bool) -> Self {
        if has_pickable_task_left {
            Self::TaskPickable
        } else {
            Self::NothingPickable
        }
    }
}
