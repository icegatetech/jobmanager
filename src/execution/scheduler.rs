//! When one pool polls each job, and which jobs a worker of it is passing over.
//!
//! A pool keeps one [`PollScheduler`] for all of its workers, so what an idle pool costs the store
//! is the number of jobs it polls, not that number times the number of workers.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use rand::Rng;
use thiserror::Error;
use tokio::time::{Duration, Instant};

use crate::core::error::Error;
use crate::core::job::JobCode;
use crate::execution::outcome::JobProgress;

/// Polling settings of a [`PollScheduler`], set through
/// [`JobsManagerBuilder::poll_interval`](crate::JobsManagerBuilder::poll_interval),
/// [`poll_jitter`](crate::JobsManagerBuilder::poll_jitter) and
/// [`max_poll_interval`](crate::JobsManagerBuilder::max_poll_interval).
///
/// Every setting is checked as it is set, so an assembled `PollSchedulerConfig` is always a set of
/// intervals a pool can poll by: there is no separate validation step and no state in between.
#[derive(Clone)]
pub struct PollSchedulerConfig {
    poll_interval: Duration,
    poll_jitter: Duration,
    max_poll_interval: Option<Duration>,
}

impl Default for PollSchedulerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl PollSchedulerConfig {
    /// How many times above the base interval the backoff ceiling sits when no ceiling was named.
    const DEFAULT_MAX_POLL_INTERVAL_FACTOR: u32 = 10;
    const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(200);
    /// Longest interval a pool accepts, either as its base or as its backoff ceiling.
    // The product spells the year out in the units the bound is stated in; the `from_hours(8760)`
    // the lint asks for names the same value in a number nobody reads as a year.
    #[allow(clippy::duration_suboptimal_units)]
    pub(crate) const LONGEST_ACCEPTED_POLL_INTERVAL: Duration = Duration::from_secs(365 * 24 * 60 * 60);
    const DEFAULT_POLL_JITTER: Duration = Duration::from_millis(50);

    /// Settings a pool polls by unless a `with_*` method replaces one of them.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            poll_interval: Self::DEFAULT_POLL_INTERVAL,
            poll_jitter: Self::DEFAULT_POLL_JITTER,
            max_poll_interval: None,
        }
    }

    /// Replaces the interval the pool polls a job at after a task start that moved it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `interval` is zero - a worker would then poll storage in a loop
    /// without ever pausing - if it is above [`Self::LONGEST_ACCEPTED_POLL_INTERVAL`], or if it is
    /// above a ceiling already named through [`Self::with_max_poll_interval`], which would turn the
    /// backoff into a speed-up.
    pub fn with_poll_interval(mut self, interval: Duration) -> Result<Self, Error> {
        if interval.is_zero() {
            return Err(Error::Other("poll interval must be positive".to_string()));
        }
        if interval > Self::LONGEST_ACCEPTED_POLL_INTERVAL {
            return Err(Error::Other("poll interval must not exceed a year".to_string()));
        }
        if self.max_poll_interval.is_some_and(|ceiling| ceiling < interval) {
            return Err(Error::Other(
                "max poll interval must not be below the poll interval".to_string(),
            ));
        }

        self.poll_interval = interval;
        Ok(self)
    }

    /// Replaces the upper bound of the random delay drawn for each poll scheduled later than now. A
    /// zero bound adds nothing.
    #[must_use]
    pub const fn with_poll_jitter(mut self, jitter: Duration) -> Self {
        self.poll_jitter = jitter;
        self
    }

    /// Replaces the ceiling the poll interval backs off to while there is no work.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `ceiling` is below the poll interval, which would turn the
    /// backoff into a speed-up, or above [`Self::LONGEST_ACCEPTED_POLL_INTERVAL`].
    pub fn with_max_poll_interval(mut self, ceiling: Duration) -> Result<Self, Error> {
        if ceiling < self.poll_interval {
            return Err(Error::Other(
                "max poll interval must not be below the poll interval".to_string(),
            ));
        }
        if ceiling > Self::LONGEST_ACCEPTED_POLL_INTERVAL {
            return Err(Error::Other("max poll interval must not exceed a year".to_string()));
        }

        self.max_poll_interval = Some(ceiling);
        Ok(self)
    }

    /// See [`JobsManagerBuilder::poll_interval`](crate::JobsManagerBuilder::poll_interval).
    pub const fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// See [`JobsManagerBuilder::poll_jitter`](crate::JobsManagerBuilder::poll_jitter).
    pub const fn poll_jitter(&self) -> Duration {
        self.poll_jitter
    }

    /// See [`JobsManagerBuilder::max_poll_interval`](crate::JobsManagerBuilder::max_poll_interval).
    pub fn max_poll_interval(&self) -> Duration {
        self.max_poll_interval
            .unwrap_or(self.poll_interval * Self::DEFAULT_MAX_POLL_INTERVAL_FACTOR)
    }
}

/// When a job is polled next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PollDue {
    At(Instant),
    /// No later task start of this pool can change anything: the job spent the iteration
    /// budget this instance's `JobDefinition` names, and an iteration number only grows.
    Never,
}

impl PollDue {
    /// The poll that answers for both: `Never` outweighs any moment, and of two moments the earlier
    /// one wins.
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Never, _) | (_, Self::Never) => Self::Never,
            (Self::At(first), Self::At(second)) => Self::At(std::cmp::min(first, second)),
        }
    }
}

/// The interval arithmetic of a pool's poll settings: the backoff, the jitter, and the wait until a
/// moment.
struct PollPolicy {
    config: PollSchedulerConfig,
}

impl PollPolicy {
    pub const fn base_interval(&self) -> Duration {
        self.config.poll_interval()
    }

    /// Interval for the poll after an idle task start: twice the last, capped at the backoff
    /// ceiling.
    pub fn calculate_backed_off_poll_interval(&self, last_poll_interval: Duration) -> Duration {
        std::cmp::min(last_poll_interval * 2, self.config.max_poll_interval())
    }

    /// Wait before the poll after a task start that found a failed task awaiting its retry:
    /// until the retry, capped at the backoff ceiling. The retry answers for the failed task alone,
    /// and any other task may become pickable before it - by a deadline, or by a write of another
    /// instance.
    pub fn calculate_retry_wait(&self, retry_at: DateTime<Utc>) -> Duration {
        std::cmp::min(
            Self::calculate_duration_until(retry_at),
            self.config.max_poll_interval(),
        )
    }

    /// The moment `wait` from `now`, spread by one draw of the jitter. A zero wait is due now and
    /// draws nothing: a job due now has work a worker can take at once.
    pub fn calculate_poll_at(&self, now: Instant, wait: Duration) -> Instant {
        if wait.is_zero() {
            return now;
        }

        now + wait + self.calculate_jitter()
    }

    /// A random share of the configured jitter, which is what keeps pools polling one job from doing
    /// it in lockstep. A zero jitter draws nothing.
    fn calculate_jitter(&self) -> Duration {
        let max_jitter_nanos = u64::try_from(self.config.poll_jitter().as_nanos()).unwrap_or(u64::MAX);
        if max_jitter_nanos == 0 {
            return Duration::ZERO;
        }

        Duration::from_nanos(rand::rng().random_range(0..max_jitter_nanos))
    }

    /// Time left until `moment`, or zero if it has already passed, and never more than
    /// [`PollSchedulerConfig::LONGEST_ACCEPTED_POLL_INTERVAL`].
    ///
    /// The cap holds the arithmetic inside the range configured intervals are checked against: a
    /// moment set through [`JobHandle::set_next_start_at`](crate::JobHandle::set_next_start_at)
    /// passes no such check, and adding a far enough one to an `Instant` panics. A job polled a year
    /// before it is due pays one task start, which schedules it again.
    pub fn calculate_duration_until(moment: DateTime<Utc>) -> Duration {
        std::cmp::min(
            (moment - Utc::now()).to_std().unwrap_or(Duration::ZERO),
            PollSchedulerConfig::LONGEST_ACCEPTED_POLL_INTERVAL,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobSlot {
    /// Waiting in [`PollScheduler::due_queue`] under `poll_at`.
    Queued { poll_at: Instant },
    Claimed {
        /// What execution ends asked for while the job was claimed, applied when the claim is
        /// released: the task start holding the claim read the state before those executions
        /// wrote it.
        overwritten_poll_due: Option<PollDue>,
    },
    /// Mirrors [`PollDue::Never`].
    // TODO(med): an instance with a higher `max_iterations` may continue the job; this pool does not
    // poll it again until it restarts.
    Exhausted,
}

/// When one job of a pool is polled next, and whether a worker is passing over it.
struct JobPoll {
    // Backoff step of this job alone: a busy job must not hold an idle neighbour at the base
    // interval, nor an idle one drag a busy neighbour up to the ceiling.
    poll_interval: Duration,
    slot: JobSlot,
}

impl JobPoll {
    /// The moment the job waits in the due queue under, if it is queued.
    pub const fn queued_at(&self) -> Option<Instant> {
        match self.slot {
            JobSlot::Queued { poll_at } => Some(poll_at),
            JobSlot::Claimed { .. } | JobSlot::Exhausted => None,
        }
    }

    pub const fn claim(&mut self) {
        self.slot = JobSlot::Claimed {
            overwritten_poll_due: None,
        };
    }

    /// Returns `false`, changing nothing, when the job is not claimed.
    pub fn release(&mut self, progress: JobProgress, policy: &PollPolicy, now: Instant) -> bool {
        let JobSlot::Claimed { overwritten_poll_due } = self.slot else {
            return false;
        };

        let (task_start_due, task_start_poll_interval) = self.calculate_poll(progress, policy, now);
        match overwritten_poll_due {
            None => {
                self.poll_interval = task_start_poll_interval;
                self.schedule_poll(task_start_due);
            }
            // An execution ended meanwhile, so the step the task start would have doubled
            // answers for nothing.
            Some(overwritten_poll_due) => {
                self.poll_interval = policy.base_interval();
                self.schedule_poll(task_start_due.merge(overwritten_poll_due));
            }
        }
        true
    }

    pub fn reschedule(&mut self, progress: JobProgress, policy: &PollPolicy, now: Instant) {
        let (requested_due, _) = self.calculate_poll(progress, policy, now);
        match self.slot {
            JobSlot::Exhausted => {}
            JobSlot::Claimed { overwritten_poll_due } => {
                self.slot = JobSlot::Claimed {
                    overwritten_poll_due: Some(
                        overwritten_poll_due.map_or(requested_due, |recorded_due| recorded_due.merge(requested_due)),
                    ),
                };
            }
            JobSlot::Queued { poll_at } => {
                self.poll_interval = policy.base_interval();
                let poll_due = match progress {
                    // The execution closed the iteration: the task start of that task saw none of
                    // the state it wrote.
                    JobProgress::IterationsEnded | JobProgress::NextIterationDueAt(_) => requested_due,
                    // The poll already scheduled answers for what that task start saw.
                    JobProgress::TaskPickable
                    | JobProgress::RetryDueAt(_)
                    | JobProgress::NothingPickable
                    | JobProgress::Stalled => PollDue::At(poll_at).merge(requested_due),
                };
                self.schedule_poll(poll_due);
            }
        }
    }

    /// The poll `progress` asks for, and the backoff step that answers for it.
    fn calculate_poll(&self, progress: JobProgress, policy: &PollPolicy, now: Instant) -> (PollDue, Duration) {
        let base_interval = policy.base_interval();
        match progress {
            JobProgress::IterationsEnded => (PollDue::Never, self.poll_interval),
            JobProgress::NextIterationDueAt(due_at) => (
                PollDue::At(policy.calculate_poll_at(now, PollPolicy::calculate_duration_until(due_at))),
                base_interval,
            ),
            JobProgress::TaskPickable => (PollDue::At(now), base_interval),
            JobProgress::RetryDueAt(retry_at) => (
                PollDue::At(policy.calculate_poll_at(now, policy.calculate_retry_wait(retry_at))),
                base_interval,
            ),
            JobProgress::NothingPickable => (PollDue::At(policy.calculate_poll_at(now, base_interval)), base_interval),
            JobProgress::Stalled => {
                let backed_off_interval = policy.calculate_backed_off_poll_interval(self.poll_interval);
                (
                    PollDue::At(policy.calculate_poll_at(now, backed_off_interval)),
                    backed_off_interval,
                )
            }
        }
    }

    const fn schedule_poll(&mut self, poll_due: PollDue) {
        self.slot = match poll_due {
            PollDue::At(poll_at) => JobSlot::Queued { poll_at },
            PollDue::Never => JobSlot::Exhausted,
        };
    }
}

/// The right of one worker to prepare one job, handed out by [`PollScheduler::try_claim_due_job`]
/// and given back by [`PollScheduler::release_job`]. It is neither `Clone` nor constructible
/// outside this module, so a job is released at most once per claim.
#[derive(Debug)]
pub(crate) struct JobClaim(JobCode);

impl JobClaim {
    pub(crate) const fn job_code(&self) -> &JobCode {
        &self.0
    }
}

/// A call naming a job the schedule cannot act on. A pool that hands back only the claims and job
/// codes the schedule gave it never meets one.
#[derive(Debug, Error)]
pub(crate) enum ScheduleError {
    #[error("job '{0}' is not on the poll schedule")]
    UnknownJob(JobCode),
    #[error("job '{0}' was released while it was not claimed")]
    NotClaimed(JobCode),
}

/// What one look at the schedule gives a pool that has a free worker.
#[derive(Debug)]
pub(crate) enum ClaimAttempt {
    /// The earliest due job, claimed for the worker the pool hands it to: no other worker of the
    /// pool is handed it until [`PollScheduler::release_job`] returns it.
    Claimed(JobClaim),
    /// No job is due before this moment.
    WaitUntil(Instant),
    /// Every job is claimed or exhausted: only a task start or an execution ending changes
    /// that.
    WaitForPoolEvent,
}

/// The poll schedule of one pool: when each job is due, and which of them a worker is passing over.
///
/// A due job is claimed for one worker at a time. The schedule never waits: the pool that owns it
/// decides when to look again, by the moment [`ClaimAttempt::WaitUntil`] names or by the next
/// release or execution end it records.
///
/// Every poll moment is kept by the monotonic clock. A moment of the system clock the domain names
/// is converted once, when the poll is scheduled; a clock set afterwards shifts that poll by as much.
pub(crate) struct PollScheduler {
    policy: PollPolicy,
    jobs: HashMap<JobCode, JobPoll>,
    /// Every queued job by poll moment, so the earliest one is found without visiting the others.
    /// Of two jobs due at the same instant the one whose code sorts first goes first.
    ///
    /// An index of [`JobPoll::queued_at`], written only by [`Self::transition_job`].
    due_queue: BTreeSet<(Instant, JobCode)>,
}

impl PollScheduler {
    /// A schedule over `job_codes`, every one of them due at once: the first task start of a
    /// pool is one per job.
    pub(crate) fn new(job_codes: Vec<JobCode>, config: PollSchedulerConfig) -> Self {
        let now = Instant::now();
        let poll_interval = config.poll_interval();
        let jobs = job_codes
            .iter()
            .map(|code| {
                let job = JobPoll {
                    poll_interval,
                    slot: JobSlot::Queued { poll_at: now },
                };
                (code.clone(), job)
            })
            .collect();
        let due_queue = job_codes.into_iter().map(|code| (now, code)).collect();

        Self {
            policy: PollPolicy { config },
            jobs,
            due_queue,
        }
    }

    /// Claims the earliest due job, or names when to look again.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleError::UnknownJob`] if the earliest due entry names a job the schedule does
    /// not hold. The entry leaves the queue with the error, so the next call moves on to the job
    /// behind it.
    pub(crate) fn try_claim_due_job(&mut self) -> Result<ClaimAttempt, ScheduleError> {
        let Some((poll_at, _)) = self.due_queue.first() else {
            return Ok(ClaimAttempt::WaitForPoolEvent);
        };
        if *poll_at > Instant::now() {
            return Ok(ClaimAttempt::WaitUntil(*poll_at));
        }

        // Taken off before the transition: an entry the transition refuses would otherwise stay
        // first in the queue and be refused on every call after this one.
        let Some((_, job_code)) = self.due_queue.pop_first() else {
            return Ok(ClaimAttempt::WaitForPoolEvent);
        };
        self.transition_job(&job_code, |job, _| job.claim())?;
        Ok(ClaimAttempt::Claimed(JobClaim(job_code)))
    }

    /// Moves the poll of `job_code` to where `progress`, the end of an execution of one of its
    /// tasks, says polling can change anything again.
    ///
    /// For a queued job, an execution that closed the iteration replaces the job's next poll: the
    /// state it wrote names when polling can change anything again, and the task start of that
    /// task saw none of it. Any other end only brings the poll closer, since the poll
    /// already scheduled answers for what that task start saw.
    ///
    /// For a claimed job, whatever the end asks for is kept until the claim is released, and the
    /// release takes the earlier of it and of what the task start asked for - `Never` outweighs
    /// any moment. Several ends recorded under one claim are combined the same way.
    pub(crate) fn reschedule_job(&mut self, job_code: &JobCode, progress: JobProgress) -> Result<(), ScheduleError> {
        let now = Instant::now();
        self.transition_job(job_code, |job, policy| job.reschedule(progress, policy, now))
    }

    /// Returns a claimed job to the schedule, due when `progress` says polling it can change
    /// anything.
    pub(crate) fn release_job(&mut self, claim: JobClaim, progress: JobProgress) -> Result<(), ScheduleError> {
        let now = Instant::now();
        let JobClaim(job_code) = claim;
        let is_released = self.transition_job(&job_code, |job, policy| job.release(progress, policy, now))?;
        if !is_released {
            return Err(ScheduleError::NotClaimed(job_code));
        }
        Ok(())
    }

    /// Applies `transition` to the state of `job_code` and moves its entry in the due queue to
    /// where the new state is queued.
    fn transition_job<T>(
        &mut self,
        job_code: &JobCode,
        transition: impl FnOnce(&mut JobPoll, &PollPolicy) -> T,
    ) -> Result<T, ScheduleError> {
        let Some(job) = self.jobs.get_mut(job_code) else {
            return Err(ScheduleError::UnknownJob(job_code.clone()));
        };
        let queued_before = job.queued_at();
        let result = transition(job, &self.policy);
        let queued_after = job.queued_at();
        if queued_before == queued_after {
            return Ok(result);
        }

        // The code taken out with the old entry goes into the new one, so a move costs one clone.
        let indexed_code = queued_before.and_then(|poll_at| self.due_queue.take(&(poll_at, job_code.clone())));
        if let Some(poll_at) = queued_after {
            let indexed_code = indexed_code.map_or_else(|| job_code.clone(), |(_, code)| code);
            self.due_queue.insert((poll_at, indexed_code));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::worker::TaskStart;

    /// How many draws a test takes when it asserts on the spread of the jitter rather than on a
    /// single value. A range of milliseconds has millions of nanoseconds in it, so this many equal
    /// draws does not happen for any reason other than a jitter that is not drawn at all.
    const JITTER_DRAWS: usize = 50;

    /// Base interval every scheduling test below measures against.
    const BASE_POLL_INTERVAL: Duration = Duration::from_millis(100);

    /// Slack allowed when comparing a moment derived from the system clock: it moves between the
    /// moment the test names and the one the schedule reads, while the paused tokio clock does not.
    const SCHEDULING_SLACK: Duration = Duration::from_millis(50);

    /// Ceiling the tests below name for themselves, so what they compare against is a value of the
    /// test rather than one the schedule derives from the same config it reads. Three times
    /// [`BASE_POLL_INTERVAL`], so the second stalled task start is already held down to it.
    const NAMED_CEILING: Duration = Duration::from_millis(300);

    /// Longer than any poll moment the tests below schedule, so a claim that has not come by then
    /// never will.
    const NO_CLAIM_WAIT: Duration = Duration::from_secs(3600);

    fn config_polled_every(interval: Duration) -> PollSchedulerConfig {
        PollSchedulerConfig::new()
            .with_poll_interval(interval)
            .expect("a positive poll interval is accepted")
            .with_poll_jitter(Duration::ZERO)
    }

    fn config_with_jitter(jitter: Duration) -> PollSchedulerConfig {
        config_polled_every(BASE_POLL_INTERVAL).with_poll_jitter(jitter)
    }

    /// Settings whose backoff stops at [`NAMED_CEILING`].
    fn config_capped_at_the_named_ceiling() -> PollSchedulerConfig {
        config_polled_every(BASE_POLL_INTERVAL)
            .with_max_poll_interval(NAMED_CEILING)
            .expect("a ceiling above the base interval is accepted")
    }

    /// Jitter is off here: it has tests of its own, and a random addend would make every scheduled
    /// moment below a range rather than a value.
    fn schedule_over(job_codes: &[&str]) -> PollScheduler {
        schedule_over_with_config(job_codes, config_polled_every(BASE_POLL_INTERVAL))
    }

    fn schedule_over_with_config(job_codes: &[&str], config: PollSchedulerConfig) -> PollScheduler {
        PollScheduler::new(job_codes.iter().map(|code| JobCode::new(*code)).collect(), config)
    }

    /// Claims `job_code` whatever its poll moment, which is the state a task start of it is
    /// in.
    fn claim_job(schedule: &mut PollScheduler, job_code: &str) -> JobClaim {
        let job_code = JobCode::new(job_code);
        let slot = schedule.jobs[&job_code].slot;
        assert!(
            matches!(slot, JobSlot::Queued { .. }),
            "the job must be queued, got {slot:?}"
        );
        schedule
            .transition_job(&job_code, |job, _| job.claim())
            .expect("the schedule must hold the job");
        JobClaim(job_code)
    }

    fn release(schedule: &mut PollScheduler, claim: JobClaim, progress: JobProgress) {
        schedule.release_job(claim, progress).expect("a claimed job is released");
    }

    fn reschedule_job(schedule: &mut PollScheduler, job_code: &str, execution_end: JobProgress) {
        schedule
            .reschedule_job(&JobCode::new(job_code), execution_end)
            .expect("the schedule must hold the job");
    }

    /// A task start of `job_code` that ended with `progress`.
    fn release_after_task_start(schedule: &mut PollScheduler, job_code: &str, progress: JobProgress) {
        let claim = claim_job(schedule, job_code);
        release(schedule, claim, progress);
    }

    fn scheduled_job_field<T>(schedule: &PollScheduler, job_code: &str, read: impl Fn(&JobPoll) -> T) -> T {
        read(&schedule.jobs[&JobCode::new(job_code)])
    }

    /// How long from now the schedule waits before `job_code` is due again.
    fn scheduled_delay(schedule: &PollScheduler, job_code: &str) -> Duration {
        scheduled_job_field(schedule, job_code, |job| match job.slot {
            JobSlot::Queued { poll_at } => poll_at.saturating_duration_since(Instant::now()),
            JobSlot::Claimed { .. } | JobSlot::Exhausted => panic!("the job must be queued, got {:?}", job.slot),
        })
    }

    fn assert_delay_near(actual: Duration, expected: Duration, what: &str) {
        assert!(
            actual <= expected && actual + SCHEDULING_SLACK >= expected,
            "{what}: got {actual:?}, expected about {expected:?}"
        );
    }

    /// The claim a pool looking now would get; fails the test if no job is due.
    fn take_due_claim(schedule: &mut PollScheduler) -> JobClaim {
        match schedule.try_claim_due_job().expect("every queued job is on the schedule") {
            ClaimAttempt::Claimed(claim) => claim,
            other => panic!("a job must be due at once, got {other:?}"),
        }
    }

    /// The code of the job a claim taken now would get, or `None` if no job is due.
    fn claim_due_job_now(schedule: &mut PollScheduler) -> Option<JobCode> {
        match schedule.try_claim_due_job().expect("every queued job is on the schedule") {
            ClaimAttempt::Claimed(claim) => {
                let job_code = claim.job_code().clone();
                release(schedule, claim, JobProgress::Stalled);
                Some(job_code)
            }
            ClaimAttempt::WaitUntil(_) | ClaimAttempt::WaitForPoolEvent => None,
        }
    }

    fn policy_with(config: PollSchedulerConfig) -> PollPolicy {
        PollPolicy { config }
    }

    fn moment_in(delay: chrono::Duration) -> DateTime<Utc> {
        Utc::now() + delay
    }

    /// A worker given a zero interval polls storage without ever pausing, which saturates a core
    /// and bills for an unbounded number of requests.
    #[test]
    fn a_zero_poll_interval_is_rejected() {
        let Err(error) = PollSchedulerConfig::new().with_poll_interval(Duration::ZERO) else {
            panic!("a zero poll interval must be rejected")
        };

        assert!(
            error.to_string().contains("poll interval must be positive"),
            "got: {error}"
        );
    }

    /// The maximum is the ceiling the backoff climbs to, so a value below the base would turn the
    /// backoff into a speed-up.
    #[test]
    fn a_maximum_poll_interval_below_the_poll_interval_is_rejected() {
        let Err(error) =
            config_polled_every(Duration::from_millis(200)).with_max_poll_interval(Duration::from_millis(100))
        else {
            panic!("a ceiling below the base interval must be rejected")
        };

        assert!(
            error.to_string().contains("max poll interval must not be below"),
            "got: {error}"
        );
    }

    /// The same pair named in the other order: a config accepting it either way would depend on
    /// which setting its caller happened to name first.
    #[test]
    fn a_poll_interval_above_a_named_maximum_is_rejected() {
        let named_ceiling = config_polled_every(Duration::from_millis(50))
            .with_max_poll_interval(Duration::from_millis(100))
            .expect("a ceiling above the base interval is accepted");

        let Err(error) = named_ceiling.with_poll_interval(Duration::from_millis(200)) else {
            panic!("a base interval above the named ceiling must be rejected")
        };

        assert!(
            error.to_string().contains("max poll interval must not be below"),
            "got: {error}"
        );
    }

    /// A ceiling equal to the base is what a job polled exactly as rarely as it backs off asks for,
    /// so it has to pass rather than sit on the wrong side of the comparison.
    #[test]
    fn a_maximum_poll_interval_equal_to_the_poll_interval_is_accepted() {
        let config = config_polled_every(Duration::from_secs(10))
            .with_max_poll_interval(Duration::from_secs(10))
            .expect("a ceiling equal to the base interval is accepted");

        assert_eq!(config.max_poll_interval(), Duration::from_secs(10));
    }

    /// An unnamed ceiling is the poll interval multiplied, so a rarely polled pool still has room
    /// to back off; a fixed ceiling would sit at or below such an interval and cancel the backoff.
    #[test]
    fn an_unnamed_maximum_poll_interval_scales_with_a_long_poll_interval() {
        let config = config_polled_every(Duration::from_secs(5));

        assert_eq!(config.max_poll_interval(), Duration::from_secs(50));
    }

    /// The same rule at the other end of the range: a frequently polled pool gets a ceiling as
    /// close as its interval is, rather than one named for a pool polled at another rate.
    #[test]
    fn an_unnamed_maximum_poll_interval_scales_with_a_short_poll_interval() {
        let config = config_polled_every(Duration::from_millis(10));

        assert_eq!(config.max_poll_interval(), Duration::from_millis(100));
    }

    /// The scheduler multiplies, doubles and adds intervals to an `Instant`, and every one of those
    /// panics on overflow - so an interval it cannot compute with is refused at the boundary rather
    /// than taken down the task start that crashes the worker.
    #[test]
    fn a_poll_interval_above_the_longest_accepted_one_is_rejected() {
        let Err(error) = PollSchedulerConfig::new().with_poll_interval(Duration::MAX) else {
            panic!("a poll interval past the accepted range must be rejected")
        };

        assert!(
            error.to_string().contains("poll interval must not exceed"),
            "got: {error}"
        );
    }

    /// The ceiling is an interval the scheduler computes with just as the base one is, so it is
    /// held to the same range - naming it is not a way around the bound.
    #[test]
    fn a_maximum_poll_interval_above_the_longest_accepted_one_is_rejected() {
        let Err(error) = PollSchedulerConfig::new().with_max_poll_interval(Duration::MAX) else {
            panic!("a ceiling past the accepted range must be rejected")
        };

        assert!(
            error.to_string().contains("max poll interval must not exceed"),
            "got: {error}"
        );
    }

    /// Turning the jitter off must leave the poll interval as it is - a pool that waits longer than
    /// it was configured to polls storage at half the rate its settings promise.
    #[test]
    fn a_zero_jitter_leaves_the_poll_interval_alone() {
        let policy = policy_with(config_with_jitter(Duration::ZERO));

        assert_eq!(policy.calculate_jitter(), Duration::ZERO);
    }

    /// The bound of the jitter is a `Duration` the caller picks freely, so the three cases around
    /// the millisecond it used to be counted in: below it, exactly it, and above it.
    #[test]
    fn a_sub_millisecond_jitter_stays_within_its_bound() {
        assert_jitter_within_bound(Duration::from_micros(500));
    }

    #[test]
    fn a_jitter_of_exactly_one_millisecond_stays_within_its_bound() {
        assert_jitter_within_bound(Duration::from_millis(1));
    }

    #[test]
    fn a_multi_millisecond_jitter_stays_within_its_bound() {
        assert_jitter_within_bound(Duration::from_millis(5));
    }

    /// Draws repeatedly, because one draw says nothing about a bound: it asserts that every draw
    /// lands in `[0, bound)` and that the jitter is really drawn.
    fn assert_jitter_within_bound(bound: Duration) {
        let policy = policy_with(config_with_jitter(bound));

        let jitters: Vec<Duration> = (0..JITTER_DRAWS).map(|_| policy.calculate_jitter()).collect();

        for jitter in &jitters {
            assert!(
                *jitter < bound,
                "a jitter of {jitter:?} is outside the bound of {bound:?}"
            );
        }
        assert!(
            jitters.iter().any(|jitter| *jitter != jitters[0]),
            "every draw returned {:?}, so no jitter was drawn",
            jitters[0]
        );
    }

    #[test]
    fn a_task_start_without_work_doubles_the_poll_interval() {
        let policy = policy_with(config_with_jitter(Duration::ZERO));

        assert_eq!(
            policy.calculate_backed_off_poll_interval(Duration::from_millis(100)),
            Duration::from_millis(200)
        );
    }

    /// The backoff is what the maximum poll interval is a ceiling for, so it must stop there
    /// instead of doubling past it.
    #[test]
    fn the_backoff_stops_at_the_maximum_poll_interval() {
        let policy = policy_with(
            config_with_jitter(Duration::ZERO)
                .with_max_poll_interval(Duration::from_millis(300))
                .expect("a ceiling above the base interval is accepted"),
        );

        assert_eq!(
            policy.calculate_backed_off_poll_interval(Duration::from_millis(200)),
            Duration::from_millis(300)
        );
    }

    /// The accepted range and the default factor are two halves of one rule: the ceiling derived
    /// from the longest accepted interval is doubled again by the backoff, and neither step may
    /// leave `Duration` behind. Raising either constant without the other panics here.
    #[test]
    fn the_longest_accepted_poll_interval_survives_the_backoff_arithmetic() {
        let policy = policy_with(config_polled_every(PollSchedulerConfig::LONGEST_ACCEPTED_POLL_INTERVAL));
        let ceiling = policy.config.max_poll_interval();

        assert_eq!(policy.calculate_backed_off_poll_interval(ceiling), ceiling);
    }

    /// Regression: with a fixed default ceiling, a pool polled less often than that ceiling had
    /// `min(interval * 2, ceiling)` collapse to the interval itself - the backoff never grew at all.
    #[test]
    fn a_backoff_grows_at_a_poll_interval_above_the_old_default() {
        let policy = policy_with(config_polled_every(Duration::from_secs(5)));

        assert_eq!(
            policy.calculate_backed_off_poll_interval(Duration::from_secs(5)),
            Duration::from_secs(10)
        );
    }

    /// Checked by scheduling a task start with work at the backed-off interval.
    #[tokio::test(start_paused = true)]
    async fn a_task_start_with_work_schedules_the_next_poll_at_the_base_interval() {
        let mut schedule = schedule_over(&["worked_job"]);

        release_after_task_start(&mut schedule, "worked_job", JobProgress::Stalled);
        release_after_task_start(&mut schedule, "worked_job", JobProgress::NothingPickable);

        assert_eq!(scheduled_delay(&schedule, "worked_job"), BASE_POLL_INTERVAL);
    }

    /// The backoff step a job carries is dropped back to the base interval by a task start
    /// that moved it, and only the next stalled task start reads that step: without the reset,
    /// a job that was idle for a while would go on backing off from where it left off, however much
    /// work it has since done.
    #[tokio::test(start_paused = true)]
    async fn a_task_start_with_work_resets_the_backoff_step_of_its_job() {
        let mut schedule = schedule_over(&["resumed_job"]);

        release_after_task_start(&mut schedule, "resumed_job", JobProgress::Stalled);
        release_after_task_start(&mut schedule, "resumed_job", JobProgress::Stalled);
        release_after_task_start(&mut schedule, "resumed_job", JobProgress::NothingPickable);
        release_after_task_start(&mut schedule, "resumed_job", JobProgress::Stalled);

        assert_eq!(scheduled_delay(&schedule, "resumed_job"), BASE_POLL_INTERVAL * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_task_start_doubles_the_interval_of_that_job() {
        let mut schedule = schedule_over(&["idle_job"]);

        release_after_task_start(&mut schedule, "idle_job", JobProgress::Stalled);

        assert_eq!(scheduled_delay(&schedule, "idle_job"), BASE_POLL_INTERVAL * 2);
    }

    /// The ceiling bounds the job's own step, not just the formula: a schedule that doubled the
    /// step itself would push a quiet job past the rate its pool was configured to poll at.
    #[tokio::test(start_paused = true)]
    async fn the_backoff_step_stops_at_the_ceiling_the_config_names() {
        let mut schedule = schedule_over_with_config(&["capped_job"], config_capped_at_the_named_ceiling());

        // Four task starts: the step reaches the ceiling on the second and has to stay there
        // for the two after it.
        for _ in 0..4 {
            release_after_task_start(&mut schedule, "capped_job", JobProgress::Stalled);
        }

        assert_eq!(scheduled_delay(&schedule, "capped_job"), NAMED_CEILING);
    }

    /// The backoff of one job must not reach another: a pool running a busy job beside an idle one
    /// used to hold both at the base interval, and holding both at the ceiling would be the same
    /// mistake mirrored.
    #[tokio::test(start_paused = true)]
    async fn the_backoff_of_an_idle_job_does_not_reach_a_busy_one() {
        let mut schedule = schedule_over(&["idle_neighbour", "busy_neighbour"]);

        release_after_task_start(&mut schedule, "idle_neighbour", JobProgress::Stalled);
        release_after_task_start(&mut schedule, "idle_neighbour", JobProgress::Stalled);
        release_after_task_start(&mut schedule, "busy_neighbour", JobProgress::NothingPickable);

        assert_eq!(scheduled_delay(&schedule, "busy_neighbour"), BASE_POLL_INTERVAL);
        assert_eq!(scheduled_delay(&schedule, "idle_neighbour"), BASE_POLL_INTERVAL * 4);
    }

    /// A completed job costs nothing until its next iteration is allowed to begin.
    #[tokio::test(start_paused = true)]
    async fn a_completed_job_is_polled_when_its_next_iteration_is_due() {
        let mut schedule = schedule_over(&["scheduled_job"]);

        release_after_task_start(
            &mut schedule,
            "scheduled_job",
            JobProgress::NextIterationDueAt(moment_in(chrono::Duration::seconds(30))),
        );

        assert_delay_near(
            scheduled_delay(&schedule, "scheduled_job"),
            Duration::from_secs(30),
            "a job due in thirty seconds must not be polled before then",
        );
    }

    /// A task start that found only a failed task waiting out its retry pause names the moment
    /// the retry is due, so the job is polled then rather than backed off as one that found nothing.
    /// The retry is due within the backoff ceiling of one second the base interval derives.
    /// Checked by mapping `TaskStart::NoTaskAvailable` to `JobProgress::Stalled` whatever it carries:
    /// the poll then lands one doubled interval away.
    #[tokio::test(start_paused = true)]
    async fn a_task_start_that_found_a_failed_task_awaiting_its_retry_polls_the_job_when_the_retry_is_due() {
        let mut schedule = schedule_over(&["retried_job"]);
        let task_start = TaskStart::NoTaskAvailable {
            retry_at: Some(moment_in(chrono::Duration::milliseconds(500))),
        };

        release_after_task_start(&mut schedule, "retried_job", JobProgress::from_task_start(&task_start));

        assert_delay_near(
            scheduled_delay(&schedule, "retried_job"),
            Duration::from_millis(500),
            "a job whose failed task is due again in half a second must be polled then",
        );
        assert_eq!(
            scheduled_job_field(&schedule, "retried_job", |job| job.poll_interval),
            BASE_POLL_INTERVAL,
            "a retry due later is not a task start without work, so it must not back off"
        );
    }

    /// Regression: a retry due beyond the backoff ceiling held the whole job until then, so a task
    /// of it released meanwhile - by a deadline, or by another instance - waited longer than a pass
    /// that found nothing would have let it. The ceiling the base interval derives is one second.
    /// Checked by scheduling the poll at the retry whatever the ceiling: it then lands thirty
    /// seconds away.
    #[tokio::test(start_paused = true)]
    async fn a_retry_due_beyond_the_backoff_ceiling_polls_the_job_at_the_ceiling() {
        let mut schedule = schedule_over(&["retried_job"]);
        let task_start = TaskStart::NoTaskAvailable {
            retry_at: Some(moment_in(chrono::Duration::seconds(30))),
        };

        release_after_task_start(&mut schedule, "retried_job", JobProgress::from_task_start(&task_start));

        assert_eq!(scheduled_delay(&schedule, "retried_job"), Duration::from_secs(1));
    }

    /// A moment already past is polled at once: the job is due, and the task start that starts
    /// its iteration is the one thing that can move it.
    #[tokio::test(start_paused = true)]
    async fn a_due_moment_in_the_past_schedules_an_immediate_poll() {
        let mut schedule = schedule_over(&["overdue_job"]);

        release_after_task_start(
            &mut schedule,
            "overdue_job",
            JobProgress::NextIterationDueAt(moment_in(-chrono::Duration::seconds(30))),
        );

        assert_eq!(claim_due_job_now(&mut schedule), Some(JobCode::new("overdue_job")));
    }

    /// Moment the job below is due at: inside the base interval, which is what the regression was
    /// about, and far enough from it that the slack of the comparison cannot bridge the two.
    const DUE_SOONER_THAN_BASE: Duration = Duration::from_millis(10);

    /// Regression: the delay used to be raised to the base interval, so a job whose iterations come
    /// round faster than the pool polls started every one of them late, by the difference.
    #[tokio::test(start_paused = true)]
    async fn a_moment_due_sooner_than_the_base_interval_is_not_delayed_to_it() {
        let mut schedule = schedule_over(&["soon_due_job"]);

        release_after_task_start(
            &mut schedule,
            "soon_due_job",
            JobProgress::NextIterationDueAt(moment_in(
                chrono::Duration::from_std(DUE_SOONER_THAN_BASE).expect("the test delay must convert"),
            )),
        );

        assert!(
            scheduled_delay(&schedule, "soon_due_job") <= DUE_SOONER_THAN_BASE,
            "a job due before the next base interval must be polled when it is due"
        );
    }

    /// A moment an executor sets through `JobHandle::set_next_start_at` passes no boundary check,
    /// unlike a configured interval, and the schedule adds it to an `Instant`, which panics on
    /// overflow. The furthest moment a `DateTime` can hold is therefore scheduled as the longest
    /// interval a worker accepts.
    #[tokio::test(start_paused = true)]
    async fn a_moment_beyond_the_longest_accepted_interval_is_scheduled_at_it() {
        let mut schedule = schedule_over(&["far_future_job"]);

        release_after_task_start(
            &mut schedule,
            "far_future_job",
            JobProgress::NextIterationDueAt(DateTime::<Utc>::MAX_UTC),
        );

        assert_eq!(
            scheduled_delay(&schedule, "far_future_job"),
            PollSchedulerConfig::LONGEST_ACCEPTED_POLL_INTERVAL
        );
    }

    /// The counterpart of the reset a task start with work makes: a task start that found
    /// the next iteration scheduled moved nothing either, but it learned exactly when the job is
    /// worth polling again, so the step it carried into that task start has answered for
    /// nothing and must not survive it.
    #[tokio::test(start_paused = true)]
    async fn a_task_start_that_found_the_next_iteration_scheduled_resets_the_backoff_step() {
        let mut schedule = schedule_over(&["rescheduled_job"]);

        release_after_task_start(&mut schedule, "rescheduled_job", JobProgress::Stalled);
        release_after_task_start(&mut schedule, "rescheduled_job", JobProgress::Stalled);
        release_after_task_start(
            &mut schedule,
            "rescheduled_job",
            JobProgress::NextIterationDueAt(moment_in(chrono::Duration::seconds(30))),
        );
        release_after_task_start(&mut schedule, "rescheduled_job", JobProgress::Stalled);

        assert_eq!(scheduled_delay(&schedule, "rescheduled_job"), BASE_POLL_INTERVAL * 2);
    }

    /// The first task start of a pool is one per job, all of them at once. Checked by
    /// enqueuing a new schedule's jobs one base interval ahead.
    #[tokio::test(start_paused = true)]
    async fn every_job_is_due_when_the_pool_starts() {
        let mut schedule = schedule_over(&["first_job", "second_job"]);

        let first_claim = take_due_claim(&mut schedule);
        let second_claim = take_due_claim(&mut schedule);

        let mut claimed_codes = vec![first_claim.job_code().as_str(), second_claim.job_code().as_str()];
        claimed_codes.sort_unstable();
        assert_eq!(claimed_codes, vec!["first_job", "second_job"]);
    }

    /// The claim is what keeps two workers of one pool from passing over one job at once. Checked
    /// by leaving a claimed job in the queue.
    #[tokio::test(start_paused = true)]
    async fn a_claimed_job_is_not_handed_to_a_second_worker() {
        let mut schedule = schedule_over(&["claimed_job"]);
        let _claim = take_due_claim(&mut schedule);

        tokio::time::advance(NO_CLAIM_WAIT).await;

        assert!(
            matches!(
                schedule.try_claim_due_job().expect("every queued job is on the schedule"),
                ClaimAttempt::WaitForPoolEvent
            ),
            "a claimed job must not be handed out again"
        );
    }

    /// A job not yet due names the moment the pool must look again. Checked by answering
    /// `WaitForPoolEvent` for it: the pool then sleeps until a report and the job waits for one.
    #[tokio::test(start_paused = true)]
    async fn a_job_due_later_is_waited_for_until_its_poll_moment() {
        let mut schedule = schedule_over(&["idle_job"]);
        let released_at = Instant::now();
        release_after_task_start(&mut schedule, "idle_job", JobProgress::NothingPickable);

        let attempt = schedule.try_claim_due_job().expect("every queued job is on the schedule");

        assert!(
            matches!(attempt, ClaimAttempt::WaitUntil(wake_at) if wake_at == released_at + BASE_POLL_INTERVAL),
            "got {attempt:?}, expected a wait of {BASE_POLL_INTERVAL:?}"
        );
    }

    /// An entry naming a job the schedule does not hold must not stay first in the queue: the pool
    /// would be refused on it forever, in a loop with no await, and every job behind it would never
    /// be handed out. Checked by reading the entry with `first` instead of taking it off the queue.
    #[tokio::test(start_paused = true)]
    async fn a_due_entry_of_an_unknown_job_leaves_the_queue_with_its_error() {
        let mut schedule = schedule_over(&["known_job"]);
        let unknown_job_due_at = schedule.due_queue.first().expect("the known job is queued").0;
        schedule.due_queue.insert((unknown_job_due_at, JobCode::new("an_unknown_job")));

        let refusal = schedule.try_claim_due_job();

        assert!(
            matches!(refusal, Err(ScheduleError::UnknownJob(ref job_code)) if job_code.as_str() == "an_unknown_job"),
            "got {refusal:?}"
        );
        assert_eq!(take_due_claim(&mut schedule).job_code().as_str(), "known_job");
    }

    /// Checked by ordering the queue by insertion alone.
    #[tokio::test(start_paused = true)]
    async fn the_earliest_due_job_is_claimed_first() {
        let mut schedule = schedule_over(&["late_job", "early_job"]);
        release_after_task_start(&mut schedule, "late_job", JobProgress::Stalled);
        release_after_task_start(&mut schedule, "early_job", JobProgress::NothingPickable);
        tokio::time::advance(BASE_POLL_INTERVAL * 4).await;

        assert_eq!(claim_due_job_now(&mut schedule), Some(JobCode::new("early_job")));
        let first_claim = take_due_claim(&mut schedule);
        assert_eq!(first_claim.job_code().as_str(), "late_job");
    }

    /// A job made due now by every task start - one whose tasks keep fanning out - must not
    /// keep the jobs already due waiting behind it. Checked by keying the queue on the poll moment
    /// alone with the job made due now placed at the moment its pool started. The waiting job is
    /// due strictly earlier and its code sorts later, so the order cannot come from a tie broken by
    /// code.
    #[tokio::test(start_paused = true)]
    async fn a_job_due_now_queues_behind_jobs_already_due() {
        let mut schedule = schedule_over(&["fanning_job", "waiting_job"]);
        let waiting_claim = claim_job(&mut schedule, "waiting_job");
        let fanning_claim = claim_job(&mut schedule, "fanning_job");
        release(&mut schedule, waiting_claim, JobProgress::NothingPickable);
        tokio::time::advance(BASE_POLL_INTERVAL * 2).await;

        release(&mut schedule, fanning_claim, JobProgress::TaskPickable);

        let claim = take_due_claim(&mut schedule);
        assert_eq!(claim.job_code().as_str(), "waiting_job");
    }

    /// Rule for a fan-out: the second task is started by the next free worker, not one poll
    /// interval later. Checked by scheduling `JobProgress::TaskPickable` at the base interval.
    #[tokio::test(start_paused = true)]
    async fn a_task_start_that_left_a_pickable_task_makes_its_job_due_at_once() {
        let mut schedule = schedule_over(&["fanning_job"]);
        release_after_task_start(&mut schedule, "fanning_job", JobProgress::Stalled);

        release_after_task_start(&mut schedule, "fanning_job", JobProgress::TaskPickable);

        assert_eq!(claim_due_job_now(&mut schedule), Some(JobCode::new("fanning_job")));
        assert_eq!(
            scheduled_job_field(&schedule, "fanning_job", |job| job.poll_interval),
            BASE_POLL_INTERVAL * 2,
            "the claim above stalled the job again from the base interval"
        );
    }

    /// Checked by leaving an exhausted job in the queue, and by letting an execution end reach it.
    #[tokio::test(start_paused = true)]
    async fn an_exhausted_job_is_never_claimed_again() {
        let mut schedule = schedule_over(&["exhausted_job"]);
        release_after_task_start(&mut schedule, "exhausted_job", JobProgress::IterationsEnded);

        reschedule_job(&mut schedule, "exhausted_job", JobProgress::TaskPickable);
        tokio::time::advance(NO_CLAIM_WAIT).await;

        assert!(
            matches!(
                schedule.try_claim_due_job().expect("every queued job is on the schedule"),
                ClaimAttempt::WaitForPoolEvent
            ),
            "an exhausted job must not be handed out"
        );
    }

    /// The task start holding the claim read the state before the execution wrote its result,
    /// so its outcome cannot answer for that result. Checked by dropping `overwritten_poll_due` on
    /// release.
    #[tokio::test(start_paused = true)]
    async fn an_execution_end_recorded_while_the_job_is_claimed_applies_when_the_claim_is_released() {
        let mut schedule = schedule_over(&["raced_job"]);
        release_after_task_start(&mut schedule, "raced_job", JobProgress::Stalled);
        let claim = claim_job(&mut schedule, "raced_job");

        reschedule_job(&mut schedule, "raced_job", JobProgress::TaskPickable);
        release(&mut schedule, claim, JobProgress::Stalled);

        assert_eq!(scheduled_delay(&schedule, "raced_job"), Duration::ZERO);
        assert_eq!(
            scheduled_job_field(&schedule, "raced_job", |job| job.poll_interval),
            BASE_POLL_INTERVAL,
            "the execution end resets the backoff step the stalled task start would have doubled"
        );
    }

    /// Two executions of one job can end while a third worker holds its claim; whichever order they
    /// land in, the earlier poll they ask for is the one applied. Checked twice: by keeping the last
    /// end recorded - the first order then lands one base interval away - and by keeping the first
    /// one - the second order then does.
    #[tokio::test(start_paused = true)]
    async fn several_execution_ends_recorded_under_one_claim_apply_the_earliest_at_release() {
        for execution_ends in [
            [JobProgress::TaskPickable, JobProgress::NothingPickable],
            [JobProgress::NothingPickable, JobProgress::TaskPickable],
        ] {
            let mut schedule = schedule_over(&["raced_job"]);
            let claim = claim_job(&mut schedule, "raced_job");

            for execution_end in execution_ends {
                reschedule_job(&mut schedule, "raced_job", execution_end);
            }
            release(&mut schedule, claim, JobProgress::Stalled);

            assert_eq!(
                scheduled_delay(&schedule, "raced_job"),
                Duration::ZERO,
                "execution ends recorded as {execution_ends:?}"
            );
            assert_eq!(
                scheduled_job_field(&schedule, "raced_job", |job| job.poll_interval),
                BASE_POLL_INTERVAL,
                "execution ends recorded as {execution_ends:?}"
            );
        }
    }

    /// The poll already scheduled answers for what the task start of that task saw,
    /// so an execution end leaving nothing to take must not push it later. Checked by replacing the
    /// poll moment with the one the execution end asks for.
    #[tokio::test(start_paused = true)]
    async fn an_execution_end_only_brings_the_poll_of_an_unclaimed_job_closer() {
        let mut schedule = schedule_over(&["fanning_job"]);
        release_after_task_start(&mut schedule, "fanning_job", JobProgress::TaskPickable);

        reschedule_job(&mut schedule, "fanning_job", JobProgress::NothingPickable);

        assert_eq!(scheduled_delay(&schedule, "fanning_job"), Duration::ZERO);
    }

    /// An execution that ended moved the job, so the step a stalled poll carried has answered for
    /// nothing. Checked by leaving `poll_interval` alone on an execution end.
    #[tokio::test(start_paused = true)]
    async fn an_execution_end_resets_the_backoff_step_of_its_job() {
        let mut schedule = schedule_over(&["resumed_job"]);
        release_after_task_start(&mut schedule, "resumed_job", JobProgress::Stalled);
        release_after_task_start(&mut schedule, "resumed_job", JobProgress::Stalled);

        reschedule_job(&mut schedule, "resumed_job", JobProgress::NothingPickable);
        release_after_task_start(&mut schedule, "resumed_job", JobProgress::Stalled);

        assert_eq!(scheduled_delay(&schedule, "resumed_job"), BASE_POLL_INTERVAL * 2);
    }

    /// A dependent unblocked by the result, or a task its deadline released, is started by the next
    /// free worker. Checked by scheduling `JobProgress::TaskPickable` at the base interval.
    #[tokio::test(start_paused = true)]
    async fn an_execution_end_leaving_a_pickable_task_makes_its_job_due_at_once() {
        let mut schedule = schedule_over(&["dependent_job"]);
        release_after_task_start(&mut schedule, "dependent_job", JobProgress::NothingPickable);

        reschedule_job(&mut schedule, "dependent_job", JobProgress::TaskPickable);

        assert_eq!(claim_due_job_now(&mut schedule), Some(JobCode::new("dependent_job")));
    }

    /// Jitter of the draws below: half the base interval, so a delay drawn twice leaves the bound
    /// in half of the draws.
    const DRAWN_JITTER: Duration = Duration::from_millis(50);

    /// The jitter spreads pools polling one job, which only a poll later than now needs: a job
    /// made due now has work a worker can take at once. Checked by adding the jitter to a poll due
    /// now.
    #[tokio::test(start_paused = true)]
    async fn a_poll_made_due_by_a_pickable_task_carries_no_jitter() {
        let mut schedule = schedule_over_with_config(&["jittered_job"], config_with_jitter(DRAWN_JITTER));

        release_after_task_start(&mut schedule, "jittered_job", JobProgress::TaskPickable);

        assert_eq!(scheduled_delay(&schedule, "jittered_job"), Duration::ZERO);
    }

    /// A quarter of [`DRAWN_JITTER`]: [`JITTER_DRAWS`] real draws all but never fall closer together.
    /// A moment named by the system clock drifts between draws by microseconds, so only the jitter
    /// spreads them this far.
    const DRAWN_JITTER_SPREAD: Duration = Duration::from_micros(12_500);

    /// The delays `schedule_poll` leaves on [`JITTER_DRAWS`] fresh schedules polled with
    /// [`DRAWN_JITTER`].
    fn draw_delays(schedule_poll: impl Fn(&mut PollScheduler)) -> Vec<Duration> {
        (0..JITTER_DRAWS)
            .map(|_| {
                let mut schedule = schedule_over_with_config(&["jittered_job"], config_with_jitter(DRAWN_JITTER));
                schedule_poll(&mut schedule);
                scheduled_delay(&schedule, "jittered_job")
            })
            .collect()
    }

    /// Asserts every delay holds one draw of [`DRAWN_JITTER`] on top of `wait`, shortened by at most
    /// `slack`, and that the draws really spread.
    fn assert_one_draw_on_top_of(delays: &[Duration], wait: Duration, slack: Duration, what: &str) {
        for delay in delays {
            assert!(
                *delay + slack >= wait && *delay < wait + DRAWN_JITTER,
                "{what}: a delay of {delay:?} is outside one draw of {DRAWN_JITTER:?} on top of {wait:?}"
            );
        }
        let spread = delays
            .iter()
            .max()
            .copied()
            .unwrap_or_default()
            .saturating_sub(delays.iter().min().copied().unwrap_or_default());
        assert!(
            spread >= DRAWN_JITTER_SPREAD,
            "{what}: the delays spread over {spread:?} only, so no jitter was drawn"
        );
    }

    /// Every poll later than now is spread by the jitter, whichever progress asks for it: a base
    /// interval, a backoff, a retry, a next iteration named by a task start and one named by a
    /// write. The jitter exists for the next iteration above all - without it every pool polls a
    /// job whose iteration falls due at the same instant. Checked by scheduling each of those
    /// moments as `now + wait`, past `PollPolicy::calculate_poll_at`, and by drawing the jitter
    /// twice.
    #[tokio::test(start_paused = true)]
    async fn every_poll_scheduled_later_than_now_carries_one_draw_of_the_jitter() {
        let base_interval_delays = draw_delays(|schedule| {
            release_after_task_start(schedule, "jittered_job", JobProgress::NothingPickable);
        });
        assert_one_draw_on_top_of(
            &base_interval_delays,
            BASE_POLL_INTERVAL,
            Duration::ZERO,
            "a base interval",
        );

        let backed_off_delays = draw_delays(|schedule| {
            release_after_task_start(schedule, "jittered_job", JobProgress::Stalled);
        });
        assert_one_draw_on_top_of(&backed_off_delays, BASE_POLL_INTERVAL * 2, Duration::ZERO, "a backoff");

        let retry_delays = draw_delays(|schedule| {
            release_after_task_start(
                schedule,
                "jittered_job",
                JobProgress::RetryDueAt(moment_in(chrono::Duration::milliseconds(500))),
            );
        });
        assert_one_draw_on_top_of(&retry_delays, Duration::from_millis(500), SCHEDULING_SLACK, "a retry");

        let next_iteration_delays = draw_delays(|schedule| {
            release_after_task_start(
                schedule,
                "jittered_job",
                JobProgress::NextIterationDueAt(moment_in(chrono::Duration::seconds(30))),
            );
        });
        assert_one_draw_on_top_of(
            &next_iteration_delays,
            Duration::from_secs(30),
            SCHEDULING_SLACK,
            "a next iteration named by a task start",
        );

        let written_next_iteration_delays = draw_delays(|schedule| {
            release_after_task_start(schedule, "jittered_job", JobProgress::NothingPickable);
            reschedule_job(
                schedule,
                "jittered_job",
                JobProgress::NextIterationDueAt(moment_in(chrono::Duration::seconds(30))),
            );
        });
        assert_one_draw_on_top_of(
            &written_next_iteration_delays,
            Duration::from_secs(30),
            SCHEDULING_SLACK,
            "a next iteration named by a write",
        );
    }

    /// A moment already past is due now, and a job due now has work a worker can take at once, so
    /// the jitter would only delay it. Checked by drawing the jitter on a zero wait too.
    #[tokio::test(start_paused = true)]
    async fn a_poll_whose_moment_has_come_carries_no_jitter() {
        let mut schedule = schedule_over_with_config(&["overdue_job", "retried_job"], config_with_jitter(DRAWN_JITTER));

        release_after_task_start(
            &mut schedule,
            "overdue_job",
            JobProgress::NextIterationDueAt(moment_in(-chrono::Duration::seconds(30))),
        );
        release_after_task_start(
            &mut schedule,
            "retried_job",
            JobProgress::RetryDueAt(moment_in(-chrono::Duration::seconds(1))),
        );

        assert_eq!(scheduled_delay(&schedule, "overdue_job"), Duration::ZERO);
        assert_eq!(scheduled_delay(&schedule, "retried_job"), Duration::ZERO);
    }

    /// The execution that wrote the end of the last iteration leaves nothing to poll. Checked
    /// by recording `JobProgress::IterationsEnded` like `NothingPickable`.
    #[tokio::test(start_paused = true)]
    async fn an_execution_that_closed_the_last_iteration_exhausts_its_job() {
        let mut schedule = schedule_over(&["finished_job"]);
        release_after_task_start(&mut schedule, "finished_job", JobProgress::NothingPickable);

        reschedule_job(&mut schedule, "finished_job", JobProgress::IterationsEnded);
        tokio::time::advance(NO_CLAIM_WAIT).await;

        assert!(
            matches!(
                schedule.try_claim_due_job().expect("every queued job is on the schedule"),
                ClaimAttempt::WaitForPoolEvent
            ),
            "an exhausted job must not be handed out"
        );
    }

    /// The same end recorded while a task start holds the claim: an iteration number only
    /// grows, so the task start that read an earlier state cannot bring the job back. Checked
    /// by dropping `overwritten_poll_due` on release.
    #[tokio::test(start_paused = true)]
    async fn an_execution_that_closed_the_last_iteration_exhausts_its_job_while_it_is_claimed() {
        let mut schedule = schedule_over(&["finished_job"]);
        let claim = claim_job(&mut schedule, "finished_job");

        reschedule_job(&mut schedule, "finished_job", JobProgress::IterationsEnded);
        release(&mut schedule, claim, JobProgress::TaskPickable);

        assert!(scheduled_job_field(&schedule, "finished_job", |job| job.slot == JobSlot::Exhausted));
        tokio::time::advance(NO_CLAIM_WAIT).await;
        assert!(
            matches!(
                schedule.try_claim_due_job().expect("every queued job is on the schedule"),
                ClaimAttempt::WaitForPoolEvent
            ),
            "an exhausted job must not be handed out"
        );
    }

    /// The state the execution wrote names the next iteration, and the poll scheduled by the
    /// task start of that task did not see it - so the poll is replaced, even by a
    /// later one. Checked by only bringing the poll closer.
    #[tokio::test(start_paused = true)]
    async fn an_execution_that_closed_the_iteration_moves_the_poll_to_the_next_one() {
        let mut schedule = schedule_over(&["interval_job"]);
        release_after_task_start(&mut schedule, "interval_job", JobProgress::NothingPickable);

        reschedule_job(
            &mut schedule,
            "interval_job",
            JobProgress::NextIterationDueAt(moment_in(chrono::Duration::seconds(30))),
        );

        assert_delay_near(
            scheduled_delay(&schedule, "interval_job"),
            Duration::from_secs(30),
            "the job must not be polled before its next iteration",
        );
    }
}
