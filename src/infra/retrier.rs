use std::future::Future;

use rand::Rng;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Retry policy applied to a single fallible operation: how many attempts to make, how long to
/// back off before each retry, and how much random jitter to add to spread out retries across
/// workers.
#[derive(Clone)]
pub struct RetrierConfig {
    /// Maximum number of retry attempts.
    pub max_attempts: usize,
    /// Random jitter added to each delay.
    pub rand_delay: Duration,
    /// Backoff delays by attempt index.
    pub delays: Vec<Duration>,
}

impl Default for RetrierConfig {
    fn default() -> Self {
        Self {
            max_attempts: 20,
            rand_delay: Duration::from_millis(50),
            delays: vec![
                Duration::from_millis(50),
                Duration::from_millis(50),
                Duration::from_millis(50),
                Duration::from_millis(100),
                Duration::from_millis(300),
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_secs(5),
                Duration::from_secs(10),
                Duration::from_mins(1),
            ],
        }
    }
}

pub(crate) struct Retrier {
    config: RetrierConfig,
}

/// Outcome of one attempt of a retried operation.
pub(crate) enum RetryStep<T, E> {
    /// The operation finished; [`Retrier::retry`] returns this value.
    Done(T),
    /// The attempt failed for a reason worth retrying. The cause travels with the decision so that
    /// spending the whole attempt budget can report *why*, instead of a bare "attempts spent".
    Retry(E),
}

pub(crate) trait RetryError: Sized {
    /// Error returned when the operation is cancelled through its `CancellationToken`.
    fn from_cancellation() -> Self;

    /// Error returned when the attempt budget is spent, carrying the cause of the last attempt.
    fn from_spent_attempts(last_cause: Self) -> Self;
}

impl Retrier {
    pub const fn new(config: RetrierConfig) -> Self {
        Self { config }
    }

    pub async fn retry<F, Fut, T, E>(&self, mut handler: F, cancel_token: &CancellationToken) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<RetryStep<T, E>, E>>,
        E: RetryError,
    {
        let mut attempt = 0;
        loop {
            let step = tokio::select! {
                () = cancel_token.cancelled() => return Err(E::from_cancellation()),
                step = handler() => step,
            }?;

            let last_cause = match step {
                RetryStep::Done(value) => return Ok(value),
                RetryStep::Retry(cause) => cause,
            };

            attempt += 1;

            if attempt >= self.config.max_attempts {
                return Err(E::from_spent_attempts(last_cause));
            }

            let delay = self.calculate_delay(attempt);
            debug!("retry attempt {} after {:?}", attempt, delay);
            tokio::select! {
                () = cancel_token.cancelled() => return Err(E::from_cancellation()),
                () = sleep(delay) => {}
            }
        }
    }

    fn calculate_delay(&self, attempt: usize) -> Duration {
        let base_delay = if attempt < self.config.delays.len() {
            self.config.delays[attempt]
        } else {
            *self.config.delays.last().unwrap_or(&Duration::from_millis(200))
        };

        // Add randomization to prevent thundering herd
        let max_jitter = u64::try_from(self.config.rand_delay.as_millis()).unwrap_or(u64::MAX);
        let jitter_ms = if max_jitter == 0 {
            0
        } else {
            rand::rng().random_range(0..max_jitter)
        };
        base_delay + Duration::from_millis(jitter_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        Cancelled,
        Transient(u32),
        AttemptsSpent(u32),
    }

    impl RetryError for TestError {
        fn from_cancellation() -> Self {
            Self::Cancelled
        }

        fn from_spent_attempts(last_cause: Self) -> Self {
            match last_cause {
                Self::Transient(marker) => Self::AttemptsSpent(marker),
                other => other,
            }
        }
    }

    fn build_fast_retrier(max_attempts: usize) -> Retrier {
        Retrier::new(RetrierConfig {
            max_attempts,
            rand_delay: Duration::from_millis(1),
            delays: vec![Duration::from_millis(1)],
        })
    }

    #[tokio::test]
    async fn test_spent_attempts_report_the_cause_of_the_last_attempt() {
        let retrier = build_fast_retrier(3);
        let mut attempt_marker = 0;

        let error = retrier
            .retry(
                || {
                    attempt_marker += 1;
                    let marker = attempt_marker;
                    async move { Ok(RetryStep::<(), TestError>::Retry(TestError::Transient(marker))) }
                },
                &CancellationToken::new(),
            )
            .await
            .expect_err("an operation that never succeeds must not report success");

        assert_eq!(error, TestError::AttemptsSpent(3));
    }

    #[tokio::test]
    async fn test_cancellation_during_an_attempt_stops_the_retry_loop() {
        let retrier = build_fast_retrier(10);
        let cancel_token = CancellationToken::new();
        let mut attempts = 0;

        let error = retrier
            .retry(
                || {
                    attempts += 1;
                    cancel_token.cancel();
                    async { Ok(RetryStep::<(), TestError>::Retry(TestError::Transient(1))) }
                },
                &cancel_token,
            )
            .await
            .expect_err("a cancelled retry must not report success");

        assert_eq!(error, TestError::Cancelled);
        assert_eq!(attempts, 1, "cancellation must stop the loop before the next attempt");
    }
}
