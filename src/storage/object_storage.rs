//! [`Storage`] expressed as objects, in one copy for every provider.
//!
//! This is the upper half of the split `AGENTS.md` states as "a backend maps state, it does not
//! decide it". Everything decided about a job lives here: which key carries an iteration, what a
//! save is conditioned on, where the cleanup listing starts and which of its keys are outdated, how
//! state is encoded, which calls retry. A provider appears only as [`Backend`], so two of them
//! differ in the requests they send and never in the rules those requests carry out.
//!
//! The split is what makes a third provider a set of requests rather than a third copy of the
//! rules.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::storage::backend::{Backend, ListingRequest, ListingStartBound, PutCondition};
use crate::storage::paths::JobPaths;
use crate::storage::state::StoredJob;
use crate::storage::state_codec::JobStateCodec;
use crate::{Job, JobCode, JobDefinitionRegistry, JobMeta, Retrier, RetryStep, Storage, StorageError, StorageResult};

/// `Backend` over the objects `B` keeps, holding everything a provider is not allowed to decide.
pub(crate) struct ObjectStorage<B: Backend> {
    store: B,
    state_object_keys: JobPaths,
    codec: Arc<dyn JobStateCodec>,
    registry: Arc<dyn JobDefinitionRegistry>,
    retrier: Retrier,
}

impl<B: Backend> ObjectStorage<B> {
    /// Job state kept in `store` under `state_object_keys`, encoded by `codec`.
    ///
    /// Built by a backend once it has made sure its container is reachable, which is why nothing
    /// here reaches the store.
    pub(crate) const fn new(
        store: B,
        state_object_keys: JobPaths,
        codec: Arc<dyn JobStateCodec>,
        registry: Arc<dyn JobDefinitionRegistry>,
        retrier: Retrier,
    ) -> Self {
        Self {
            store,
            state_object_keys,
            codec,
            registry,
            retrier,
        }
    }

    fn serialize_job(&self, job: &Job) -> StorageResult<Vec<u8>> {
        self.codec.serialize(&StoredJob::from_job(job))
    }

    fn deserialize_job(&self, state_bytes: &[u8], version: &str) -> StorageResult<Job> {
        let stored_job = self.codec.deserialize(state_bytes)?;
        let job_def = self
            .registry
            .get_job(&JobCode::new(stored_job.code()))
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(stored_job.into_job(
            job_def.max_iterations(),
            job_def.iteration_interval(),
            job_def.task_limits(),
            version,
        ))
    }

    /// Key the listing of a job's outdated iterations starts at, as this provider's boundary
    /// understands it.
    ///
    /// Names carry the inverted iteration number, so the ascending key order is the descending
    /// iteration order: every iteration at or below the boundary sits after the key of the oldest
    /// one that must be kept. What keeps a kept iteration undeletable is the boundary check over
    /// the listed keys and not where the listing starts.
    fn build_listing_start_key(&self, job_code: &JobCode, retention_boundary: u64) -> StorageResult<String> {
        match self.store.listing_start_bound() {
            ListingStartBound::Inclusive => {
                Ok(self.state_object_keys.build_job_iteration_path(job_code, retention_boundary))
            }
            ListingStartBound::Exclusive => {
                let oldest_kept_iter_num = retention_boundary.checked_add(1).ok_or_else(|| {
                    StorageError::Other(format!("Retention boundary {retention_boundary} has no next iteration"))
                })?;
                Ok(self.state_object_keys.build_job_iteration_path(job_code, oldest_kept_iter_num))
            }
        }
    }
}

#[async_trait::async_trait]
impl<S: Backend> Storage for ObjectStorage<S> {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        // TODO(low): perhaps should try to get current job iteration first and read new file on
        // miss. But if job has few tasks, we'll miss often and make extra requests

        let job_code_for_retry = job_code.clone();
        self.retrier
            .retry(
                move || {
                    let job_code = job_code_for_retry.clone();
                    async move {
                        // The listing is retried along with the read that follows it: a store that
                        // refused one of them is what this loop exists for, and `?` would end the
                        // whole read on a refusal the next attempt would have cleared.
                        let job_meta = match self.find_job_meta(&job_code, cancel_token).await {
                            Ok(job_meta) => job_meta,
                            Err(e) => return e.into_retry_step(),
                        };
                        match self.get_job_by_meta(&job_meta, cancel_token).await {
                            Ok(job) => Ok(RetryStep::Done(job)),
                            Err(e) if e.is_retryable() || e.is_conflict() => Ok(RetryStep::Retry(e)),
                            Err(e) => Err(e),
                        }
                    }
                },
                cancel_token,
            )
            .await
    }

    #[tracing::instrument(skip(self, cancel_token), fields(job_version = %job_meta.version))]
    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let key = self
            .state_object_keys
            .build_job_iteration_path(&job_meta.code, job_meta.iter_num);

        let state_bytes = self.store.get_object(&key, &job_meta.version, cancel_token).await?;

        self.deserialize_job(&state_bytes, job_meta.version.as_str())
    }

    #[tracing::instrument(skip(self, cancel_token), fields(job_code = %job_code))]
    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let prefix = self.state_object_keys.build_job_prefix(job_code);

        let listed_page = self
            .store
            .list_objects(
                ListingRequest {
                    prefix: &prefix,
                    start_key: None,
                    // Inverted iteration numbers put the newest state first, so one object is the
                    // whole answer.
                    page_size: 1,
                    cursor: None,
                },
                cancel_token,
            )
            .await?;

        let Some(newest_object) = listed_page.objects.into_iter().next() else {
            return Err(StorageError::NotFound(
                "No job state objects found for requested job".to_string(),
            ));
        };
        // `NotFound` is what has a worker create the job from iteration 1, so a provider that named
        // the object and left its version out must not reach the caller as one: the job is stored,
        // and recreating it would put iteration 1 beside a live history.
        let Some(version) = newest_object.version else {
            return Err(StorageError::Backend(format!(
                "Listing named job state object {} without a version",
                newest_object.key
            )));
        };

        Ok(JobMeta {
            code: job_code.clone(),
            iter_num: self.state_object_keys.parse_iter_num(&newest_object.key)?,
            version,
        })
    }

    #[tracing::instrument(skip(self, cancel_token), fields(job_version = %job_meta.version))]
    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let key = self
            .state_object_keys
            .build_job_iteration_path(&job_meta.code, job_meta.iter_num);

        let Some((version, state_bytes)) = self.store.get_changed_object(&key, &job_meta.version, cancel_token).await?
        else {
            return Ok(None);
        };

        Ok(Some(self.deserialize_job(&state_bytes, &version)?))
    }

    #[tracing::instrument(skip(self, cancel_token, job), fields(job_version = %job.version()))]
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let is_new_iter = job.version().is_empty();
        let version = job.version().to_string();
        let serialized_state = Arc::new(self.serialize_job(job)?);
        let key = self.state_object_keys.build_job_iteration_path(job.code(), job.iter_num());

        if is_new_iter {
            debug!(
                "Saving next job iteration (id: {}, code: {}, iter: {}, status: {:?})",
                job.id().to_string(),
                job.code().clone(),
                job.iter_num(),
                job.status().clone()
            );
        } else {
            debug!(
                "Saving current job iteration (id: {}, code: {}, iter: {}, status: {:?})",
                job.id().to_string(),
                job.code().clone(),
                job.iter_num(),
                job.status().clone()
            );
        }

        let stored_version = self
            .retrier
            .retry(
                move || {
                    let key = key.clone();
                    let serialized_state = Arc::clone(&serialized_state);
                    let version = version.clone();
                    async move {
                        let condition = if is_new_iter {
                            PutCondition::CreateOnly
                        } else {
                            PutCondition::MatchVersion(&version)
                        };

                        match self
                            .store
                            .put_object(&key, serialized_state.as_ref().clone(), condition, cancel_token)
                            .await
                        {
                            Ok(stored_version) => Ok(RetryStep::Done(stored_version)),
                            Err(e) if e.is_retryable() => Ok(RetryStep::Retry(e)),
                            Err(e) => Err(e),
                        }
                    }
                },
                cancel_token,
            )
            .await?;
        job.update_version(stored_version);

        debug!(
            "Job iteration saved (id: {}, code: {}, iter: {}, status: {:?})",
            job.id().to_string(),
            job.code().clone(),
            job.iter_num(),
            job.status().clone()
        );

        Ok(())
    }

    #[tracing::instrument(skip(self, cancel_token), fields(job_code = %job_code))]
    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }

        let prefix = self.state_object_keys.build_job_prefix(job_code);
        let start_key = self.build_listing_start_key(job_code, retention_boundary)?;

        let mut outdated_iter_nums = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            if cancel_token.is_cancelled() {
                return Err(StorageError::Cancelled);
            }

            let listed_page = self
                .store
                .list_objects(
                    ListingRequest {
                        prefix: &prefix,
                        start_key: Some(start_key.as_str()),
                        page_size: self.store.list_page_size(),
                        cursor,
                    },
                    cancel_token,
                )
                .await?;

            for object in listed_page.objects {
                match self.state_object_keys.parse_iter_num(&object.key) {
                    Ok(iter_num) if iter_num <= retention_boundary => outdated_iter_nums.push(iter_num),
                    // Reached on a listing wider than the boundary asked for - see
                    // `ListingStartBound`. Dropping the key here is what keeps the newest state
                    // object undeletable even then.
                    Ok(iter_num) => debug!("Skipping iteration {iter_num} above retention boundary of job {job_code}"),
                    Err(e) => debug!(
                        "Skipping object {} that is not a state of the current codec: {e}",
                        object.key
                    ),
                }
            }

            cursor = listed_page.next_cursor;
            if cursor.is_none() {
                return Ok(outdated_iter_nums);
            }
        }
    }

    #[tracing::instrument(skip(self, cancel_token), fields(job_code = %job_code, iterations = iter_nums.len()))]
    async fn delete_job_iterations(
        &self,
        job_code: &JobCode,
        iter_nums: &[u64],
        cancel_token: &CancellationToken,
    ) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        if iter_nums.is_empty() {
            return Ok(());
        }

        let keys: Vec<String> = iter_nums
            .iter()
            .map(|iter_num| self.state_object_keys.build_job_iteration_path(job_code, *iter_num))
            .collect();

        self.store.delete_objects(&keys, cancel_token).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;

    use super::*;
    use crate::RetrierConfig;
    use crate::storage::backend::{ListedObject, ListedPage};
    use crate::storage::paths::DEFAULT_STATE_PREFIX;
    use crate::storage::state_codec::JobStateCodecKind;
    use crate::tests::common::UnusedJobRegistry;

    /// Key of iteration 1 under the JSON codec, stated rather than built so the case does not read
    /// the layout back off the builder under test.
    const NEWEST_STATE_KEY: &str = "jobs/job/state-18446744073709551614.json";

    /// The same iteration written by a pool of another codec, which is what a container that was
    /// served by one codec and then by another holds.
    const NEWEST_STATE_KEY_OF_ANOTHER_CODEC: &str = "jobs/job/state-18446744073709551614.cbor";

    /// Retention boundary the listing cases ask cleanup about: the newest iteration it may delete.
    const TESTED_RETENTION_BOUNDARY: u64 = 2;

    /// Key of the iteration equal to [`TESTED_RETENTION_BOUNDARY`], stated rather than built for the
    /// reason [`NEWEST_STATE_KEY`] carries.
    const BOUNDARY_ITERATION_KEY: &str = "jobs/job/state-18446744073709551613.json";

    /// Key of the oldest iteration that must be kept - the one after the boundary.
    const OLDEST_KEPT_ITERATION_KEY: &str = "jobs/job/state-18446744073709551612.json";

    fn refuse_unused_call(operation: &str) -> StorageError {
        StorageError::Other(format!("this case must not reach {operation}"))
    }

    /// Backend double whose listing answers with the objects a case names, which is what lets a case
    /// state what the provider reported about the newest state object - a version included or left
    /// out - without a store that reports it.
    struct ListingBackend {
        listed_objects: Vec<(String, Option<String>)>,
        /// Listings still to be refused with a status a store clears on its own, before the objects
        /// are answered.
        listing_refusals: AtomicUsize,
        /// Bound this double declares, which is the whole of what the listing start key is
        /// calculated from.
        listing_start_bound: ListingStartBound,
        /// Boundary key of every listing this double was handed, which is where the calculation is
        /// observable at all - nothing else carries it out of `list_job_outdated_iterations`.
        requested_start_keys: Mutex<Vec<Option<String>>>,
    }

    impl ListingBackend {
        fn requested_start_keys(&self) -> Vec<Option<String>> {
            self.requested_start_keys.lock().clone()
        }
    }

    #[async_trait::async_trait]
    impl Backend for ListingBackend {
        // Rebuilt variant by variant rather than cloned: the bound is a production type, and giving
        // it a derive for a double's sake would widen it for a reason no caller has.
        fn listing_start_bound(&self) -> ListingStartBound {
            match self.listing_start_bound {
                ListingStartBound::Inclusive => ListingStartBound::Inclusive,
                ListingStartBound::Exclusive => ListingStartBound::Exclusive,
            }
        }

        fn list_page_size(&self) -> i32 {
            1
        }

        async fn put_object(
            &self,
            _key: &str,
            _body: Vec<u8>,
            _condition: PutCondition<'_>,
            _cancel_token: &CancellationToken,
        ) -> StorageResult<String> {
            Err(refuse_unused_call("put_object"))
        }

        /// The iteration the listing named is not stored here: a case reaching this far is about
        /// what happened before the read, and `NotFound` is what the store answers when cleanup
        /// removed the object between the listing and the read.
        async fn get_object(
            &self,
            _key: &str,
            _expected_version: &str,
            _cancel_token: &CancellationToken,
        ) -> StorageResult<Vec<u8>> {
            Err(StorageError::NotFound("the listed iteration is not stored".to_string()))
        }

        async fn get_changed_object(
            &self,
            _key: &str,
            _known_version: &str,
            _cancel_token: &CancellationToken,
        ) -> StorageResult<Option<(String, Vec<u8>)>> {
            Err(refuse_unused_call("get_changed_object"))
        }

        async fn list_objects(
            &self,
            request: ListingRequest<'_>,
            _cancel_token: &CancellationToken,
        ) -> StorageResult<ListedPage> {
            self.requested_start_keys.lock().push(request.start_key.map(str::to_string));

            if self
                .listing_refusals
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| left.checked_sub(1))
                .is_ok()
            {
                return Err(StorageError::ServiceUnavailable);
            }

            Ok(ListedPage {
                objects: self
                    .listed_objects
                    .iter()
                    .map(|(key, version)| ListedObject {
                        key: key.clone(),
                        version: version.clone(),
                    })
                    .collect(),
                next_cursor: None,
            })
        }

        async fn delete_objects(&self, _keys: &[String], _cancel_token: &CancellationToken) -> StorageResult<()> {
            Err(refuse_unused_call("delete_objects"))
        }
    }

    fn build_storage_over_listing(listed_objects: Vec<(String, Option<String>)>) -> ObjectStorage<ListingBackend> {
        build_storage_over_store(listed_objects, 0, ListingStartBound::Exclusive)
    }

    /// The same storage over a store that refuses `listing_refusals` listings before answering.
    fn build_storage_over_listing_refused(
        listed_objects: Vec<(String, Option<String>)>,
        listing_refusals: usize,
    ) -> ObjectStorage<ListingBackend> {
        build_storage_over_store(listed_objects, listing_refusals, ListingStartBound::Exclusive)
    }

    /// The same storage over a store that declares `listing_start_bound` and holds no state object,
    /// which is what a case about the boundary key rather than about the listed objects asks for.
    fn build_storage_over_bound(listing_start_bound: ListingStartBound) -> ObjectStorage<ListingBackend> {
        build_storage_over_store(Vec::new(), 0, listing_start_bound)
    }

    fn build_storage_over_store(
        listed_objects: Vec<(String, Option<String>)>,
        listing_refusals: usize,
        listing_start_bound: ListingStartBound,
    ) -> ObjectStorage<ListingBackend> {
        let codec = JobStateCodecKind::Json.build();
        let state_object_keys = JobPaths::new(DEFAULT_STATE_PREFIX.to_string(), codec.as_ref());

        ObjectStorage::new(
            ListingBackend {
                listed_objects,
                listing_refusals: AtomicUsize::new(listing_refusals),
                listing_start_bound,
                requested_start_keys: Mutex::new(Vec::new()),
            },
            state_object_keys,
            codec,
            Arc::new(UnusedJobRegistry),
            Retrier::new(RetrierConfig::default()),
        )
    }

    #[tokio::test]
    async fn an_empty_listing_reads_as_a_job_that_is_not_stored() {
        let storage = build_storage_over_listing(Vec::new());

        let error = storage
            .find_job_meta(&JobCode::new("job"), &CancellationToken::new())
            .await
            .expect_err("a job whose prefix holds no state object must not be found");

        assert!(matches!(error, StorageError::NotFound(_)), "got: {error:?}");
    }

    /// A listing that named the object and left its version out is the provider refusing, not the
    /// job being absent: `NotFound` is what has a worker create the job from iteration 1, and doing
    /// that here would put iteration 1 beside the history the listing just named.
    #[tokio::test]
    async fn a_state_object_listed_without_a_version_is_refused_rather_than_read_as_an_absent_job() {
        let storage = build_storage_over_listing(vec![(NEWEST_STATE_KEY.to_string(), None)]);

        let error = storage
            .find_job_meta(&JobCode::new("job"), &CancellationToken::new())
            .await
            .expect_err("a state object whose version the provider left out must not be found");

        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
    }

    /// The newest object under a job's prefix belongs to a codec this backend does not read, which
    /// is what a container served by one codec and then by another holds. The read is refused, and
    /// the refusal is neither of the two answers that lose state silently.
    ///
    /// `NotFound` is the first of them: it has a worker create the job from iteration 1 beside a
    /// live history, which is the outcome the version check above was written to prevent. Skipping
    /// to the newest object of this backend's own codec is the second and the worse: the worker
    /// would discover an older iteration and then create the newer one under its own extension - a
    /// create-only write the store accepts, because no object of this codec holds that key - leaving
    /// two live histories under one prefix, each invisible to the other.
    ///
    /// A refusal a repetition could clear is the third thing this must not be: no attempt of the
    /// `Retrier` changes what codec the stored object carries.
    ///
    /// Checked by breaking it: answering `NotFound` for a key that does not parse, or skipping such
    /// a key the way `list_job_outdated_iterations` does, fails one assertion each.
    #[tokio::test]
    async fn a_newest_state_object_of_another_codec_is_refused_rather_than_read_as_an_absent_job() {
        let storage = build_storage_over_listing(vec![(
            NEWEST_STATE_KEY_OF_ANOTHER_CODEC.to_string(),
            Some("etag-1".to_string()),
        )]);

        let error = storage
            .find_job_meta(&JobCode::new("job"), &CancellationToken::new())
            .await
            .expect_err("a state object of a codec this backend does not read must not name an iteration");

        assert!(
            !matches!(error, StorageError::NotFound(_)),
            "a stored job must not read as an absent one, got: {error:?}"
        );
        assert!(
            !error.is_retryable(),
            "no repetition changes the codec of the stored object, got: {error:?}"
        );
    }

    /// A listing the store refused with a status it clears on its own is repeated by the same loop
    /// that repeats the read after it. Ending the read there instead would lose the whole pass to
    /// one `503`, and no SDK repeats the request any more - every backend leaves that to `Retrier`.
    ///
    /// Checked by breaking it: letting the refusal out of the retry closure on `?`, as this read did
    /// before, ends the call as `service unavailable` and the case fails on the variant.
    #[tokio::test]
    async fn a_refused_listing_is_repeated_rather_than_ending_the_read_that_began_with_it() {
        let storage =
            build_storage_over_listing_refused(vec![(NEWEST_STATE_KEY.to_string(), Some("etag-1".to_string()))], 1);

        let Err(error) = storage.get_job(&JobCode::new("job"), &CancellationToken::new()).await else {
            panic!("the double holds no state object to read")
        };

        assert!(
            matches!(error, StorageError::NotFound(_)),
            "the read must have reached the object the repeated listing named, got: {error:?}"
        );
    }

    /// The listing of a job's tail starts where the bound of its provider requires: a store that
    /// leaves the boundary key out of its answer is given the key of the oldest iteration that must
    /// be kept, and one that answers with the boundary key is given the boundary itself.
    ///
    /// Getting this backwards in the harmful direction drops the iteration equal to the boundary out
    /// of every sweep, so the tail grows by one iteration per sweep forever - and neither emulator
    /// says so, because a listing wider than asked for is absorbed by the boundary check over the
    /// listed keys.
    ///
    /// Both keys are stated literally rather than taken from `JobPaths`, which is the builder the
    /// calculation under test runs on.
    ///
    /// Checked by breaking it: swapping the two arms of `build_listing_start_key` hands each bound
    /// the other one's key, and both assertions fail.
    #[tokio::test]
    async fn a_cleanup_listing_starts_at_the_key_the_bound_of_its_provider_requires() {
        let inclusive = build_storage_over_bound(ListingStartBound::Inclusive);
        let exclusive = build_storage_over_bound(ListingStartBound::Exclusive);

        for storage in [&inclusive, &exclusive] {
            storage
                .list_job_outdated_iterations(
                    &JobCode::new("job"),
                    TESTED_RETENTION_BOUNDARY,
                    &CancellationToken::new(),
                )
                .await
                .expect("a store that answers a listing must read as a tail");
        }

        assert_eq!(
            inclusive.store.requested_start_keys(),
            vec![Some(BOUNDARY_ITERATION_KEY.to_string())],
            "a store answering with the boundary key must be given the boundary itself"
        );
        assert_eq!(
            exclusive.store.requested_start_keys(),
            vec![Some(OLDEST_KEPT_ITERATION_KEY.to_string())],
            "a store leaving the boundary key out must be given the key of the oldest kept iteration"
        );
    }

    /// The control: the same double reaches a job when the version is there, so the case above
    /// refuses on the missing version rather than on a fixture that never listed anything.
    #[tokio::test]
    async fn a_state_object_listed_with_a_version_names_the_iteration_it_carries() {
        let storage = build_storage_over_listing(vec![(NEWEST_STATE_KEY.to_string(), Some("etag-1".to_string()))]);

        let job_meta = storage
            .find_job_meta(&JobCode::new("job"), &CancellationToken::new())
            .await
            .expect("a listed state object carrying a version must name the current iteration");

        assert_eq!(job_meta.iter_num, 1);
        assert_eq!(job_meta.version, "etag-1");
    }
}
