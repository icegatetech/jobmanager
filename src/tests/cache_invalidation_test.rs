use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::counting_metrics::CountingMetrics;
use super::common::s3_container::S3TestContainer;
use super::common::storage_wrapper::CountingStorage;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    CachedStorage, Job, JobCode, JobDefinition, JobDefinitionId, JobStateCodecKind, JobsManager, MetricsSink,
    NoopMetrics, S3StorageConfig, Storage, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// Bound on the waits of the pools below: a job that never finishes must fail the test rather than
/// hang the suite.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// `CachedStorage` must skip the `GET` when the cached job is still current, and take it when the
/// job was written behind its back. The counting wrapper sits between the cache and the backend,
/// so what is asserted is the request count the backend would be billed for.
#[tokio::test]
async fn test_cache_invalidation() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let storage = Arc::new(CountingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let cached_storage = Arc::new(CachedStorage::new(
        Arc::clone(&storage) as Arc<dyn Storage>,
        Arc::new(NoopMetrics),
    ));
    let cancel_token = CancellationToken::new();
    let worker_id = Uuid::from_u128(1);

    let task_def = TaskDefinition::new(TaskCode::from("cache_task"), Duration::from_secs(5));

    let job_code = JobCode::new("test_cache_job");

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(task_def, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let mut job = Job::new(&job_def, HashMap::new(), worker_id)?;
    job.work(&worker_id)?;

    // A write that bypasses the cache, as another worker's would.
    storage.save_job(&mut job, &cancel_token).await?;
    assert_eq!(job.version(), "1");

    // First read: nothing is cached yet, so both the metadata and the job itself are fetched.
    let job_from_cache = cached_storage.get_job(&job_code, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 1);
    assert_eq!(storage.get_by_meta_calls(), 1);
    assert_eq!(job_from_cache.version(), "1");

    // Saving through the cache refreshes it, so no read is needed to keep it current.
    cached_storage.save_job(&mut job, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 1);
    assert_eq!(storage.get_by_meta_calls(), 1);
    assert_eq!(job.version(), "2");

    // Cache hit: the metadata is re-read to check currency, the job itself is not.
    let job_from_cache = cached_storage.get_job(&job_code, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 2);
    assert_eq!(storage.get_by_meta_calls(), 1);
    assert_eq!(job_from_cache.version(), "2");

    // Another bypassing write leaves the cache stale.
    storage.save_job(&mut job, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 2);
    assert_eq!(storage.get_by_meta_calls(), 1);
    assert_eq!(job.version(), "3");

    // Cache miss: the version moved, so the job is fetched again.
    let job_from_cache = cached_storage.get_job(&job_code, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 3);
    assert_eq!(storage.get_by_meta_calls(), 2);
    assert_eq!(job_from_cache.version(), "3");

    Ok(())
}

/// Which backend a pool ends up reading through, asserted on the only thing that tells the two
/// apart from outside: the read cache is the sole source of these two measurements, so a pool
/// running without it must produce neither.
///
/// Both pools run against one object store under prefixes of their own, because the object store is
/// what makes the cache observable at all - an in-memory backend is never wrapped by it.
#[tokio::test]
async fn only_a_pool_keeping_the_read_cache_reports_cache_reads() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let store = S3TestContainer::start().await?;

    let cached_reads = run_job_on_object_store(&store, "cache-on", true).await?;
    assert!(
        cached_reads.cache_hits() + cached_reads.cache_misses() > 0,
        "a cached pool must read through the cache, got {} hits and {} misses",
        cached_reads.cache_hits(),
        cached_reads.cache_misses()
    );

    let uncached_reads = run_job_on_object_store(&store, "cache-off", false).await?;
    assert_eq!(uncached_reads.cache_hits(), 0);
    assert_eq!(uncached_reads.cache_misses(), 0);
    Ok(())
}

/// Runs one job to completion on `store` under `bucket_prefix`, and returns what its pool measured.
///
/// The job carries two tasks because a worker runs at most one task of a job per pass: the pass
/// that creates the job writes it without reading it, so a single-task job would finish before the
/// cache is ever consulted.
async fn run_job_on_object_store(
    store: &S3TestContainer,
    bucket_prefix: &str,
    cache_storage: bool,
) -> Result<Arc<CountingMetrics>, Box<dyn std::error::Error>> {
    let sink = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("cache_switch_job");

    let mut builder = JobsManager::builder()
        .s3(S3StorageConfig::new(
            store.endpoint(),
            store.username(),
            store.password(),
            "test-jobs",
            "us-east-1",
        )
        .with_bucket_prefix(bucket_prefix)
        .with_job_state_codec(JobStateCodecKind::Json))
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(50))
        .job(job_code.clone(), |j| {
            j.max_iterations(1);
            for task_code in ["first", "second"] {
                j.add_task(
                    TaskDefinition::new(task_code, Duration::from_secs(10)),
                    task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
                );
            }
        });
    if !cache_storage {
        builder = builder.no_cache();
    }

    let handle = builder.build().await?.start()?;
    tokio::time::timeout(WAIT_TIMEOUT, handle.wait_for_job_completion(&job_code)).await??;
    handle.shutdown().await?;

    Ok(sink)
}
