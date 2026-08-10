use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{Delete, ObjectIdentifier},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    Error, Job, JobCode, JobDefinitionRegistry, JobMeta, JobStatus, MetricsSink, Retrier, RetrierConfig, RetryStep,
    Storage, StorageError, StorageResult, Task, TaskCode, TaskStatus,
    storage::s3_error::{classify_delete_failures, map_s3_error},
};

// TODO(high): add test s3 storage with Toxiproxy for testing network problems (chaos test))

const JOB_STATE_FILE_PREFIX: &str = "state-";

/// Prefix a bucket's job state objects live under unless overridden.
const DEFAULT_BUCKET_PREFIX: &str = "jobs";

/// Timeout of a single S3 operation unless overridden.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Keys requested per `LIST` page while scanning a job's outdated iterations. 1000 is the maximum
/// a single `ListObjectsV2` response can carry, so the common empty-tail scan costs one request.
/// Being the protocol maximum, it doubles as the upper bound
/// [`S3StorageConfig::with_list_page_size`] accepts.
const DEFAULT_LIST_PAGE_SIZE: i32 = 1000;

/// Keys sent per multi-object delete request; 1000 is the maximum `DeleteObjects` accepts. Being
/// the protocol maximum, it doubles as the upper bound
/// [`S3StorageConfig::with_delete_batch_size`] accepts.
const DEFAULT_DELETE_BATCH_SIZE: usize = 1000;

trait JobStateCodec: Send + Sync {
    fn file_extension(&self) -> &'static str;
    fn content_type(&self) -> &'static str;
    fn serialize(&self, job: &JobJson) -> StorageResult<Vec<u8>>;
    fn deserialize(&self, data: &[u8]) -> StorageResult<JobJson>;
}

/// Serialization format used to persist job state as an S3 object.
#[derive(Debug, Clone, Copy)]
pub enum JobStateCodecKind {
    /// Human-readable JSON. Larger on the wire and slower to (de)serialize than `Cbor`, but
    /// lets an operator read or hand-edit a job's state object directly in an S3 console.
    Json,
    /// Compact binary CBOR. Prefer this once state objects are only ever read by the crate
    /// itself, since the smaller payload reduces S3 request latency and storage cost.
    Cbor,
}

impl JobStateCodecKind {
    fn build(self) -> Arc<dyn JobStateCodec> {
        match self {
            Self::Json => Arc::new(JsonJobStateCodec),
            Self::Cbor => Arc::new(CborJobStateCodec),
        }
    }
}

struct JsonJobStateCodec;

impl JobStateCodec for JsonJobStateCodec {
    fn file_extension(&self) -> &'static str {
        ".json"
    }

    fn content_type(&self) -> &'static str {
        "application/json"
    }

    fn serialize(&self, job: &JobJson) -> StorageResult<Vec<u8>> {
        serde_json::to_vec_pretty(job).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    fn deserialize(&self, data: &[u8]) -> StorageResult<JobJson> {
        serde_json::from_slice(data).map_err(|e| StorageError::Serialization(e.to_string()))
    }
}

struct CborJobStateCodec;

impl JobStateCodec for CborJobStateCodec {
    fn file_extension(&self) -> &'static str {
        ".cbor"
    }

    fn content_type(&self) -> &'static str {
        "application/cbor"
    }

    fn serialize(&self, job: &JobJson) -> StorageResult<Vec<u8>> {
        let mut buffer = Vec::new();
        ciborium::ser::into_writer(job, &mut buffer).map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(buffer)
    }

    fn deserialize(&self, data: &[u8]) -> StorageResult<JobJson> {
        ciborium::de::from_reader(data).map_err(|e| StorageError::Serialization(e.to_string()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TaskJson {
    id: Uuid,
    code: String,
    status: TaskStatus,
    created_by_worker: Uuid,
    #[serde(default)]
    timeout_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    processing_by: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deadline_at: Option<DateTime<Utc>>,
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

#[derive(Debug, Serialize, Deserialize)]
struct JobJson {
    id: Uuid,
    code: String,
    iter_num: u64,
    status: JobStatus,
    tasks: Vec<TaskJson>,
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

/// Connection details of the S3-compatible bucket a pool keeps job state in, passed to
/// [`JobsManagerBuilder::s3`](crate::JobsManagerBuilder::s3).
///
/// Build with [`S3StorageConfig::new`] and override the optional parts with the `with_*` methods.
pub struct S3StorageConfig {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    bucket_name: String,
    region: String,
    bucket_prefix: String,
    job_state_codec: JobStateCodecKind,
    request_timeout: Duration,
    retrier_config: RetrierConfig,
    list_page_size: i32,
    delete_batch_size: usize,
}

impl S3StorageConfig {
    /// Connection details of the bucket holding job state. Everything else takes a default that
    /// the `with_*` methods override.
    ///
    /// Pass an `https://` endpoint to use TLS - there is no separate switch for it.
    pub fn new(
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        bucket_name: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            bucket_name: bucket_name.into(),
            region: region.into(),
            bucket_prefix: DEFAULT_BUCKET_PREFIX.to_string(),
            job_state_codec: JobStateCodecKind::Json,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            retrier_config: RetrierConfig::default(),
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            delete_batch_size: DEFAULT_DELETE_BATCH_SIZE,
        }
    }

    /// Prefix every job's state objects live under. Defaults to `jobs`.
    #[must_use]
    pub fn with_bucket_prefix(mut self, bucket_prefix: impl Into<String>) -> Self {
        self.bucket_prefix = bucket_prefix.into();
        self
    }

    /// Serialization format of persisted job state. Defaults to [`JobStateCodecKind::Json`].
    ///
    /// Changing this on a bucket that already holds state leaves the previously written objects
    /// unreadable *and* invisible to cleanup - see [`JobStateCodecKind`].
    #[must_use]
    pub const fn with_job_state_codec(mut self, job_state_codec: JobStateCodecKind) -> Self {
        self.job_state_codec = job_state_codec;
        self
    }

    /// Timeout applied to a single S3 operation and to each of its attempts. Defaults to 5s.
    #[must_use]
    pub const fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Retry policy of the storage operations that retry internally. Defaults to
    /// [`RetrierConfig::default`].
    #[must_use]
    pub fn with_retrier_config(mut self, retrier_config: RetrierConfig) -> Self {
        self.retrier_config = retrier_config;
        self
    }

    /// Keys one cleanup `LIST` asks for. Defaults to 1000, which is the maximum a single
    /// `ListObjectsV2` response carries; lower it only to bound the size of a single response.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] outside `1..=1000`.
    pub fn with_list_page_size(mut self, list_page_size: i32) -> Result<Self, Error> {
        if !(1..=DEFAULT_LIST_PAGE_SIZE).contains(&list_page_size) {
            return Err(Error::Other(format!(
                "s3 list page size must be within 1..={DEFAULT_LIST_PAGE_SIZE}"
            )));
        }
        self.list_page_size = list_page_size;
        Ok(self)
    }

    /// Keys one multi-object delete carries. Defaults to 1000, the maximum `DeleteObjects`
    /// accepts on AWS; lower it for a backend whose limit is smaller.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] outside `1..=1000`. Zero would make cleanup split its work into
    /// empty chunks and panic.
    pub fn with_delete_batch_size(mut self, delete_batch_size: usize) -> Result<Self, Error> {
        if !(1..=DEFAULT_DELETE_BATCH_SIZE).contains(&delete_batch_size) {
            return Err(Error::Other(format!(
                "s3 delete batch size must be within 1..={DEFAULT_DELETE_BATCH_SIZE}"
            )));
        }
        self.delete_batch_size = delete_batch_size;
        Ok(self)
    }
}

/// S3-backed `Storage`: each job's state is one object.
///
/// Concurrent writers are resolved with conditional PUTs (`If-Match` on the current `ETag` for
/// updates, `If-None-Match: *` for a job's first iteration) rather than an external lock service.
pub struct S3Storage {
    // TODO(med): save job settings separately and provide an API for changing settings
    client: Client,
    bucket_name: String,
    bucket_prefix: String,
    codec: Arc<dyn JobStateCodec>,
    registry: Arc<dyn JobDefinitionRegistry>,
    retrier: Retrier,
    metrics: Arc<dyn MetricsSink>,
    list_page_size: i32,
    delete_batch_size: usize,
}

impl S3Storage {
    /// Connects to `config.endpoint` and ensures `config.bucket_name` exists, creating it if
    /// missing.
    ///
    /// Bucket existence/creation is retried per `config.retrier_config`; a persistent failure
    /// (including one from a concurrent creator returning a non-409 error) is returned as
    /// [`Error::Other`](crate::Error::Other).
    pub async fn new(
        config: S3StorageConfig,
        registry: Arc<dyn JobDefinitionRegistry>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<Self, Error> {
        info!("Starting jobmanager with s3 storage {}", config.endpoint);

        // TODO(med): add creds options
        let credentials = aws_sdk_s3::config::Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "static",
        );

        // Initialize AWS SDK config
        let mut sdk_config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()))
            .credentials_provider(credentials);

        let timeout_config = TimeoutConfig::builder()
            .operation_timeout(config.request_timeout)
            .operation_attempt_timeout(config.request_timeout)
            .build();
        sdk_config_loader = sdk_config_loader.timeout_config(timeout_config);

        let sdk_config = sdk_config_loader.load().await;

        // Build S3 client config
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

        s3_config_builder = s3_config_builder.endpoint_url(config.endpoint).force_path_style(true);

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        // TODO(med): add check that conditional requests work for specific S3.
        // For example, some S3 backends ignore the header and atomicity breaks.

        let retrier = Retrier::new(config.retrier_config.clone());
        let cancel_token = CancellationToken::new();

        // Check if bucket exists, create if needed
        let bucket_name = config.bucket_name.clone();
        let client_for_retry = client.clone();
        retrier
            .retry(
                move || {
                    let client = client_for_retry.clone();
                    let bucket_name = bucket_name.clone();
                    async move {
                        match client.head_bucket().bucket(&bucket_name).send().await {
                            Ok(_) => {
                                info!("Bucket {} exists", bucket_name);
                                Ok(RetryStep::Done(()))
                            }
                            Err(aws_sdk_s3::error::SdkError::ServiceError(se)) if se.raw().status().as_u16() == 404 => {
                                match client.create_bucket().bucket(&bucket_name).send().await {
                                    Ok(_) => {
                                        info!("Created bucket {}", bucket_name);
                                        Ok(RetryStep::Done(()))
                                    }
                                    Err(aws_sdk_s3::error::SdkError::ServiceError(se))
                                        if se.raw().status().as_u16() == 409 =>
                                    {
                                        info!("Bucket {} already exists", bucket_name);
                                        Ok(RetryStep::Done(()))
                                    }
                                    Err(e) => {
                                        let mapped = map_s3_error(&e);
                                        if mapped.is_retryable() {
                                            Ok(RetryStep::Retry(mapped))
                                        } else {
                                            Err(mapped)
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let mapped = map_s3_error(&e);
                                if mapped.is_retryable() {
                                    Ok(RetryStep::Retry(mapped))
                                } else {
                                    Err(mapped)
                                }
                            }
                        }
                    }
                },
                &cancel_token,
            )
            .await
            .map_err(|e| Error::Other(format!("Failed to init bucket: {e}")))?;

        Ok(Self {
            client,
            bucket_name: config.bucket_name,
            bucket_prefix: config.bucket_prefix,
            codec: config.job_state_codec.build(),
            registry,
            retrier,
            metrics,
            list_page_size: config.list_page_size,
            delete_batch_size: config.delete_batch_size,
        })
    }

    fn record_s3_ok(&self, operation: &str, start: Instant) {
        self.metrics.record_storage_operation(operation, "OK", start.elapsed());
    }

    fn record_s3_err<E: std::fmt::Debug>(&self, operation: &str, err: &aws_sdk_s3::error::SdkError<E>, start: Instant) {
        let status = if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = err {
            service_err.raw().status().as_u16().to_string()
        } else {
            "ERR".to_string()
        };
        self.metrics.record_storage_operation(operation, &status, start.elapsed());
    }

    /// Records an operation whose request succeeded but whose response turned out to be a failure:
    /// a body that could not be read, or one reporting per-key errors.
    ///
    /// Such a failure has no HTTP status of its own - the status was `200` - so it is labelled
    /// `ERR` rather than a code that would read as if the request itself had failed.
    fn record_s3_response_failure(&self, operation: &str, start: Instant) {
        self.metrics.record_storage_operation(operation, "ERR", start.elapsed());
    }

    fn build_job_path(&self, job_code: &JobCode) -> String {
        format!("{}/{}/", self.bucket_prefix, job_code.as_str())
    }

    // buildStatePath builds path to state file with inverted iterNum for S3 sorting
    fn build_state_path(&self, job_code: &JobCode, iter_num: u64) -> String {
        // Invert iterNum so new files are at the beginning of list (S3 sorts by name) and LIST request is fast.
        // If we use generic `ObjectStore`, then it does not guarantee the order.
        let inv_iter_num = u64::MAX - iter_num;
        let extension = self.codec.file_extension();
        format!(
            "{}{}{:020}{}",
            self.build_job_path(job_code),
            JOB_STATE_FILE_PREFIX,
            inv_iter_num,
            extension
        )
    }

    // parseIterNumFromFilePath parses iterNum from file path
    fn parse_iter_num_from_path(&self, file_path: &str) -> StorageResult<u64> {
        // Expected format: {prefix}/{jobCode}/state-00001{ext}
        let parts: Vec<&str> = file_path.split('/').collect();
        if parts.len() < 2 {
            return Err(StorageError::Other(format!("Cannot split file path {file_path}")));
        }

        let filename = parts[parts.len() - 1];
        let extension = self.codec.file_extension();
        if !filename.starts_with(JOB_STATE_FILE_PREFIX) || !filename.ends_with(extension) {
            return Err(StorageError::Other(format!("Invalid filename format {filename}")));
        }

        let iter_num_str = filename.trim_start_matches(JOB_STATE_FILE_PREFIX).trim_end_matches(extension);

        let inv_iter_num: u64 = iter_num_str
            .parse()
            .map_err(|e| StorageError::Other(format!("Failed to parse iter_num: {e}")))?;

        // Restore original iterNum
        Ok(u64::MAX - inv_iter_num)
    }

    fn serialize_job(&self, job: &Job) -> StorageResult<Vec<u8>> {
        let job_json = Self::job_to_json(job);
        self.codec.serialize(&job_json)
    }

    fn deserialize_job(&self, data: &[u8], version: &str) -> StorageResult<Job> {
        let job_json = self.codec.deserialize(data)?;
        let job_def = self
            .registry
            .get_job(&JobCode::new(job_json.code.as_str()))
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(Self::job_from_json(
            job_json,
            job_def.max_iterations(),
            job_def.iteration_interval(),
            job_def.task_limits(),
            version,
        ))
    }

    fn task_to_json(task: &Task) -> TaskJson {
        TaskJson {
            id: *task.id(),
            code: task.code().to_string(),
            status: task.status().clone(),
            timeout_ms: task.timeout().num_milliseconds(),
            created_by_worker: task.created_by_worker(),
            processing_by: task.processing_by_worker(),
            started_at: task.started_at(),
            completed_at: task.completed_at(),
            deadline_at: task.deadline_at(),
            attempt: task.attempt(),
            max_attempts: task.max_attempts(),
            input: task.input().to_vec(),
            output: task.output().to_vec(),
            error: task.error_msg().to_string(),
            depends_on: task.depends_on().to_vec(),
        }
    }

    fn job_to_json(job: &Job) -> JobJson {
        let tasks: Vec<TaskJson> = job.tasks_as_iter().map(Self::task_to_json).collect();

        JobJson {
            id: *job.id(),
            code: job.code().to_string(),
            iter_num: job.iter_num(),
            status: job.status().clone(),
            tasks,
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

    fn task_from_json(json: TaskJson) -> Task {
        Task::restore(
            json.id,
            TaskCode::new(json.code),
            json.status,
            json.processing_by,
            json.created_by_worker,
            chrono::Duration::milliseconds(json.timeout_ms),
            json.started_at,
            json.completed_at,
            json.deadline_at,
            json.attempt,
            json.max_attempts,
            json.input,
            json.output,
            json.error,
            json.depends_on,
        )
    }

    fn job_from_json(
        json: JobJson,
        max_iterations: Option<u64>,
        iteration_interval: Option<Duration>,
        task_limits: crate::TaskLimits,
        version: &str,
    ) -> Job {
        let tasks: Vec<Task> = json.tasks.into_iter().map(Self::task_from_json).collect();

        Job::restore(
            json.id,
            JobCode::new(json.code),
            version.to_string(),
            json.iter_num,
            json.status,
            tasks,
            json.updated_by,
            json.started_at,
            json.running_at,
            json.completed_at,
            json.next_start_at,
            json.metadata.unwrap_or_default(),
            max_iterations,
            iteration_interval,
            task_limits,
        )
    }

    async fn put_next_iteration(&self, key: &str, job_serialized: Vec<u8>) -> StorageResult<Option<String>> {
        // Use if-none-match="*" for new iteration
        let start = Instant::now();
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(ByteStream::from(job_serialized))
            .content_type(self.codec.content_type())
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(output) => {
                self.record_s3_ok("PUT", start);
                Ok(output.e_tag().map(Self::normalize_etag))
            }
            Err(e) => {
                self.record_s3_err("PUT", &e, start);
                Err(map_s3_error(&e))
            }
        }
    }

    async fn put_current_iteration(
        &self,
        key: &str,
        job_serialized: Vec<u8>,
        version: &str,
    ) -> StorageResult<Option<String>> {
        // TODO(med): to check - set {PutObjectOptions{DisableMultipart: true} and send correct file. When
        // multipart is on there may be problems with the etag. Atomic write with If-Match
        let start = Instant::now();
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(ByteStream::from(job_serialized))
            .content_type(self.codec.content_type())
            .if_match(version)
            .send()
            .await;

        match result {
            Ok(output) => {
                self.record_s3_ok("PUT", start);
                Ok(output.e_tag().map(Self::normalize_etag))
            }
            Err(e) => {
                self.record_s3_err("PUT", &e, start);
                Err(map_s3_error(&e))
            }
        }
    }

    /// Deletes one state object. `DELETE` answers 204 for a key that is already gone, which is
    /// what makes iteration cleanup idempotent.
    async fn delete_state_object(&self, key: &str) -> StorageResult<()> {
        let start = Instant::now();
        match self.client.delete_object().bucket(&self.bucket_name).key(key).send().await {
            Ok(_) => {
                self.record_s3_ok("DELETE", start);
                Ok(())
            }
            Err(e) => {
                self.record_s3_err("DELETE", &e, start);
                Err(map_s3_error(&e))
            }
        }
    }

    /// Deletes up to `delete_batch_size` state objects in one request.
    ///
    /// A multi-object delete reports per-key failures inside a `200` response, so the request only
    /// counts as successful once the body reports no failure - both for the returned result and for
    /// the metric the request is recorded under. Any reported failure is an error, including one
    /// whose entry carries no key; whether it is retryable is decided by
    /// [`classify_delete_failures`].
    async fn delete_state_objects(&self, keys: &[String]) -> StorageResult<()> {
        let mut objects = Vec::with_capacity(keys.len());
        for key in keys {
            objects.push(
                ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .map_err(|e| StorageError::Other(format!("Failed to build delete entry for {key}: {e}")))?,
            );
        }
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .build()
            .map_err(|e| StorageError::Other(format!("Failed to build delete request: {e}")))?;

        let start = Instant::now();
        let output = match self
            .client
            .delete_objects()
            .bucket(&self.bucket_name)
            .delete(delete)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                self.record_s3_err("DELETE", &e, start);
                return Err(map_s3_error(&e));
            }
        };

        let Some(failure) = classify_delete_failures(output.errors()) else {
            self.record_s3_ok("DELETE", start);
            return Ok(());
        };

        self.record_s3_response_failure("DELETE", start);
        Err(failure)
    }

    fn normalize_etag(etag: &str) -> String {
        // Some S3 implementation (Ceph) return ETag with quotes.
        etag.trim().trim_matches('"').to_string()
    }
}

#[async_trait::async_trait]
#[allow(private_interfaces)]
impl Storage for S3Storage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        // TODO(low): perhaps should try to get current job iteration first and read new file on miss.
        // But if job has few tasks, we'll miss often and make extra requests

        let job_code_for_retry = job_code.clone();
        self.retrier
            .retry(
                move || {
                    let job_code = job_code_for_retry.clone();
                    async move {
                        let job_meta = self.find_job_meta(&job_code, cancel_token).await?;
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
        let key = self.build_state_path(&job_meta.code, job_meta.iter_num);

        let start = Instant::now();
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .if_match(&job_meta.version)
            .send()
            .await;

        let output = match result {
            Ok(output) => output,
            Err(e) => {
                self.record_s3_err("GET", &e, start);
                return Err(map_s3_error(&e));
            }
        };

        let data = output
            .body
            .collect()
            .await
            .map_err(|e| {
                self.record_s3_response_failure("GET", start);
                StorageError::S3(format!("Failed to read job body: {e}"))
            })?
            .into_bytes();
        self.record_s3_ok("GET", start);

        let job = self.deserialize_job(&data, job_meta.version.as_str())?;

        Ok(job)
    }

    #[tracing::instrument(skip(self, cancel_token), fields(job_code = %job_code))]
    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let prefix = self.build_job_path(job_code);

        let start = Instant::now();
        let result = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(&prefix)
            .max_keys(1) // Take only first file since inverted name order is used (DESC)
            .send()
            .await;

        let result = match result {
            Ok(output) => {
                self.record_s3_ok("LIST", start);
                output
            }
            Err(e) => {
                self.record_s3_err("LIST", &e, start);
                return Err(map_s3_error(&e));
            }
        };

        let contents = result.contents();
        if let Some(object) = contents.first()
            && let (Some(key), Some(etag)) = (object.key(), object.e_tag())
        {
            let iter_num = self.parse_iter_num_from_path(key)?;
            return Ok(JobMeta {
                code: job_code.clone(),
                iter_num,
                version: Self::normalize_etag(etag),
            });
        }

        Err(StorageError::NotFound(
            "No job state objects found for requested job".to_string(),
        ))
    }

    // SaveJob saves job atomically with Version check
    #[tracing::instrument(skip(self, cancel_token, job), fields(job_version = %job.version()))]
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let is_new_iter = job.version().is_empty();
        let version = job.version().to_string();
        let data = Arc::new(self.serialize_job(job)?);
        let key = self.build_state_path(job.code(), job.iter_num());

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

        let etag_opt = self
            .retrier
            .retry(
                move || {
                    let key = key.clone();
                    let data = Arc::clone(&data);
                    let version = version.clone();
                    async move {
                        let result = if is_new_iter {
                            self.put_next_iteration(&key, data.as_ref().clone()).await
                        } else {
                            self.put_current_iteration(&key, data.as_ref().clone(), &version).await
                        };

                        match result {
                            Ok(etag) => Ok(RetryStep::Done(etag)),
                            Err(e) if e.is_retryable() => Ok(RetryStep::Retry(e)),
                            Err(e) => Err(e),
                        }
                    }
                },
                cancel_token,
            )
            .await?;

        match etag_opt {
            Some(etag) => job.update_version(etag),
            None => {
                return Err(StorageError::Other(format!(
                    "missing etag after save_job for job {} (iter: {})",
                    job.code(),
                    job.iter_num()
                )));
            }
        }

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

        let prefix = self.build_job_path(job_code);
        // Names carry the inverted iteration number, so the ascending key order is descending
        // iteration order: everything at or below the boundary sits after the key of the oldest
        // iteration that must be kept.
        let oldest_kept_iter_num = retention_boundary.checked_add(1).ok_or_else(|| {
            StorageError::Other(format!("Retention boundary {retention_boundary} has no next iteration"))
        })?;
        let start_after = self.build_state_path(job_code, oldest_kept_iter_num);

        let mut outdated_iter_nums = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            if cancel_token.is_cancelled() {
                return Err(StorageError::Cancelled);
            }

            let start = Instant::now();
            let result = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .prefix(&prefix)
                .start_after(start_after.as_str())
                .max_keys(self.list_page_size)
                .set_continuation_token(continuation_token)
                .send()
                .await;

            let output = match result {
                Ok(output) => {
                    self.record_s3_ok("LIST", start);
                    output
                }
                Err(e) => {
                    self.record_s3_err("LIST", &e, start);
                    return Err(map_s3_error(&e));
                }
            };

            for object in output.contents() {
                let Some(key) = object.key() else { continue };
                match self.parse_iter_num_from_path(key) {
                    Ok(iter_num) if iter_num <= retention_boundary => outdated_iter_nums.push(iter_num),
                    // Reached only by a backend that ignores `start_after`. Dropping the key here
                    // is what keeps the newest state object undeletable even then.
                    Ok(iter_num) => debug!("Skipping iteration {iter_num} above retention boundary of job {job_code}"),
                    Err(e) => debug!("Skipping object {key} that is not a state of the current codec: {e}"),
                }
            }

            continuation_token = output.next_continuation_token().map(ToString::to_string);
            if continuation_token.is_none() {
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

        // The steady-state case is a single iteration falling out of the retention window, and a
        // single-object DELETE is free at every provider, while a multi-object one is not.
        if let [iter_num] = iter_nums {
            return self.delete_state_object(&self.build_state_path(job_code, *iter_num)).await;
        }

        for chunk in iter_nums.chunks(self.delete_batch_size) {
            if cancel_token.is_cancelled() {
                return Err(StorageError::Cancelled);
            }
            let keys: Vec<String> = chunk
                .iter()
                .map(|iter_num| self.build_state_path(job_code, *iter_num))
                .collect();
            self.delete_state_objects(&keys).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, retry::RetryConfig};
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;

    use super::*;
    use crate::JobDefinition;

    fn build_config() -> S3StorageConfig {
        S3StorageConfig::new("http://localhost:9000", "key", "secret", "jobs", "us-east-1")
    }

    /// Registry double for the delete path, which never resolves a job definition: reaching for one
    /// is a defect, so it fails instead of returning something plausible.
    struct UnusedJobRegistry;

    impl JobDefinitionRegistry for UnusedJobRegistry {
        fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error> {
            Err(Error::Other(format!(
                "a delete must not need the definition of job {code}"
            )))
        }
    }

    /// Builds a storage whose S3 client answers with `response`.
    ///
    /// Constructed field by field rather than through [`S3Storage::new`], which reaches a real
    /// endpoint to check the bucket and builds a client of its own; there is no other seam for a
    /// canned response, and adding one to the production configuration for a test would be a
    /// second way to configure the same thing.
    fn build_storage_answering(response: http::Response<SdkBody>) -> S3Storage {
        let request = http::Request::builder()
            .uri("http://localhost:9000/jobs")
            .body(SdkBody::empty())
            .expect("the scripted request must build");
        let http_client = StaticReplayClient::new(vec![ReplayEvent::new(request, response)]);

        let s3_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("key", "secret", None, None, "static"))
            .endpoint_url("http://localhost:9000")
            .force_path_style(true)
            .http_client(http_client)
            // The retry budget under test is the cleaner's; an SDK-level retry would spend requests
            // the request quota of this crate does not account for.
            .retry_config(RetryConfig::disabled())
            .build();

        S3Storage {
            client: Client::from_conf(s3_config),
            bucket_name: "jobs".to_string(),
            bucket_prefix: DEFAULT_BUCKET_PREFIX.to_string(),
            codec: JobStateCodecKind::Json.build(),
            registry: Arc::new(UnusedJobRegistry),
            retrier: Retrier::new(RetrierConfig::default()),
            metrics: Arc::new(crate::NoopMetrics),
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            delete_batch_size: DEFAULT_DELETE_BATCH_SIZE,
        }
    }

    /// The `200` a multi-object delete answers with when it deleted one key and could not delete
    /// the other, as documented for `DeleteObjects`.
    fn build_delete_response(failure: Option<&str>) -> http::Response<SdkBody> {
        let failure_element = failure.map_or_else(String::new, |code| {
            format!(
                "<Error><Key>jobs/job/state-00000000000000000002.json</Key><Code>{code}</Code>\
                 <Message>scripted failure</Message></Error>"
            )
        });
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Deleted><Key>jobs/job/state-00000000000000000001.json</Key></Deleted>\
             {failure_element}</DeleteResult>"
        );

        http::Response::builder()
            .status(200)
            .header("content-type", "application/xml")
            .body(SdkBody::from(body))
            .expect("the scripted response must build")
    }

    fn build_deleted_keys() -> Vec<String> {
        vec![
            "jobs/job/state-00000000000000000001.json".to_string(),
            "jobs/job/state-00000000000000000002.json".to_string(),
        ]
    }

    /// A per-key failure arrives inside a `200`, so nothing but the parsed body distinguishes this
    /// from a clean delete - and a transient one has to come back retryable for the cleaner to
    /// retry it at all.
    #[tokio::test]
    async fn test_multi_object_delete_reporting_a_transient_failure_returns_a_retryable_error() {
        let storage = build_storage_answering(build_delete_response(Some("SlowDown")));

        let error = storage
            .delete_state_objects(&build_deleted_keys())
            .await
            .expect_err("a body reporting a failed key must not be reported as success");

        assert!(
            matches!(error, StorageError::RateLimited),
            "a throttled key must reach the caller as the rate-limit error, got: {error:?}"
        );
        assert!(error.is_retryable(), "a throttled key must be retried");
    }

    #[tokio::test]
    async fn test_multi_object_delete_reporting_a_permanent_failure_returns_a_non_retryable_error() {
        let storage = build_storage_answering(build_delete_response(Some("AccessDenied")));

        let error = storage
            .delete_state_objects(&build_deleted_keys())
            .await
            .expect_err("a body reporting a failed key must not be reported as success");

        assert!(!error.is_retryable(), "a rejected key must be attempted exactly once");
    }

    #[tokio::test]
    async fn test_multi_object_delete_without_reported_failures_succeeds() {
        let storage = build_storage_answering(build_delete_response(None));

        storage
            .delete_state_objects(&build_deleted_keys())
            .await
            .expect("a body reporting no failure is a successful delete");
    }

    #[test]
    fn test_delete_batch_size_of_zero_is_rejected() {
        assert!(build_config().with_delete_batch_size(0).is_err());
    }

    #[test]
    fn test_delete_batch_size_above_the_protocol_maximum_is_rejected() {
        assert!(build_config().with_delete_batch_size(1001).is_err());
    }

    #[test]
    fn test_list_page_size_below_one_is_rejected() {
        assert!(build_config().with_list_page_size(0).is_err());
    }

    #[test]
    fn test_list_page_size_above_the_protocol_maximum_is_rejected() {
        assert!(build_config().with_list_page_size(1001).is_err());
    }

    #[test]
    fn test_delete_batch_size_at_the_accepted_bounds_is_applied() {
        let lowest = build_config()
            .with_delete_batch_size(1)
            .expect("1 is inside the accepted range");
        let highest = build_config()
            .with_delete_batch_size(1000)
            .expect("1000 is the protocol maximum");

        assert_eq!(lowest.delete_batch_size, 1);
        assert_eq!(highest.delete_batch_size, 1000);
    }

    #[test]
    fn test_list_page_size_at_the_accepted_bounds_is_applied() {
        let lowest = build_config().with_list_page_size(1).expect("1 is inside the accepted range");
        let highest = build_config().with_list_page_size(1000).expect("1000 is the protocol maximum");

        assert_eq!(lowest.list_page_size, 1);
        assert_eq!(highest.list_page_size, 1000);
    }
}
