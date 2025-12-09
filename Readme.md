# JobManager
A simple diskless Go library for managing jobs and executing tasks within jobs. Uses S3 as storage backend.

## Why JobManager?

Traditional distributed task queues like Celery, Bull, or Sidekiq require dedicated infrastructure—Redis, RabbitMQ, PostgreSQL—adding operational complexity, costs, and potential failure points. You need to manage brokers, ensure high availability, handle state persistence, and coordinate worker deployments.

JobManager takes a radically simpler approach: **use S3 (or any S3-compatible object storage) as your only dependency**. No message brokers, no databases, no coordination servers. Just workers, your code, and S3.

### Key Features

- **Zero Infrastructure**: No Redis, PostgreSQL, or RabbitMQ to manage—just S3
- **Stateless Workers**: Workers hold no local state and can restart anytime without data loss
- **Atomic Operations**: Uses S3 ETag-based optimistic locking for conflict-free concurrent updates
- **Horizontal Scaling**: Spin up or down workers instantly without coordination overhead
- **Cost Optimized**: Adaptive polling and intelligent caching minimize S3 API requests
- **Fault Tolerant**: Automatic task retry with deadlines and heartbeat monitoring
- **Dynamic Workflows**: Tasks can create new tasks on-the-fly based on results

## Use Cases

- **Data Pipelines**: Build ETL workflows with sequential task dependencies (extract → transform → load). Each task can inspect previous results and create follow-up tasks dynamically.

- **Scheduled & Recurring Jobs**: Run periodic tasks like daily reports, database cleanups, or batch synchronizations. Set `maxIterations` to control how many times a job repeats.

- **Dynamic Workflows**: Implement fan-out processing where one task spawns multiple parallel subtasks. Perfect for batch operations like image resizing, webhook deliveries, or multi-step data processing.

- **Distributed Processing**: Multiple workers collaboratively process tasks from a shared job queue. Great for API webhooks, document processing, or any embarrassingly parallel workload.

## When to Use

**JobManager is ideal when:**
- ✅ You already use S3 or S3-compatible storage (AWS S3, MinIO, DigitalOcean Spaces, etc.)
- ✅ You want simple task coordination without infrastructure overhead
- ✅ You need horizontally scalable, stateless workers
- ✅ Your workloads are intermittent or moderate volume
- ✅ You value operational simplicity over ultra-low latency

**Consider alternatives if:**
- ❌ You need sub-second task latency (JobManager uses polling, typically 200ms-2s intervals)
- ❌ You have extremely high throughput needs (thousands of tasks per second)
- ❌ You require complex task routing or priority queues
- ❌ You need to perform a large number of tasks inside the job. But you can partition tasks into multiple jobs, then the concurrency of job processing will decrease.

## Algorithm

- Each **job** is stored in a single **file** `state-{inverted_iter_num}.json` (e.g., `state-18446744073709551614.json`).
    - The file uses **inverted iter_num** (`MaxUint64 - iterNum`) so that newer iterations appear first in S3 LIST operations (S3 sorts by name), making LIST requests faster.
    - A new file is created when **starting a new iteration** of the job (when status transitions to `started`). `iter_num` is a monotonically increasing number.
    - We'll conventionally refer to the job state file as `job.json`.
- Each **job type** is stored in its own **folder** `/jobs/job-name` (conventional naming).
- All **workers are equal**.
- **Tasks** are stored in `job.json` in a **map** so workers can quickly find their task by id.
- How **workers check for new tasks** when a worker is idle:
    - Each worker knows which jobs exist and the path to each job.
    - Worker reads all `job.json` files (`state-{inverted_iter_num}.json`) in parallel from each job folder. First, we LIST (ListObjectsV2) to find the state file with maximum iter_num (minimum inverted number) - this is the latest state.
        - Worker maintains in-memory cache for each job containing: ETag, `nextPoll` time, and `exhausted` flag (whether job reached maxIterations limit).
        - If the time to check the job hasn't come yet (`nextPoll`), we don't read the state unnecessarily.
        - If job is `exhausted` (reached iteration limit), we don't poll it anymore.
        - Read job state - GET `job.json` with `If-None-Match:<cached-etag>`:
            - 304 → nothing changed, 1 request, no response body.
            - 200 + body → there are changes.
        - If ETag is new (file changed), parse state and save ETag.
    - This involves many reads. With 1 worker, 1 job, reading once per second = 2.6M requests/month or ~$1 just for job polling in AWS S3.
        - Actually no. This is a strange case where we constantly read the job looking for tasks - this shouldn't happen. If there are no tasks, the job has a delay and worker won't read unnecessarily. If the job has tasks being processed by other workers but no `todo` tasks, the worker will increase polling interval. Also consider dynamically adjusting worker count - if no tasks, kill idle workers (keep one).
    - When no tasks are available, increase polling interval: 200ms → 500ms → 1-2s (example values).
- If worker reads a job and sees that `status=completed` or `status=failed` + start time is not delayed, worker takes the job for processing **(new job iteration)**.
    - Worker knows (through code) which tasks need to be executed within the job (at least initial tasks, since tasks can change during job execution).
    - Initial tasks are added to job state (`task_code=do_work, status=todo, worker_id=null, started_at=null, deadline_at=null`).
    - Worker immediately takes the first task from the job (`status=started, worker_id, started_at, deadline_at, attempt=1`).
    - Job status is set to `status=started, worker_id, started_at`.
    - Job file is atomically created (`If-None-Match: *`) with the next sequential number.
- If worker **reads** a job with **`status=started`**, it looks at tasks:
    - If there are tasks with status **`todo`**, current worker takes a task to process.
        - If there are multiple `todo` tasks, randomize task selection to reduce competition between workers (achieved through Go map iteration randomness).
        - Worker marks task as `status=started, ...`.
        - Worker atomically rewrites job state file.
    - Else: worker checks `deadline_at` time: if deadline expired, current worker tries to start (take for execution) the task - atomically (`If-Match`) writes `status=started,...`.
    - Else: worker looks at `status=failed` and takes task to work (atomically changes task status in file).
- When **worker takes a task** to work:
    - To write **new task state** (if work was done), worker:
        - In a loop:
            - Worker first reads job state (`GET If-None-Match:<cached-etag>`). If `ETag` changed, worker gets new state; if state unchanged, applies changes to old state.
            - Worker adds their changes to in-memory state.
            - Worker atomically (`If-Match`) writes new state to file.
            - If worker gets version error, goes to next iteration after small random delay.
    - Every n ms worker updates task's `deadline_at` in job state (heartbeat). This allows setting a small `deadline_at` so other workers don't take the task in parallel. Along with `deadline_at`, worker writes `heartbeat_at`.
    - If worker encounters an error during task execution, it atomically changes task status in file to `failed`.
    - If worker successfully **completes a task**:
        - Worker checks status of other tasks in state - if all have `status=completed`, worker change job status to `completed`.
        - Atomically changes job state in file.
- If job has `maxIterations` set and the limit is reached, worker marks job as `exhausted` in cache and stops polling it.
