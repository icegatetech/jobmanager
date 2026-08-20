use std::{collections::HashMap, sync::Arc};

use crate::{Error, JobCode, JobDefinition, JobDefinitionRegistry, TaskCode, TaskExecutor};

/// Immutable collection of job definitions and task executors.
#[derive(Clone)]
pub struct JobRegistry {
    jobs_by_code: HashMap<JobCode, JobDefinition>,
    task_executors_by_key: HashMap<String, Arc<dyn TaskExecutor>>,
}

impl JobRegistry {
    /// Indexes the given definitions by job code and flattens their executors into a
    /// `(job code, task code)` lookup, so the same task code may carry different executors in
    /// different jobs.
    ///
    /// The registry is the authority on which jobs a worker polls, and it is also consulted when
    /// job state is loaded from storage: a persisted job whose code is absent here cannot be
    /// deserialized.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `jobs` is empty, if any job code is empty, or if two
    /// definitions share a code.
    pub(crate) fn new(jobs: Vec<JobDefinition>) -> Result<Self, Error> {
        if jobs.is_empty() {
            return Err(Error::Other("jobs cannot be empty".into()));
        }

        let mut jobs_by_code = HashMap::with_capacity(jobs.len());
        let mut task_executors_by_key = HashMap::new();

        for job_def in jobs {
            let job_code = job_def.code().clone();
            if job_code.as_str().is_empty() {
                return Err(Error::Other("job code cannot be empty".into()));
            }
            if jobs_by_code.contains_key(&job_code) {
                return Err(Error::Other(format!("job {job_code} has duplicate")));
            }

            for (task_code, executor) in job_def.task_executors() {
                let key = Self::task_executor_key(&job_code, task_code);
                task_executors_by_key.insert(key, executor.clone());
            }

            jobs_by_code.insert(job_code, job_def);
        }

        Ok(Self {
            jobs_by_code,
            task_executors_by_key,
        })
    }

    pub(crate) fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error> {
        self.jobs_by_code
            .get(code)
            .cloned()
            .ok_or_else(|| Error::Other(format!("job definition {code} not found")))
    }

    pub(crate) fn get_task_executor(
        &self,
        job_code: &JobCode,
        task_code: &TaskCode,
    ) -> Result<Arc<dyn TaskExecutor>, Error> {
        let key = Self::task_executor_key(job_code, task_code);
        self.task_executors_by_key
            .get(&key)
            .map(Arc::clone)
            .ok_or_else(|| Error::Other(format!("executor for task {task_code} and job {job_code} not exist")))
    }

    // TODO(low): add iterator
    pub(crate) fn list_jobs(&self) -> Vec<JobCode> {
        self.jobs_by_code.keys().cloned().collect()
    }

    /// How many jobs are registered, for a caller that needs the count rather than the codes -
    /// [`Self::list_jobs`] clones every code to answer that.
    pub(crate) fn jobs_count(&self) -> usize {
        self.jobs_by_code.len()
    }

    fn task_executor_key(job_code: &JobCode, task_code: &TaskCode) -> String {
        format!("{job_code}:{task_code}")
    }
}

impl JobDefinitionRegistry for JobRegistry {
    fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error> {
        self.get_job(code)
    }
}
