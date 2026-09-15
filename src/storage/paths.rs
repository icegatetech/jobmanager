//! Object keys shared by backends. Inverted iteration numbers put the newest state first
//! in an ascending listing; changing this layout requires migrating existing objects.

use crate::storage::state_codec::JobStateCodec;
use crate::{JobCode, StorageError, StorageResult};

const JOB_STATE_FILE_PREFIX: &str = "state-";

/// Prefix a store's job state objects live under unless a backend's configuration overrides it.
/// Shared so that two backends pointed at one store find each other's objects.
pub(crate) const DEFAULT_STATE_PREFIX: &str = "jobs";

/// Object keys of one backend: the prefix its state objects live under and the extension of the
/// codec that writes them.
///
/// Both are held together because a key is only readable back by the codec that produced it: a path
/// built with one extension and parsed with another names an iteration that is invisible to
/// cleanup.
pub(crate) struct JobPaths {
    path_prefix: String,
    file_extension: &'static str,
}

impl JobPaths {
    pub(crate) fn new(path_prefix: String, codec: &dyn JobStateCodec) -> Self {
        Self {
            path_prefix,
            file_extension: codec.file_extension(),
        }
    }

    /// Prefix every state object of `job_code` shares, which is what a `LIST` of the job asks for.
    pub(crate) fn build_job_prefix(&self, job_code: &JobCode) -> String {
        format!("{}/{}/", self.path_prefix, job_code.as_str())
    }

    pub(crate) fn build_job_iteration_path(&self, job_code: &JobCode, iter_num: u64) -> String {
        let inv_iter_num = u64::MAX - iter_num;
        format!(
            "{}{}{:020}{}",
            self.build_job_prefix(job_code),
            JOB_STATE_FILE_PREFIX,
            inv_iter_num,
            self.file_extension
        )
    }

    /// Iteration number `file_path` carries, or [`StorageError::Other`] when the path is not a
    /// state object of this codec.
    pub(crate) fn parse_iter_num(&self, file_path: &str) -> StorageResult<u64> {
        let parts: Vec<&str> = file_path.split('/').collect();
        if parts.len() < 2 {
            return Err(StorageError::Other(format!("Cannot split file path {file_path}")));
        }

        let filename = parts[parts.len() - 1];
        if !filename.starts_with(JOB_STATE_FILE_PREFIX) || !filename.ends_with(self.file_extension) {
            return Err(StorageError::Other(format!("Invalid filename format {filename}")));
        }

        let iter_num_str = filename
            .trim_start_matches(JOB_STATE_FILE_PREFIX)
            .trim_end_matches(self.file_extension);

        let inv_iter_num: u64 = iter_num_str
            .parse()
            .map_err(|e| StorageError::Other(format!("Failed to parse iter_num: {e}")))?;

        Ok(u64::MAX - inv_iter_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::state_codec::JobStateCodecKind;

    fn build_job_code() -> JobCode {
        JobCode::new("job")
    }

    fn build_json_keys() -> JobPaths {
        JobPaths::new("jobs".to_string(), JobStateCodecKind::Json.build().as_ref())
    }

    #[test]
    fn a_state_path_carries_the_inverted_iteration_number() {
        assert_eq!(
            build_json_keys().build_job_iteration_path(&build_job_code(), 1),
            "jobs/job/state-18446744073709551614.json"
        );
    }

    #[test]
    fn a_later_iteration_sorts_before_an_earlier_one() {
        let keys = build_json_keys();
        let earlier_path = keys.build_job_iteration_path(&build_job_code(), 1);
        let later_path = keys.build_job_iteration_path(&build_job_code(), 2);

        assert!(
            later_path < earlier_path,
            "{later_path} must sort before {earlier_path}"
        );
    }

    #[test]
    fn every_boundary_iteration_survives_the_round_trip() {
        let keys = build_json_keys();

        for iter_num in [0_u64, 1, u64::MAX] {
            let path = keys.build_job_iteration_path(&build_job_code(), iter_num);

            let parsed_iter_num = keys.parse_iter_num(&path).expect("a path this module built must parse");

            assert_eq!(parsed_iter_num, iter_num, "path was {path}");
        }
    }

    #[test]
    fn a_path_of_another_codec_is_refused() {
        let error = build_json_keys()
            .parse_iter_num("jobs/job/state-18446744073709551614.cbor")
            .expect_err("a path of another codec must not parse");

        assert!(matches!(error, StorageError::Other(_)), "got: {error}");
    }

    #[test]
    fn a_path_without_a_directory_is_refused() {
        let error = build_json_keys()
            .parse_iter_num("state-18446744073709551614.json")
            .expect_err("a path carrying no job directory must not parse");

        assert!(matches!(error, StorageError::Other(_)), "got: {error}");
    }

    #[test]
    fn a_path_whose_number_is_not_a_number_is_refused() {
        let error = build_json_keys()
            .parse_iter_num("jobs/job/state-latest.json")
            .expect_err("a path whose number is not a number must not parse");

        assert!(matches!(error, StorageError::Other(_)), "got: {error}");
    }
}
