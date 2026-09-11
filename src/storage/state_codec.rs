//! Serialization of stored job state, shared by object-store backends.

use std::sync::Arc;

use crate::storage::state::StoredJob;
use crate::{StorageError, StorageResult};

pub(crate) trait JobStateCodec: Send + Sync {
    fn file_extension(&self) -> &'static str;
    fn content_type(&self) -> &'static str;
    fn serialize(&self, job: &StoredJob) -> StorageResult<Vec<u8>>;
    fn deserialize(&self, data: &[u8]) -> StorageResult<StoredJob>;
}

/// Serialization format used to persist job state as a storage object.
#[derive(Debug, Clone, Copy)]
pub enum JobStateCodecKind {
    /// Human-readable JSON. Larger on the wire and slower to (de)serialize than `Cbor`, but
    /// lets an operator read or hand-edit a job's state object directly in a storage console.
    Json,
    /// Compact binary CBOR. Prefer this once state objects are only ever read by the crate
    /// itself, since the smaller payload reduces request latency and storage cost.
    Cbor,
}

impl JobStateCodecKind {
    pub(crate) fn build(self) -> Arc<dyn JobStateCodec> {
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

    fn serialize(&self, job: &StoredJob) -> StorageResult<Vec<u8>> {
        serde_json::to_vec_pretty(job).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    fn deserialize(&self, data: &[u8]) -> StorageResult<StoredJob> {
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

    fn serialize(&self, job: &StoredJob) -> StorageResult<Vec<u8>> {
        let mut buffer = Vec::new();
        ciborium::ser::into_writer(job, &mut buffer).map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(buffer)
    }

    fn deserialize(&self, data: &[u8]) -> StorageResult<StoredJob> {
        ciborium::de::from_reader(data).map_err(|e| StorageError::Serialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Extensions locate existing objects; changing one makes previously saved state invisible.
    #[test]
    fn each_codec_names_its_file_extension_and_content_type() {
        let json_codec = JobStateCodecKind::Json.build();
        let cbor_codec = JobStateCodecKind::Cbor.build();

        assert_eq!(json_codec.file_extension(), ".json");
        assert_eq!(json_codec.content_type(), "application/json");
        assert_eq!(cbor_codec.file_extension(), ".cbor");
        assert_eq!(cbor_codec.content_type(), "application/cbor");
    }
}
