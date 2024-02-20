use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct AppConfig {
    /// Map a storageClass to the volumeSnapshotClass it should use when taking snapshots.
    pub snap_class_mappings: Vec<StorageClassToSnapshotClass>,

    pub cluster_name: String,

    pub backup_job_image: String,

    pub worker_service_account_name: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct StorageClassToSnapshotClass {
    pub storage_class: String,
    pub snapshot_class: String,
}
