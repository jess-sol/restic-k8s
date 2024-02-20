use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::SecretRef;

pub static BACKUP_JOB_FINALIZER: &str = "ros.io/backup-job";

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
// #[cfg_attr(test, derive(Default))]
#[kube(kind = "BackupJob", group = "ros.io", version = "v1", namespaced)]
#[kube(status = "BackupJobStatus", shortname = "backup-job")]
#[kube(
    printcolumn = r#"{"name":"Status", "type":"string", "description":"Status of BackupJob", "jsonPath":".status.state"}"#
)]
#[kube(
    printcolumn = r#"{"name":"StartTime", "type":"date", "description":"When backup was started", "jsonPath":".status.startTime"}"#
)]
#[kube(printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".status.creationTime"}"#)]
pub struct BackupJobSpec {
    pub source_pvc: String,

    pub before_snapshot: Option<String>,
    pub after_snapshot: Option<String>,

    pub repository: SecretRef,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct BackupJobStatus {
    pub state: BackupJobState,
    pub start_time: Option<DateTime<Utc>>,
    pub finish_time: Option<DateTime<Utc>>,
    pub destination_snapshot: Option<String>,
    pub backup_job: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub enum BackupJobState {
    NotStarted,
    CreatingSnapshot,
    BackingUp,
    Finished,
    Failed,
}

impl Default for BackupJobState {
    fn default() -> Self {
        Self::NotStarted
    }
}
