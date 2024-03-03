use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
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
#[kube(printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".status.startTime"}"#)]
#[serde(rename_all = "camelCase")]
pub struct BackupJobSpec {
    pub source_pvc: String,

    pub before_snapshot: Option<String>,
    pub after_snapshot: Option<String>,

    pub repository: SecretRef,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupJobStatus {
    pub start_time: Option<Time>,
    pub finish_time: Option<Time>,
    pub phase: BackupJobState,
    pub conditions: Vec<Condition>,
    pub logs: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
pub enum BackupJobState {
    NotStarted,
    CreatingSnapshot,
    WaitingBackup,
    BackingUp,
    Finished,
    Failed,
}

impl Default for BackupJobState {
    fn default() -> Self {
        Self::NotStarted
    }
}
