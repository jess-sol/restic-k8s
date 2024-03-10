use chrono::{DateTime, Utc};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub static BACKUP_SET_FINALIZER: &str = "ros.io/backup-set";

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
// #[cfg_attr(test, derive(Default))]
#[kube(kind = "BackupSet", group = "ros.io", version = "v1")]
#[kube(status = "BackupSetStatus", shortname = "backup-set")]
#[kube(
    printcolumn = r#"{"name":"Completions", "type":"string", "jsonPath":".status.completions"}"#
)]
#[kube(
    printcolumn = r#"{"name":"Status", "type":"string", "description":"Status of BackupSet", "jsonPath":".status.state"}"#
)]
#[kube(printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#)]
#[serde(rename_all = "camelCase")]
pub struct BackupSetSpec {
    pub selector: LabelSelector,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupSetStatus {
    pub state: BackupSetState,
    pub finish_time: Option<DateTime<Utc>>,
    pub completions: String,
    pub statistics: Statistics,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Statistics {
    pub total_jobs: usize,
    pub running_jobs: usize,
    pub finished_jobs: usize,
    pub failed_jobs: usize,
    pub unstarted_jobs: usize,
}

impl ToString for Statistics {
    fn to_string(&self) -> String {
        format!(
            "{}→ {}↑ {}↓ / {}",
            self.unstarted_jobs + self.running_jobs,
            self.finished_jobs,
            self.failed_jobs,
            self.total_jobs
        )
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
pub enum BackupSetState {
    #[default]
    Running,
    Finished,
    FinishedWithFailures,
}

impl BackupSetState {
    pub fn as_str(&self) -> &'static str {
        match self {
            BackupSetState::Running => "Running",
            BackupSetState::Finished => "Finished",
            BackupSetState::FinishedWithFailures => "FinishedWithFailures",
        }
    }
}
