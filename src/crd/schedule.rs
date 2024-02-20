use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::SecretRef;

pub static BACKUP_SCHEDULE_FINALIZER: &str = "ros.io/backup-schedule";

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
// #[cfg_attr(test, derive(Default))]
#[kube(kind = "BackupSchedule", group = "ros.io", version = "v1", namespaced)]
#[kube(status = "BackupScheduleStatus", shortname = "backup-schedule")]
pub struct BackupScheduleSpec {
    /// Reference to the secret containing the necessary environment variables to connect to the
    /// Restic repository.
    /// See https://volsync.readthedocs.io/en/stable/usage/restic/index.html for details.
    pub repository: SecretRef,

    pub interval: CronJobInterval,
    pub prune_interval: CronJobInterval,
    pub check_interval: CronJobInterval,

    pub retain: RetentionSpec,

    /// List of backup plans to run on schedule. The first to match a workload or PVC will be used,
    /// overriding any following plans.
    pub plans: Vec<BackupPlanSpec>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct BackupPlanSpec {
    /// Selects resources which should be backed up. May select Deployments, Statefulsets, PVCs,
    /// etc.
    pub selector: Vec<CombinatorSelector>,

    /// Any workload resources selected by `selector` will then have their PVCs filtered using this
    /// selector.
    pub pvc_selector: Vec<CombinatorSelector>,

    /// Run in the pod a PVC is mounted to before a snapshot is taken of the PVC
    pub before_snapshot: String,

    /// Run in the pod a PVC is mounted to after a snapshot is taken of the PVC
    pub after_snapshot: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub enum CombinatorSelector {
    // If left blank will match any resource
    Any(Vec<UnarySelector>),
    // If left blank will match any resource
    All(Vec<UnarySelector>),
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub enum UnarySelector {
    Has(WorkloadSelector),
    Not(WorkloadSelector),
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub enum WorkloadSelector {
    Name(String),
    NameMatching(String),
    Label { name: String, value: String },
    Annotation { name: String, value: String },
    Type(String),
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct RetentionSpec {
    pub hourly: u32,
    pub daily: u32,
    pub weekly: u32,
    pub monthly: u32,
    pub yearly: u32,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct BackupScheduleStatus {
    pub condition: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct CronJobInterval(String);
