pub mod job;
pub mod schedule;

pub use job::*;
use k8s_openapi::List;
pub use schedule::*;

use kube::CustomResourceExt as _;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub fn generate_crds() -> String {
    serde_yaml::to_string(&List {
        items: vec![BackupJob::crd(), BackupSchedule::crd()],
        ..Default::default()
    })
    .unwrap()
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct SecretRef {
    pub name: String,
    pub namespace: Option<String>,
}
