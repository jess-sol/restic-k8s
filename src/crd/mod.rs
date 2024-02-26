pub mod job;
pub mod schedule;

use std::fmt::Display;

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

impl Display for SecretRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref ns) = self.namespace {
            write!(f, "{}/", ns)?;
        }
        write!(f, "{}", self.name)?;
        Ok(())
    }
}
