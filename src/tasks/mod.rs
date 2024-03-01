use chrono::{DateTime, Utc};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
use kube::{
    core::GroupVersionKind,
    discovery::ApiResource,
    runtime::events::{Recorder, Reporter},
    Client, Resource,
};

use crate::{
    crd::{BackupScheduleState, BackupSetState},
    WALLE,
};

pub mod backup;
pub mod schedule;
pub mod set;

/// Cache some expensive kubernetes operations so they're easily accessible throughout operator.
#[derive(Clone)]
pub struct KubeManager {
    client: Client,
    pub snapshot_ar: ApiResource,
    reporter: Reporter,
}

impl KubeManager {
    pub async fn new(client: Client) -> Result<Self, kube::Error> {
        // TODO - Support v1 and v1beta1
        // let apigroup =
        //     kube::discovery::pinned_group(&client, &GroupVersion::gv("snapshot.storage.k8s.io", "v1"))
        //         .await
        //         .unwrap();
        // println!("{:?}", apigroup.recommended_kind("VolumeSnapshot"));
        let gvk = GroupVersionKind::gvk("snapshot.storage.k8s.io", "v1", "VolumeSnapshot");
        let (snapshot_ar, _caps) = kube::discovery::pinned_kind(&client, &gvk).await?;

        Ok(Self { client, snapshot_ar, reporter: format!("{WALLE}-controller").into() })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub fn recorder(&self, resource: &impl Resource<DynamicType = ()>) -> Recorder {
        Recorder::new(self.client(), self.reporter.clone(), resource.object_ref(&()))
    }
}

trait DateTimeFormatK8s
where
    Self: Sized,
{
    fn to_k8s_ts(&self) -> String;
    fn to_restic_ts(&self) -> String;
    fn to_k8s_label(&self) -> String;
    fn from_k8s_ts(timestamp: &str) -> Result<Self, chrono::ParseError>;
}

impl DateTimeFormatK8s for DateTime<Utc> {
    fn to_k8s_ts(&self) -> String {
        self.to_rfc3339_opts(chrono::SecondsFormat::Secs, true) //.replace('T', " ").replace('Z', "")
    }

    fn to_k8s_label(&self) -> String {
        self.to_rfc3339_opts(chrono::SecondsFormat::Secs, true).replace(':', ".")
    }

    fn to_restic_ts(&self) -> String {
        self.to_rfc3339_opts(chrono::SecondsFormat::Secs, true).replace('T', " ").replace('Z', "")
    }

    fn from_k8s_ts(timestamp: &str) -> Result<Self, chrono::ParseError> {
        // Ok(DateTime::parse_from_rfc3339(&format!("{}Z", timestamp.replace(' ', "T")))?.to_utc())
        Ok(DateTime::parse_from_rfc3339(timestamp)?.to_utc())
    }
}

pub fn label_selector_to_filter(selector: &LabelSelector) -> String {
    let mut result = Vec::new();

    if let Some(ref selector) = selector.match_labels {
        for (k, v) in selector {
            result.push(format!("{k}={v}"));
        }
    }

    if let Some(ref selectors) = selector.match_expressions {
        for LabelSelectorRequirement { key, operator, values } in selectors {
            let values = values.as_ref().map(|x| x.join(",")).unwrap_or_default();
            match operator.as_str() {
                "In" => result.push(format!("{key} in ({values})")),
                "NotIn" => result.push(format!("{key} notin ({values})")),
                "Exists" => result.push(key.clone()),
                "DoesNotExist" => result.push(format!("!{key}")),
                x => unreachable!("Invalid operator provided in label selector {}", x),
            }
        }
    }

    result.join(",")
}

impl From<&BackupSetState> for BackupScheduleState {
    fn from(value: &BackupSetState) -> Self {
        match value {
            BackupSetState::Running => BackupScheduleState::Running,
            BackupSetState::Finished => BackupScheduleState::Finished,
            BackupSetState::FinishedWithFailures => BackupScheduleState::FinishedWithFailures,
        }
    }
}
