use std::{collections::BTreeMap, fs::read_to_string};

use chrono::{DateTime, Utc};
use k8s_openapi::{
    api::core::v1::PersistentVolumeClaim,
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::{Condition, LabelSelector, LabelSelectorRequirement, Time},
    },
};
use kube::{
    core::GroupVersionKind,
    discovery::ApiResource,
    runtime::events::{Recorder, Reporter},
    Client, Resource,
};

use crate::Result;
use crate::{
    config::StorageClassToSnapshotClass,
    crd::{BackupScheduleState, BackupSetState, FieldOperator, FieldSelector},
    WALLE,
};

pub mod backup;
pub mod job;
pub mod schedule;
pub mod set;

/// Cache some expensive kubernetes operations so they're easily accessible throughout operator.
#[derive(Clone)]
pub struct KubeManager {
    client: Client,
    pub snapshot_ar: ApiResource,
    reporter: Reporter,
    pub operator_namespace: String,
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

        let operator_namespace =
            read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .unwrap_or_else(|_| client.default_namespace().to_owned());

        Ok(Self {
            client,
            snapshot_ar,
            reporter: format!("{WALLE}-controller").into(),
            operator_namespace,
        })
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
    fn to_restic_ts(&self) -> String;
    fn to_k8s_label(&self) -> String;
}

impl DateTimeFormatK8s for DateTime<Utc> {
    fn to_k8s_label(&self) -> String {
        self.to_rfc3339_opts(chrono::SecondsFormat::Secs, true).replace(':', ".")
    }

    fn to_restic_ts(&self) -> String {
        self.to_rfc3339_opts(chrono::SecondsFormat::Secs, true).replace('T', " ").replace('Z', "")
    }
}

pub fn build_label_filter(
    selector: Option<&LabelSelector>, additional_label_selector: &[&str],
) -> String {
    let mut selectors = additional_label_selector.to_vec();
    let to_filter;
    if let Some(selector) = selector {
        to_filter = label_selector_to_filter(selector);
        selectors.push(&to_filter);
    }

    selectors.join(",")
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

pub fn field_selector_to_filter(selectors: &Vec<FieldSelector>) -> String {
    let mut result = Vec::new();
    for FieldSelector { field, operator, value } in selectors {
        match operator {
            FieldOperator::Equals => result.push(format!("{field}={value}")),
            FieldOperator::DoesNotEqual => result.push(format!("{field}!={value}")),
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

/// Merge new conditions into existing conditions, overwriting when necessary.
fn update_conditions(conditions: &mut Vec<Condition>, new_conditions: Vec<Condition>) {
    let mut new_conditions: BTreeMap<_, _> =
        new_conditions.into_iter().map(|x| (x.type_.clone(), x)).collect();

    for current in conditions.iter_mut() {
        if let Some(mut new) = new_conditions.remove(&current.type_) {
            if current.status == new.status {
                new.last_transition_time.0 = current.last_transition_time.0;
            }
            *current = new;
        }
    }

    conditions.extend(new_conditions.into_values());
}

/// Fix last_transition_time in new_conditions. If type/status didn't change, use old timestamp,
/// not new one.
fn merge_conditions(new_conditions: &mut Vec<Condition>, current_conditions: &[Condition]) {
    let current_conditions: BTreeMap<_, _> =
        current_conditions.iter().map(|x| (x.type_.clone(), x)).collect();
    for new in new_conditions {
        if let Some(current) = current_conditions.get(&new.type_) {
            if current.status == new.status {
                new.last_transition_time.0 = current.last_transition_time.0;
            }
        };
    }
}

struct PartialCondition<'a> {
    reason: &'a str,
    status: &'a str,
    message: &'a str,
}

impl<'a> PartialCondition<'a> {
    fn into_condition(self, type_: &str, generation: Option<i64>) -> Condition {
        Condition {
            last_transition_time: Time(Utc::now()),
            message: self.message.to_string(),
            observed_generation: generation,
            reason: self.reason.to_string(),
            status: self.status.to_string(),
            type_: type_.to_string(),
        }
    }
}

#[derive(Debug)]
struct SourcePvc(PersistentVolumeClaim);
impl SourcePvc {
    fn source_pvc_snap_class(
        &self, snap_class_mappings: &[StorageClassToSnapshotClass],
    ) -> Option<String> {
        let storage_class = self.source_pvc_storage_class()?;
        snap_class_mappings
            .iter()
            .find(|x| x.storage_class == *storage_class)
            .map(|x| x.snapshot_class.clone())
    }

    fn source_pvc_storage_class(&self) -> Option<&str> {
        self.0.spec.as_ref().and_then(|x| x.storage_class_name.as_deref())
    }

    fn source_pvc_storage_size(&self) -> Option<&Quantity> {
        self.0
            .spec
            .as_ref()
            .and_then(|x| x.resources.as_ref())
            .and_then(|x| x.requests.as_ref())
            .and_then(|x| x.get("storage"))
    }
}
