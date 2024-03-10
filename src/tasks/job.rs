use k8s_openapi::api::{
    batch::v1::Job,
    core::v1::{Secret, ServiceAccount},
    rbac::v1::{ClusterRole, RoleBinding},
};
use kube::{
    api::{Patch, PatchParams, PostParams},
    Api, Resource, ResourceExt as _,
};
use serde_json::{json, Value};
use snafu::ResultExt as _;
use std::{collections::BTreeMap, env, fmt::Display};

use crate::{
    crd::{BackupJob, BackupSchedule, SecretRef},
    Context, KubeSnafu, Result, WALLE,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum JobType {
    Backup,
    Prune,
    Check,
}

impl Display for JobType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl JobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Prune => "prune",
            Self::Check => "check",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Backup => "Backup",
            Self::Prune => "Prune",
            Self::Check => "Check",
        }
    }

    pub(super) fn scheduler_field(&self) -> &'static str {
        match self {
            Self::Backup => "backupTimestamp",
            Self::Prune => "pruneTimestamp",
            Self::Check => "checkTimestamp",
        }
    }

    pub(super) fn last_run_field(&self) -> &'static str {
        match self {
            Self::Backup => "lastBackupRun",
            Self::Prune => "lastPruneRun",
            Self::Check => "lastCheckRun",
        }
    }
}

pub(super) trait JobOwner {
    fn get_owner_label(&self) -> &str;
    fn get_repo_ref(&self) -> &SecretRef;
}

impl JobOwner for BackupJob {
    fn get_owner_label(&self) -> &str {
        "ros.io/backup-job"
    }

    fn get_repo_ref(&self) -> &SecretRef {
        &self.spec.repository
    }
}

impl JobOwner for BackupSchedule {
    fn get_owner_label(&self) -> &str {
        "ros.io/backup-schedule"
    }

    fn get_repo_ref(&self) -> &SecretRef {
        &self.spec.repository
    }
}

pub(super) async fn create_job<T: Clone + Resource<DynamicType = ()> + JobOwner + 'static>(
    owner: &T, job: JobType, ctx: &Context<T>, config: serde_json::Value,
) -> Result<Job> {
    let name = owner.name_any();
    let namespace = owner.namespace().unwrap_or_else(|| ctx.kube.operator_namespace.clone());
    let owner_label = owner.get_owner_label();
    let repo_ref = owner.get_repo_ref();

    let repo_namespace = repo_ref.namespace.as_deref().unwrap_or(&ctx.kube.operator_namespace);
    ensure_rbac(&ctx.kube.client(), &namespace, &repo_ref.name, repo_namespace)
        .await
        .context(KubeSnafu { msg: "Unable to ensure job RBAC" })?;

    let mut spec = serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "generateName": format!("{WALLE}-{}-{name}-", job.as_str()),
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/created-by": WALLE,
                owner_label: name,
                "ros.io/backup-job-type": job.as_str(),
            },
            "ownerReferences": [owner.controller_owner_ref(&()).unwrap()],
        },
        "spec": {
            "backoffLimit": 3,
            "template": {
                // TODO - Allow config to specify annotations/labels/etc. for jobs
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/created-by": WALLE,
                    }
                },
                "spec": {
                    "serviceAccountName": "walle-worker",
                    "containers": [{
                        "name": "restic",
                        "image": &ctx.config.backup_job_image,
                        "imagePullPolicy": "Always",
                        "args": [job.as_str()],
                        "env": [
                            { "name": "RUST_BACKTRACE", "value": env::var("RUST_BACKTRACE").unwrap_or_default() },
                            { "name": "RUST_LOG", "value": env::var("RUST_LOG").unwrap_or_default() },

                            { "name": "REPOSITORY_SECRET", "value": repo_ref.to_string() },
                            { "name": "OPERATOR_NAMESPACE", "value": ctx.kube.operator_namespace },
                            { "name": "K8S_CLUSTER_NAME", "value": ctx.config.cluster_name },
                            { "name": "TRACE_ID", "value": crate::telemetry::get_trace_id().to_string() },
                        ],
                    }],
                    "restartPolicy": "Never",
                }
            }
        }
    });

    merge(&mut spec, config);

    let jobs: Api<Job> = Api::namespaced(ctx.kube.client(), &namespace);
    jobs.create(
        &PostParams::default(),
        &serde_json::from_value(spec).expect("Failed to serialize predefined Job template for job"),
    )
    .await
    .with_context(|_| KubeSnafu { msg: "Failed to create Job for backupjob" })
}

/// Ensure RBAC is properly configured for the job to be able to access the resources it needs
/// when created with the provided service account in the provided namespace.
/// Namely access to the repository_secret reference.
async fn ensure_rbac(
    client: &kube::Client, namespace: &str, repository_secret: &str, repository_secret_ns: &str,
) -> Result<(), kube::Error> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), repository_secret_ns);
    let secret = secrets.get(repository_secret).await?;

    // 1. Create serviceaccount/walle-worker in the backup job namespace
    let service_accounts: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);

    let service_account = match service_accounts.get_opt("walle-worker").await? {
        Some(sa) => sa,
        None => {
            service_accounts
                .create(
                    &PostParams::default(),
                    &serde_json::from_value(json!({
                        "apiVersion": "v1",
                        "kind": "ServiceAccount",
                        "metadata": {
                            "name": "walle-worker",
                            // TODO - Add helm labels
                        }
                    }))
                    .expect("Invalid predefined service account json"),
                )
                .await?
        }
    };

    // 2. Create clusterrole
    let cluster_roles: Api<ClusterRole> = Api::all(client.clone());

    let role_name = format!("walle-read-{}", repository_secret);

    let cluster_role = match cluster_roles.get_opt(&role_name).await? {
        Some(role) => role,
        None => {
            cluster_roles
                .create(
                    &PostParams::default(),
                    &serde_json::from_value(json!({
                        "apiVersion": "rbac.authorization.k8s.io/v1",
                        "kind": "ClusterRole",
                        "metadata": {
                            "name": role_name,
                            // TODO - Cluster -> namespace ownership doesn't work
                            "ownerReferences": [secret.controller_owner_ref(&())],
                        },
                        "rules": [{
                            "apiGroups": [""],
                            "resources": ["secrets"],
                            "resourceNames": [repository_secret],
                            "verbs": ["get"],
                        }],
                    }))
                    .expect("Invalid predefined cluster role json"),
                )
                .await?
        }
    };

    // 3. Create or update rolebinding in secret_ns
    let role_bindings: Api<RoleBinding> = Api::namespaced(client.clone(), repository_secret_ns);

    let binding_name = format!("walle-read-{}", repository_secret);
    let _role_binding = match role_bindings.get_opt(&binding_name).await? {
        Some(existing_binding) => {
            let mut subjects =
                existing_binding.subjects.as_ref().map_or_else(Vec::new, |x| x.clone());

            if !subjects.iter().any(|x| {
                x.name == service_account.name_any() && x.namespace.as_deref() == Some(namespace)
            }) {
                subjects.push(k8s_openapi::api::rbac::v1::Subject {
                    kind: "ServiceAccount".into(),
                    name: service_account.name_any(),
                    namespace: Some(namespace.into()),
                    ..Default::default()
                });
            }

            role_bindings
                .patch(
                    &existing_binding.name_any(),
                    &PatchParams::apply(WALLE),
                    &Patch::Merge(json!({
                        "subjects": subjects,
                    })),
                )
                .await?
        }
        None => {
            role_bindings
                .create(
                    &PostParams::default(),
                    &serde_json::from_value(json!({
                        "apiVersion": "rbac.authorization.k8s.io/v1",
                        "kind": "RoleBinding",
                        "metadata": {
                            "name": format!("walle-read-{}", repository_secret),
                            "ownerReferences": [secret.controller_owner_ref(&())],
                        },
                        "roleRef": {
                            "apiGroup": "rbac.authorization.k8s.io",
                            "kind": "ClusterRole",
                            "name": cluster_role.name_any(),
                        },
                        "subjects": [{
                            "kind": "ServiceAccount",
                            "name": service_account.name_any(),
                            "namespace": namespace,
                        }]
                    }))
                    .expect("Invalid predefined cluster role json"),
                )
                .await?
        }
    };

    Ok(())
}

fn merge(a: &mut Value, b: Value) {
    match (a, b) {
        // Merge objects by key
        (Value::Object(a), Value::Object(b)) => {
            for (k, v) in b {
                if v.is_null() {
                    a.remove(&k);
                } else {
                    merge(a.entry(k).or_insert(Value::Null), v);
                }
            }
        }
        // If list of objects with field 'name', merge by 'name' field
        (Value::Array(a), Value::Array(b))
            if a.iter().all(|x| x.get("name").is_some())
                && b.iter().all(|x| x.get("name").is_some()) =>
        {
            let mut by_name: BTreeMap<_, _> = b
                .into_iter()
                .filter_map(|x| Some((x.get("name")?.as_str()?.to_owned(), x)))
                .collect();
            for el in a.iter_mut() {
                if let Some(new_el) =
                    el.get("name").and_then(|x| x.as_str()).and_then(|x| by_name.remove(x))
                {
                    merge(el, new_el);
                }
            }
            a.extend(by_name.into_values());
        }
        // Default to atomic merge
        (a, b) => *a = b,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::merge;

    #[test]
    fn merge_by_name_works() {
        let mut a = json!({ "containers": [
            { "name": "test", "args": ["blah"], "image": "blah" },
            { "name": "test2", "args": ["2"] }
        ]});

        let b = json!({ "containers": [
            { "name": "test", "args": ["other"] },
            { "name": "test3", "args": ["3"] }
        ]});

        let expected = json!({ "containers": [
            { "name": "test", "args": ["other"], "image": "blah" },
            { "name": "test2", "args": ["2"] },
            { "name": "test3", "args": ["3"] }
        ]});

        merge(&mut a, b);
        assert_eq!(a, expected);
    }
}
