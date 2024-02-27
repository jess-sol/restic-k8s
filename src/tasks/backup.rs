use chrono::{DateTime, Utc};
use k8s_openapi::api::{
    batch::v1::Job,
    core::v1::{PersistentVolumeClaim, Secret, ServiceAccount},
    rbac::v1::{ClusterRole, RoleBinding},
};
use kube::{
    api::{Patch, PatchParams, PostParams},
    Api, Client, Resource, ResourceExt as _,
};
use serde_json::json;
use snafu::{OptionExt as _, ResultExt as _};

use crate::{config::AppConfig, crd::BackupJob, InvalidPVCSnafu, KubeSnafu, Result, WALLE};

/// Ensure RBAC is properly configured for the job to be able to access the resources it needs
/// when created with the provided service account in the provided namespace.
/// Namely access to the repository_secret reference.
pub async fn ensure_rbac(
    client: &kube::Client, namespace: String, repository_secret: String,
    repository_secret_ns: String,
) {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), &repository_secret_ns);
    let secret = secrets.get(&repository_secret).await.unwrap();

    // 1. Create serviceaccount/walle-worker in the backup job namespace
    let service_accounts: Api<ServiceAccount> = Api::namespaced(client.clone(), &namespace);

    let service_account = if let Some(sa) = service_accounts.get_opt("walle-worker").await.unwrap()
    {
        sa
    } else {
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
            .await
            .unwrap()
    };

    // 2. Create clusterrole
    let cluster_roles: Api<ClusterRole> = Api::all(client.clone());

    let role_name = format!("walle-read-{}", repository_secret);
    let cluster_role = if let Some(role) = cluster_roles.get_opt(&role_name).await.unwrap() {
        role
    } else {
        cluster_roles
            .create(
                &PostParams::default(),
                &serde_json::from_value(json!({
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "ClusterRole",
                    "metadata": {
                        "name": role_name,
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
            .await
            .unwrap()
    };

    // 3. Create or update rolebinding in secret_ns
    let role_bindings: Api<RoleBinding> = Api::namespaced(client.clone(), &repository_secret_ns);

    let binding_name = format!("walle-read-{}", repository_secret);
    let role_binding =
        if let Some(_role_binding) = role_bindings.get_opt(&binding_name).await.unwrap() {
            role_bindings
                .patch(
                    &binding_name,
                    &PatchParams::apply(WALLE),
                    &Patch::Merge(json!({
                        "subjects": [{
                            "kind": "ServiceAccount",
                            "name": service_account.name_any(),
                            "namespace": namespace,
                        }]
                    })),
                )
                .await
                .unwrap()
        } else {
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
                .await
                .unwrap()
        };
}

pub async fn create_backup_job(
    backup_job: &BackupJob, pvc: &PersistentVolumeClaim, snapshot_creation_time: &DateTime<Utc>,
    operator_namespace: String, settings: &AppConfig, client: &Client,
) -> Result<Job> {
    let name = format!("{WALLE}-{}-", backup_job.spec.source_pvc);
    let namespace = backup_job.namespace().unwrap();

    let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &namespace);

    let snapshot_name =
        backup_job.status.as_ref().and_then(|x| x.destination_snapshot.as_deref()).unwrap();

    let storage_class = pvc
        .spec
        .as_ref()
        .and_then(|x| x.storage_class_name.as_ref())
        .with_context(|| InvalidPVCSnafu)?;
    let storage_size = pvc
        .spec
        .as_ref()
        .and_then(|x| x.resources.as_ref())
        .and_then(|x| x.requests.as_ref())
        .and_then(|x| x.get("storage").cloned())
        .with_context(|| InvalidPVCSnafu)?;

    let pvc = pvcs
        .create(
            &PostParams::default(),
            &serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "generateName": format!("walle-backup-{}", name),
                },
                "spec": {
                    "storageClassName": storage_class,
                    "dataSource": {
                        "name": snapshot_name,
                        "kind": "VolumeSnapshot",
                        "apiGroup": "snapshot.storage.k8s.io"
                    },
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {
                        "requests": {
                            "storage": storage_size,
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let mount_path = format!("/data/{}/", backup_job.spec.source_pvc);
    let start_time = snapshot_creation_time
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        .replace('T', " ")
        .replace('Z', "");

    jobs.create(
        &PostParams::default(),
        &serde_json::from_value(json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "generateName": format!("{WALLE}-backup-{}", name),
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/created-by": WALLE,
                },
                "ownerReferences": backup_job.controller_owner_ref(&()).unwrap(),
            },
            "spec": {
                "template": {
                    "spec": {
                        // TODO - Use specific name of SA in ensure_rbac
                        "serviceAccountName": "walle-worker",
                        "containers": [{
                            "name": "restic",
                            "image": &settings.backup_job_image,
                            "imagePullPolicy": "Always",
                            "args": "backup",
                            "env": [
                                // { "name": "RUST_BACKTRACE", "value": "full".to_string() },
                                // { "name": "RUST_LOG", "value": "trace".to_string() },

                                { "name": "REPOSITORY_SECRET", "value": backup_job.spec.repository },
                                { "name": "OPERATOR_NAMESPACE", "value": &operator_namespace },
                                { "name": "K8S_CLUSTER_NAME", "value": settings.cluster_name },
                                { "name": "TRACE_ID", "value": crate::telemetry::get_trace_id().to_string() },

                                { "name": "SOURCE_PATH", "value": mount_path },
                                { "name": "SNAPSHOT_TIME", "value": start_time },
                                { "name": "PVC_NAME", "value": backup_job.spec.source_pvc },
                            ],
                            "volumeMounts": [{
                                "name": "snapshot",
                                "mountPath": mount_path,
                            }]
                        }],
                        "restartPolicy": "Never",
                        "volumes": [{
                            "name": "snapshot",
                            "persistentVolumeClaim": { "claimName": pvc.name_any() }
                        }],
                    }
                }
            }
        }))
        .unwrap(),
    )
    .await
    .with_context(|_| KubeSnafu { msg: "Failed to create snapshot for backupjob" })
}
