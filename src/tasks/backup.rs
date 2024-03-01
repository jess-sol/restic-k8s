use chrono::{DateTime, Utc};
use k8s_openapi::api::{
    batch::v1::Job,
    core::v1::{PersistentVolumeClaim, Secret, ServiceAccount},
    rbac::v1::{ClusterRole, RoleBinding},
};
use kube::{
    api::{Patch, PatchParams, PostParams},
    Api, Resource, ResourceExt as _,
};
use serde_json::json;
use snafu::{OptionExt as _, ResultExt as _};

use crate::{crd::BackupJob, InvalidPVCSnafu, KubeSnafu, Result, WALLE};

use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs::read_to_string};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams, LogParams, PropagationPolicy};
use kube::core::DynamicObject;
use kube::runtime::events::{Event, EventType};

use kube::runtime::controller::Action;
use tracing::{debug, error, warn};

use crate::crd::{BackupJobState, BackupJobStatus};
use crate::tasks::DateTimeFormatK8s;
use crate::Context;

impl BackupJob {
    pub async fn reconcile(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        let recorder = ctx.kube.recorder(self);
        let ns = self.namespace().unwrap();
        let name = self.name_any();
        let backup_jobs: Api<BackupJob> = Api::namespaced(ctx.kube.client(), &ns);
        let ps = PatchParams::apply(WALLE);

        // Set initial status if none
        let Some(status) = self.status.as_ref() else {
            let _o = backup_jobs
                .patch_status(
                    &name,
                    &ps,
                    &Patch::Merge(json!({
                        "status": BackupJobStatus::default(),
                    })),
                )
                .await
                .with_context(|_| KubeSnafu { msg: "Failed up update backupjob status" })?;

            return Ok(Action::requeue(Duration::from_secs(5)));
        };

        let snapshot_name = status.destination_snapshot.as_deref();
        let job_name = status.backup_job.as_deref();

        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.kube.client(), &ns);
        let Some(pvc) = pvcs.get_opt(&self.spec.source_pvc).await.with_whatever_context(|_| {
            format!("Unable to fetch sourcePvc for backupjob {}/{}", ns, name)
        })?
        else {
            recorder
                .publish(Event {
                    type_: EventType::Warning,
                    reason: "MissingPVC".into(),
                    note: Some("Unable to find source_pvc".to_string()),
                    action: "Waiting".into(),
                    secondary: None,
                })
                .await
                .with_context(|_| KubeSnafu { msg: "Unable to send event for backupjob" })?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        };

        let pvc_spec = pvc.spec.as_ref().with_context(|| InvalidPVCSnafu)?;
        let storage_class =
            pvc_spec.storage_class_name.as_ref().with_context(|| InvalidPVCSnafu)?;

        let snap_class = ctx
            .config
            .snap_class_mappings
            .iter()
            .find(|x| x.storage_class == *storage_class)
            .map(|x| &x.snapshot_class);

        let snapshots =
            Api::<DynamicObject>::namespaced_with(ctx.kube.client(), &ns, &ctx.kube.snapshot_ar);

        match status.state {
            BackupJobState::NotStarted => {
                let created_snap = snapshots
                    .create(
                        &PostParams::default(),
                        &serde_json::from_value(json!({
                            "apiVersion": "snapshot.storage.k8s.io/v1",
                            "kind": "VolumeSnapshot",
                            "metadata": {
                                "generateName": format!("{WALLE}-{}", &self.spec.source_pvc),
                                "namespace": self.namespace(),
                                "labels": {
                                    "app.kubernetes.io/created-by": WALLE,
                                },
                                "ownerReferences": [self.controller_owner_ref(&()).unwrap()],
                            },
                            "spec": {
                                "volumeSnapshotClassName": snap_class,
                                "source": {
                                    "persistentVolumeClaimName": &self.spec.source_pvc,
                                }
                            }
                        }))
                        .with_whatever_context(|_| {
                            format!(
                                "Failed to parse predefined VolumeSnapshot for BackupJob {}/{}",
                                ns, name,
                            )
                        })?,
                    )
                    .await
                    .with_context(|_| KubeSnafu {
                        msg: "Failed to create snapshot for backupjob",
                    })?;

                let _o = backup_jobs
                    .patch_status(
                        &name,
                        &ps,
                        &Patch::Merge(json!({
                            "status": {
                                "state": BackupJobState::CreatingSnapshot,
                                "destinationSnapshot": Some(created_snap.name_any()),
                            },
                        })),
                    )
                    .await
                    .with_context(|_| KubeSnafu { msg: "Failed up update backupjob status" })?;
            }
            BackupJobState::CreatingSnapshot => {
                let snapshot_name = snapshot_name.with_whatever_context(|| format!("BackupJob {}/{} in CreatingSnapshot state without status.destinationSnapshot set", ns, name))?;
                let snapshot = snapshots.get(snapshot_name).await.with_context(|_| KubeSnafu {
                    msg: format!("Unable to get referenced snapshot: {}", snapshot_name),
                })?;

                let Some(snapshot_status) = snapshot.data.get("status") else {
                    return Ok(Action::requeue(Duration::from_secs(5)));
                };

                let is_ready = snapshot_status
                    .get("readyToUse")
                    .map(|x| x.as_bool().unwrap_or(false))
                    .unwrap_or(false);

                if !is_ready {
                    return Ok(Action::requeue(Duration::from_secs(5)));
                }

                let snapshot_creation_time = snapshot_status
                    .get("creationTime")
                    .and_then(|x| x.as_str())
                    .map(|x| {
                        chrono::DateTime::from_str(x)
                            .expect("Unable to parse creationTime from snapshot status")
                    })
                    .unwrap_or(Utc::now());

                let operator_namespace =
                    self.spec.repository.namespace.clone().unwrap_or_else(|| {
                        read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                            .expect("Unable to get default repository secret namespace")
                    });

                if let Err(err) = ensure_rbac(
                    &ctx.kube.client(),
                    &ns,
                    &self.spec.repository.name,
                    &operator_namespace,
                )
                .await
                {
                    error!(name, ns, ?err, "Unable to configure rbac to create backup job");
                    return Ok(Action::requeue(Duration::from_secs(60)));
                }

                // Set state here because creating backupjob will trigger reconciliation, and
                // sometimes that leads to two Jobs being created.
                let _o = backup_jobs
                    .patch_status(
                        &name,
                        &ps,
                        &Patch::Merge(json!({
                            "status": {
                                "state": BackupJobState::BackingUp,
                                "startTime": Some(snapshot_creation_time.to_k8s_ts()),
                            },
                        })),
                    )
                    .await
                    .with_context(|_| KubeSnafu { msg: "Failed up update backupjob status" })?;

                debug!(name, ns, "Creating Job for BackupJob");
                let created_job = create_backup_job(
                    self,
                    &pvc,
                    &snapshot_creation_time,
                    operator_namespace,
                    &ctx,
                )
                .await?;

                let _o = backup_jobs
                    .patch_status(
                        &name,
                        &ps,
                        &Patch::Merge(json!({
                            "status": {
                                "backupJob": Some(created_job.name_any()),
                            },
                        })),
                    )
                    .await
                    .with_context(|_| KubeSnafu { msg: "Failed up update backupjob status" })?;
            }

            BackupJobState::BackingUp => {
                // Ignore reconcilations that happen when state is backingup, but job_name isn't
                // set. This is a transient state.
                let Some(job_name) = job_name else {
                    return Ok(Action::await_change());
                };

                let jobs: Api<Job> = Api::namespaced(ctx.kube.client(), &ns);
                let Some(job) = jobs
                    .get_opt(job_name)
                    .await
                    .with_context(|_| KubeSnafu { msg: "Failed to retrieve jobs" })?
                else {
                    // TODO - Mark BackupJob as failure?
                    return Ok(Action::requeue(Duration::from_secs(5)));
                };

                let succeeded = job.status.as_ref().map(|x| x.succeeded.unwrap_or(0)).unwrap_or(0);
                let failed = job.status.as_ref().map(|x| x.failed.unwrap_or(0)).unwrap_or(0);

                let backoff_limit = job.spec.as_ref().and_then(|x| x.backoff_limit).unwrap_or(3);
                if succeeded > 0 || failed >= backoff_limit {
                    // Fetch logs of latest job to add to completion event
                    let logs = latest_logs_of_job(ctx.kube.client(), &job).await;
                    let logs = match logs {
                        Ok(Some(ref logs)) => recover_block_in_string(
                            logs,
                            "<<<<<<<<<< START OUTPUT",
                            "END OUTPUT >>>>>>>>>>",
                        ),
                        Ok(None) => {
                            warn!(name, ns, "No logs available for BackupJob");
                            None
                        }
                        Err(err) => {
                            error!(name, ns, ?err, "Failed to fetch logs for BackupJob");
                            None
                        }
                    };

                    if let Err(err) = cleanup_backup_job(&ctx, self).await {
                        error!(name, ns, ?err, "Failed to cleanup BackupJob after finished/failed");
                    }

                    let is_success = succeeded > 0;

                    let _o = backup_jobs
                        .patch_status(
                            &name,
                            &ps,
                            &Patch::Merge(json!({
                                "status": {
                                    "state": if is_success {
                                        BackupJobState::Finished
                                    } else {
                                        BackupJobState::Failed
                                    },
                                    "backupJob": Option::<String>::None,
                                    "destinationSnapshot": Option::<String>::None,
                                    "finishTime": Some(Utc::now().to_k8s_ts()),
                                },
                            })),
                        )
                        .await
                        .with_context(|_| KubeSnafu { msg: "Failed up update backupjob status" })?;

                    recorder
                        .publish(Event {
                            type_: if is_success { EventType::Normal } else { EventType::Warning },
                            reason: "TaskLogs".into(),
                            note: logs.map(|logs| format!("Restic logs: {}", logs)),
                            action: if is_success { "Finished".into() } else { "Failed".into() },
                            secondary: None,
                        })
                        .await
                        .with_context(|_| KubeSnafu {
                            msg: "Unable to send event for backupjob",
                        })?;
                }
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            BackupJobState::Finished | BackupJobState::Failed => {
                return Ok(Action::await_change());
            }
        }

        // If no events were received, check back every 5 minutes
        Ok(Action::requeue(Duration::from_secs(5 * 60)))
    }

    // Finalizer cleanup (the object was deleted, ensure nothing is orphaned)
    pub async fn cleanup(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        let recorder = ctx.kube.recorder(self);
        let result = recorder
            .publish(Event {
                type_: EventType::Normal,
                reason: "DeleteRequested".into(),
                note: Some(format!("Delete `{}`", self.name_any())),
                action: "Deleting".into(),
                secondary: None,
            })
            .await;
        // Don't let failure to update the events of the resource stop the finalizer from
        // completing. This breaks the deletion of namespaces.
        if let Err(err) = result {
            warn!(?err, backup_job = ?self, "Failed to add deletion event to backupjob");
        }
        Ok(Action::await_change())
    }
}

async fn cleanup_backup_job(ctx: &Context<BackupJob>, job: &BackupJob) -> Result<()> {
    let snapshots = Api::<DynamicObject>::namespaced_with(
        ctx.kube.client(),
        &job.namespace().unwrap(),
        &ctx.kube.snapshot_ar,
    );

    let snapshot_name = job.status.as_ref().and_then(|x| x.destination_snapshot.as_deref());
    let job_name = job.status.as_ref().and_then(|x| x.backup_job.as_deref());

    if let Some(job_name) = job_name {
        let jobs: Api<Job> = Api::namespaced(ctx.kube.client(), &job.namespace().unwrap());
        let _job = jobs
            .delete(
                job_name,
                &DeleteParams {
                    propagation_policy: Some(PropagationPolicy::Foreground),
                    ..Default::default()
                },
            )
            .await
            .with_context(|_| KubeSnafu { msg: "Failed to cleanup job associated with backupjob" });
    }
    if let Some(snapshot_name) = snapshot_name {
        let _ss =
            snapshots.delete(snapshot_name, &DeleteParams::default()).await.with_context(|_| {
                KubeSnafu { msg: "Failed to cleanup snapshot associated with backupjob" }
            });
    }

    Ok(())
}

async fn latest_logs_of_job(
    client: kube::Client, job: &Job,
) -> Result<Option<String>, kube::Error> {
    let pods: Api<Pod> = Api::default_namespaced(client);
    let matching_pods = pods
        .list(&ListParams::default().labels(&format!("controller-uid={}", job.uid().unwrap())))
        .await?;
    let latest_pod = matching_pods
        .items
        .into_iter()
        .max_by(|x, y| x.creation_timestamp().cmp(&y.creation_timestamp()));

    Ok(match latest_pod {
        Some(pod) => Some(pods.logs(&pod.name_any(), &LogParams::default()).await?),
        None => None,
    })
}

fn recover_block_in_string<'a>(
    lines: &'a str, delim_start: &'_ str, delim_end: &'_ str,
) -> Option<&'a str> {
    let (_, tail) = lines.split_once(delim_start)?;
    let (block, _) = tail.split_once(delim_end)?;
    Some(block)
}

/// Ensure RBAC is properly configured for the job to be able to access the resources it needs
/// when created with the provided service account in the provided namespace.
/// Namely access to the repository_secret reference.
pub async fn ensure_rbac(
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
            role_bindings
                .patch(
                    &existing_binding.name_any(),
                    &PatchParams::apply(WALLE),
                    &Patch::Merge(json!({
                        "subjects": [{
                            "kind": "ServiceAccount",
                            "name": service_account.name_any(),
                            "namespace": namespace,
                        }]
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

pub async fn create_backup_job(
    backup_job: &BackupJob, pvc: &PersistentVolumeClaim, snapshot_creation_time: &DateTime<Utc>,
    operator_namespace: String, ctx: &Context<BackupJob>,
) -> Result<Job> {
    let name = format!("{WALLE}-{}-", backup_job.spec.source_pvc);
    let namespace = backup_job.namespace().whatever_context(
        "Unable to get namespace of existing backup_job, this shouldn't happen.",
    )?;

    let jobs: Api<Job> = Api::namespaced(ctx.kube.client(), &namespace);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.kube.client(), &namespace);

    let snapshot_name = backup_job
        .status
        .as_ref()
        .and_then(|x| x.destination_snapshot.as_deref())
        .whatever_context(
            "No destinationSnapshot specified, but trying to create Job, status state inconsistent.",
        )?;

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
                "ownerReferences": [backup_job.controller_owner_ref(&()).unwrap()],
            },
            "spec": {
                "backoffLimit": 3,
                "template": {
                    "spec": {
                        // TODO - Use specific name of SA in ensure_rbac
                        "serviceAccountName": "walle-worker",
                        "containers": [{
                            "name": "restic",
                            "image": &ctx.config.backup_job_image,
                            "imagePullPolicy": "Always",
                            "args": ["backup"],
                            "env": [
                                { "name": "RUST_BACKTRACE", "value": env::var("RUST_BACKTRACE").unwrap_or_default() },
                                { "name": "RUST_LOG", "value": env::var("RUST_LOG").unwrap_or_default() },

                                { "name": "REPOSITORY_SECRET", "value": backup_job.spec.repository.to_string() },
                                { "name": "OPERATOR_NAMESPACE", "value": &operator_namespace },
                                { "name": "K8S_CLUSTER_NAME", "value": ctx.config.cluster_name },
                                { "name": "TRACE_ID", "value": crate::telemetry::get_trace_id().to_string() },

                                { "name": "SOURCE_PATH", "value": mount_path },
                                { "name": "SNAPSHOT_TIME", "value": snapshot_creation_time.to_restic_ts() },
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
