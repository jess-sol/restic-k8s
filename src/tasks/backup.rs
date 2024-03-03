use chrono::{DateTime, Utc};
use k8s_openapi::{
    api::{
        batch::v1::{Job, JobStatus},
        core::v1::{PersistentVolumeClaim, Secret, ServiceAccount},
        rbac::v1::{ClusterRole, RoleBinding},
    },
    apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::Condition},
};
use kube::{
    api::{Patch, PatchParams, PostParams},
    Api, Resource, ResourceExt as _,
};
use serde_json::json;
use snafu::{OptionExt as _, ResultExt as _};

use crate::{
    config::StorageClassToSnapshotClass, crd::BackupJob, InvalidPVCSnafu, KubeSnafu, Result, WALLE,
};

use std::sync::Arc;
use std::time::Duration;
use std::{cmp::Reverse, str::FromStr as _};
use std::{env, fs::read_to_string};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams, LogParams, PropagationPolicy};
use kube::core::DynamicObject;
use kube::runtime::events::{Event, EventType};

use kube::runtime::controller::Action;
use tracing::{debug, error, info, warn};

use crate::crd::{BackupJobState, BackupJobStatus};
use crate::tasks::DateTimeFormatK8s;
use crate::Context;

use super::{Conditions, PartialCondition};

struct BackupJobInternalState {
    snapshot: Option<DynamicObject>,
    job: Option<Job>,
    source_pvc: Option<PersistentVolumeClaim>,
}

struct JobState {
    active: i32,
    succeeded: i32,
    failed: i32,
    backoff_limit: i32,
}

impl JobState {
    fn active(&self) -> bool {
        self.active > 0
    }
    fn succeeded(&self) -> bool {
        self.succeeded > 0
    }
    fn failed(&self) -> bool {
        self.failed > self.backoff_limit
    }

    fn completed(&self) -> bool {
        self.succeeded() || self.failed()
    }
}

impl BackupJobInternalState {
    fn job_state(&self) -> JobState {
        let backoff_limit = self
            .job
            .as_ref()
            .and_then(|x| x.spec.as_ref())
            .and_then(|x| x.backoff_limit)
            .unwrap_or(3);

        let active = self.job_status().and_then(|x| x.active).unwrap_or(0);
        let succeeded = self.job_status().and_then(|x| x.succeeded).unwrap_or(0);
        let failed = self.job_status().and_then(|x| x.failed).unwrap_or(0);
        JobState { active, succeeded, failed, backoff_limit }
    }

    fn job_status(&self) -> Option<&JobStatus> {
        self.job.as_ref().and_then(|x| x.status.as_ref())
    }

    fn snapshot_status(&self) -> Option<&serde_json::Value> {
        self.snapshot.as_ref().and_then(|x| x.data.get("status"))
    }

    fn snapshot_ready_to_use(&self) -> bool {
        self.snapshot_status()
            .and_then(|x| x.get("readyToUse"))
            .map(|x| x.as_bool().unwrap_or(false))
            .unwrap_or(false)
    }

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
        self.source_pvc
            .as_ref()
            .and_then(|x| x.spec.as_ref())
            .and_then(|x| x.storage_class_name.as_deref())
    }

    fn source_pvc_storage_size(&self) -> Option<&Quantity> {
        self.source_pvc
            .as_ref()
            .and_then(|x| x.spec.as_ref())
            .and_then(|x| x.resources.as_ref())
            .and_then(|x| x.requests.as_ref())
            .and_then(|x| x.get("storage"))
    }
}

impl BackupJob {
    pub async fn reconcile(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        // let recorder = ctx.kube.recorder(self);
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

            return Ok(Action::await_change());
        };

        // Get state of child resources, use to compute current conditions
        let state = self.get_state(ctx.clone()).await?;
        let conditions = Conditions::new(&status.conditions);

        // Don't try to reconcile after job marked finished
        if status.finish_time.is_some() {
            if let Err(err) = cleanup_backup_job(&ctx, &state, self).await {
                error!(name, ns, ?err, "Failed to cleanup BackupJob after finished/failed");
            }
            return Ok(Action::await_change());
        }

        let snap_class = state
            .source_pvc_snap_class(&ctx.config.snap_class_mappings)
            .context(InvalidPVCSnafu)?;

        let snapshots =
            Api::<DynamicObject>::namespaced_with(ctx.kube.client(), &ns, &ctx.kube.snapshot_ar);

        // Create snapshot if it doesn't exist, and hadn't already completed
        if state.snapshot.is_none()
            && conditions.map_or("SnapshotReady", true, |x| x.status != "True")
        {
            let _created_snap = snapshots
                .create(
                    &PostParams::default(),
                    &serde_json::from_value(json!({
                        "apiVersion": "snapshot.storage.k8s.io/v1",
                        "kind": "VolumeSnapshot",
                        "metadata": {
                            "generateName": format!("{WALLE}-{}-", &self.spec.source_pvc),
                            "namespace": self.namespace(),
                            "labels": {
                                "app.kubernetes.io/created-by": WALLE,
                                "ros.io/backup-job": name,
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
                .with_context(|_| KubeSnafu { msg: "Failed to create snapshot for backupjob" })?;
        };

        // Wait till snapshot is ready, then create BackupJob if it doesn't exist and hadn't
        // already completed
        if state.snapshot_ready_to_use()
            && state.job.is_none()
            && conditions.map_or("BackupState", true, |x| x.status != "True")
        {
            let snapshot_creation_time = state
                .snapshot_status()
                .and_then(|x| x.get("creationTime"))
                .and_then(|x| x.as_str())
                .map(|x| {
                    chrono::DateTime::from_str(x)
                        .expect("Unable to parse creationTime from snapshot status")
                })
                .unwrap_or(Utc::now());

            let operator_namespace = self.spec.repository.namespace.clone().unwrap_or_else(|| {
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

            debug!(name, ns, "Creating Job for BackupJob");
            create_backup_job(self, &state, &snapshot_creation_time, operator_namespace, &ctx)
                .await?;
        }

        // If backup Job is completed, and still exists in the cluster, fetch logs from Job, record finish_time,
        // and attempt to cleanup
        let log_res;
        let mut logs = None;
        let mut finish_time = None;
        if state.job_state().completed()
            && conditions.map_or("BackupState", false, |x| x.status == "True")
        {
            log_res = latest_logs_of_job(ctx.kube.client(), state.job.as_ref().unwrap()).await;
            logs = match log_res {
                Ok(Some(ref logs)) => Some(
                    recover_block_in_string(
                        logs,
                        "<<<<<<<<<< START OUTPUT",
                        "END OUTPUT >>>>>>>>>>",
                    )
                    .unwrap_or(logs),
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

            finish_time = Some(
                state
                    .job_status()
                    .and_then(|x| x.completion_time.as_ref())
                    .map(|x| x.0)
                    .unwrap_or(Utc::now()),
            );
        }

        let phase = Self::compute_phase(&state);
        let mut new_conditions = self.compute_conditions(&state, &conditions).await?;
        conditions.merge_new(&mut new_conditions);

        // Keep state up-to-date
        backup_jobs
            .patch_status(
                &name,
                &ps,
                &Patch::Merge(json!({
                    "status": {
                        "conditions": new_conditions,
                        "finishTime": finish_time,
                        "phase": phase,
                        "logs": logs,
                    },
                })),
            )
            .await
            .with_context(|_| KubeSnafu { msg: "Failed up update BackupJob status" })?;

        // If no events were received, check back every 5 minutes
        Ok(Action::requeue(Duration::from_secs(5 * 60)))
    }

    async fn get_state(&self, ctx: Arc<Context<Self>>) -> Result<BackupJobInternalState> {
        let ns = self.namespace().unwrap();
        let name = self.name_any();

        let snapshot_api =
            Api::<DynamicObject>::namespaced_with(ctx.kube.client(), &ns, &ctx.kube.snapshot_ar);
        let mut snapshot = snapshot_api
            .list(&ListParams::default().labels(&format!("ros.io/backup-job={}", name)))
            .await
            .context(KubeSnafu { msg: "Failed to get VolumeSnapshot associated with BackupJob" })?
            .items;
        snapshot.sort_unstable_by_key(|x| Reverse(x.creation_timestamp()));

        let job_api = Api::<Job>::namespaced(ctx.kube.client(), &ns);
        let mut job = job_api
            .list(&ListParams::default().labels(&format!("ros.io/backup-job={}", name)))
            .await
            .context(KubeSnafu { msg: "Failed to get Job associated with BackupJob" })?
            .items;
        job.sort_unstable_by_key(|x| Reverse(x.creation_timestamp()));

        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.kube.client(), &ns);
        let source_pvc = pvcs.get_opt(&self.spec.source_pvc).await.with_context(|_| KubeSnafu {
            msg: format!("Unable to fetch sourcePvc for backupjob {}/{}", ns, name),
        })?;

        Ok(BackupJobInternalState {
            job: job.into_iter().next(),
            snapshot: snapshot.into_iter().next(),
            source_pvc,
        })
    }

    async fn compute_conditions<'a>(
        &'a self, state: &BackupJobInternalState, prev: &Conditions<'a>,
    ) -> Result<Vec<Condition>> {
        let mut conditions = Vec::new();

        // Check state of snapshot
        let condition = if state.snapshot_ready_to_use() {
            PartialCondition { status: "True", reason: "VolumeSnapshotReady", message: "Ready" }
        } else if state.snapshot.is_some() {
            PartialCondition {
                status: "False",
                reason: "VolumeSnapshotWaiting",
                message: "VolumeSnapshot is created but not ready",
            }
        } else if prev.map_or("SnapshotReady", false, |x| x.reason != "NotCreated") {
            PartialCondition {
                status: "False",
                reason: "NotCreated",
                message: "No VolumeSnapshots are associated with BackupJob",
            }
        } else {
            PartialCondition {
                status: "False",
                reason: "NoSnapshotFound",
                message: "VolumeSnapshot associated with BackupJob went missing",
            }
        };

        conditions.push(condition.into_condition("SnapshotReady", self.meta().generation));

        // Check state of Job
        let condition = if state.job.is_some() {
            let job_state = state.job_state();

            if job_state.active() {
                PartialCondition { status: "False", reason: "JobRunning", message: "Job is active" }
            } else if job_state.succeeded() {
                PartialCondition {
                    status: "True",
                    reason: "JobFinished",
                    message: "Job has at least one successful run",
                }
            } else if job_state.failed() {
                PartialCondition {
                    status: "True",
                    reason: "JobFailed",
                    message: "Job has reached maximum number of failed runs",
                }
            } else {
                PartialCondition {
                    status: "False",
                    reason: "JobWaiting",
                    message: "Job has no active runs",
                }
            }
        } else if prev.map_or("BackupState", false, |x| x.reason != "NotCreated") {
            PartialCondition {
                status: "False",
                reason: "JobMissing",
                message: "Job associated with BackupJob went missing",
            }
        } else {
            PartialCondition {
                status: "False",
                reason: "NotCreated",
                message: "No Jobs are associated with BackupJob",
            }
        };

        conditions.push(condition.into_condition("BackupState", self.meta().generation));
        Ok(conditions)
    }

    fn compute_phase(state: &BackupJobInternalState) -> BackupJobState {
        if state.snapshot.is_some() {
            if state.snapshot_ready_to_use() {
                let backup_state = state.job_state();
                if backup_state.active() {
                    BackupJobState::BackingUp
                } else if backup_state.succeeded() {
                    BackupJobState::Finished
                } else if backup_state.failed() {
                    BackupJobState::Failed
                } else {
                    BackupJobState::WaitingBackup
                }
            } else {
                BackupJobState::CreatingSnapshot
            }
        } else {
            BackupJobState::NotStarted
        }
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

async fn cleanup_backup_job(
    ctx: &Context<BackupJob>, state: &BackupJobInternalState, job: &BackupJob,
) -> Result<()> {
    let snapshots = Api::<DynamicObject>::namespaced_with(
        ctx.kube.client(),
        &job.namespace().unwrap(),
        &ctx.kube.snapshot_ar,
    );

    if let Some(ref job) = state.job {
        let jobs: Api<Job> = Api::namespaced(ctx.kube.client(), &job.namespace().unwrap());
        let _job = jobs
            .delete(
                &job.name_any(),
                &DeleteParams {
                    propagation_policy: Some(PropagationPolicy::Foreground),
                    ..Default::default()
                },
            )
            .await
            .with_context(|_| KubeSnafu { msg: "Failed to cleanup job associated with backupjob" });
    }

    if let Some(ref snapshot) = state.snapshot {
        let _ss =
            snapshots.delete(&snapshot.name_any(), &DeleteParams::default()).await.with_context(
                |_| KubeSnafu { msg: "Failed to cleanup snapshot associated with backupjob" },
            );
    }

    Ok(())
}

async fn latest_logs_of_job(
    client: kube::Client, job: &Job,
) -> Result<Option<String>, kube::Error> {
    let pods: Api<Pod> = Api::namespaced(client, job.namespace().as_deref().unwrap());
    let matching_pods = pods
        .list(&ListParams::default().labels(&format!("controller-uid={}", job.uid().unwrap())))
        .await?;
    let mut matching_pods = matching_pods.items;
    matching_pods.sort_unstable_by_key(|x| Reverse(x.creation_timestamp()));
    info!(pods = ?matching_pods.iter().map(|x| x.name_any()).collect::<Vec<_>>(), "LOG PODS");
    let latest_pod = matching_pods.first();

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
    Some(block.trim())
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

async fn create_backup_job(
    backup_job: &BackupJob, state: &BackupJobInternalState, snapshot_creation_time: &DateTime<Utc>,
    operator_namespace: String, ctx: &Context<BackupJob>,
) -> Result<Job> {
    let name = backup_job.name_any();
    let namespace = backup_job.namespace().whatever_context(
        "Unable to get namespace of existing BackupJob, this shouldn't happen.",
    )?;

    let jobs: Api<Job> = Api::namespaced(ctx.kube.client(), &namespace);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.kube.client(), &namespace);

    let storage_class = state.source_pvc_storage_class().context(InvalidPVCSnafu)?;
    let storage_size = state.source_pvc_storage_size().context(InvalidPVCSnafu)?;

    let snapshot = state
        .snapshot
        .as_ref()
        .whatever_context("Trying to create BackupJob but no Snapshot found")?;

    let pvc = pvcs
        .create(
            &PostParams::default(),
            &serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "generateName": format!("walle-backup-{}-", name),
                    "labels": {
                        "app.kubernetes.io/created-by": WALLE,
                    },
                    "ownerReferences": [snapshot.controller_owner_ref(&ctx.kube.snapshot_ar).unwrap()],
                },
                "spec": {
                    "storageClassName": storage_class,
                    "dataSource": {
                        "name": snapshot.name_any(),
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
                "generateName": format!("{WALLE}-backup-{}-", name),
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/created-by": WALLE,
                    "ros.io/backup-job": backup_job.name_any(),
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
