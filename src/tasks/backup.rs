use chrono::{DateTime, Utc};
use k8s_openapi::{
    api::{batch::v1::Job, core::v1::PersistentVolumeClaim},
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
};
use kube::{
    api::{Patch, PatchParams, PostParams, PropagationPolicy},
    Api, Resource, ResourceExt as _,
};
use serde_json::{json, Value};
use snafu::{OptionExt as _, ResultExt as _};

use crate::{crd::BackupJob, tasks::merge_conditions, InvalidPVCSnafu, KubeSnafu, Result, WALLE};

use std::sync::Arc;
use std::time::Duration;
use std::{cmp::Reverse, str::FromStr as _};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams, LogParams};
use kube::core::DynamicObject;
use kube::runtime::events::{Event, EventType};

use kube::runtime::controller::Action;
use tracing::{debug, error, info, warn};

use crate::crd::BackupJobState;
use crate::tasks::DateTimeFormatK8s;
use crate::Context;

use super::{
    job::{create_job, JobType},
    PartialCondition, SourcePvc,
};

#[derive(Debug)]
struct SnapshotExt(DynamicObject);
impl SnapshotExt {
    fn snapshot_status(&self) -> Option<&Value> {
        self.0.data.get("status")
    }

    fn snapshot_ready_to_use(&self) -> bool {
        self.snapshot_status()
            .and_then(|x| x.get("readyToUse"))
            .map(|x| x.as_bool().unwrap_or(false))
            .unwrap_or(false)
    }

    fn snapshot_creation_time(&self) -> Option<DateTime<Utc>> {
        self.snapshot_status().and_then(|x| x.get("creationTime")).and_then(Value::as_str).map(
            |x| {
                chrono::DateTime::from_str(x)
                    .expect("Unable to parse creationTime from snapshot status")
            },
        )
    }
}

#[derive(Debug)]
struct JobExt(Job);
impl JobExt {
    fn job_active(&self) -> bool {
        self.0.status.as_ref().and_then(|x| x.active).unwrap_or(0) > 0
    }
    fn job_succeeded(&self) -> bool {
        self.0.status.as_ref().and_then(|x| x.succeeded).unwrap_or(0) > 0
    }
    fn job_failed(&self) -> bool {
        let backoff_limit = self.0.spec.as_ref().and_then(|x| x.backoff_limit).unwrap_or(3);
        self.0.status.as_ref().and_then(|x| x.failed).unwrap_or(0) > backoff_limit
    }
    fn job_completed(&self) -> bool {
        self.job_succeeded() || self.job_failed()
    }

    fn job_completion_time(&self) -> Option<&DateTime<Utc>> {
        self.0.status.as_ref().and_then(|x| x.completion_time.as_ref()).map(|x| &x.0)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum InternalState {
    SourcePVCMissing,
    NotStarted {
        source_pvc: SourcePvc,
    },
    SnapshotExists {
        source_pvc: SourcePvc,
        snapshot: SnapshotExt,
    },
    JobExists {
        source_pvc: SourcePvc,
        snapshot: SnapshotExt,
        job: JobExt,
    },
    Finished {
        source_pvc: Option<SourcePvc>,
        snapshot: Option<SnapshotExt>,
        job: Option<JobExt>,
        finish_time: DateTime<Utc>,
    },
}

impl std::fmt::Display for InternalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternalState::SourcePVCMissing => write!(f, "SourcePVCMissing"),
            InternalState::NotStarted { .. } => write!(f, "NotStarted"),
            InternalState::SnapshotExists { .. } => write!(f, "SnapshotExists"),
            InternalState::JobExists { .. } => write!(f, "JobExists"),
            InternalState::Finished { .. } => write!(f, "Finished"),
        }
    }
}

impl InternalState {
    fn new(
        source_pvc: Option<PersistentVolumeClaim>, snapshot: Option<DynamicObject>,
        job: Option<Job>, finish_time: Option<DateTime<Utc>>,
    ) -> Self {
        if let Some(finish_time) = finish_time {
            return Self::Finished {
                source_pvc: source_pvc.map(SourcePvc),
                snapshot: snapshot.map(SnapshotExt),
                job: job.map(JobExt),
                finish_time,
            };
        }

        let Some(source_pvc) = source_pvc else {
            return Self::SourcePVCMissing;
        };

        let source_pvc = SourcePvc(source_pvc);
        let Some(snapshot) = snapshot else {
            return Self::NotStarted { source_pvc };
        };

        let snapshot = SnapshotExt(snapshot);
        let Some(job) = job else {
            return Self::SnapshotExists { source_pvc, snapshot };
        };

        let job = JobExt(job);
        Self::JobExists { source_pvc, snapshot, job }
    }

    fn conditions(&self, generation: Option<i64>) -> Vec<Condition> {
        let mut conditions = Vec::new();

        let condition = match self {
            Self::SourcePVCMissing => PartialCondition {
                status: "False",
                reason: "NotCreated",
                message: "Source PVC missing",
            },
            Self::NotStarted { .. } => PartialCondition {
                status: "False",
                reason: "NotCreated",
                message: "No VolumeSnapshots are associated with BackupJob",
            },
            Self::SnapshotExists { .. } => PartialCondition {
                status: "False",
                reason: "Waiting",
                message: "VolumeSnapshot is created but not ready",
            },
            Self::JobExists { .. } | Self::Finished { snapshot: Some(_), .. } => PartialCondition {
                status: "True",
                reason: "Ready",
                message: "VolumeSnapshot is ready",
            },
            Self::Finished { .. } => PartialCondition {
                status: "True",
                reason: "CleanedUp",
                message: "BackupJob finished and Snapshot doesn't exist",
            },
        };
        conditions.push(condition.into_condition("SnapshotReady", generation));

        let condition = match self {
            Self::SourcePVCMissing | Self::NotStarted { .. } | Self::SnapshotExists { .. } => {
                PartialCondition {
                    status: "False",
                    reason: "NotCreated",
                    message: "No Jobs are associated with BackupJob",
                }
            }
            Self::JobExists { job, .. } | Self::Finished { job: Some(job), .. } => {
                if job.job_active() {
                    PartialCondition {
                        status: "False",
                        reason: "JobRunning",
                        message: "Job is active",
                    }
                } else if job.job_succeeded() {
                    PartialCondition {
                        status: "True",
                        reason: "JobFinished",
                        message: "Job has at least one successful run",
                    }
                } else if job.job_failed() {
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
            }
            Self::Finished { .. } => PartialCondition {
                status: "True",
                reason: "CleanedUp",
                message: "BackupJob finished and Snapshot doesn't exist",
            },
        };
        conditions.push(condition.into_condition("BackupState", generation));
        conditions
    }
}

impl BackupJob {
    pub async fn reconcile(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        // let recorder = ctx.kube.recorder(self);
        let ns = self.namespace().unwrap();
        let name = self.name_any();
        let backup_jobs: Api<BackupJob> = Api::namespaced(ctx.kube.client(), &ns);
        let ps = PatchParams::apply(WALLE);

        // Get state of child resources, use to compute current conditions
        let state = self.get_state(ctx.clone()).await?;
        info!(name, ns, %state, "Reconciliation starting for BackupJob");

        match state {
            InternalState::SourcePVCMissing { .. } => {}

            InternalState::NotStarted { ref source_pvc } => {
                let snap_class = source_pvc
                    .source_pvc_snap_class(&ctx.config.snap_class_mappings)
                    .context(InvalidPVCSnafu)?;

                let snapshot_api = Api::<DynamicObject>::namespaced_with(
                    ctx.kube.client(),
                    &ns,
                    &ctx.kube.snapshot_ar,
                );

                snapshot_api
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
                    .with_context(|_| KubeSnafu {
                        msg: "Failed to create snapshot for backupjob",
                    })?;
            }

            InternalState::SnapshotExists { ref source_pvc, ref snapshot }
                if snapshot.snapshot_ready_to_use() =>
            {
                debug!(name, ns, "Creating Job for BackupJob");
                create_backup_job(self, source_pvc, snapshot, &ctx).await?;
            }
            InternalState::SnapshotExists { .. } => {}

            // If backup Job is completed, and still exists in the cluster, fetch logs from Job, record finish_time,
            // and attempt to cleanup
            InternalState::JobExists { ref job, .. } if job.job_completed() => {
                let log_res = latest_logs_of_job(ctx.kube.client(), &job.0).await;
                let logs = match log_res {
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

                backup_jobs
                    .patch_status(
                        &name,
                        &ps,
                        &Patch::Merge(json!({
                            "status": {
                                "finishTime": Time(job.job_completion_time().cloned().unwrap_or_else(Utc::now)),
                                "logs": logs,
                            },
                        })),
                    )
                    .await
                    .with_context(|_| KubeSnafu { msg: "Failed up update BackupJob status" })?;
            }
            InternalState::JobExists { .. } => {}

            // Don't try to reconcile after job marked finished, because resources were cleaned up.
            InternalState::Finished { ref snapshot, ref job, .. } => {
                if let Err(err) =
                    cleanup_backup_job(&ctx, self, snapshot.as_ref(), job.as_ref()).await
                {
                    error!(name, ns, ?err, "Failed to cleanup BackupJob after finished/failed");
                }

                // After reaching finished state, the job's status should no longer be updated
                return Ok(Action::await_change());
            }
        }

        let phase = Self::compute_phase(&state);
        let mut conditions = state.conditions(self.meta().generation);
        merge_conditions(
            &mut conditions,
            self.status.as_ref().map_or(&[], |x| x.conditions.as_slice()),
        );

        // Keep state up-to-date
        backup_jobs
            .patch_status(
                &name,
                &ps,
                &Patch::Merge(json!({
                    "status": {
                        "conditions": conditions,
                        "phase": phase,
                    },
                })),
            )
            .await
            .with_context(|_| KubeSnafu { msg: "Failed up update BackupJob status" })?;

        // If no events were received, check back every 5 minutes
        Ok(Action::requeue(Duration::from_secs(5 * 60)))
    }

    async fn get_state(&self, ctx: Arc<Context<Self>>) -> Result<InternalState> {
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

        Ok(InternalState::new(
            source_pvc,
            snapshot.into_iter().next(),
            job.into_iter().next(),
            self.status.as_ref().and_then(|x| x.finish_time.clone().map(|x| x.0)),
        ))
    }

    fn compute_phase(state: &InternalState) -> BackupJobState {
        match state {
            InternalState::SourcePVCMissing => BackupJobState::Failed,
            InternalState::NotStarted { .. } => BackupJobState::NotStarted,
            InternalState::SnapshotExists { .. } => BackupJobState::CreatingSnapshot,
            InternalState::JobExists { job, .. } => {
                if job.job_active() {
                    BackupJobState::BackingUp
                } else if job.job_succeeded() {
                    BackupJobState::Finished
                } else if job.job_failed() {
                    BackupJobState::Failed
                } else {
                    BackupJobState::WaitingBackup
                }
            }
            InternalState::Finished { .. } => BackupJobState::Finished,
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
    ctx: &Context<BackupJob>, backup_job: &BackupJob, snapshot: Option<&SnapshotExt>,
    job: Option<&JobExt>,
) -> Result<()> {
    let dp = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
        ..Default::default()
    };
    if let Some(job) = job {
        let jobs: Api<Job> = Api::namespaced(ctx.kube.client(), &backup_job.namespace().unwrap());
        if let Err(err) = jobs.delete(&job.0.name_any(), &dp).await {
            error!(?err, "Failed to cleanup Job subresource");
        }
    }

    if let Some(snapshot) = snapshot {
        let snapshots = Api::<DynamicObject>::namespaced_with(
            ctx.kube.client(),
            &backup_job.namespace().unwrap(),
            &ctx.kube.snapshot_ar,
        );
        if let Err(ref err) = snapshots.delete(&snapshot.0.name_any(), &dp).await {
            error!(?err, "Failed to cleanup Snapshot subresource");
        }
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

async fn create_backup_job(
    backup_job: &BackupJob, source_pvc: &SourcePvc, snapshot: &SnapshotExt,
    ctx: &Context<BackupJob>,
) -> Result<Job> {
    let name = backup_job.name_any();
    let namespace = backup_job.namespace().whatever_context(
        "Unable to get namespace of existing BackupJob, this shouldn't happen.",
    )?;

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.kube.client(), &namespace);

    let storage_class = source_pvc.source_pvc_storage_class().context(InvalidPVCSnafu)?;
    let storage_size = source_pvc.source_pvc_storage_size().context(InvalidPVCSnafu)?;

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
                    "ownerReferences": [snapshot.0.controller_owner_ref(&ctx.kube.snapshot_ar).unwrap()],
                },
                "spec": {
                    "storageClassName": storage_class,
                    "dataSource": {
                        "name": snapshot.0.name_any(),
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

    let snapshot_creation_time = snapshot.snapshot_creation_time().unwrap_or(Utc::now());
    let mount_path = format!("/data/{}/", backup_job.spec.source_pvc);

    let job_extra = json!({
        "spec": {
            "template": {
                "spec": {
                    "securityContext": {
                        "fsGroup": 65532, // TODO - Represents the nonroot user group in the walle-worker docker image. Don't hardcode
                    },
                    "containers": [{
                        "name": "restic",
                        "env": [
                            { "name": "SOURCE_PATH", "value": mount_path },
                            { "name": "SNAPSHOT_TIME", "value": snapshot_creation_time.to_restic_ts() },
                            { "name": "PVC_NAME", "value": backup_job.spec.source_pvc },
                        ],
                        "volumeMounts": [{
                            "name": "snapshot",
                            "mountPath": mount_path,
                        }]
                    }],
                    "volumes": [{
                        "name": "snapshot",
                        "persistentVolumeClaim": { "claimName": pvc.name_any() }
                    }],
                }
            }
        }
    });
    create_job(backup_job, JobType::Backup, ctx, job_extra).await
}
