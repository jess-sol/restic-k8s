use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Pod;
use serde_json::json;
use std::{
    cmp::Reverse,
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    crd::{
        BackupJob, BackupSchedule, BackupScheduleState, BackupScheduleStatus, BackupSet,
        BackupSetState,
    },
    tasks::DateTimeFormatK8s,
    Context, KubeSnafu, Result, WALLE,
};
use kube::{
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams},
    runtime::{
        controller::Action,
        events::{Event, EventType},
    },
    Api, Resource, ResourceExt,
};
use snafu::ResultExt as _;

impl BackupSchedule {
    pub async fn reconcile(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        let name = self.name_any();

        let backup_schedules: Api<BackupSchedule> = Api::all(ctx.kube.client());
        let backup_sets: Api<BackupSet> = Api::all(ctx.kube.client());

        let recorder = ctx.kube.recorder(self);

        debug!(name, "Reconciling BackupSchedule");

        // Set initial status if none
        let Some(status) = self.status.as_ref() else {
            let _o = backup_schedules
                .patch_status(
                    &self.name_any(),
                    &PatchParams::apply(WALLE),
                    &Patch::Apply(json!({
                        "apiVersion": "ros.io/v1",
                        "kind": "BackupSchedule",
                        "status": BackupScheduleStatus::default()
                    })),
                )
                .await
                .with_context(|_| KubeSnafu { msg: "Failed up update backupschedule status" })?;

            return Ok(Action::requeue(Duration::from_secs(5)));
        };

        let sets = backup_sets
            .list(&ListParams::default().labels(&format!("ros.io/backup-schedule={name}")))
            .await
            .context(KubeSnafu { msg: "Failed to get BackupSets for BackupSchedule" })?;

        let mut sets = sets.items;
        sets.sort_unstable_by_key(|x| Reverse(x.creation_timestamp()));

        // Delete extra sets
        let to_delete = sets
            .iter()
            .filter(|x| x.status.as_ref().map(|x| &x.state) == Some(&BackupSetState::Finished))
            .skip(self.spec.keep_succeeded)
            .chain(
                sets.iter()
                    .filter(|x| {
                        x.status.as_ref().map(|x| &x.state)
                            == Some(&BackupSetState::FinishedWithFailures)
                    })
                    .skip(self.spec.keep_failed),
            )
            .map(|x| (x.name_any(), x.object_ref(&())));

        for (set_name, set_ref) in to_delete {
            info!(backup_schedule = name, backup_set = set_name, "Cleaning up old BackupSet");
            if let Err(err) = backup_sets.delete(&set_name, &DeleteParams::default()).await {
                error!(
                    backup_schedule = name,
                    backup_set = set_name,
                    ?err,
                    "Failed to cleanup old BackupSet"
                );
            } else if let Err(err) = recorder
                .publish(Event {
                    type_: EventType::Warning,
                    reason: "DeletedOldBackupSet".into(),
                    note: Some("Deleted old BackupSet".into()),
                    action: "DeletingBackupSet".into(),
                    secondary: Some(set_ref),
                })
                .await
            {
                error!(name, ?err, "Failed to add event to backup schedule");
            }
        }

        // If latest BackupSet state has changed, reflect it in schedule state
        if let Some(latest_set) = sets.first() {
            if let Some(ref set_status) = latest_set.status {
                if BackupScheduleState::from(&set_status.state) != status.state {
                    if let Err(err) = backup_schedules
                        .patch_status(
                            &name,
                            &PatchParams::apply(WALLE),
                            &Patch::Merge(json!({
                                "apiVersion": "ros.io/v1",
                                "kind": "BackupSchedule",
                                "status": {
                                    "state": set_status.state,
                                }
                            })),
                        )
                        .await
                    {
                        error!(name, ?err, "Unable to set status for BackupSchedule");
                    }
                }
            }
        }

        // Check children of batch using labels, update current status to track job statistics.
        // Check if all jobs have completed, set BackupSchedule state accordingly
        match status.state {
            BackupScheduleState::Running => {
                // Keep state updated

                // Copy stats
            }
            BackupScheduleState::Waiting
            | BackupScheduleState::Finished
            | BackupScheduleState::FinishedWithFailures => {}
        }

        Ok(Action::requeue(Duration::from_secs(60 * 5)))
    }

    pub async fn cleanup(&self, _ctx: Arc<Context<Self>>) -> Result<Action> {
        // let recorder = ctx.diagnostics.read().await.recorder(ctx.client.clone(), self);
        // let result = recorder
        //     .publish(Event {
        //         type_: EventType::Normal,
        //         reason: "DeleteRequested".into(),
        //         note: Some(format!("Delete `{}`", self.name_any())),
        //         action: "Deleting".into(),
        //         secondary: None,
        //     })
        //     .await;
        // // Don't let failure to update the events of the resource stop the finalizer from
        // // completing. This breaks the deletion of namespaces.
        // if let Err(err) = result {
        //     warn!(?err, backup_job = ?self, "Failed to add deletion event to backupjob");
        // }
        Ok(Action::await_change())
    }

    // Find all matching workloads, and create backupjobs for them
    pub async fn run(&self, ctx: &Context<Self>) -> Result<()> {
        let name = self.name_any();

        let mut pvcs = BTreeMap::new();
        for plan in &self.spec.plans {
            // TODO - Move body of loop into function and do better error handling

            assert_eq!(plan.type_, "pod", "Currently only able to target pods");
            let api: Api<Pod> = Api::all(ctx.kube.client());

            let mut lp = ListParams::default();
            if let Some(ref ls) = plan.label_selector {
                lp = lp.labels(ls);
            }
            if let Some(ref fs) = plan.field_selector {
                lp = lp.fields(fs);
            }

            let resources = api
                .list(&lp)
                .await
                .whatever_context("Unable to list pod workloads to create scheduled backup jobs")?;

            // Get PVCs
            debug!("Getting PVCs of resources to create BackupJobs for");
            for pod in resources {
                let Some(volumes) = pod.spec.as_ref().and_then(|x| x.volumes.as_ref()) else {
                    continue;
                };
                for volume in volumes {
                    let Some(ref claim) = volume.persistent_volume_claim else { continue };
                    let pvc_meta =
                        (pod.meta().namespace.clone().unwrap(), claim.claim_name.clone());

                    pvcs.insert(pvc_meta, plan);
                }
            }
            debug!("Creating BackupJobs for {} PVCs", pvcs.len());
        }

        let mut namespaced_jobs = BTreeMap::new();

        // let backup_batch =
        //     self.status.as_ref().and_then(|x| x.backup_batch.as_ref()).whatever_context(
        //         "Unable to create BackupJob because schedule doesn't have backupBatch set.",
        //     )?;

        let set_id = Utc::now().to_k8s_label();

        // First create a BackupSet to attach BackupJobs to
        let set_api: Api<BackupSet> = Api::all(ctx.kube.client());
        let set = set_api
            .create(
                &PostParams::default(),
                &serde_json::from_value(json!({
                    "apiVersion": "ros.io/v1",
                    "kind": "BackupSet",
                    "metadata": {
                        "generateName": format!("{name}-"),
                        "ownerReferences": [self.controller_owner_ref(&())],
                        "labels": {
                            "ros.io/backup-schedule": name,
                        }
                    },
                    "spec": {
                        "selector": {
                            "matchLabels": {
                                "ros.io/backup-schedule": name,
                                "ros.io/backupset": set_id,
                            }
                        }
                    }
                }))
                .expect("Invalid predefined BackupSet spec"),
            )
            .await
            .context(KubeSnafu { msg: "Failed to create BackupSet for scheduled backup" })?;

        for ((pvc_ns, pvc_name), plan) in pvcs {
            let api = namespaced_jobs
                .entry(pvc_ns)
                .or_insert_with_key(|ns| Api::<BackupJob>::namespaced(ctx.kube.client(), ns));

            api.create(
                &PostParams::default(),
                &serde_json::from_value(json!({
                    "apiVersion": "ros.io/v1",
                    "kind": "BackupJob",
                    "metadata": {
                        "generateName": format!("{}-{}-", set.name_any(), pvc_name),
                        "ownerReferences": [set.controller_owner_ref(&())],
                        "labels": {
                            "ros.io/backup-schedule": name,
                            "ros.io/backupset": set_id,
                        }
                    },
                    "spec": {
                        "sourcePvc": pvc_name,
                        "repository": self.spec.repository,

                        // TODO - Figure out how to run stuff in workload correctly
                        // "before_snapshot": plan.before_snapshot,
                        // "after_snapshot": plan.after_snapshot,
                    }
                }))
                .expect("Invalid predefined BackupJob spec"),
            )
            .await
            .unwrap();
        }
        Ok(())
    }
}

const MICROSECONDS: u64 = Duration::from_secs(1).as_micros() as u64;

pub struct Scheduler {
    ctx: Arc<Context<BackupSchedule>>,
}

impl Scheduler {
    pub fn new(ctx: Arc<Context<BackupSchedule>>) -> Self {
        Self { ctx }
    }

    /// Loop through schedules and run on cron schedule
    pub async fn run(self) {
        let ctx = self.ctx;

        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Unable to calculate time since epoch");
        let offset = Duration::from_micros(since_epoch.as_micros() as u64 % MICROSECONDS);
        // Get start time on whole second of system clock
        let start = Instant::now() - offset;

        loop {
            trace!("Checking for backup schedules to run");
            // Check if any schedules have a matching cron, if they do, kick them off
            while let Some(schedule) = ctx.store.find(Self::should_run) {
                Self::run_schedule(schedule, &ctx).await;
            }

            // Sleep until next full second since start
            let run_delta = Instant::now().duration_since(start);
            let offset =
                Duration::from_micros(MICROSECONDS - run_delta.as_micros() as u64 % MICROSECONDS);
            tokio::time::sleep(offset).await;
        }
    }

    fn should_run(schedule: &BackupSchedule) -> bool {
        schedule
            .status
            .as_ref()
            .and_then(|x| x.scheduler.backup_timestamp.as_ref())
            .map(|x| {
                schedule
                    .spec
                    .interval
                    .as_ref()
                    .map(|int| int.passed_interval(&DateTime::from_k8s_ts(x).unwrap()))
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    }

    async fn run_schedule(schedule: Arc<BackupSchedule>, ctx: &Context<BackupSchedule>) {
        let name = schedule.name_any();

        let schedules: Api<BackupSchedule> = Api::all(ctx.kube.client());
        let recorder = ctx.kube.recorder(&*schedule);

        debug!(name, "Attempting to check scheduled backup");

        // TODO - Need to add assurance that same schedule won't get kicked off multiple times
        // while looping through store?

        // Check schedule isn't currently running, if it is, add a warning event and skip this interval
        if schedule.status.as_ref().map(|x| &x.state) == Some(&BackupScheduleState::Running) {
            warn!(name, "Skipping scheduled backup, it's still running");
            if let Err(err) = recorder
                .publish(Event {
                    type_: EventType::Warning,
                    reason: "SkippedBackup".into(),
                    note: Some("Skipping scheduled backup, previous run hasn't finished".into()),
                    action: "SkipBackup".into(),
                    secondary: None,
                })
                .await
            {
                error!(name, ?err, "Failed to add event to backup schedule");
            }

            if let Err(err) = schedules
                .patch_status(
                    &schedule.name_any(),
                    &PatchParams::apply(WALLE),
                    &Patch::Merge(json!({
                        "apiVersion": "ros.io/v1",
                        "kind": "BackupSchedule",
                        "status": {
                            "scheduler": {
                                "backupTimestamp": Utc::now().to_k8s_ts()
                            }
                        }
                    })),
                )
                .await
            {
                error!(name, ?err, "Unable to set status for backup schedule");
            }

            return;
        }

        info!(name, "Starting backup schedule job");
        if let Err(err) = recorder
            .publish(Event {
                type_: EventType::Normal,
                reason: "RunningBackup".into(),
                note: Some("Beginning scheduled backup".into()),
                action: "Backup".into(),
                secondary: None,
            })
            .await
        {
            error!(name, ?err, "Failed to add event to backup schedule");
        }

        let timestamp = Utc::now().to_k8s_ts();

        match schedules
            .patch_status(
                &name,
                &PatchParams::apply(WALLE),
                &Patch::Merge(json!({
                    "apiVersion": "ros.io/v1",
                    "kind": "BackupSchedule",
                    "status": {
                        "scheduler": {
                            "backupTimestamp": timestamp,
                        },
                        "lastBackupRun": timestamp,
                        "state": BackupScheduleState::Running,
                    }
                })),
            )
            .await
        {
            Ok(schedule) => {
                if let Err(err) = schedule.run(ctx).await {
                    error!(name, ?err, "Failed to run backup schedule");
                }
            }
            Err(err) => {
                error!(name, ?err, "Unable to set status for backup schedule");
            }
        }
    }
}
