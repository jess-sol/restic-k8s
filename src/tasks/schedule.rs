use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Pod;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    crd::{
        BackupJob, BackupJobState, BackupSchedule, BackupScheduleState, BackupScheduleStatus,
        LastRunStats,
    },
    Context, KubeSnafu, Result, WALLE,
};
use kube::{
    api::{ListParams, Patch, PatchParams, PostParams},
    runtime::{
        controller::Action,
        events::{Event, EventType},
    },
    Api, Resource, ResourceExt,
};
use snafu::{OptionExt, ResultExt as _};

impl BackupSchedule {
    pub async fn reconcile(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        let name = self.name_any();
        let namespace = self.namespace().unwrap_or_else(|| "<unknown>".to_string());

        let backup_schedules: Api<BackupSchedule> =
            Api::namespaced(ctx.kube.client(), self.namespace().as_ref().unwrap());

        let recorder = ctx.kube.recorder(self);

        debug!(name, namespace, "Reconciling BackupSchedule");

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

            return Ok(Action::requeue(Duration::ZERO));
        };

        // Check children of batch using labels, update current status to track job statistics.
        // Check if all jobs have completed, set BackupSchedule state accordingly
        match status.state {
            BackupScheduleState::Running => {
                let backup_job_api: Api<BackupJob> = Api::all(ctx.kube.client());
                let backup_jobs = backup_job_api
                    .list(&ListParams::default().labels(&format!(
                        "ros.io/schedule-timestamp={}",
                        status.backup_batch.as_deref().unwrap(),
                    )))
                    .await
                    .context(KubeSnafu {
                        msg: "Failed to list BackupJobs associated with BackupSchedule",
                    })?;

                // Get stats of backup jobs in current batch
                let mut stats =
                    LastRunStats { total_jobs: backup_jobs.items.len(), ..Default::default() };

                for backup_job in backup_jobs.iter() {
                    match backup_job.status.as_ref().map(|x| &x.state) {
                        Some(&BackupJobState::BackingUp | &BackupJobState::CreatingSnapshot) => {
                            stats.running_jobs += 1
                        }
                        Some(BackupJobState::Finished) => stats.finished_jobs += 1,
                        Some(BackupJobState::Failed) => stats.failed_jobs += 1,
                        Some(BackupJobState::NotStarted) | None => stats.unstarted_jobs += 1,
                    }
                }

                let mut new_status = json!({ "lastRunStats": stats });

                // If all jobs finished, set state and create event
                if stats.finished_jobs + stats.failed_jobs > 0
                    && stats.unstarted_jobs == 0
                    && stats.running_jobs == 0
                {
                    let state = if stats.failed_jobs == 0 {
                        BackupScheduleState::Finished
                    } else {
                        BackupScheduleState::FinishedWithFailures
                    };

                    let event = if stats.failed_jobs == 0 {
                        Event {
                            type_: EventType::Normal,
                            reason: "FinishedBackup".into(),
                            note: Some("Backup finished with no errors or warnings".into()),
                            action: "FinishedBackup".into(),
                            secondary: None,
                        }
                    } else {
                        Event {
                            type_: EventType::Warning,
                            reason: "FinishedBackupWithFailures".into(),
                            note: Some(format!(
                                "Backup finished with {} failing jobs",
                                stats.failed_jobs
                            )),
                            action: "FinishedBackupWithFailures".into(),
                            secondary: None,
                        }
                    };

                    if let Err(err) = recorder.publish(event).await {
                        error!(name, namespace, ?err, "Failed to add event to backup schedule");
                    }

                    new_status = json!({
                        "state": state,
                        "lastRunStats": stats
                    });
                }

                // Update status with new run stats and maybe state
                if let Err(err) = backup_schedules
                    .patch_status(
                        &name,
                        &PatchParams::apply(WALLE),
                        &Patch::Merge(json!({
                            "apiVersion": "ros.io/v1",
                            "kind": "BackupSchedule",
                            "status": new_status,
                        })),
                    )
                    .await
                {
                    error!(
                        name,
                        namespace,
                        ?err,
                        "Unable to set last_backup_run for backup schedule"
                    );
                }
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

        let backup_batch =
            self.status.as_ref().and_then(|x| x.backup_batch.as_ref()).whatever_context(
                "Unable to create BackupJob because schedule doesn't have backupBatch set.",
            )?;

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
                        "generateName": format!("{}-{}-", self.name_any(), pvc_name),
                        "ownerReferences": [self.controller_owner_ref(&())],
                        "labels": {
                            "ros.io/schedule-timestamp": backup_batch,
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
            .and_then(|x| x.last_backup_run.as_ref())
            .map(|x| {
                schedule
                    .spec
                    .interval
                    .as_ref()
                    .map(|int| {
                        int.passed_interval(&DateTime::parse_from_rfc3339(x).unwrap().to_utc())
                    })
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    }

    async fn run_schedule(schedule: Arc<BackupSchedule>, ctx: &Context<BackupSchedule>) {
        let name = schedule.name_any();
        let namespace = schedule.namespace().unwrap_or_else(|| "<unknown>".to_string());

        let schedules: Api<BackupSchedule> =
            Api::namespaced(ctx.kube.client(), schedule.meta().namespace.as_ref().unwrap());
        let recorder = ctx.kube.recorder(&*schedule);

        debug!(name, namespace, "Attempting to check scheduled backup");

        // Check schedule isn't currently running, if it is, add a warning event and skip this interval
        if schedule.status.as_ref().map(|x| &x.state) == Some(&BackupScheduleState::Running) {
            warn!(name, namespace, "Skipping scheduled backup, it's still running");
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
                error!(name, namespace, ?err, "Failed to add event to backup schedule");
            }

            if let Err(err) = schedules
                .patch_status(
                    &schedule.name_any(),
                    &PatchParams::apply(WALLE),
                    &Patch::Merge(json!({
                        "apiVersion": "ros.io/v1",
                        "kind": "BackupSchedule",
                        "status": {
                            // TODO - Reorganize status so scheduling timestamps aren't easy to use
                            // outside of scheduler
                            "lastBackupRun": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        }
                    })),
                )
                .await
            {
                error!(name, namespace, ?err, "Unable to set status for backup schedule");
            }

            return;
        }

        info!(name, namespace, "Starting backup schedule job");
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
            error!(name, namespace, ?err, "Failed to add event to backup schedule");
        }

        let timestamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        match schedules
            .patch_status(
                &name,
                &PatchParams::apply(WALLE),
                &Patch::Merge(json!({
                    "apiVersion": "ros.io/v1",
                    "kind": "BackupSchedule",
                    "status": {
                        "lastBackupRun": timestamp,
                        "backupBatch": timestamp.replace(':', "."),
                        "state": BackupScheduleState::Running,
                    }
                })),
            )
            .await
        {
            Ok(schedule) => {
                if let Err(err) = schedule.run(ctx).await {
                    error!(name, namespace, ?err, "Failed to run backup schedule");
                }
            }
            Err(err) => {
                error!(name, namespace, ?err, "Unable to set status for backup schedule");
            }
        }
    }
}
