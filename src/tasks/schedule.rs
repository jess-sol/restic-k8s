use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Pod;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, error, info};

use crate::{
    crd::{BackupJob, BackupSchedule, BackupScheduleStatus},
    Context, KubeSnafu, Result, WALLE,
};
use kube::{
    api::{ListParams, Patch, PatchParams, PostParams},
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
        reflector::Store,
    },
    Api, Client, Resource, ResourceExt,
};
use snafu::ResultExt as _;

impl BackupSchedule {
    pub async fn reconcile(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        // Monitor BackupSchedule
        // - Create BackupJobs, if immediately is true (default)
        // - Add entry to cron
        // Run cron
        // - Create BackupJobs

        let backup_schedules: Api<BackupSchedule> =
            Api::namespaced(ctx.client.clone(), self.namespace().as_ref().unwrap());

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

        Ok(Action::requeue(Duration::from_secs(60 * 5)))
    }

    pub async fn cleanup(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
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
    pub async fn run(&self, client: &kube::Client) -> Result<()> {
        let mut pvcs = BTreeMap::new();
        for plan in &self.spec.plans {
            // TODO - Move body of loop into function and do better error handling

            assert_eq!(plan.type_, "pod", "Currently only able to target pods");
            let api: Api<Pod> = Api::all(client.clone());

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
        }

        let mut namespaced_jobs = BTreeMap::new();

        for ((pvc_ns, pvc_name), plan) in pvcs {
            let api = namespaced_jobs
                .entry(pvc_ns)
                .or_insert_with_key(|ns| Api::<BackupJob>::namespaced(client.clone(), ns));

            api.create(
                &PostParams::default(),
                &serde_json::from_value(json!({
                    "apiVersion": "ros.io/v1",
                    "kind": "BackupJob",
                    "metadata": {
                        "generateName": format!("{}-{}-", self.name_any(), pvc_name),
                        "ownerReferences": [self.controller_owner_ref(&())],
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
    store: Store<BackupSchedule>,
}

impl Scheduler {
    pub fn new(store: Store<BackupSchedule>) -> Self {
        Self { store }
    }

    /// Loop through schedules and run on cron schedule
    pub async fn run(self, client: Client, reporter: Reporter) {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Unable to calculate time since epoch");
        let offset = Duration::from_micros(since_epoch.as_micros() as u64 % MICROSECONDS);
        // Get start time on whole second of system clock
        let start = Instant::now() - offset;

        loop {
            debug!("Checking for backup schedules to run");
            // Check if any schedules have a matching cron, if they do, kick them off
            while let Some(schedule) = self.store.find(Self::should_run) {
                let name = schedule.name_any();
                let namespace = schedule.namespace().unwrap_or_else(|| "<unknown>".to_string());
                info!(name, namespace, "Starting backup schedule job");

                if let Err(err) =
                    Self::run_schedule(schedule, client.clone(), reporter.clone()).await
                {
                    error!(name, namespace, ?err, "Failed to run backup schedule")
                }
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

    async fn run_schedule(
        schedule: Arc<BackupSchedule>, client: Client, reporter: Reporter,
    ) -> Result<()> {
        let schedules: Api<BackupSchedule> =
            Api::namespaced(client.clone(), schedule.meta().namespace.as_ref().unwrap());
        let recorder = Recorder::new(client.clone(), reporter.clone(), schedule.object_ref(&()));
        let _ = recorder
            .publish(Event {
                type_: EventType::Normal,
                reason: "RunningBackup".into(),
                note: Some("Beginning scheduled backup".into()),
                action: "Backup".into(),
                secondary: None,
            })
            .await;

        let _o = schedules
            .patch_status(
                &schedule.name_any(),
                &PatchParams::apply(WALLE),
                &Patch::Apply(json!({
                    "apiVersion": "ros.io/v1",
                    "kind": "BackupSchedule",
                    "status": {
                        "lastBackupRun": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    }
                })),
            )
            .await
            .whatever_context("Unable to set last_backup_run for backup schedule")?;

        schedule.run(&client).await
    }
}
