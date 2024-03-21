use chrono::Utc;
use k8s_openapi::{
    api::{
        batch::v1::Job,
        core::v1::{PersistentVolumeClaim, Pod},
    },
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::{
    cmp::Reverse,
    collections::BTreeMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    crd::{
        BackupJob, BackupSchedule, BackupScheduleState, BackupScheduleStatus, BackupSet,
        BackupSetState, RetentionSpec,
    },
    tasks::{
        field_selector_to_filter,
        job::{create_job, JobType},
        label_selector_to_filter, update_conditions, DateTimeFormatK8s, PartialCondition,
    },
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
use snafu::{OptionExt as _, ResultExt as _};

use super::SourcePvc;

impl BackupSchedule {
    pub async fn reconcile(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        let name = self.name_any();

        let backup_schedules: Api<BackupSchedule> = Api::all(ctx.kube.client());
        let backup_sets: Api<BackupSet> = Api::all(ctx.kube.client());
        let job_api: Api<Job> = Api::namespaced(ctx.kube.client(), &ctx.kube.operator_namespace);

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

        // Delete extra BackupSets
        self.delete_old(&backup_sets, &ctx, "").await?;
        self.delete_old(&job_api, &ctx, "check").await?;
        self.delete_old(&job_api, &ctx, "prune").await?;

        let mut new_conditions = Vec::new();

        // Set BackupReady condition based on Job state
        let backup_condition = if let Some(status) = sets.first().and_then(|x| x.status.as_ref()) {
            PartialCondition {
                reason: status.state.as_str(),
                status: if status.state == BackupSetState::Finished { "True" } else { "False" },
                message: "BackupSet state",
            }
        } else {
            PartialCondition {
                reason: "Waiting",
                status: "False",
                message: "No BackupSets found for BackupSchedule",
            }
        };
        new_conditions.push(backup_condition.into_condition("BackupReady", self.meta().generation));

        // Set CheckOk condition based on BackupSet state
        let check_jobs = job_api
            .list(
                &ListParams::default()
                    .labels(&format!("ros.io/backup-schedule={name},ros.io/backup-job-type=check")),
            )
            .await
            .context(KubeSnafu { msg: "Failed to get Jobs for BackupSchedule" })?;

        let mut check_job = check_jobs.items;
        check_job.sort_unstable_by_key(|x| Reverse(x.creation_timestamp()));
        if let Some(check_job) = check_job.first() {
            if let Some(ref status) = check_job.status {
                let condition = if status.active.unwrap_or(0) > 0 {
                    PartialCondition {
                        status: "False",
                        reason: "Running",
                        message: "Check job running",
                    }
                } else if status.succeeded.unwrap_or(0) > 0 {
                    PartialCondition {
                        status: "True",
                        reason: "Finished",
                        message: "Check job succeeded",
                    }
                } else if status.failed.unwrap_or(0)
                    > check_job.spec.as_ref().and_then(|x| x.backoff_limit).unwrap_or(3)
                {
                    PartialCondition {
                        status: "False",
                        reason: "Failed",
                        message: "Repository check failed",
                    }
                } else {
                    PartialCondition {
                        status: "False",
                        reason: "Waiting",
                        message: "Job exists but is not running",
                    }
                };

                new_conditions.push(condition.into_condition("CheckOk", self.meta().generation));
            }
        } else if status.conditions.iter().any(|x| x.type_ == "CheckOk" && x.reason == "Running") {
            let condition = PartialCondition {
                status: "False",
                reason: "Waiting",
                message: "Job went missing while running",
            };

            new_conditions.push(condition.into_condition("CheckOk", self.meta().generation));
        }

        // Set PruneOk condition based on Job state
        let prune_jobs = job_api
            .list(
                &ListParams::default()
                    .labels(&format!("ros.io/backup-schedule={name},ros.io/backup-job-type=prune")),
            )
            .await
            .context(KubeSnafu { msg: "Failed to get Jobs for BackupSchedule" })?;

        let mut prune_job = prune_jobs.items;
        prune_job.sort_unstable_by_key(|x| Reverse(x.creation_timestamp()));
        if let Some(prune_job) = prune_job.first() {
            if let Some(ref status) = prune_job.status {
                let condition = if status.active.unwrap_or(0) > 0 {
                    PartialCondition {
                        status: "False",
                        reason: "Running",
                        message: "Prune job running",
                    }
                } else if status.succeeded.unwrap_or(0) > 0 {
                    PartialCondition {
                        status: "True",
                        reason: "Finished",
                        message: "Prune job succeeded",
                    }
                } else if status.failed.unwrap_or(0)
                    > prune_job.spec.as_ref().and_then(|x| x.backoff_limit).unwrap_or(3)
                {
                    PartialCondition {
                        status: "False",
                        reason: "Failed",
                        message: "Repository pruning failed",
                    }
                } else {
                    PartialCondition {
                        status: "False",
                        reason: "Waiting",
                        message: "Job exists but is not running",
                    }
                };

                new_conditions.push(condition.into_condition("PruneOk", self.meta().generation));
            }
        } else if status.conditions.iter().any(|x| x.type_ == "PruneOk" && x.reason == "Running") {
            let condition = PartialCondition {
                status: "False",
                reason: "Waiting",
                message: "Job went missing while running",
            };

            new_conditions.push(condition.into_condition("PruneOk", self.meta().generation));
        }

        let mut conditions = status.conditions.clone();
        update_conditions(&mut conditions, new_conditions);

        let state = self.get_state(&conditions);
        if let Err(err) = backup_schedules
            .patch_status(
                &name,
                &PatchParams::apply(WALLE),
                &Patch::Merge(json!({
                    "apiVersion": "ros.io/v1",
                    "kind": "BackupSchedule",
                    "status": {
                        "state": state,
                        "conditions": conditions,
                    }
                })),
            )
            .await
        {
            error!(name, ?err, "Unable to set status for BackupSchedule");
        }

        Ok(Action::requeue(Duration::from_secs(60 * 5)))
    }

    fn get_state(&self, conditions: &[Condition]) -> BackupScheduleState {
        let conditions: BTreeMap<_, _> = conditions.iter().map(|x| (x.type_.clone(), x)).collect();

        let mut state = BackupScheduleState::Waiting;

        if let Some(check_reason) = conditions.get("CheckOk").map(|x| x.reason.as_str()) {
            match check_reason {
                "Running" => state = BackupScheduleState::Running,
                "Failed" => return BackupScheduleState::CheckFailed,
                _ => {}
            }
        }

        if let Some(backup_reason) = conditions.get("BackupReady").map(|x| x.reason.as_str()) {
            match backup_reason {
                "Running" => state = BackupScheduleState::Running,
                "Finished" => state = BackupScheduleState::Finished,
                "FinishedWithFailures" => return BackupScheduleState::FinishedWithFailures,
                _ => {}
            }
        }

        if let Some(prune_reason) = conditions.get("PruneOk").map(|x| x.reason.as_str()) {
            match prune_reason {
                "Running" => state = BackupScheduleState::Running,
                "Failed" => return BackupScheduleState::FinishedWithFailures,
                _ => {}
            }
        }

        state
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

    fn should_run(&self) -> bool {
        self.should_run_backup() || self.should_run_check() || self.should_run_prune()
    }

    fn should_run_prune(&self) -> bool {
        let Some(ref prune) = self.spec.prune else { return false };

        let Some(ref status) = self.status else { return true };
        let Some(ref timestamp) = status.scheduler.prune_timestamp else { return true };

        prune.interval.passed_interval(&timestamp.0)
    }

    fn should_run_check(&self) -> bool {
        let Some(ref check) = self.spec.check else { return false };

        let Some(ref status) = self.status else { return true };
        let Some(ref timestamp) = status.scheduler.check_timestamp else { return true };

        check.interval.passed_interval(&timestamp.0)
    }

    fn should_run_backup(&self) -> bool {
        let Some(ref interval) = self.spec.interval else { return false };

        let Some(ref status) = self.status else { return true };
        let Some(ref timestamp) = status.scheduler.backup_timestamp else { return true };

        interval.passed_interval(&timestamp.0)
    }

    // Find all matching workloads, and create backupjobs for them
    async fn run(&self, ctx: &Context<Self>) -> Result<()> {
        let name = self.name_any();

        let mut pvcs = BTreeMap::new();
        for plan in &self.spec.plans {
            // TODO - Move body of loop into function and do better error handling

            assert_eq!(plan.type_, "pod", "Currently only able to target pods");
            let api: Api<Pod> = Api::all(ctx.kube.client());

            let mut lp = ListParams::default();
            if let Some(ref ls) = plan.label_selector {
                lp = lp.labels(&label_selector_to_filter(ls));
            }
            if let Some(ref fs) = plan.field_selector {
                lp = lp.fields(&field_selector_to_filter(fs));
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

                    // Ensure first plan wins if a PVC is referenced multiple times
                    if pvcs.get(&pvc_meta).is_none() {
                        pvcs.insert(pvc_meta, plan);
                    }
                }
            }
            debug!("Creating BackupJobs for {} PVCs", pvcs.len());
        }

        let mut namespaced_jobs = BTreeMap::new();
        let mut namespaced_pvcs = BTreeMap::new();

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
                .entry(pvc_ns.clone())
                .or_insert_with_key(|ns| Api::<BackupJob>::namespaced(ctx.kube.client(), ns));
            let pvc_api = namespaced_pvcs.entry(pvc_ns.clone()).or_insert_with_key(|ns| {
                Api::<PersistentVolumeClaim>::namespaced(ctx.kube.client(), ns)
            });

            // Ensure there's a snap_class_mapping, if not skip PVC
            let Some(pvc) = pvc_api
                .get_opt(&pvc_name)
                .await
                .context(KubeSnafu { msg: "Failed to get PVC referenced by workload to backup" })?
            else {
                error!(pvc_ns, pvc_name, "Unable to located PVC referenced in workload to backup");
                continue;
            };
            let pvc = SourcePvc(pvc);
            let snap_class = pvc.source_pvc_snap_class(&ctx.config.snap_class_mappings);

            if snap_class.is_none() {
                warn!(pvc_ns, pvc_name, "PVC referenced in workload is in storage class which has no volumesnapshotclass mapping. Skipping backup.");
                continue;
            }

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

    async fn delete_old<
        T: Clone + std::fmt::Debug + DeserializeOwned + Resource<DynamicType = ()> + JobState,
    >(
        &self, api: &Api<T>, ctx: &Context<BackupSchedule>, job_type: &str,
    ) -> Result<()> {
        let recorder = ctx.kube.recorder(self);
        let kind = T::kind(&());
        let name = self.name_any();
        let mut labels = format!("ros.io/backup-schedule={name}");
        if !job_type.is_empty() {
            labels = format!("{labels},ros.io/backup-job-type={job_type}");
        }

        let list = api
            .list(&ListParams::default().labels(&labels))
            .await
            .context(KubeSnafu { msg: format!("Failed to get {kind} for BackupSchedule") })?;

        let mut items = list.items;
        items.sort_unstable_by_key(|x| Reverse(x.creation_timestamp()));
        let to_delete = items
            .iter()
            .filter(|x| x.succeeded())
            .skip(self.spec.keep_succeeded)
            .chain(items.iter().filter(|x| x.failed()).skip(self.spec.keep_failed))
            .map(|x| (x.name_any(), x.object_ref(&())));

        for (item_name, item_ref) in to_delete {
            info!(backup_schedule = name, backup_set = item_name, "Cleaning up old {kind}");
            if let Err(err) = api.delete(&item_name, &DeleteParams::default()).await {
                error!(backup_schedule = name, item_name, ?err, "Failed to cleanup old {kind}");
            } else if let Err(err) = recorder
                .publish(Event {
                    type_: EventType::Warning,
                    reason: format!("DeletedOld{kind}"),
                    note: Some(format!("Deleted old {kind}")),
                    action: format!("Deleting{kind}"),
                    secondary: Some(item_ref),
                })
                .await
            {
                error!(name, ?err, "Failed to add event to BackupSchedule");
            }
        }

        Ok(())
    }
}

trait JobState {
    fn succeeded(&self) -> bool;
    fn failed(&self) -> bool;
    fn active(&self) -> bool;
}

impl JobState for BackupSet {
    fn succeeded(&self) -> bool {
        self.status.as_ref().map(|x| x.state == BackupSetState::Finished).unwrap_or(false)
    }

    fn failed(&self) -> bool {
        self.status
            .as_ref()
            .map(|x| x.state == BackupSetState::FinishedWithFailures)
            .unwrap_or(false)
    }

    fn active(&self) -> bool {
        self.status.as_ref().map(|x| x.state == BackupSetState::Running).unwrap_or(false)
    }
}

impl JobState for Job {
    fn succeeded(&self) -> bool {
        self.status.as_ref().and_then(|x| x.succeeded).unwrap_or(0) > 0
    }

    fn failed(&self) -> bool {
        let backoff_limit = self.spec.as_ref().and_then(|x| x.backoff_limit).unwrap_or(3);
        self.status.as_ref().and_then(|x| x.failed).unwrap_or(0) > backoff_limit
    }

    fn active(&self) -> bool {
        self.status.as_ref().and_then(|x| x.active).unwrap_or(0) > 0
    }
}

impl RetentionSpec {
    fn fields(&self) -> [(&str, Option<u32>); 5] {
        [
            ("hourly", self.hourly),
            ("daily", self.daily),
            ("weekly", self.weekly),
            ("monthly", self.monthly),
            ("yearly", self.yearly),
        ]
    }
}

impl FromStr for RetentionSpec {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut values = BTreeMap::new();
        let fields = s.split(',');
        for field in fields {
            let mut iter = field.rsplitn(2, '=');
            match (iter.next(), iter.next()) {
                (Some(v), Some(n)) => {
                    let v: u32 = v.parse().map_err(|x| format!("Unable to parse {n}: {x}"))?;
                    values.insert(n, v);
                }
                (Some(x), _) => return Err(format!("Missing value for field {x}")),
                (None, _) => continue,
            }
        }

        Ok(Self {
            hourly: values.remove("hourly"),
            daily: values.remove("daily"),
            weekly: values.remove("weekly"),
            monthly: values.remove("monthly"),
            yearly: values.remove("yearly"),
        })
    }
}

impl ToString for RetentionSpec {
    fn to_string(&self) -> String {
        let mut values = Vec::new();

        for (name, value) in self.fields() {
            if let Some(value) = value {
                values.push(format!("{name}={value}"));
            }
        }

        values.join(",")
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
            while let Some(schedule) = ctx.store.find(BackupSchedule::should_run) {
                // Only run one job type at a time, this way schedule doesn't need to be cloned
                // Also each job type works on the same repo, so starting them all at once will
                // force all but the first to wait for a lock anyways.
                if schedule.should_run_backup() {
                    Self::run_job(JobType::Backup, schedule, &ctx).await;
                } else if schedule.should_run_check() {
                    Self::run_job(JobType::Check, schedule, &ctx).await;
                } else if schedule.should_run_prune() {
                    Self::run_job(JobType::Prune, schedule, &ctx).await;
                }
            }

            // Sleep until next full second since start
            let run_delta = Instant::now().duration_since(start);
            let offset =
                Duration::from_micros(MICROSECONDS - run_delta.as_micros() as u64 % MICROSECONDS);
            tokio::time::sleep(offset).await;
        }
    }

    async fn run_job(
        job_type: JobType, schedule: Arc<BackupSchedule>, ctx: &Context<BackupSchedule>,
    ) {
        let name = schedule.name_any();

        let schedules: Api<BackupSchedule> = Api::all(ctx.kube.client());
        let recorder = ctx.kube.recorder(&*schedule);

        debug!(name, "Attempting to run scheduled {job_type} job");

        // TODO - Check condition of specific run type
        // Check if schedule is currently running; if it is, add a warning event and skip this interval
        if schedule.status.as_ref().map(|x| &x.state) == Some(&BackupScheduleState::Running) {
            warn!(name, "Skipping scheduled {job_type} job, it's still running");
            if let Err(err) = recorder
                .publish(Event {
                    type_: EventType::Warning,
                    reason: format!("Skipped{}", job_type.name()),
                    note: Some(format!(
                        "Skipping scheduled {job_type} job, previous run hasn't finished"
                    )),
                    action: format!("Skip{}", job_type.name()),
                    secondary: None,
                })
                .await
            {
                error!(name, ?err, "Failed to add event to BackupSchedule");
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
                                job_type.scheduler_field(): Time(Utc::now())
                            }
                        }
                    })),
                )
                .await
            {
                error!(name, ?err, "Unable to set status for BackupSchedule");
            }

            return;
        }

        info!(name, "Starting BackupSchedule {job_type} job");
        if let Err(err) = recorder
            .publish(Event {
                type_: EventType::Normal,
                reason: format!("Running{}", job_type.name()),
                note: Some(format!("Beginning scheduled {job_type} job")),
                action: job_type.name().into(),
                secondary: None,
            })
            .await
        {
            error!(name, ?err, "Failed to add event to BackupSchedule");
        }

        let timestamp = Utc::now();

        match schedules
            .patch_status(
                &name,
                &PatchParams::apply(WALLE),
                &Patch::Merge(json!({
                    "apiVersion": "ros.io/v1",
                    "kind": "BackupSchedule",
                    "status": {
                        "scheduler": {
                            job_type.scheduler_field(): Time(Utc::now())
                        },
                        job_type.last_run_field(): Time(timestamp),
                        "state": BackupScheduleState::Running,
                    }
                })),
            )
            .await
        {
            Ok(schedule) => {
                if job_type == JobType::Backup {
                    if let Err(err) = schedule.run(ctx).await {
                        error!(name, ?err, "Failed to run BackupSchedule");
                    }
                } else if job_type == JobType::Prune {
                    let extra_config = json!({
                        "spec": {
                            "template": {
                                "spec": {
                                    "containers": [{
                                        "name": "restic",
                                        "env": [
                                            { "name": "RETAIN", "value": schedule.spec.prune.as_ref().unwrap().retain.to_string() },
                                        ]
                                    }],
                                }
                            }
                        }
                    });

                    if let Err(err) = create_job(&schedule, job_type, ctx, extra_config).await {
                        error!(name, ?err, "Failed to create prune job for BackupSchedule");
                    }
                } else if job_type == JobType::Check {
                    if let Err(err) = create_job(&schedule, job_type, ctx, json!({})).await {
                        error!(name, ?err, "Failed to create check job for BackupSchedule");
                    }
                }
            }
            Err(err) => {
                error!(name, ?err, "Unable to set status for BackupSchedule");
            }
        }
    }
}
