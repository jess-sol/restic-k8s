use chrono::Utc;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info};

use crate::{
    crd::{BackupJob, BackupJobState, BackupSet, BackupSetState, BackupSetStatus, Statistics},
    tasks::{label_selector_to_filter, DateTimeFormatK8s},
    Context, KubeSnafu, Result, WALLE,
};
use kube::{
    api::{ListParams, Patch, PatchParams},
    runtime::{
        controller::Action,
        events::{Event, EventType},
    },
    Api, ResourceExt as _,
};
use snafu::ResultExt as _;

impl BackupSet {
    pub async fn reconcile(&self, ctx: Arc<Context<Self>>) -> Result<Action> {
        let name = self.name_any();
        let backup_sets: Api<BackupSet> = Api::all(ctx.kube.client());
        let recorder = ctx.kube.recorder(self);

        debug!(name, "Reconciling BackupSet");

        // TODO - Get owning BackupSchedule, add as secondary for events

        // Set initial status if none
        let Some(status) = self.status.as_ref() else {
            let _o = backup_sets
                .patch_status(
                    &self.name_any(),
                    &PatchParams::apply(WALLE),
                    &Patch::Apply(json!({
                        "apiVersion": "ros.io/v1",
                        "kind": "BackupSet",
                        "status": BackupSetStatus::default()
                    })),
                )
                .await
                .with_context(|_| KubeSnafu { msg: "Failed up update BackupSet status" })?;

            return Ok(Action::requeue(Duration::from_secs(5)));
        };

        match status.state {
            BackupSetState::Running => {
                let backup_job_api: Api<BackupJob> = Api::all(ctx.kube.client());

                info!(
                    set = name,
                    selector = label_selector_to_filter(&self.spec.selector),
                    "Label selector"
                );
                // self.spec.selector
                let backup_jobs = backup_job_api
                    .list(
                        &ListParams::default()
                            .labels(&label_selector_to_filter(&self.spec.selector)),
                    )
                    .await
                    .context(KubeSnafu {
                        msg: "Failed to list BackupJobs associated with BackupSet",
                    })?;

                // Get stats of backup jobs in current batch
                let mut stats =
                    Statistics { total_jobs: backup_jobs.items.len(), ..Default::default() };

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

                let mut new_status = json!({
                    "statistics": stats,
                    "completions": stats.to_string(),
                });

                // If all jobs finished, set state and create event
                if stats.finished_jobs + stats.failed_jobs > 0
                    && stats.unstarted_jobs == 0
                    && stats.running_jobs == 0
                {
                    let state = if stats.failed_jobs == 0 {
                        BackupSetState::Finished
                    } else {
                        BackupSetState::FinishedWithFailures
                    };

                    let finish_time = Utc::now();

                    let event = if stats.failed_jobs == 0 {
                        Event {
                            type_: EventType::Normal,
                            reason: "FinishedBackups".into(),
                            note: Some("Backups finished with no errors or warnings".into()),
                            action: "FinishedBackups".into(),
                            secondary: None,
                        }
                    } else {
                        Event {
                            type_: EventType::Warning,
                            reason: "FinishedWithFailures".into(),
                            note: Some(format!(
                                "Backups finished with {} failing jobs",
                                stats.failed_jobs
                            )),
                            action: "FinishedWithFailures".into(),
                            secondary: None,
                        }
                    };

                    if let Err(err) = recorder.publish(event).await {
                        error!(name, ?err, "Failed to add event to BackupSet");
                    }

                    new_status = json!({
                        "state": state,
                        "finish_time": finish_time.to_k8s_ts(),
                        "statistics": stats,
                        "completions": stats.to_string(),
                    });
                }

                // Update status with new run stats and maybe state
                if let Err(err) = backup_sets
                    .patch_status(
                        &name,
                        &PatchParams::apply(WALLE),
                        &Patch::Merge(json!({
                            "apiVersion": "ros.io/v1",
                            "kind": "BackupSet",
                            "status": new_status,
                        })),
                    )
                    .await
                {
                    error!(name, ?err, "Unable to set status for BackupSet");
                }
            }

            BackupSetState::Finished | BackupSetState::FinishedWithFailures => {}
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
}
