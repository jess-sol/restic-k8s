use config::AppConfig;
use futures::StreamExt;
use k8s_openapi::api::batch::v1::Job;
use std::{future, hash::Hash, sync::Arc, time::Duration};
use tasks::{schedule::Scheduler, KubeManager};

use chrono::{DateTime, Utc};
use crd::{BackupJob, BackupSchedule, BackupSet};
use kube::{
    api::ListParams,
    core::{DynamicObject, GroupVersionKind},
    runtime::{
        controller::Action, events::Reporter, finalizer::finalizer,
        finalizer::Event as FinalizerEvent, reflector::Store, watcher, Controller,
    },
    Api, Client, Resource, ResourceExt,
};
use serde::Serialize;
use snafu::ResultExt as _;
use tokio::{join, sync::RwLock};
use tracing::{error, field, info, instrument, warn, Span};

pub mod config;
pub mod crd;
pub mod error;
pub mod metrics;
pub mod tasks;
pub mod telemetry;

pub use error::*;

use crate::crd::{BACKUP_JOB_FINALIZER, BACKUP_SET_FINALIZER};

pub const WALLE: &str = "walle";

/// State shared between the controller and the web server
#[derive(Clone, Default)]
pub struct State {
    /// Diagnostics populated by the reconciler
    diagnostics: Arc<RwLock<Diagnostics>>,
    /// Metrics registry
    registry: prometheus::Registry,
    /// Application configuration
    config: AppConfig,
}

/// State wrapper around the controller outputs for the web server
impl State {
    pub fn new(config: AppConfig) -> Self {
        Self { config, ..Default::default() }
    }

    /// Application configuration
    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    /// Metrics getter
    pub fn metrics(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }

    /// State getter
    pub async fn diagnostics(&self) -> Diagnostics {
        self.diagnostics.read().await.clone()
    }

    // Create a Controller Context that can update State
    pub fn to_context<T: Clone + Resource + 'static>(
        &self, kube: KubeManager, store: Store<T>,
    ) -> Arc<Context<T>>
    where
        T::DynamicType: Eq + Hash,
    {
        Arc::new(Context {
            kube,
            config: self.config.clone(),
            // metrics: Metrics::default().register(&self.registry).unwrap(),
            diagnostics: self.diagnostics.clone(),
            store,
        })
    }
}

// Context for our reconciler
#[derive(Clone)]
pub struct Context<T: Clone + Resource + 'static>
where
    T::DynamicType: Eq + Hash,
{
    /// Kubernetes client
    pub kube: KubeManager,
    /// Diagnostics read by the web server
    pub diagnostics: Arc<RwLock<Diagnostics>>,
    /// Prometheus metrics
    // pub metrics: Metrics,
    /// Application configuration
    pub config: AppConfig,

    pub store: Store<T>,
}

// Diagnostics to be exposed by the web server
#[derive(Clone, Serialize)]
pub struct Diagnostics {
    #[serde(deserialize_with = "from_ts")]
    pub last_event: DateTime<Utc>,
    #[serde(skip)]
    pub reporter: Reporter,
}

impl Default for Diagnostics {
    fn default() -> Self {
        Self { last_event: Utc::now(), reporter: format!("{WALLE}-controller").into() }
    }
}

/// Initialize the controller and shared state (given the crd is installed)
pub async fn run(state: State) {
    let client = Client::try_default().await.expect("failed to create kube Client");

    let jobs = tokio::task::spawn(run_backup_jobs(client.clone(), state.clone()));
    let batches = tokio::task::spawn(run_backup_set(client.clone(), state.clone()));
    let schedule = tokio::task::spawn(run_backup_schedules(client, state));
    let _ = join!(jobs, batches, schedule);
}

pub async fn run_backup_schedules(client: Client, state: State) {
    let backup_schedules = Api::<BackupSchedule>::all(client.clone());
    if let Err(e) = backup_schedules.list(&ListParams::default().limit(1)).await {
        error!("CRD is not queryable; {e:?}. Is the CRD installed?");
        info!("Installation: cargo run --bin crdgen | kubectl apply -f -");
        std::process::exit(1);
    }

    let wc = watcher::Config::default().any_semantic();
    let backup_sets = Api::<BackupSet>::all(client.clone());
    let jobs = Api::<Job>::all(client.clone());

    let kube_manager = KubeManager::new(client.clone()).await.unwrap();

    let controller = Controller::new(backup_schedules, wc.clone())
        .owns(backup_sets, wc.clone())
        .owns(jobs, wc.clone())
        .shutdown_on_signal();
    let context = state.to_context(kube_manager, controller.store());
    tokio::task::spawn(
        controller
            .run(reconcile_backup_schedule, error_policy_backup_schedule, context.clone())
            .filter_map(|x| async move { std::result::Result::ok(x) })
            .for_each(|_| future::ready(())),
    );

    let scheduler = Scheduler::new(context);
    tokio::task::spawn(scheduler.run());
}

#[instrument(skip(ctx, backup_schedule), fields(trace_id))]
async fn reconcile_backup_schedule(
    backup_schedule: Arc<BackupSchedule>, ctx: Arc<Context<BackupSchedule>>,
) -> Result<Action> {
    let trace_id = telemetry::get_trace_id();
    Span::current().record("trace_id", &field::display(&trace_id));
    // let _timer = ctx.metrics.count_and_measure();
    ctx.diagnostics.write().await.last_event = Utc::now();
    let backup_schedules: Api<BackupSchedule> = Api::all(ctx.kube.client());

    info!("Reconciling BackupSchedule {}", backup_schedule.name_any());
    finalizer(&backup_schedules, BACKUP_JOB_FINALIZER, backup_schedule, |event| async {
        match event {
            FinalizerEvent::Cleanup(backup_schedule) => backup_schedule.cleanup(ctx.clone()).await,
            FinalizerEvent::Apply(backup_schedule) => backup_schedule.reconcile(ctx.clone()).await,
        }
    })
    .await
    .with_context(|_| FinalizerSnafu)
}

fn error_policy_backup_schedule(
    backup_schedule: Arc<BackupSchedule>, error: &AppError, ctx: Arc<Context<BackupSchedule>>,
) -> Action {
    warn!("reconcile failed: {:?}", error);
    // ctx.metrics.reconcile_failure(&backup_schedule.name_any(), error); // TODO
    Action::requeue(Duration::from_secs(5 * 60))
}

pub async fn run_backup_set(client: Client, state: State) {
    let backup_sets = Api::<BackupSet>::all(client.clone());
    if let Err(e) = backup_sets.list(&ListParams::default().limit(1)).await {
        error!("CRD is not queryable; {e:?}. Is the CRD installed?");
        info!("Installation: cargo run --bin crdgen | kubectl apply -f -");
        std::process::exit(1);
    }

    let wc = watcher::Config::default().any_semantic();
    let backup_jobs = Api::<BackupJob>::all(client.clone());

    let kube_manager = KubeManager::new(client.clone()).await.unwrap();

    let controller =
        Controller::new(backup_sets, wc.clone()).owns(backup_jobs, wc.clone()).shutdown_on_signal();
    let context = state.to_context(kube_manager, controller.store());

    controller
        .run(reconcile_backup_set, error_policy_backup_set, context.clone())
        .filter_map(|x| async move { std::result::Result::ok(x) })
        .for_each(|_| future::ready(()))
        .await
}

#[instrument(skip(ctx, backup_set), fields(trace_id))]
async fn reconcile_backup_set(
    backup_set: Arc<BackupSet>, ctx: Arc<Context<BackupSet>>,
) -> Result<Action> {
    let trace_id = telemetry::get_trace_id();
    Span::current().record("trace_id", &field::display(&trace_id));
    // let _timer = ctx.metrics.count_and_measure();
    ctx.diagnostics.write().await.last_event = Utc::now();
    let backup_sets: Api<BackupSet> = Api::all(ctx.kube.client());

    info!("Reconciling BackupSet {}", backup_set.name_any());
    finalizer(&backup_sets, BACKUP_SET_FINALIZER, backup_set, |event| async {
        match event {
            FinalizerEvent::Cleanup(backup_set) => backup_set.cleanup(ctx.clone()).await,
            FinalizerEvent::Apply(backup_set) => backup_set.reconcile(ctx.clone()).await,
        }
    })
    .await
    .with_context(|_| FinalizerSnafu)
}

fn error_policy_backup_set(
    backup_set: Arc<BackupSet>, error: &AppError, ctx: Arc<Context<BackupSet>>,
) -> Action {
    warn!("Reconcile failed: {:?}", error);
    // ctx.metrics.reconcile_failure(&backup_schedule.name_any(), error); // TODO
    Action::requeue(Duration::from_secs(5 * 60))
}

pub async fn run_backup_jobs(client: Client, state: State) {
    let backup_jobs = Api::<BackupJob>::all(client.clone());
    if let Err(e) = backup_jobs.list(&ListParams::default().limit(1)).await {
        error!("CRD is not queryable; {e:?}. Is the CRD installed?");
        info!("Installation: cargo run --bin crdgen | kubectl apply -f -");
        std::process::exit(1);
    }

    let wc = watcher::Config::default().any_semantic();
    let jobs = Api::<Job>::all(client.clone());

    let gvk = GroupVersionKind::gvk("snapshot.storage.k8s.io", "v1", "VolumeSnapshot");
    let (snapshot_ar, _caps) = kube::discovery::pinned_kind(&client, &gvk).await.unwrap();
    let kube_manager = KubeManager::new(client).await.unwrap();
    let snapshots =
        Api::<DynamicObject>::all_with(kube_manager.client(), &kube_manager.snapshot_ar);

    let controller = Controller::new(backup_jobs, wc.clone())
        .owns(jobs, wc.clone())
        .owns_with(snapshots, snapshot_ar, wc)
        .shutdown_on_signal();
    let store = controller.store();
    controller
        .run(reconcile_backup_job, error_policy_backup_job, state.to_context(kube_manager, store))
        .filter_map(|x| async move { std::result::Result::ok(x) })
        .for_each(|_| future::ready(()))
        .await;
}

#[instrument(skip(ctx, backup_job), fields(trace_id))]
async fn reconcile_backup_job(
    backup_job: Arc<BackupJob>, ctx: Arc<Context<BackupJob>>,
) -> Result<Action> {
    let trace_id = telemetry::get_trace_id();
    Span::current().record("trace_id", &field::display(&trace_id));
    // let _timer = ctx.metrics.count_and_measure();
    ctx.diagnostics.write().await.last_event = Utc::now();
    let ns = backup_job.namespace().unwrap(); // doc is namespace scoped
    let backup_jobs: Api<BackupJob> = Api::namespaced(ctx.kube.client(), &ns);

    info!("Reconciling BackupJob {} in {}", backup_job.name_any(), ns);
    finalizer(&backup_jobs, BACKUP_JOB_FINALIZER, backup_job, |event| async {
        match event {
            FinalizerEvent::Cleanup(backup_job) => backup_job.cleanup(ctx.clone()).await,
            FinalizerEvent::Apply(backup_job) => backup_job.reconcile(ctx.clone()).await,
        }
    })
    .await
    .with_context(|_| FinalizerSnafu)
}

fn error_policy_backup_job(
    backup_job: Arc<BackupJob>, error: &AppError, ctx: Arc<Context<BackupJob>>,
) -> Action {
    warn!("reconcile failed: {:?}", error);
    // ctx.metrics.reconcile_failure(&backup_job.name_any(), error);
    Action::requeue(Duration::from_secs(5 * 60))
}
