use config::AppConfig;
use futures::StreamExt;
use k8s_openapi::{
    api::{
        batch::v1::Job,
        core::v1::{PersistentVolumeClaim, Secret, ServiceAccount},
        rbac::v1::{ClusterRole, RoleBinding},
    },
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use serde_json::json;
use std::{
    collections::BTreeMap, fs::read_to_string, future, str::FromStr, sync::Arc, time::Duration,
};

use chrono::{DateTime, Utc};
use crd::{BackupJob, BackupJobState, BackupSchedule};
use kube::{
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams, PropagationPolicy},
    core::{DynamicObject, GroupVersionKind},
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
        finalizer::{finalizer, Event as FinalizerEvent},
        watcher, Controller,
    },
    Api, Client, Resource as _, ResourceExt,
};
use serde::Serialize;
use snafu::{OptionExt, ResultExt as _};
use tokio::sync::RwLock;
use tracing::{error, field, info, instrument, warn, Span};

pub mod config;
pub mod crd;
pub mod error;
pub mod metrics;
pub mod telemetry;

pub use error::*;
use metrics::Metrics;

use crate::crd::{BackupJobStatus, BACKUP_JOB_FINALIZER};

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
    pub fn to_context(&self, client: Client) -> Arc<Context> {
        Arc::new(Context {
            client,
            config: self.config.clone(),
            metrics: Metrics::default().register(&self.registry).unwrap(),
            diagnostics: self.diagnostics.clone(),
        })
    }
}

// Context for our reconciler
#[derive(Clone)]
pub struct Context {
    /// Kubernetes client
    pub client: Client,
    /// Diagnostics read by the web server
    pub diagnostics: Arc<RwLock<Diagnostics>>,
    /// Prometheus metrics
    pub metrics: Metrics,
    /// Application configuration
    pub config: AppConfig,
}

#[instrument(skip(ctx, backup_job), fields(trace_id))]
async fn reconcile(backup_job: Arc<BackupJob>, ctx: Arc<Context>) -> Result<Action> {
    let trace_id = telemetry::get_trace_id();
    Span::current().record("trace_id", &field::display(&trace_id));
    let _timer = ctx.metrics.count_and_measure();
    ctx.diagnostics.write().await.last_event = Utc::now();
    let ns = backup_job.namespace().unwrap(); // doc is namespace scoped
    let backup_jobs: Api<BackupJob> = Api::namespaced(ctx.client.clone(), &ns);

    info!("Reconciling Document \"{}\" in {}", backup_job.name_any(), ns);
    finalizer(&backup_jobs, BACKUP_JOB_FINALIZER, backup_job, |event| async {
        match event {
            FinalizerEvent::Apply(backup_job) => backup_job.reconcile(ctx.clone()).await,
            FinalizerEvent::Cleanup(backup_job) => backup_job.cleanup(ctx.clone()).await,
        }
    })
    .await
    .with_context(|_| FinalizerSnafu)
}

fn error_policy(doc: Arc<BackupJob>, error: &AppError, ctx: Arc<Context>) -> Action {
    warn!("reconcile failed: {:?}", error);
    ctx.metrics.reconcile_failure(&doc, error);
    Action::requeue(Duration::from_secs(5 * 60))
}

impl BackupSchedule {
    async fn reconcile(&self, ctx: Arc<Context>) -> Result<Action> {
        // create_backup_jobs()
        // 1. For each plan:
        //   1. Find workloads based on selector & namespace_selector
        //   2. Get PVCs tied to workloads
        //   3. Filter PVCs based on pvc_selector
        //   4. Add PVCs, and their related workload to list, this means last entry wins (if
        //      multiple plans cover the same PVC)
        // 2. For each PVC in list, create BackupJob

        // Monitor BackupSchedule
        // - Create BackupJobs, if immediately is true (default)
        // - Add entry to cron
        // Run cron
        // - Create BackupJobs

        Ok(Action::requeue(Duration::from_secs(60 * 5)))
    }
}

impl BackupJob {
    async fn reconcile(&self, ctx: Arc<Context>) -> Result<Action> {
        let client = ctx.client.clone();
        let recorder = ctx.diagnostics.read().await.recorder(client.clone(), self);
        let ns = self.namespace().unwrap();
        let name = self.name_any();
        let backup_jobs: Api<BackupJob> = Api::namespaced(client.clone(), &ns);
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

            return Ok(Action::requeue(Duration::ZERO));
        };

        let snapshot_name = status.destination_snapshot.as_deref();
        let job_name = status.backup_job.as_deref();

        // TODO - Support v1 and v1beta1
        // let apigroup =
        //     kube::discovery::pinned_group(&client, &GroupVersion::gv("snapshot.storage.k8s.io", "v1"))
        //         .await
        //         .unwrap();
        // println!("{:?}", apigroup.recommended_kind("VolumeSnapshot"));
        let gvk = GroupVersionKind::gvk("snapshot.storage.k8s.io", "v1", "VolumeSnapshot");
        let (ar, _caps) = kube::discovery::pinned_kind(&client, &gvk).await.unwrap();
        let snapshots = Api::<DynamicObject>::namespaced_with(client.clone(), &ns, &ar);

        match status.state {
            BackupJobState::NotStarted => {
                let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &ns);
                let Some(pvc) = pvcs.get_opt(&self.spec.source_pvc).await.unwrap() else {
                    recorder
                        .publish(Event {
                            type_: EventType::Warning,
                            reason: "MissingPVC".into(),
                            note: Some("Unable to find source_pvc".to_string()),
                            action: "Waiting".into(),
                            secondary: None,
                        })
                        .await
                        .with_context(|_| KubeSnafu {
                            msg: "Unable to send event for backupjob",
                        })?;
                    return Ok(Action::requeue(Duration::from_secs(30)));
                };
                let pvc_spec = pvc.spec.with_context(|| InvalidPVCSnafu)?;
                let storage_class = pvc_spec.storage_class_name.with_context(|| InvalidPVCSnafu)?;

                let snap_class = ctx
                    .config
                    .snap_class_mappings
                    .iter()
                    .find(|x| x.storage_class == storage_class)
                    .map(|x| &x.snapshot_class);

                let created_snap = snapshots
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
                        .unwrap(),
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
                                "destination_snapshot": Some(created_snap.name_any()),
                            },
                        })),
                    )
                    .await
                    .with_context(|_| KubeSnafu { msg: "Failed up update backupjob status" })?;
            }
            BackupJobState::CreatingSnapshot => {
                let snapshot =
                    snapshots.get(snapshot_name.unwrap()).await.with_context(|_| KubeSnafu {
                        msg: format!(
                            "Unable to get referenced snapshot: {}",
                            snapshot_name.unwrap_or("<No snapshot name>")
                        ),
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

                let job_builder = JobBuilder {
                    name_ref: format!("{WALLE}-{}-", &self.spec.source_pvc),
                    namespace: ns.clone(),
                    service_account: ctx.config.worker_service_account_name.clone(),
                    repository_secret: self.spec.repository.name.clone(),
                    repository_secret_ns: operator_namespace.clone(),
                    owner_references: vec![self.controller_owner_ref(&()).unwrap()],
                    image: ctx.config.backup_job_image.clone(),
                    task: "backup".to_string(),
                    env: maplit::btreemap![
                        "REPOSITORY_SECRET" => self.spec.repository.to_string(),
                        "OPERATOR_NAMESPACE" => operator_namespace.clone(),
                        "K8S_CLUSTER_NAME" => ctx.config.cluster_name.clone(),
                        "TRACE_ID" => telemetry::get_trace_id().to_string(),

                        "SOURCE_PATH" => format!("/data/{}/", self.spec.source_pvc),
                        "SNAPSHOT_TIME" => snapshot_creation_time.to_string(),
                        "PVC_NAME" => self.spec.source_pvc.clone(),
                    ],
                };

                let client = kube::Client::try_default().await.unwrap();
                job_builder.ensure_rbac(&client).await;
                let created_job = job_builder.create_job(&client).await.unwrap();

                let _o = backup_jobs
                    .patch_status(
                        &name,
                        &ps,
                        &Patch::Merge(json!({
                            "status": {
                                "state": BackupJobState::BackingUp,
                                "start_time": Some(snapshot_creation_time),
                                "backup_job": Some(created_job.name_any()),
                            },
                        })),
                    )
                    .await
                    .with_context(|_| KubeSnafu { msg: "Failed up update backupjob status" })?;
            }

            BackupJobState::BackingUp => {
                let jobs: Api<Job> = Api::namespaced(client.clone(), &ns);
                let Some(job) = jobs
                    .get_opt(job_name.unwrap())
                    .await
                    .with_context(|_| KubeSnafu { msg: "Failed to retrieve jobs" })?
                else {
                    // TODO - Recreate job?
                    return Ok(Action::requeue(Duration::from_secs(5)));
                };

                let succeeded = job.status.as_ref().map(|x| x.succeeded.unwrap_or(0)).unwrap_or(0);
                let failed = job.status.as_ref().map(|x| x.failed.unwrap_or(0)).unwrap_or(0);

                if succeeded > 0 || failed >= 5 {
                    // Cleanup
                    if let Some(job_name) = job_name {
                        let jobs: Api<Job> = Api::namespaced(client.clone(), &ns);
                        let _job = jobs
                            .delete(
                                job_name,
                                &DeleteParams {
                                    propagation_policy: Some(PropagationPolicy::Foreground),
                                    ..Default::default()
                                },
                            )
                            .await
                            .with_context(|_| KubeSnafu {
                                msg: "Failed to cleanup job associated with backupjob",
                            });
                    }
                    if let Some(snapshot_name) = snapshot_name {
                        let _ss = snapshots
                            .delete(snapshot_name, &DeleteParams::default())
                            .await
                            .with_context(|_| KubeSnafu {
                                msg: "Failed to cleanup snapshot associated with backupjob",
                            });
                    }

                    let state = if succeeded > 0 {
                        BackupJobState::Finished
                    } else {
                        BackupJobState::Failed
                    };

                    let _o = backup_jobs
                        .patch_status(
                            &name,
                            &ps,
                            &Patch::Merge(json!({
                                "status": {
                                    "state": state,
                                    "backup_job": Option::<String>::None,
                                    "destination_snapshot": Option::<String>::None,
                                    "finish_time": Some(Utc::now()),
                                },
                            })),
                        )
                        .await
                        .with_context(|_| KubeSnafu { msg: "Failed up update backupjob status" })?;
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
    async fn cleanup(&self, ctx: Arc<Context>) -> Result<Action> {
        let recorder = ctx.diagnostics.read().await.recorder(ctx.client.clone(), self);
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

struct JobBuilder {
    name_ref: String, // What the job is working on, usually a pvc name
    namespace: String,

    service_account: String,
    repository_secret: String,
    repository_secret_ns: String,

    owner_references: Vec<OwnerReference>,
    image: String,
    task: String,
    env: BTreeMap<&'static str, String>,
}

impl JobBuilder {
    /// Ensure RBAC is properly configured for the job to be able to access the resources it needs
    /// when created with the provided service account in the provided namespace.
    /// Namely access to the repository_secret reference.
    pub async fn ensure_rbac(&self, client: &kube::Client) {
        let secrets: Api<Secret> = Api::namespaced(client.clone(), &self.repository_secret_ns);
        let secret = secrets.get(&self.repository_secret).await.unwrap();

        // 1. Create serviceaccount/walle-worker in the backup job namespace
        let service_accounts: Api<ServiceAccount> =
            Api::namespaced(client.clone(), &self.namespace);

        let service_account =
            if let Some(sa) = service_accounts.get_opt("walle-worker").await.unwrap() {
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

        let role_name = format!("walle-read-{}", self.repository_secret);
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
                            "resourceNames": [self.repository_secret],
                            "verbs": ["get"],
                        }],
                    }))
                    .expect("Invalid predefined cluster role json"),
                )
                .await
                .unwrap()
        };

        // 3. Create or update rolebinding in secret_ns
        let role_bindings: Api<RoleBinding> =
            Api::namespaced(client.clone(), &self.repository_secret_ns);

        let binding_name = format!("walle-read-{}", self.repository_secret);
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
                                "namespace": self.namespace,
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
                                "name": format!("walle-read-{}", self.repository_secret),
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
                                "namespace": self.namespace,
                            }]
                        }))
                        .expect("Invalid predefined cluster role json"),
                    )
                    .await
                    .unwrap()
            };
    }

    pub async fn create_job(&self, client: &Client) -> Result<Job> {
        let jobs: Api<Job> = Api::namespaced(client.clone(), &self.namespace);

        jobs.create(&PostParams::default(), &serde_json::from_value(self.job_spec()).unwrap())
            .await
            .with_context(|_| KubeSnafu { msg: "Failed to create snapshot for backupjob" })
    }

    fn job_spec(&self) -> serde_json::Value {
        let env = self
            .env
            .iter()
            .map(|(k, v)| json!({ "name": k, "value": v }))
            .collect::<serde_json::Value>();

        json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "generateName": format!("{WALLE}-{}-{}-", &self.task, &self.name_ref),
                "namespace": self.namespace,
                "labels": {
                    "app.kubernetes.io/created-by": WALLE,
                },
                "ownerReferences": self.owner_references.as_slice(),
            },
            "spec": {
                "template": {
                    "spec": {
                        "serviceAccountName": &self.service_account,
                        "containers": [{
                            "name": "restic",
                            "image": &self.image,
                            "args": &[&self.task],
                            "env": &env,
                        }],
                        "restartPolicy": "Never",
                    }
                }
            }
        })
    }
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
impl Diagnostics {
    fn recorder(&self, client: Client, doc: &BackupJob) -> Recorder {
        Recorder::new(client, self.reporter.clone(), doc.object_ref(&()))
    }
}

/// Initialize the controller and shared state (given the crd is installed)
pub async fn run(state: State) {
    let client = Client::try_default().await.expect("failed to create kube Client");
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
    let snapshots = Api::<DynamicObject>::all_with(client.clone(), &snapshot_ar);

    Controller::new(backup_jobs, wc.clone())
        .owns(jobs, wc.clone())
        .owns_with(snapshots, snapshot_ar, wc)
        .shutdown_on_signal()
        .run(reconcile, error_policy, state.to_context(client))
        .filter_map(|x| async move { std::result::Result::ok(x) })
        .for_each(|_| future::ready(()))
        .await;
}
