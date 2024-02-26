use std::{collections::BTreeMap, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use k8s_openapi::api::core::v1::Namespace;
use kube::{
    api::{DeleteParams, PostParams, WatchParams},
    core::{DynamicObject, GroupVersionKind, WatchEvent},
    runtime::watcher::watch_object,
    Api, ResourceExt,
};
use serde_json::json;
use test_context::AsyncTestContext;
use tokio::task::{JoinHandle, JoinSet};
use walle::{config::AppConfig, crd::BackupJobState, State};

pub struct MyContext {
    pub client: kube::Client,
    pub operator: Option<JoinHandle<()>>,
    pub test_namespaces: Vec<Namespace>,
}

impl MyContext {
    pub async fn start_operator(&mut self, settings: AppConfig) {
        let state = State::new(settings);
        self.operator = Some(tokio::task::spawn(walle::run(state)));
    }

    pub async fn create_test_ns(&mut self, name: &str) -> Result<Namespace, kube::Error> {
        let namespaces: Api<Namespace> = Api::all(self.client.clone());
        let ns = namespaces
            .create(
                &PostParams::default(),
                &serde_json::from_value(json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "generateName": format!("walle-test-{name}-"),
                    }
                }))
                .unwrap(),
            )
            .await?;
        self.test_namespaces.push(ns.clone());
        Ok(ns)
    }

    pub async fn backup_jobs(&self, ns: &str) -> Result<Api<DynamicObject>, kube::Error> {
        let gvk = GroupVersionKind::gvk("ros.io", "v1", "BackupJob");
        let (backupjob_ar, _caps) = kube::discovery::pinned_kind(&self.client, &gvk).await?;
        Ok(Api::<DynamicObject>::namespaced_with(self.client.clone(), ns, &backupjob_ar))
    }

    pub async fn wait_for_job_state(&self, ns: &str, name: &str, state: BackupJobState) {
        let backup_jobs = self.backup_jobs(ns).await.unwrap();
        let mut stream = watch_object(backup_jobs, name).boxed();
        while let Some(data) = stream.try_next().await.unwrap().flatten() {
            if let Some(status) = data.data.get("status") {
                if let Some(cstate) = status.get("state") {
                    if *cstate == serde_json::to_value(&state).unwrap() {
                        break;
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl AsyncTestContext for MyContext {
    async fn setup() -> Self {
        walle::telemetry::init().await;

        MyContext {
            client: kube::Client::try_default().await.unwrap(),
            test_namespaces: Vec::new(),
            operator: None,
        }
    }

    async fn teardown(self) {
        println!("Starting teardown");
        let mut ns_to_delete: Vec<_> = self.test_namespaces.iter().map(|x| x.name_any()).collect();

        // Ensure test namespaces are deleted
        let namespaces: Api<Namespace> = Api::all(self.client.clone());
        for ns in self.test_namespaces {
            let namespaces = namespaces.clone();
            tokio::task::spawn(async move {
                namespaces.delete(ns.name_any().as_str(), &DeleteParams::default()).await
            });
        }

        let mut stream =
            namespaces.watch_metadata(&WatchParams::default(), "0").await.unwrap().boxed();
        while let Some(status) = stream.try_next().await.unwrap() {
            if let WatchEvent::Deleted(s) = status {
                println!("Deleted namespace {}", s.metadata.name.as_deref().unwrap());
                ns_to_delete.retain(|x| x != s.metadata.name.as_deref().unwrap());
                if ns_to_delete.is_empty() {
                    break;
                }
            }
        }

        if let Some(operator) = self.operator {
            // TODO - Graceful option?
            operator.abort_handle();
        }
    }
}
