use common::MyContext;
use k8s_openapi::api::{
    core::v1::{Node, PersistentVolumeClaim, Secret, ServiceAccount},
    rbac::v1::{ClusterRole, RoleBinding},
};
use kube::{api::PostParams, Api, ResourceExt};
use serde_json::json;
use test_context::test_context;
use walle::{config::StorageClassToSnapshotClass, crd::BackupJobState};

mod common;

#[test_context(MyContext)]
#[tokio::test]
async fn test_backup_job_rbac_creation(ctx: &mut MyContext) {
    // Validate current context is minikube
    let nodes: Api<Node> = Api::all(ctx.client.clone());
    assert!(nodes.get_opt("minikube").await.expect("Unable to get nodes").is_some());

    // Create testing namespaces
    let ns_a = ctx.create_test_ns("a").await.unwrap();
    let ns_b = ctx.create_test_ns("b").await.unwrap();

    // Start operator
    ctx.start_operator(walle::config::AppConfig {
        snap_class_mappings: vec![StorageClassToSnapshotClass {
            storage_class: "csi-hostpath-sc".to_string(),
            snapshot_class: "csi-hostpath-snapclass".to_string(),
        }],
        cluster_name: "minikube".to_string(),
        backup_job_image: "alpine:latest".to_string(),
    })
    .await;

    // Create secret in ns a
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns_a.name_any());
    secrets
        .create(
            &PostParams::default(),
            &serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": "test-repo",
                },
                "data": {}
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    // Create test PVC
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), &ns_b.name_any());
    let pvc = pvcs
        .create(
            &PostParams::default(),
            &serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "name": "test-volume",
                },
                "spec": {
                    "storageClassName": "csi-hostpath-sc",
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {
                        "requests": {
                            "storage": "1Gi"
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    // Create BackupJob in ns b using secret in ns a
    let backup_jobs = ctx.backup_jobs(&ns_b.name_any()).await.unwrap();
    let job = backup_jobs
        .create(
            &PostParams::default(),
            &serde_json::from_value(json!({
                "apiVersion": "ros.io/v1",
                "kind": "BackupJob",
                "metadata": {
                    "name": "test-job",
                },
                "spec": {
                    "sourcePvc": "test-volume",
                    "repository": {
                        "name": "test-repo",
                        "namespace": ns_a.name_any(),
                    }
                }
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    // Wait for job to reach BackingUp state
    ctx.wait_for_job_state(&ns_b.name_any(), "test-job", BackupJobState::BackingUp).await;

    // Validate service account created in ns b
    let service_accounts: Api<ServiceAccount> =
        Api::namespaced(ctx.client.clone(), &ns_b.name_any());
    assert!(service_accounts.get_opt("walle-worker").await.unwrap().is_some());

    // Validate clusterrole
    let cluster_roles: Api<ClusterRole> = Api::all(ctx.client.clone());
    assert!(cluster_roles.get_opt("walle-read-test-repo").await.unwrap().is_some());

    // Validate rolebinding in ns b
    let rolebindings: Api<RoleBinding> = Api::namespaced(ctx.client.clone(), &ns_a.name_any());
    assert!(rolebindings.get_opt("walle-read-test-repo").await.unwrap().is_some());
}
