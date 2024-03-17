Walle
===

Simple Kubernetes backups.

This Operator is designed to provide simple backups of PVCs in StorageClasses
that implement snapshotting. It works by snapshotting selected PVCs, mounting
those snapshots in a job container, and backing them up to a restic repository.

Please don't use this, it's really not that well tested or built, and
definitely isn't finished.

CRDS
---

**BackupSchedule** - Defines what will get backed up (via a list of field and
label selectors known as plans), when (via an interval scheduling syntax), and
where to (via a secret reference describing the destination Restic repo). Also
may configure common cleanup tasks for the Restic repository, such as pruning
and checking (Not implemented). Settings of the first matching plan will be
used for PVCs in multiple plans.

**BackupSet** - Defines a set of *BackupJobs* which are grouped together in a
single run of a *BackupSchedule*. Currently it's not possible to create a
*BackupSet* manually.

**BackupJob** - Defines a specific workload or PVC to be backed up. Normally
created by a *BackupSchedule*, but may also be created manually to create a
snapshot of a workload or PVC.

Annotations
---
*Not currently implemented.* This is future state, annotations will be
available to help customize how to backup a workload.

For workloads:
- ros.io/before-backup - Command will be run in the container(s) before
  starting snapshot
- ros.io/after-backup - Command will be run in the container(s) after snapshot
  has finished
- ros.io/backup-command - Command will be run within a container, and by
  default the stdout of the command will be

Getting Started
---

1. Install operator
    ```bash
    cargo run --bin crdgen | kubectl apply -f -

    kubectl create namespace walle
    helm upgrade -n walle --install walle ./helm/charts/walle -f example/values.yml
    ```

2. Create secret describing Restic repository
    ```bash
    # See restic docs for details on available environment variables
    # https://restic.readthedocs.io/en/latest/030_preparing_a_new_repo.html
    kubectl create secret generic -n walle restic-repo \
      --from-literal INITIALIZE_REPO=yes \
      --from-literal RESTIC_PASSWORD=averysecurepassword \
      --from-literal RESTIC_REST_USERNAME=user \
      --from-literal RESTIC_REST_PASSWORD=pass
    ```

3. Create a BackupSchedule
    ```bash
    cat <<'EOF' | k apply -f -
    ---
    apiVersion: ros.io/v1
    kind: BackupSchedule
    metadata:
      name: example-schedule
    spec:
      interval: 6h
      plans:
      - type: pod
      repository:
        name: restic-repo
        namespace: walle
    EOF
    ```
Restoring
---

Right now restoring backups is a manual process. Likely there'll never be a
restore job type CRD, but instead a CLI command run from an administrator's
workstation which will restore files to PVs in a cluster.

Here is an example of a manual restoration:

1. Ensure no reconciliation loop is running on workload being restored, then
   delete any pods utilizing PVC to be restored

2. Create new pod which mounts PVC
    ```yaml
    apiVersion: v1
    kind: Pod
    metadata:
      name: restic-restore
      namespace: <NAMESPACE>
    spec:
      containers:
      - name: postgres
        image: debian:stable-slim
        command: [sleep, infinity]
        volumeMounts:
        - mountPath: /data
          name: restore
      volumes:
      - name: restore
        persistentVolumeClaim:
          claimName: <PVC NAME TO RESTORE TO>
    ```

3. Mount Restic backups, this'll allow ownership and permissions to kept
    ```bash
    mkdir tmp
    resticprofile <PROFILE>.mount --tag <PVC NAME> tmp/
    ```

4. Finally restore!
    ```bash
    tar -C tmp/tags/<PVC NAME>/latest -cf - . | pv | kubectl exec -i -n <NAMESPACE> restic-restore -- tar -C /data -xf -
    ```

Building images
---
```bash
docker build -f worker/Dockerfile . -t ghcr.io/jess-sol/walle/worker:latest
docker build . -t ghcr.io/jess-sol/walle/operator:latest
```

Development
---
```bash
minikube start --addons volumesnapshots,csi-hostpath-driver
kubectl patch storageclass csi-hostpath-sc -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
k get crd | grep ros.io | awk '{print $1}' | xargs k delete crd
cargo run --bin crdgen | k apply -f -

k apply -f example/workload.yml
k create secret generic restic-rest-config --from-env-file=example/restic-secret.env

RUST_LOG=info,walle=debug APP_CONFIG=example/config.yml cargo run # Or with Helm

k apply -f example/backup-job.yml
# Or
k apply -f example/backup-schedule.yml
```
