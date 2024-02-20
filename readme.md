Simple Kubernetes backups
===

This Operator is designed to provide simple backups of PVCs in StorageClasses
that implement snapshotting. It works by snapshotting selected PVCs, mounting
those snapshots in a job container, and backing them up to a restic repository.

Please don't use this, it's really not that well tested or built, and
definitely isn't finished.

CRDS
---

**BackupSchedule** - Defines what will get backed up (via a list of selectors),
when (via a crontab scheduling syntax), and where to (via a secret describing
the destination Restic repo). Also may configure common cleanup tasks for the
Restic repository, such as pruning and checking.

**BackupJob** - Defines a specific workload or PVC to be backed up. Normally
created by a *BackupSchedule*, but may also be created manually to create a
snapshot of a workload or PVC.

Annotations
---
For workloads:
- ros.io/before-backup - Command will be run in the container(s) before
  starting snapshot
- ros.io/after-backup - Command will be run in the container(s) after snapshot
  has finished
- ros.io/backup-command - Command will be run within a container, and by
  default the stdout of the command will be


Development
---

```
minikube start --addons volumesnapshots,csi-hostpath-driver
kubectl patch storageclass csi-hostpath-sc -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
k get crd | grep ros.io | awk '{print $1}' | xargs k delete crd
cargo run --bin crdgen | k apply -f -

docker build -f worker/Dockerfile . -t ghcr.io/jess-sol/walle/job:latest
minikube image load ghcr.io/jess-sol/walle/job:latest

k apply -f example/pvc.yml
k create secret generic restic-rest-config --from-env-file=example/restic-secret.env

APP_CONFIG=example/config.yml cargo run

k apply -f example/backup-job.yml
```


TODO
---
- [ ] Job spec needs to mount snapshot
- [ ] Job spec needs to add env vars from secret
- [ ] Create job Dockerfile
- [ ] Record failures of job in BackupJob event stream or status
