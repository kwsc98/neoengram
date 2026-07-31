# Kubernetes Volume-bound Agent

This directory defines the 0.0.1 deployment profile for one existing business PVC:

```text
one business PVC = one StorageVolume = one resident AgentInstance
```

The Agent mounts the complete business PVC at `/volume`. Its identity, private key, certificate, SQLite
Ledger, and recovery state live on a separate RWO PVC at `/var/lib/neoengram-agent`. The central service,
Web application, and Agent state database must not mount the business PVC.

These manifests are forward-looking deployment templates. The repository does not yet contain a runnable
Agent daemon, registration transport, or the `health` command used by the probes. Replace the image only
after those interfaces exist; applying the example unchanged will not produce a working Agent.

## Preconditions

- The namespace and business PVC already exist. A Pod can reference only a PVC in its own namespace.
- The business PVC maps to exactly one NeoEngram StorageVolume. Do not reuse an overlapping NFS export or
  alias as a second writable StorageVolume.
- Use `ReadWriteMany` when the resident Agent and business Pods may run on different nodes. `ReadWriteOnce`
  is acceptable only for a same-node POC with enforced co-scheduling; it limits nodes, not application writers.
- The business volume is a filesystem volume. For NFS, validate NFSv4.1/4.2, hard-mount, locking, rename,
  fsync, permissions, stale-handle, and failover behavior before using it for data.
- UID/GID `65532:65532` can traverse and write the prepared business root. The state volume root is owned by
  `65532:65532` with mode `0700`. The template deliberately has no Pod-level `fsGroup`: the Agent verifies
  these permissions but never recursively changes an existing business PVC. Prepare ownership through the
  storage system or a separately reviewed, state-volume-only initialization procedure.
- The business root contains `/volume/.neoengram-volume-marker` as a regular file whose single-line value is
  the configured StorageVolume ID. A missing, symbolic-link, malformed, or mismatched marker fails closed.
  This marker detects configuration drift; it cannot prove a Kubernetes PVC UID or provide storage fencing.
- A TenantAdmin has issued a 15-minute, one-time bootstrap token scoped to the intended Tenant,
  EdgeCluster, StorageVolume descriptor, access mode, and PVC reference. The platform administrator receives
  it only to deploy this Agent.

## Prepare The Manifests

Copy the four templates once per business PVC. Use two distinct identifiers:

- `volume-example` is the complete OpenAPI StorageVolume ID. Keep it in the ConfigMap and annotations; it may
  be 128 characters and contain characters such as `:` that Kubernetes labels reject.
- `volume-safe-slug` is a stable DNS-1123 label, preferably a short readable prefix plus a collision-resistant
  hash and no more than 40 characters. Use it only in Kubernetes names, selectors, and labels.

Replace every `volume-example`, `volume-safe-slug`, `example`, or `replace-with-...` value. Resource names
must remain Volume-specific; never let two Agents in one namespace share a ConfigMap, bootstrap Secret, or
state PVC. In particular, set:

- all resource names and labels, namespace, Tenant, EdgeCluster, Region, and StorageVolume IDs;
- the existing business PVC claim name;
- the declared PVC namespace/claim and expected marker in `configmap.yaml`;
- the public token ID in `registration.token_id` for stable bootstrap lookup/audit;
- a durable RWO StorageClass for `agent-state-pvc.yaml`;
- the central HTTPS endpoint and a real, digest-pinned Agent image;
- a new bootstrap token in a local copy of `secret.example.yaml`.

Run `bash check-manifests.sh` before rendering or applying a copy. It enforces the 0.0.1 placement and
container-security invariants without contacting a cluster. It is not Kubernetes OpenAPI validation; the
rendered manifests must additionally pass the target cluster's server-side dry run and admission policies.

Before starting the Agent, mount the business PVC through an independently reviewed maintenance path and
atomically create the regular marker file with the exact StorageVolume ID plus a trailing newline. Never let
the Agent silently create, replace, or follow a symbolic-link marker: a missing marker is an approval blocker,
and a mismatch means the Deployment is attached to the wrong managed root.

Do not commit the rendered Secret. Prefer creating it directly from a protected file:

```sh
kubectl -n <namespace> create secret generic neoengram-agent-bootstrap-<volume-safe-slug> \
  --from-file=bootstrap-token=/secure/path/bootstrap-token
```

The token authenticates only the registration request. It must not authorize a control session, Job,
Tenant queue, or Volume ownership. The Agent generates its private key and stable registration request ID
before the first network request and persists both on the state PVC. After approval, the center invalidates
the bootstrap token and the Agent persists its issued identity and certificate on the state PVC.

## Apply And Approve

Apply the non-secret resources, create the Secret, then create the Deployment:

```sh
kubectl apply -f agent-state-pvc.yaml
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
```

The Agent initiates all connections to the center. It does not receive a ServiceAccount token, call the
Kubernetes API, expose a Service/Ingress, or depend on an Operator. No Service, Ingress, HPA, Role, or
RoleBinding belongs in this profile.

Cluster operators should additionally apply their namespace default-deny policy and an egress allowlist for
DNS, the configured center, approved object endpoints, and the business storage service. Those addresses are
environment-specific, so this directory does not ship a permissive or nonfunctional NetworkPolicy example.

The center creates an idempotent `pending_approval` Storage enrollment, not a Ready Volume. TenantAdmin
reviews only the public Volume/PVC scope, Agent version, public-key identity summary, and sanitized probe
result. Separately, the platform operator verifies the Deployment, actual PVC attachment, marker, and raw
mount evidence through an internal operational channel; raw mount fingerprints never enter the public API.
Approval creates or binds the StorageVolume as Unavailable and creates the AgentInstance. Only an approved
identity with a valid certificate, session, matching generations, healthy RW observation, and completed
recovery can make the Volume Ready or receive a Job.

Approval is a control-plane trust gate, not a filesystem permission gate: this Pod already has its declared
PVC mount. Bootstrap requires evidence from the real mount, including its marker and RW probe, so 0.0.1 does
not support approval before all data access. That policy requires a future pre-mount enrollment contract and
deployment workflow; removing the PVC from this template does not create a valid two-stage registration.

## Operational Invariants

- Keep `replicas: 1`, `strategy.type: Recreate`, and no HPA. Never use a DaemonSet for this profile.
- Mount the whole business PVC at `/volume`; do not use `subPath` for the Agent.
- Never put Agent identity, SQLite, WAL/SHM, or Ledger files on `/volume`.
- Reuse the same state PVC for an ordinary Pod restart. A lost or replaced state PVC requires a new
  registration and first approval; it must not inherit an Agent ID from the business volume.
- If a bootstrapped candidate is rejected or its review window expires, retire that installation identity
  and key. Re-enrollment requires freshly initialized Agent state, a new key, token, and request identity.
  A token that expires before bootstrap has no candidate binding and only requires a new token request.
- A ConfigMap or Secret replacement requires a manual Recreate rollout and a new revision annotation.
- Delete the one-time bootstrap Secret after the enrollment has consumed it and the Agent has persisted its
  issued identity/certificate. The Secret volume is optional so ordinary restarts use the state PVC instead
  of retaining or reusing bootstrap authority. If state is lost, create a fresh Secret and approval request.
- Liveness checks only local process/ledger health. Readiness additionally requires approval, an active
  center session, and a matching mount fingerprint; loss of the center must not cause destructive restart
  loops.
- Kubernetes rollout settings and central generations provide cooperative fencing only. They do not stop a
  partitioned or compromised process that still owns RW storage credentials.

Use [TAKEOVER.md](TAKEOVER.md) for every Agent replacement that cannot reuse the original identity safely.
