# Kubernetes Volume-bound Agent

These manifests implement the Gateway-only Agent network topology. Each Agent connects to its EdgeCluster's
multi-replica GatewayPool and has no Central endpoint or fallback. See
[`docs/synapse-gateway-architecture.md`](../../../docs/synapse-gateway-architecture.md). The checked-in
Gateway and Agent runtimes implement H2/mTLS identity validation and fail closed until an authenticated
Central control session and a generation-current Agent route are established. Production certificate
provisioning, two-replica E2E, and cutover validation remain deployment gates.

This directory defines the 0.0.1 deployment profile for one existing business PVC:

```text
one business PVC = one StorageVolume = one resident AgentInstance
```

The Agent mounts the complete business PVC at `/volume`. Signing and approved identity, bootstrap polling
watermark, health state, session recovery state, per-Tenant Job Ledger, and durable outbound report queue
belong on a separate RWO PVC at `/var/lib/neoengram-agent`. The central service, Web application, and Agent
state databases must not mount the business PVC. Workspaces and immutable Chunk bytes belong to the business
PVC; the Agent stores each Chunk at
`/volume/.neoengram/objects/tenants/<tenant>/artifacts/<artifact>/objects/<object_id>`. The center stores Manifests,
Index state, and placement evidence, but never receives or persists Chunk payloads.

The repository contains the runnable `neoengram-agent` binary in the `neoengram-agentd` package. The Agent
initiates the control connection to its configured GatewayPool with the independent OpenAPI action
`POST /agent/session/channel/open`, then keeps an HTTP/2 full-duplex NDJSON stream open so Central can
logically invoke the Agent through the Gateway by pushing Assignment and Decision frames downstream.
Heartbeat and Job reports flow upstream on the same channel. Bootstrap, MetadataBatch pages, and Index pages remain separate
action-style POST operations under `/agent/*`; there is no center-facing missing-object or object-upload
operation. The legacy message-list poll is compatibility and manual-recovery only. Approved Ed25519 keys
authenticate every upstream frame or unary request, and the bootstrap token never becomes a session
credential. The configured `trust_bundle_file` is the exclusive server-auth trust root for both unary and
streaming Gateway requests; system roots are not used by the production construction path. Agent workload
certificate issuance, renewal and production credential provisioning remain deployment responsibilities; runtime
mTLS identity validation and certificate installation are implemented. The example image is
only a placeholder and must be replaced with a real, digest-pinned build before applying these manifests.

For HTTPS, CA trust is necessary but not sufficient. Standard TLS verification requires the configured
`gateway_endpoint` host to appear as a DNS or IP SAN in the serving certificate. The Agent additionally
requires exactly one workload URI SAN under `gateway_workload_trust_domain`, with the shape
`spiffe://<trust-domain>/workloads/edge-clusters/<edge_cluster_id>/gateway-pools/<gateway_pool_id>/gateway-replicas/<gateway_replica_id>`.
The URI must name the configured `edge_cluster_id`; a certificate for an Agent, another EdgeCluster, or a
different trust domain fails closed even when it chains to the configured CA. Because the Pool Service may
route to any Replica, every activated Replica certificate must cover the Pool `gateway_endpoint` host as well
as that Replica's registered bootstrap, control, and peer endpoint hosts.

Central actively connects to registered Gateway replicas, while the Agent still initiates its only active
edge connection. The Gateway does not mount this business PVC and cannot replace Agent-side hash verification
or durability barriers.

## Preconditions

- The namespace and business PVC already exist. A Pod can reference only a PVC in its own namespace.
- The business PVC maps to exactly one NeoEngram StorageVolume. Do not reuse an overlapping NFS export or
  alias as a second writable StorageVolume.
- Use `ReadWriteMany` when the resident Agent and business Pods may run on different nodes. `ReadWriteOnce`
  is acceptable only for a same-node POC with enforced co-scheduling; it limits nodes, not application writers.
- The business volume is a filesystem volume. For NFS, validate NFSv4.1/4.2, hard-mount, locking, rename,
  fsync, permissions, stale-handle, and failover behavior before using it for data.
- The business PVC must have capacity and inode headroom for both Playground files and immutable Chunk data.
  A Commit verifies and atomically publishes Chunks into `/volume/.neoengram/objects`; the state PVC is not a
  payload cache or fallback object store.
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
- The EdgeCluster has a Central/provisioner-verified Ready GatewayPool with at least two production
  replicas and observed readiness/failover evidence, a trusted Pool endpoint, and NetworkPolicy that permits
  Agent-to-Gateway traffic but denies Agent-to-Central Agent-listener traffic. The declaration `Ready` alone
  is not proof that `minimum_ready_replicas` is currently satisfied.

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
- the frozen 64-character descriptor digest supplied for the enrollment in
  `volume_descriptor_digest`;
- the public token ID in `registration.token_id` for stable bootstrap lookup/audit;
- a durable RWO StorageClass for `agent-state-pvc.yaml`;
- the local GatewayPool HTTPS endpoint, `gateway_workload_trust_domain`, its PEM CA bundle, and Central's
  command-signing public key bundle. `/agent/*` at that origin must reach the Gateway Agent listener. The
  endpoint host must match the Gateway leaf DNS/IP SAN, while the leaf's sole URI SAN must identify a
  GatewayReplica in this Agent's configured EdgeCluster and trust domain. The Agent rejects missing,
  malformed, oversized, writable, unknown-generation, wrongly scoped, or revoked trust material and has no
  Central endpoint fallback. Replace `central-command-trust.json` with strict JSON containing the current
  and rotating Ed25519 SPKI keys before deploying;
- a real, digest-pinned Agent image;
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
Tenant queue, or Volume ownership. The Agent generates its Ed25519 private key and stable registration request
ID before the first network request and persists both on the state PVC. After approval, the center binds the
approved Agent identity and the Agent persists that identity on the state PVC. Channel frames and unary
session-scoped actions are signed by that approved key; no bearer credential is derived from the bootstrap
token.

## Apply And Approve

Apply the non-secret resources, create the Secret, then create the Deployment:

```sh
kubectl apply -f agent-state-pvc.yaml
kubectl apply -f configmap.yaml
kubectl apply -f networkpolicy.yaml
kubectl apply -f deployment.yaml
```

The Agent initiates all network connections and connects only to the local GatewayPool. It does not receive a ServiceAccount token, call the
Kubernetes API, expose a Service/Ingress, or depend on an Operator. No Service, Ingress, HPA, Role, or
RoleBinding belongs in this profile.

The included NetworkPolicy limits Agent egress to cluster DNS and the selected local GatewayPool's Agent
listener. It contains no Central destination, so a matching CNI-enforced deployment cannot use the old
Central Agent endpoint. Adjust namespaces and labels without broadening that authority boundary. The control
path carries metadata but no Chunk payloads in the first milestone.

The center creates an idempotent `pending_approval` Storage enrollment, not a Ready Volume. TenantAdmin
reviews only the public Volume/PVC scope, Agent version, public-key identity summary, and sanitized probe
result. Separately, the platform operator verifies the Deployment, actual PVC attachment, marker, and raw
mount evidence through an internal operational channel; raw mount fingerprints never enter the public API.
Approval creates or binds the StorageVolume as Unavailable and creates the AgentInstance. Only an approved
identity with a valid signed session, matching generations, healthy RW observation, and completed recovery
can make the Volume Ready or receive a Job.

Approval is a control-plane trust gate, not a filesystem permission gate: this Pod already has its declared
PVC mount. Bootstrap requires evidence from the real mount, including its marker and RW probe, so 0.0.1 does
not support approval before all data access. That policy requires a future pre-mount enrollment contract and
deployment workflow; removing the PVC from this template does not create a valid two-stage registration.

## Operational Invariants

- Keep `replicas: 1`, `strategy.type: Recreate`, and no HPA. Never use a DaemonSet for this profile.
- Mount the whole business PVC at `/volume`; do not use `subPath` for the Agent.
- Never put Agent identity, SQLite, WAL/SHM, or Ledger files on `/volume`.
- Keep immutable Chunks under
  `/volume/.neoengram/objects/tenants/<tenant>/artifacts/<artifact>/objects/<object_id>`; never redirect that
  CAS to the Agent state PVC or a Server filesystem. The Server persists only signed placement evidence and
  authoritative logical metadata.
- Cross-Volume copy is a later Gateway data-plane milestone. Its fixed path is source Agent -> source Gateway
  -> destination Gateway -> destination Agent; payload must not be proxied through or persisted by Central,
  and neither Gateway may mount a business Volume.
- Reuse the same state PVC for an ordinary Pod restart. A lost or replaced state PVC requires a new
  registration and first approval; it must not inherit an Agent ID from the business volume.
- If a bootstrapped candidate is rejected or its review window expires, retire that installation identity
  and key. Re-enrollment requires freshly initialized Agent state, a new key, token, and request identity.
  A token that expires before bootstrap has no candidate binding and only requires a new token request.
- A ConfigMap or Secret replacement requires a manual Recreate rollout and a new revision annotation.
- Delete the one-time bootstrap Secret after the enrollment has consumed it and the Agent has persisted its
  approved identity. The Secret volume is optional so ordinary restarts use the state PVC instead
  of retaining or reusing bootstrap authority. If state is lost, create a fresh Secret and approval request.
- Startup and liveness check the daemon-owned health record. Readiness must fail closed until an approved,
  generation-current session has completed mount recovery and reported a healthy heartbeat. Loss of the
  configured GatewayPool must not cause destructive restart loops.
- Kubernetes rollout settings and central generations provide cooperative fencing only. They do not stop a
  partitioned or compromised process that still owns RW storage credentials.

Use [TAKEOVER.md](TAKEOVER.md) for every Agent replacement that cannot reuse the original identity safely.
