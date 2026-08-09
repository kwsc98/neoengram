# Synapse Gateway Kubernetes example

> **This directory is an example template, not a production activation bundle.**
> The PEM and activation-token values are placeholders. Do not apply these files to a
> production cluster until an external provisioner has created and delivered credentials,
> registered the exact endpoints in Central, rendered the NetworkPolicy CIDRs, and completed
> the activation/restart and certificate-rotation runbook.

The production PKI prerequisite is the offline Root CA plus a KMS/HSM-backed online Intermediate
exposed through `WorkloadCertificateIssuer`; neither is supplied by this directory. G1 cutover also
requires the complete two-replica business E2E and real-cluster failover evidence. Cross-cluster payload
transfer and read-only S3 are later G2/G3 milestones; Gateway S3 is an access protocol, not a Central
durability backend. See [`docs/synapse-gateway-architecture.md`](../../../docs/synapse-gateway-architecture.md).

This directory models one EdgeCluster GatewayPool with two independently managed replicas. It
uses two explicit `Deployment` documents (`gateway-pool-example-r0` and `gateway-pool-example-r1`)
instead of a Deployment replica count or a StatefulSet ordinal. Kubernetes cannot substitute a
pod ordinal into a Secret reference, so a single template would either reuse credentials or expose
all replica credentials to every Pod. Each document therefore fixes all of the following values:

| Replica | `GatewayReplicaId` | listener Secret | activation Secret | Central bootstrap Service |
| --- | --- | --- | --- | --- |
| r0 | `gateway-pool-example-r0` | `synapse-gateway-identity-gateway-pool-example-r0` | `synapse-gateway-activation-gateway-pool-example-r0` | `https://synapse-gateway-gateway-pool-example-r0.synapse-gateway-example.svc.cluster.local:8443` |
| r1 | `gateway-pool-example-r1` | `synapse-gateway-identity-gateway-pool-example-r1` | `synapse-gateway-activation-gateway-pool-example-r1` | `https://synapse-gateway-gateway-pool-example-r1.synapse-gateway-example.svc.cluster.local:8443` |

Central must persist these per-replica Service origins as the Registry `bootstrap_endpoint`,
`control_endpoint` (port `9443`) and `peer_endpoint` (port `10443`). The load-balanced
`synapse-gateway` Service on port `8443` is the Agent Pool entry point; it is not an authority for
Replica identity and must not be used as a Central Replica endpoint.

Register each Replica with `software_version` equal to the deployed `synapse-gateway` package
version, `supported_protocol_versions: [1]`, and the exact capability set
`["agent-control-v1", "peer-forward-v1", "route-lease-v1"]`. Central compares the persisted
values with every Replica hello and fails the connection closed on any mismatch.

The activated workload identity contract is independent of CA trust. A Gateway control listener accepts
Central only when its client certificate contains exactly the URI SAN
`spiffe://<trust-domain>/workloads/central`; an Agent or GatewayReplica URI from the same CA is not a Central
identity. Each activated Gateway certificate likewise contains exactly one Replica URI SAN:
`spiffe://<trust-domain>/workloads/edge-clusters/<edge_cluster_id>/gateway-pools/<gateway_pool_id>/gateway-replicas/<gateway_replica_id>`.

## Credential provisioning contract

The repository deliberately does not generate or store credentials. A provisioner/operator outside
this example must:

1. Create the `GatewayReplica` in Central and capture the one-time activation token. Central stores
   only its digest; the plaintext token belongs in that Replica's activation Secret and must never
   be copied to the other Replica.
2. Generate one Ed25519 PKCS#8 private key per Replica and an initial server-auth bootstrap certificate whose
   DNS/IP SAN exactly covers that Replica's registered bootstrap Service host. This certificate authenticates
   only the activation endpoint; it is not proof of an activated workload identity. Put the key, certificate
   and workload CA bundle in the matching identity Secret. The key file must be mode `0440` or stricter after
   projection.
3. Render the two Secret manifests with real values, keep them in an external secret manager, and
   apply them before the matching Deployment. The checked-in `REPLACE_WITH_*` values intentionally
   fail closed in the Gateway process.
4. Start the Pod and let Central perform challenge/proof, certificate issuance and delivery. The issuer must
   return a GatewayReplica leaf with `clientAuth` and `serverAuth` EKUs, the sole Replica URI SAN shown above,
   and the complete DNS/IP SAN set extracted from the Pool Agent endpoint, optional S3 endpoint, and this
   Replica's bootstrap, control, and peer endpoints. The response is rejected if this set differs from the
   issuance request, including extra unregistered names. The Gateway writes the delivered workload chain only
   to a memory-backed short-lived buffer and
   reports `gateway_restart_required`; the provisioner must install the approved chain/key as the
   next version of the listener Secret and roll that one Deployment. A Gateway-side provisioner
   must promote the exact locally delivered chain; the Central management API intentionally does
   not expose a second plaintext certificate handoff. Reconstructing or reissuing the certificate
   after ACK would break the Registry fingerprint binding. This example does not include that
   controller or Secret-manager integration, so it is not a production activation solution.
5. After the restart, verify the Registry certificate fingerprint, sole URI SAN, DNS/IP SAN set, EKUs and
   generation. Keep `SYNAPSE_GATEWAY_WORKLOAD_TRUST_DOMAIN` as a required long-lived runtime setting, but
   remove the three `SYNAPSE_GATEWAY_BOOTSTRAP_*` path settings, the bootstrap volume/mount, and the
   one-time activation Secret from that Replica's rendered Deployment. The listener TLS key remains in the
   promoted workload identity Secret; removing its separate bootstrap-path reference does not remove the
   active identity. Revoke the token according to the Central runbook. Never silently reuse a Secret for a
   replacement Replica; create a new stable `GatewayReplicaId` and credential set.

The explicit manifests make identity ownership auditable and avoid relying on Pod names, which are
not durable identities for a Deployment. To add replicas, copy both Deployment/Service/Secret
documents, choose a new immutable `GatewayReplicaId`, and register all three endpoint origins in
Central before changing the Pool state. Do not simply increase `replicas` on either Deployment.

## Applying a rendered example

1. Render a namespace and labels for the Central control plane, Agent namespace, and any peer
   Gateway namespaces. Add cluster-specific `ipBlock` rules to `networkpolicy.yaml` when Central or
   a peer Gateway is outside this Kubernetes cluster.
2. Replace the image digest, Pool/Cluster IDs, certificate SANs, trust domain, and all
   `REPLACE_WITH_*` values in the Secret files using the provisioner. Keep private keys and tokens
   out of Git and out of ConfigMaps.
3. Run `bash check-manifests.sh`. It validates both fixed Replica documents, unique Secret refs,
   service endpoint selectors, TLS/bootstrap paths, security settings, and the no-business-Volume
   invariant, and renders `kustomization.yaml` when `kubectl` is available. It intentionally
   reports that external provisioning is still required.
4. Apply only the rendered overlay (`kubectl apply -k <rendered-overlay>`), then perform Central
   activation and the restart described above. Do not apply the checked-in placeholder base. There
   is no old Central endpoint fallback in this profile.

Required Pod anti-affinity keeps the two Replica Pods in this Pool off the same
`kubernetes.io/hostname`; the PodDisruptionBudget protects node-drain style evictions, but neither
mechanism serializes updates to two separate Deployments. The provisioner must activate and roll one
Replica at a time and wait for it to become Ready before touching the other Replica. Each container has a real `preStop` drain
hook: `/proc/1/exe --pre-stop-drain` sends `SIGUSR1` to the Gateway PID 1 and waits 20 seconds.
During that interval liveness remains successful, readiness fails closed, and every new protocol
request, including a new stream on an existing H2 connection, receives `503`. The Gateway stops
heartbeat and RouteAcquire/RouteRenew traffic, spends at most two seconds attempting concurrent
RouteRelease for its active routes, and then sends `Drain` to Central. Central persists the Replica
as `Draining`, fences the control session, and closes active streams; if Central is unavailable, the
local renewal fence remains in force and unreleased leases expire at their normal short TTL.
Kubernetes then sends `SIGTERM`; the 60-second termination grace period leaves 40 seconds for final
connection shutdown after the endpoint-removal interval. The image
entrypoint must be the `synapse-gateway` binary so `/proc/1/exe` cannot resolve to a shell or supervisor.

The Gateway mounts only per-replica Secrets and a small `emptyDir.medium: Memory` certificate
delivery buffer. It never mounts a PVC, host path, NFS export, CAS directory, Volume, or object
payload. The memory buffer survives a container restart in the same Pod but not Pod replacement.
Losing the Pod before the provisioner persists the exact delivered chain can leave activation
unable to resume safely and requires an explicit revoke/re-provision recovery path. This is a
deliberate production blocker, not a durability guarantee supplied by these example manifests.

Health probes use HTTPS because non-loopback listeners fail closed without a TLS identity. The
listener Secret is mounted read-only at `/var/run/secrets/synapse-gateway/listener`; the activation
Secret is mounted read-only at `/var/run/secrets/synapse-gateway/bootstrap`; and the temporary
certificate delivery path is `/var/run/secrets/synapse-gateway/workload` during activation. These last two
mounts and all three bootstrap path settings are absent from the post-activation overlay. The workload trust
domain remains configured independently; a TLS or non-loopback Gateway refuses to start without it.
