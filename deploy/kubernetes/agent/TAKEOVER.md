# Manual Agent Takeover

0.0.1 has no Kubernetes Operator, Agent Kubernetes API access, automatic failover, or storage-side strong
fencing. This runbook is the only supported way to replace an AgentInstance for an existing StorageVolume.
It provides cooperative fencing and must fail closed whenever the old writer cannot be proven stopped.

## Planned Or Incident Takeover

1. Mark the StorageVolume unavailable/draining in the central administrative plane and freeze all new read-
   write Assignments for the entire Volume, not only one Playground. Stop or make read-only every external
   business/Playground Pod that can write this PVC; central Assignment freeze does not fence direct POSIX I/O.
2. Record the current AgentInstance, active session, outstanding Jobs and leases, mount fingerprint,
   `credential_generation`, `config_generation`, `session_generation`, `mount_generation`, and
   `owner_generation`.
3. Stop the old Deployment. Confirm through the node and storage service that the old process and NFS/CSI
   client no longer perform I/O. Waiting for a Pod object to disappear is insufficient evidence.
4. Revoke the old Agent certificate/session, stop lease renewal, and wait for outstanding lease deadlines.
   Preserve its state PVC for recovery evidence; never mount that state PVC into the replacement Agent.
5. If the old writer cannot be proven stopped, stop here. Keep the StorageVolume unavailable and escalate to
   storage administration. Recreate and generation changes are not storage-side fencing.
6. Create a fresh state PVC and update the single-replica Recreate Deployment to mount it together with the
   same business PVC at `/volume`. Do not run old and replacement Deployments concurrently.
7. Give the replacement a new one-time bootstrap token. It generates a new key and registration request,
   dials the center, and remains `pending_approval`.
8. The platform operator compares the new public key, declared Tenant/EdgeCluster/StorageVolume, actual PVC
   attachment, marker, and raw mount evidence with the reviewed workload and storage records. TenantAdmin
   approves from the public identity summary and sanitized probe only after that out-of-band check succeeds.
9. Atomically revoke any remaining old ownership and advance the relevant credential, config, session,
   mount, and owner generations. Bind `active_rw_agent_id` to the new approved identity. A stale generation
   must not renew leases, receive Assignments, or publish results.
10. Let the new Agent rebuild cache from central metadata, inspect every Playground journal and worktree on
    the Volume, and run explicit RecoverJobs. Do not replay a normal Job whose filesystem side effects are
    unknown.
11. Reconcile central IndexVersions, journals, running Jobs, and snapshot/playground health. Return the
    StorageVolume to Ready only after full-volume recovery succeeds.

## Rollback

Do not remount the old state PVC or restore the old AgentInstance after generations have advanced. A rollback
is another takeover: freeze the Volume, prove the current writer stopped, register a fresh identity, advance
generations, and recover again.

## Evidence To Retain

- approval/revocation actor, time, request ID, and Agent public-key fingerprint;
- old and new AgentInstance IDs and all generation values;
- Deployment revision, image digest, data/state PVC identities, and mount fingerprints;
- old-process stop and storage-client quiescence evidence;
- affected Jobs/leases, RecoverJob results, journal reconciliation, and final Volume state.
