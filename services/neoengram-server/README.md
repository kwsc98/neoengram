# NeoEngram Server

`neoengram-server` is the Central network composition root. Its public listener uses Fusen 0.9.0
for user and management APIs. When Agent enrollment is enabled, Central also builds the Agent
domain handler and a Registry-driven outbound connector for every Active Gateway Replica. It does
not accept a static Replica endpoint list: connection targets come from the persisted Gateway
Registry.

The target control topology is `Central -> GatewayPool <- Agent`. Production Agent traffic never
enters the public Fusen listener and Central does not expose an Agent listener. Object payload bytes
remain on the approved StorageVolume and never enter Central or Gateway.

The tunneled Agent API remains contract-first:
[`neoengram-agent-api.yaml`](../../docs/openapi/neoengram-agent-api.yaml) defines the internal
action boundary used by Gateway forwarding. Every operation is an action-style POST, and all Agent,
session, Job, batch, page, and object identities are carried in the JSON body:

- `POST /agent/enrollment/bootstrap`
- `POST /agent/enrollment/status/query`
- `POST /agent/session/open`
- `POST /agent/session/channel/open`
- `POST /agent/session/heartbeat/report`
- `POST /agent/session/message/list/query`
- `POST /agent/job/report/create`
- `POST /agent/job/metadata/batch/stage`
- `POST /agent/job/metadata/page/stage`
- `POST /agent/job/index/page/query`
- `POST /agent/session/close`

Agent opens an HTTP/2 full-duplex NDJSON edge stream to its local Gateway. Central independently
opens an H2 control stream to each registered Replica; Gateway forwards the internal Agent actions
without becoming metadata authority. HTTP/2 DATA boundaries have no protocol meaning, LF terminates
each JSON frame, and every upstream frame carries its own approved-key Ed25519 proof. Central signs
downstream Assignment and Decision frames, and Gateway does not rewrite either signature payload.
Session and route generations fence stale boots and connections. The legacy message-list action is
compatibility and manual-recovery only.

Production control connections require mTLS, exact workload URI SANs and the configured trust
domain. The external KMS/HSM-backed certificate issuer, command signer, certificate lifecycle
controller and exact Secret promotion remain deployment integrations; their absence must fail
closed. Direct inter-cluster payload transfer, PostgreSQL Central HA and S3 are later milestones.

## Artifact authority

Artifact is the logical data authority. A Playground is a writable derivation of one Artifact and a Snapshot is a read-only derivation of one immutable Artifact Commit; neither resource may create or redefine its parent Artifact. Playground creation validates the Artifact head and the selected Ready Volume in the same catalog transaction, and derives the Region instead of accepting it from the caller. Managed Add publishes only the authoritative Playground Index in `authority.sqlite3`; the control catalog does not keep a second IndexVersion. A later Commit transaction must freeze that Index and CAS the Artifact head before Snapshot delivery can be enabled.

Control-catalog schema v5 therefore refuses to infer an Artifact from any v4 Playground row. Before upgrading such a development database, export the legacy Playground records, remove them from the v4 catalog, start v5, create each Artifact explicitly through `/api/artifact/create`, and recreate its Playgrounds. The failed migration is transactional and leaves the v4 database unchanged.

## Migration-only development listener

`--agent-bind` exists only for loopback integration tests and migration diagnostics. Configuration
validation rejects it outside `--development` and rejects non-loopback addresses. Do not publish or
route `/agent/*` to this listener. New Agent configurations contain only a Gateway endpoint and trust
bundle; there is no Central fallback.

## Development startup

Create a private 32-byte keyring. The server rejects symlinks, files owned by another Unix user, and files with any group or other permissions.

```sh
umask 077
key="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=')"
jq -n --arg key "$key" \
  '{version: 1, active_key_id: "development-key", keys: {"development-key": $key}}' \
  > /tmp/neoengram-enrollment-keyring.json
chmod 600 /tmp/neoengram-enrollment-keyring.json
```

Start the public listener and the explicit development-only fixture:

```sh
cargo run -p neoengram-server -- \
  --authority-dir /tmp/neoengram-authority \
  --bind 127.0.0.1:8080 \
  --development \
  --development-token local-development-token \
  --development-tenants tenant-local \
  --agent-enrollment-enabled \
  --agent-bind 127.0.0.1:8081 \
  --agent-enrollment-keyring-file /tmp/neoengram-enrollment-keyring.json
```

Verify the public listener:

```sh
curl -i http://127.0.0.1:8080/health/live
curl -i http://127.0.0.1:8080/health/ready
```

Public business requests require `NeoEngram-API-Version: 1` and a Bearer token. In production, use
OIDC/JWKS plus a deny-by-default RBAC file; development authentication is loopback-only. User API TLS
terminates at the ingress or reverse proxy.

For Gateway control, production startup additionally requires the CA bundle, Central workload
certificate/private key and trust domain through the `NEOENGRAM_SERVER_GATEWAY_TLS_*` and
`NEOENGRAM_SERVER_GATEWAY_WORKLOAD_TRUST_DOMAIN` settings. Central polls the Registry and actively
connects to each eligible Replica. Gateway activation also requires the externally supplied
`WorkloadCertificateIssuer` and bootstrap transport described in
[`synapse-gateway-architecture.md`](../../docs/synapse-gateway-architecture.md); the CLI composition
does not pretend to provide the production KMS/HSM adapter.

SQLite is a single-process authority. Do not run more than one server replica against the same authority directory.
