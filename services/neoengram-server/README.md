# NeoEngram Server

`neoengram-server` is the network composition root. The public listener uses Fusen 0.9.0. When Agent enrollment is enabled, the same process also starts a separate Hyper listener for the Agent control and metadata API. Both listeners share the SQLite authority and control catalog. Object payload bytes remain on the approved StorageVolume and never enter either Server listener.

The Agent listener is contract-first: [`neoengram-agent-api.yaml`](../../docs/openapi/neoengram-agent-api.yaml) is an independent OpenAPI 3.1 contract. Every operation is an action-style POST, and all Agent, session, Job, batch, page, and object identities are carried in the JSON body:

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

The development control transport is an Agent-initiated HTTP/2 full-duplex NDJSON stream opened through the action-style `POST /agent/session/channel/open` operation. HTTP/2 DATA boundaries have no protocol meaning; LF terminates each JSON frame, and every upstream frame carries its own approved-key Ed25519 proof. No session bearer token is issued. Session identity and generation fence stale boots, assignments are redelivered until Accepted, and decisions are redelivered until the Finalized acknowledgement. The legacy `POST /agent/session/message/list/query` action is compatibility and manual-recovery only; it is not part of the daemon's primary runtime path. Unary actions carry enrollment, authoritative Index pages, and metadata only. Production mTLS, direct inter-Volume transfer, PostgreSQL, and HA are outside this development profile.

## Artifact authority

Artifact is the logical data authority. A Playground is a writable derivation of one Artifact and a Snapshot is a read-only derivation of one immutable Artifact Commit; neither resource may create or redefine its parent Artifact. Playground creation validates the Artifact head and the selected Ready Volume in the same catalog transaction, and derives the Region instead of accepting it from the caller. Managed Add publishes only the authoritative Playground Index in `authority.sqlite3`; the control catalog does not keep a second IndexVersion. A later Commit transaction must freeze that Index and CAS the Artifact head before Snapshot delivery can be enabled.

Control-catalog schema v5 therefore refuses to infer an Artifact from any v4 Playground row. Before upgrading such a development database, export the legacy Playground records, remove them from the v4 catalog, start v5, create each Artifact explicitly through `/api/artifact/create`, and recreate its Playgrounds. The failed migration is transactional and leaves the v4 database unchanged.

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

Start both loopback listeners:

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

Public business requests require `NeoEngram-API-Version: 1` and a Bearer token. In production, use OIDC/JWKS plus a deny-by-default RBAC file; development authentication is loopback-only. TLS terminates at the ingress or reverse proxy. Route `/agent/*` to the Agent listener, not the public Fusen listener.

SQLite is a single-process authority. Do not run more than one server replica against the same authority directory.
