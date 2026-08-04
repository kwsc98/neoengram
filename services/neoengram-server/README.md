# NeoEngram Server

`neoengram-server` is the network composition root. The public listener uses Fusen 0.9.0. When enrollment is enabled, the same process also starts a separate Hyper listener for Agent bootstrap and status traffic. Both listeners share one `SqliteAuthority` and one `AgentRegistryService`.

The current Agent listener exposes only:

- `POST /v1/agents/bootstrap`
- `POST /v1/agents/bootstrap/status`

It does not issue certificates or provide an Agent session, heartbeat, readiness, assignment, or Job transport.

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

Public business requests require `NeoEngram-API-Version: 1` and a Bearer token. In production, use OIDC/JWKS plus a deny-by-default RBAC file; development authentication is loopback-only. TLS terminates at the ingress or reverse proxy. Route `/v1/agents/*` to the Agent listener, not the public Fusen listener.

SQLite is a single-process authority. Do not run more than one server replica against the same authority directory.
