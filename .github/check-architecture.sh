#!/usr/bin/env bash
set -euo pipefail

fail() { echo "architecture check failed: $*" >&2; exit 1; }
command -v cargo >/dev/null || fail "cargo is required"
command -v jq >/dev/null || fail "jq is required"
command -v rg >/dev/null || fail "ripgrep is required"

rg -q '^members = \["crates/\*", "services/\*", "apps/neoengram-cli"\]$' Cargo.toml || fail "workspace layout changed"
metadata="$(cargo metadata --no-deps --format-version 1 --locked)"
expected=$'neoengram\nneoengram-agent\nneoengram-central\nneoengram-domain\nneoengram-gateway\nneoengram-runtime'
actual="$(jq -r '.packages[].name' <<<"$metadata" | sort)"
[[ "$actual" == "$expected" ]] || { printf '%s\n' "$actual" >&2; fail "legacy package remains in workspace"; }

if rg -n 'neoengram-(agentd|core|protocol|engine|fs|server)|services/neoengramd|neoengramd::|use neoengramd|NEOENGRAM_SERVER_' \
  crates services apps/neoengram-cli --glob 'Cargo.toml' --glob '*.rs' --glob '!**/target/**'; then
  fail "legacy package or split authority reference remains in production source"
fi

deps() {
  jq -r --arg p "$1" '.packages[] | select(.name == $p) | .dependencies[] |
    select(.kind == null and (.name | startswith("neoengram"))) | .name' <<<"$metadata" | sort
}
[[ "$(deps neoengram-domain)" == "" ]] || fail "domain must be dependency leaf"
[[ "$(deps neoengram-runtime)" == "neoengram-domain" ]] || fail "runtime must depend only on domain"
[[ "$(deps neoengram-agent)" == $'neoengram-domain\nneoengram-runtime' ]] || fail "agent boundary changed"
[[ "$(deps neoengram-central)" == $'neoengram-domain\nneoengram-runtime' ]] || fail "central must be canonical"
[[ "$(deps neoengram-gateway)" == "neoengram-domain" ]] || fail "gateway must depend only on domain"

jq -e '.packages[] | select(.name == "neoengram-central") |
  any(.targets[]; .name == "neoengram-central" and (.kind | index("bin")))' <<<"$metadata" >/dev/null ||
  fail "Central must provide neoengram-central binary"
jq -e '.packages[] | select(.name == "neoengram-agent") |
  any(.targets[]; .name == "neoengram-agent" and (.kind | index("bin")))' <<<"$metadata" >/dev/null ||
  fail "Agent must provide neoengram-agent binary"

if rg -n 'message-list|message/list/query|AgentMessageListQuery|PROTOCOL_VERSION_V1|schemas/v1' \
  crates services apps docs deploy README.md --glob '!**/target/**'; then
  fail "legacy protocol remains"
fi
if rg -n '/v[0-9]+/|/agents/\{|/api/v[0-9]+/' docs/openapi --glob '*.yaml'; then
  fail "versioned or resource-id routes remain"
fi

action_registry="$(
  cargo run --quiet --locked --offline -p neoengram-domain \
    --example export_action_registry
)"
jq -e '.schema_version == 1 and
  (.central_routes | length > 0) and
  (.public_openapi | length > 0) and
  (.agent_actions | length > 0) and
  (.gateway_fixed_actions | length > 0)' <<<"$action_registry" >/dev/null ||
  fail "action registry export is invalid"
mkdir -p target/architecture
printf '%s\n' "$action_registry" >target/architecture/action-registry.json

controller_routes="$({
  rg -o --no-filename 'method = "(GET|POST)", path = "[^"]+"' \
    services/neoengram-central/src/controller --glob '*.rs' || true
} | sed -E 's/method = "([^"]+)", path = "([^"]+)"/\1 \2/' | LC_ALL=C sort)"
registry_routes="$(
  jq -r '.central_routes[] | "\(.method) \(.path)"' <<<"$action_registry" |
    LC_ALL=C sort
)"
if [[ "$controller_routes" != "$registry_routes" ]]; then
  printf 'Central controller routes:\n%s\nRegistry routes:\n%s\n' \
    "$controller_routes" "$registry_routes" >&2
  fail "Central controller routes differ from the action registry"
fi

test -f apps/neoengram-cli/Cargo.toml || fail "CLI must live under apps/neoengram-cli"
if rg -n '\b(sqlx|rusqlite|fusen|neoengram-server|neoengramd)\b' services/neoengram-gateway/src services/neoengram-gateway/Cargo.toml; then
  fail "Gateway must not own Central storage or HTTP business adapters"
fi
if rg -n 'agent_bind|AgentMessageListQuery|message-list' deploy docs crates services apps; then
  fail "removed listener or polling compatibility remains"
fi

bash deploy/kubernetes/agent/check-manifests.sh
bash deploy/kubernetes/gateway/check-manifests.sh
echo "architecture checks passed"
