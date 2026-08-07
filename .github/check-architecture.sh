#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "architecture check failed: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || fail "cargo is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v rg >/dev/null 2>&1 || fail "ripgrep is required"

rg -q '^members = \["crates/\*", "services/\*"\]$' Cargo.toml || \
  fail "workspace members must remain crates/* and services/*"
rg -q '^version = "0\.2\.0"$' Cargo.toml || fail "workspace version must remain 0.2.0"
rg -q '^rust-version = "1\.97\.0"$' Cargo.toml || fail "workspace MSRV must remain 1.97.0"

workspace_metadata="$(cargo metadata --no-deps --format-version 1 --locked)"

expected_packages=$'neoengram\nneoengram-agent\nneoengram-agentd\nneoengram-core\nneoengram-engine\nneoengram-fs\nneoengram-protocol\nneoengram-server\nneoengram-standalone\nneoengramd'
actual_packages="$(
  jq -r '.packages[].name' <<<"$workspace_metadata" | LC_ALL=C sort
)"
if [[ "$actual_packages" != "$expected_packages" ]]; then
  echo "$actual_packages" >&2
  fail "workspace package set differs from the P0 architecture"
fi

invalid_versions="$(
  jq -r \
    '.packages[] | select(.version != "0.2.0" or .rust_version != "1.97.0") |
     "\(.name): version=\(.version), rust-version=\(.rust_version)"' \
    <<<"$workspace_metadata"
)"
if [[ -n "$invalid_versions" ]]; then
  echo "$invalid_versions" >&2
  fail "all workspace packages must inherit version 0.2.0 and MSRV 1.97.0"
fi

invalid_publish="$(
  jq -r \
    '.packages[] |
     if (.name == "neoengram" or .name == "neoengram-core") then
       select(.publish != null) | "\(.name) must remain publishable"
     else
       select(.publish != []) | "\(.name) must remain workspace-private"
     end' \
    <<<"$workspace_metadata"
)"
if [[ -n "$invalid_publish" ]]; then
  echo "$invalid_publish" >&2
  fail "workspace publish policy changed"
fi

assert_internal_dependencies() {
  local package="$1"
  local expected_normal="$2"
  local expected_dev="$3"
  local actual_normal
  local actual_dev

  actual_normal="$(
    jq -r --arg package "$package" \
      '.packages[] | select(.name == $package) | .dependencies[] |
       select(.kind == null) |
       select(.name == "neoengramd" or (.name | startswith("neoengram-"))) |
       .name' \
      <<<"$workspace_metadata" | LC_ALL=C sort
  )"
  actual_dev="$(
    jq -r --arg package "$package" \
      '.packages[] | select(.name == $package) | .dependencies[] |
       select(.kind == "dev") |
       select(.name == "neoengramd" or (.name | startswith("neoengram-"))) |
       .name' \
      <<<"$workspace_metadata" | LC_ALL=C sort
  )"

  if [[ "$actual_normal" != "$expected_normal" || "$actual_dev" != "$expected_dev" ]]; then
    echo "$package normal dependencies:" >&2
    echo "$actual_normal" >&2
    echo "$package dev dependencies:" >&2
    echo "$actual_dev" >&2
    fail "$package violates the internal dependency direction"
  fi
}

assert_internal_dependencies neoengram $'neoengram-core\nneoengram-engine\nneoengram-standalone' ''
assert_internal_dependencies neoengram-core '' ''
assert_internal_dependencies neoengram-engine 'neoengram-core' ''
assert_internal_dependencies neoengram-fs $'neoengram-core\nneoengram-engine' ''
assert_internal_dependencies neoengram-protocol 'neoengram-core' ''
assert_internal_dependencies neoengram-standalone \
  $'neoengram-core\nneoengram-engine\nneoengram-fs' ''
assert_internal_dependencies neoengram-agent \
  $'neoengram-core\nneoengram-engine\nneoengram-protocol' 'neoengramd'
assert_internal_dependencies neoengram-agentd \
  $'neoengram-agent\nneoengram-core\nneoengram-engine\nneoengram-fs\nneoengram-protocol' ''
assert_internal_dependencies neoengramd $'neoengram-core\nneoengram-protocol' ''
assert_internal_dependencies neoengram-server \
  $'neoengram-core\nneoengram-engine\nneoengram-protocol\nneoengramd' \
  'neoengram-agentd'

assert_no_direct_dependency() {
  local package="$1"
  local dependency="$2"

  if jq -e --arg package "$package" --arg dependency "$dependency" \
    '.packages[] | select(.name == $package) |
     any(.dependencies[]; .name == $dependency or .rename == $dependency)' \
    >/dev/null <<<"$workspace_metadata"; then
    fail "$package must not depend on $dependency"
  fi
}

assert_no_fusen_dependency() {
  local package="$1"

  if jq -e --arg package "$package" \
    '.packages[] | select(.name == $package) |
     any(.dependencies[]; .name == "fusen" or (.name | startswith("fusen-")))' \
    >/dev/null <<<"$workspace_metadata"; then
    fail "$package must remain independent of Fusen"
  fi
}

# Check Cargo package identities instead of manifest text so aliases are caught
# without matching comments, descriptions, or similarly named features.
http_transport_dependencies=(
  actix-http
  actix-web
  axum
  h2
  http
  http-body
  http-body-util
  hyper
  hyper-util
  poem
  reqwest
  rocket
  salvo
  surf
  tide
  tiny_http
  tonic
  tower-http
  ureq
  warp
)
for package in neoengram-protocol neoengram-agent neoengramd; do
  assert_no_fusen_dependency "$package"
  for dependency in "${http_transport_dependencies[@]}"; do
    assert_no_direct_dependency "$package" "$dependency"
  done
done

assert_no_fusen_dependency neoengram-agentd
for dependency in neoengram-server neoengramd; do
  assert_no_direct_dependency neoengram-agentd "$dependency"
done

assert_no_binary_target() {
  local package="$1"
  if ! jq -e --arg package "$package" \
    '.packages[] | select(.name == $package) |
     [.targets[].kind[]] | index("bin") == null' \
    >/dev/null <<<"$workspace_metadata"; then
    fail "$package must remain library-only during P0"
  fi
}

assert_no_binary_target neoengram-agent
assert_no_binary_target neoengramd

if ! jq -e \
  '.packages[] | select(.name == "neoengram-server") |
   [.targets[].kind[]] | index("bin") != null' \
  >/dev/null <<<"$workspace_metadata"; then
  fail "neoengram-server must provide the HTTP server binary"
fi

if ! jq -e \
  '.packages[] | select(.name == "neoengram-agentd") |
   [.targets[] | select(.name == "neoengram-agent") | .kind[]] | index("bin") != null' \
  >/dev/null <<<"$workspace_metadata"; then
  fail "neoengram-agentd must provide the neoengram-agent binary"
fi

if ! jq -e \
  '.packages[] | select(.name == "neoengram") |
   [.targets[].kind[]] | index("bin") != null' \
  >/dev/null <<<"$workspace_metadata"; then
  fail "neoengram must keep its CLI binary target"
fi

assert_manifest_excludes() {
  local manifest="$1"
  local pattern="$2"
  if rg -n "$pattern" "$manifest"; then
    fail "$manifest contains a forbidden dependency"
  fi
}

assert_manifest_excludes \
  crates/neoengram-protocol/Cargo.toml \
  'neoengram-(engine|fs|standalone|agent)|rusqlite|sqlx|diesel|aws-sdk'
assert_manifest_excludes \
  crates/neoengram-agent/Cargo.toml \
  'neoengram-standalone|sqlx|diesel'
assert_manifest_excludes \
  services/neoengramd/Cargo.toml \
  'neoengram-(engine|fs|standalone)|rusqlite|diesel|aws-sdk'
assert_manifest_excludes \
  services/neoengram-server/Cargo.toml \
  'neoengram-standalone|neoengram-agent\s*=|rusqlite|sqlx|diesel|aws-sdk'
assert_manifest_excludes \
  services/neoengram-agentd/Cargo.toml \
  'rusqlite|sqlx|diesel'
rg -q '^fusen-rs = "=0\.9\.0"$' services/neoengram-server/Cargo.toml || \
  fail "neoengram-server must pin fusen-rs exactly to 0.9.0"
for manifest in \
  crates/neoengram-core/Cargo.toml \
  crates/neoengram-engine/Cargo.toml \
  crates/neoengram-fs/Cargo.toml \
  crates/neoengram-standalone/Cargo.toml; do
  assert_manifest_excludes "$manifest" 'sqlx|diesel'
done
rg -q '^sqlx = \{ workspace = true, optional = true \}$' services/neoengramd/Cargo.toml || \
  fail "SQLx must remain an optional neoengramd authority adapter dependency"

for layer in controller service; do
  if rg -n '\bsqlx\b' services/neoengram-server/src \
    --glob "**/$layer.rs" --glob "**/$layer/**"; then
    fail "neoengram-server $layer must not access SQLx"
  fi
done
if rg -n '\bneoengramd\b' services/neoengram-server/src \
  --glob '**/controller.rs' --glob '**/controller/**'; then
  fail "neoengram-server controllers must call services instead of neoengramd directly"
fi
controller_routes="$({
  rg -o --no-filename 'method = "(GET|POST)", path = "[^"]+"' \
    services/neoengram-server/src \
    --glob '**/controller.rs' --glob '**/controller/**' || true
} | LC_ALL=C sort)"
expected_controller_routes=$'method = "GET", path = "/health/live"\nmethod = "GET", path = "/health/ready"\nmethod = "POST", path = "/api/artifact/commit/graph/query"\nmethod = "POST", path = "/api/artifact/create"\nmethod = "POST", path = "/api/artifact/list/query"\nmethod = "POST", path = "/api/artifact/query"\nmethod = "POST", path = "/api/job/add/create"\nmethod = "POST", path = "/api/job/add/finalize"\nmethod = "POST", path = "/api/job/query"\nmethod = "POST", path = "/api/playground/change/list/query"\nmethod = "POST", path = "/api/playground/commit/create"\nmethod = "POST", path = "/api/playground/create"\nmethod = "POST", path = "/api/playground/dataset/profile/query"\nmethod = "POST", path = "/api/playground/file/list/query"\nmethod = "POST", path = "/api/playground/file/metadata/query"\nmethod = "POST", path = "/api/playground/list/query"\nmethod = "POST", path = "/api/playground/precommit/cancel"\nmethod = "POST", path = "/api/playground/precommit/query"\nmethod = "POST", path = "/api/playground/precommit/restart"\nmethod = "POST", path = "/api/playground/precommit/start"\nmethod = "POST", path = "/api/playground/query"\nmethod = "POST", path = "/api/snapshot/create"\nmethod = "POST", path = "/api/snapshot/list/query"\nmethod = "POST", path = "/api/snapshot/query"\nmethod = "POST", path = "/api/storage/enrollment/approve"\nmethod = "POST", path = "/api/storage/enrollment/list/query"\nmethod = "POST", path = "/api/storage/enrollment/query"\nmethod = "POST", path = "/api/storage/enrollment/reject"\nmethod = "POST", path = "/api/storage/enrollment/token/create"\nmethod = "POST", path = "/api/storage/volume/create"\nmethod = "POST", path = "/api/storage/volume/list/query"\nmethod = "POST", path = "/api/storage/volume/query"\nmethod = "POST", path = "/api/system/version/query"\nmethod = "POST", path = "/api/tenant/create"\nmethod = "POST", path = "/api/tenant/list/query"\nmethod = "POST", path = "/api/tenant/query"'
if [[ "$controller_routes" != "$expected_controller_routes" ]]; then
  echo "$controller_routes" >&2
  fail "neoengram-server must register exactly the approved public HTTP routes"
fi
agent_openapi=docs/openapi/neoengram-agent-api.yaml
[[ -f "$agent_openapi" ]] || fail "the independent Agent OpenAPI contract is missing"
rg -q '^openapi: 3\.1\.0$' "$agent_openapi" || \
  fail "the Agent contract must use OpenAPI 3.1"
rg -q '^x-neoengram-body-security-schemes:$' "$agent_openapi" || \
  fail "the Agent contract must define its body-carried security schemes"
for security_scheme in \
  BootstrapTokenAndEd25519 \
  InstallationEd25519 \
  ApprovedAgentEd25519 \
  ApprovedAgentEd25519PerFrame; do
  rg -q "^  ${security_scheme}:$" "$agent_openapi" || \
    fail "Agent OpenAPI is missing body-carried security scheme ${security_scheme}"
done

agent_paths=(
  /agent/enrollment/bootstrap
  /agent/enrollment/status/query
  /agent/session/open
  /agent/session/channel/open
  /agent/session/heartbeat/report
  /agent/session/message/list/query
  /agent/job/report/create
  /agent/job/metadata/batch/stage
  /agent/job/metadata/page/stage
  /agent/job/index/page/query
  /agent/job/manifest/page/query
  /agent/session/close
)
agent_path_constants=(
  AGENT_ENROLLMENT_BOOTSTRAP_PATH
  AGENT_ENROLLMENT_STATUS_QUERY_PATH
  AGENT_SESSION_OPEN_PATH
  AGENT_SESSION_CHANNEL_OPEN_PATH
  AGENT_SESSION_HEARTBEAT_REPORT_PATH
  AGENT_SESSION_MESSAGE_LIST_QUERY_PATH
  AGENT_JOB_REPORT_CREATE_PATH
  AGENT_JOB_METADATA_BATCH_STAGE_PATH
  AGENT_JOB_METADATA_PAGE_STAGE_PATH
  AGENT_JOB_INDEX_PAGE_QUERY_PATH
  AGENT_JOB_MANIFEST_PAGE_QUERY_PATH
  AGENT_SESSION_CLOSE_PATH
)
for index in "${!agent_paths[@]}"; do
  path="${agent_paths[$index]}"
  path_constant="${agent_path_constants[$index]}"
  rg -Fq "  ${path}:" "$agent_openapi" || fail "Agent OpenAPI is missing ${path}"
  rg -Fq "pub const ${path_constant}: &str = \"${path}\";" \
    crates/neoengram-protocol/src/agent_api.rs || \
    fail "Agent protocol does not define ${path_constant} as ${path}"
  rg -q "neoengram_protocol::${path_constant}" \
    services/neoengram-server/src/agent_transport/mod.rs || \
    fail "Agent Hyper adapter does not dispatch ${path_constant}"
done
[[ "$(rg -c '^  /[^ ]*:$' "$agent_openapi")" == "${#agent_paths[@]}" ]] || \
  fail "Agent OpenAPI must expose exactly the approved action paths"
[[ "$(rg -c '^    post:$' "$agent_openapi")" == "${#agent_paths[@]}" ]] || \
  fail "every Agent OpenAPI action must use POST"
[[ "$(rg -c '^      summary:' "$agent_openapi")" == "${#agent_paths[@]}" ]] || \
  fail "every Agent OpenAPI action must define a summary"
[[ "$(rg -c '^      description: >-$' "$agent_openapi")" == "${#agent_paths[@]}" ]] || \
  fail "every Agent OpenAPI action must define a description"
[[ "$(rg -c '^      security: \[\]$' "$agent_openapi")" == "${#agent_paths[@]}" ]] || \
  fail "every Agent OpenAPI action must explicitly declare body-carried security"
[[ "$(rg -c '^      x-neoengram-body-security:' "$agent_openapi")" == \
  "${#agent_paths[@]}" ]] || \
  fail "every Agent OpenAPI action must name its body-carried security scheme"
[[ "$(rg -c '^      requestBody:' "$agent_openapi")" == "${#agent_paths[@]}" ]] || \
  fail "every Agent OpenAPI action must carry its request, including identities, in the body"
if rg -n '^  /.*[{}].*:$|^    (get|put|patch|delete):' "$agent_openapi"; then
  fail "Agent OpenAPI must not use path parameters or non-POST methods"
fi
if rg -n '^ +parameters:$|^ +in: (path|query|header|cookie)$' "$agent_openapi"; then
  fail "Agent OpenAPI must not carry identities or action input outside the request body"
fi
[[ "$(rg -c '^      x-neoengram-http2-duplex:$' "$agent_openapi")" == "1" ]] || \
  fail "Agent OpenAPI must define exactly one HTTP/2 full-duplex channel action"
for channel_contract_line in \
  '        transport: http2-full-duplex' \
  '        primaryControlTransport: true' \
  '        mediaType: application/x-ndjson' \
  '        h2DataFrameBoundaries: ignored' \
  '        frameDelimiter: LF' \
  '        maxFrameBytes: 1048576' \
  '        delimiterExcludedFromFrameBytes: true' \
  '        finalFrameRequiresDelimiter: true' \
  '        firstUpstreamFrameType: channel.open' \
  '        firstDownstreamFrameType: channel.opened' \
  '        upstreamAuthentication: body-carried-ed25519-proof-per-frame'; do
  rg -Fq "$channel_contract_line" "$agent_openapi" || \
    fail "Agent channel contract is missing: ${channel_contract_line#        }"
done
[[ "$(rg -c '^ +application/x-ndjson:$' "$agent_openapi")" == "2" ]] || \
  fail "Agent channel request and response must both use application/x-ndjson"
[[ "$(rg -c '^      x-neoengram-compatibility-only: true$' "$agent_openapi")" == "1" ]] || \
  fail "the legacy message-list query must remain compatibility-only"
rg -Fq "pub const MAX_AGENT_CHANNEL_FRAME_BYTES: usize = MAX_CONTROL_MESSAGE_BYTES;" \
  crates/neoengram-protocol/src/agent_api.rs || \
  fail "Agent protocol must cap channel frames at the control-message limit"
rg -Fq "pub const MAX_CONTROL_MESSAGE_BYTES: usize = 1024 * 1024;" \
  crates/neoengram-protocol/src/lib.rs || \
  fail "Agent channel frame limit must remain exactly 1 MiB"
if rg -n '/v1/agents|/agents/\{' \
  crates/neoengram-protocol/src/agent_api.rs \
  services/neoengram-agentd/src \
  services/neoengram-server/src/agent_transport \
  "$agent_openapi"; then
  fail "Agent transport must use unversioned action paths with identities in JSON bodies"
fi
if rg -n '\b(assign_job|expire_add_job|resume_publication)\b' \
  services/neoengram-server/src \
  --glob '**/controller.rs' --glob '**/controller/**'; then
  fail "internal scheduling and recovery methods must not be HTTP controllers"
fi
while IFS= read -r sqlx_source; do
  if rg -n '\b(JobRepository|AssignmentOutbox|MetadataBatchStager|ObjectCatalog|IndexPublisher|AuditSink|AgentRegistry)\b' \
    "$sqlx_source"; then
    fail "SQLx repository implementations must remain in neoengramd mapper modules: $sqlx_source"
  fi
done < <(rg -l '\bsqlx\b' services/neoengramd/src \
  --glob '*.rs' --glob '!**/mapper/**' --glob '!mapper.rs')
if rg -n '\bmapper(::|\b)' services/neoengramd/src/datasource --glob '*.rs'; then
  fail "neoengramd datasource modules must not depend on mappers"
fi
rg -q '^neoengram-engine\.workspace = true$' crates/neoengram-standalone/Cargo.toml || \
  fail "Standalone must compose the execution engine"
rg -q '^neoengram-fs\.workspace = true$' crates/neoengram-standalone/Cargo.toml || \
  fail "Standalone must compose filesystem adapters through neoengram-fs"

for manifest in \
  crates/neoengram-engine/Cargo.toml \
  crates/neoengram-fs/Cargo.toml \
  crates/neoengram-protocol/Cargo.toml \
  crates/neoengram-standalone/Cargo.toml \
  crates/neoengram-agent/Cargo.toml \
  services/neoengram-agentd/Cargo.toml \
  services/neoengram-server/Cargo.toml \
  services/neoengramd/Cargo.toml; do
  rg -q '^publish = false$' "$manifest" || fail "$manifest must remain workspace-private"
done

if [[ -e crates/neoengram-client ]]; then
  fail "neoengram-client is intentionally deferred until a user transport exists"
fi

web=apps/neoengram-web
[[ -f "$web/package.json" ]] || fail "the Vue Web package is missing"
[[ -f "$web/package-lock.json" ]] || fail "the Vue Web lockfile is missing"
[[ -f "$web/src/api/generated/openapi.d.ts" ]] || \
  fail "the generated Web OpenAPI types are missing"
[[ ! -e "$web/Cargo.toml" ]] || fail "the Vue Web package must remain outside Cargo"
[[ "$(<"$web/.node-version")" == "22.12.0" ]] || \
  fail "the Vue Web package must pin Node.js 22.12.0"
jq -e \
  '.private == true and .engines.node == ">=22.12.0" and
   .scripts["api:generate"] != null and .scripts["api:check"] != null' \
  "$web/package.json" >/dev/null || fail "the Vue Web package metadata changed"
web_internal_dependencies="$(
  jq -r \
    '[(.dependencies // {}), (.devDependencies // {})] | add | keys[] |
     select(test("^neoengram(-|$)"))' \
    "$web/package.json"
)"
[[ -z "$web_internal_dependencies" ]] || \
  fail "the Vue Web package must not depend on Rust workspace packages"

openapi=docs/openapi/neoengram-api.yaml
[[ -f "$openapi" ]] || fail "the public OpenAPI contract is missing"
rg -q '^openapi: 3\.1\.0$' "$openapi" || fail "the public contract must use OpenAPI 3.1"
if rg -n '^  /v[0-9]+/|^  /api/v[0-9]+/|^  /api/[^:]+:[^:]+:$' "$openapi"; then
  fail "public API paths must use unversioned module/action hierarchy"
fi
business_paths=(
  /api/system/version/query
  /api/tenant/list/query
  /api/tenant/query
  /api/tenant/create
  /api/storage/volume/list/query
  /api/storage/volume/query
  /api/storage/volume/create
  /api/storage/enrollment/token/create
  /api/storage/enrollment/list/query
  /api/storage/enrollment/query
  /api/storage/enrollment/approve
  /api/storage/enrollment/reject
  /api/project/list/query
  /api/artifact/list/query
  /api/artifact/query
  /api/artifact/create
  /api/artifact/commit/graph/query
  /api/artifact/commit/diff/query
  /api/playground/list/query
  /api/playground/query
  /api/playground/create
  /api/playground/precommit/start
  /api/playground/precommit/query
  /api/playground/precommit/restart
  /api/playground/precommit/cancel
  /api/playground/file/list/query
  /api/playground/change/list/query
  /api/playground/file/metadata/query
  /api/playground/dataset/profile/query
  /api/playground/commit/create
  /api/snapshot/list/query
  /api/snapshot/query
  /api/snapshot/create
  /api/snapshot/delivery/retry
  /api/snapshot/file/list/query
  /api/snapshot/activity/list/query
  /api/snapshot/dataset/profile/query
  /api/job/add/create
  /api/job/query
  /api/job/add/finalize
)
for path in "${business_paths[@]}" /health/live /health/ready; do
  rg -q "^  ${path}:$" "$openapi" || fail "public OpenAPI is missing $path"
done
for internal_operation in assignJob expireAddJob resumePublication; do
  if rg -n "operationId: ${internal_operation}$" "$openapi"; then
    fail "$internal_operation must remain outside the public OpenAPI"
  fi
done
rg -q '^    ApiVersion:$' "$openapi" || fail "the API version header is not defined"
rg -q '^    BearerAuth:$' "$openapi" || fail "the public Bearer security scheme is not defined"
rg -q '^    ProblemDetails:$' "$openapi" || fail "RFC 9457 Problem Details is not defined"
authenticated_business_method_count=$((${#business_paths[@]} - 1))
[[ "$(rg -c '#/components/parameters/ApiVersion' "$openapi")" == \
  "$authenticated_business_method_count" ]] || \
  fail "every public business method must require the API version header"
[[ "$(rg -c 'BearerAuth: \[\]' "$openapi")" == "$authenticated_business_method_count" ]] || \
  fail "every public business method must require Bearer authentication"

terminal_output="$({
  rg -n '\b(e?print|e?println|dbg)!\s*\(' crates services \
    --glob '*.rs' \
    --glob '!neoengram/src/main.rs' \
    --glob '!neoengram/src/cli/**' \
    --glob '!crates/neoengram/src/main.rs' \
    --glob '!crates/neoengram/src/cli/**' || true
})"
if [[ -n "$terminal_output" ]]; then
  echo "$terminal_output" >&2
  fail "terminal output is only allowed in the neoengram CLI adapter"
fi

engine_environment="$({
  rg -n 'std::env|current_dir\(|rusqlite|clap::|println!|eprintln!' \
    crates/neoengram-engine/src || true
})"
if [[ -n "$engine_environment" ]]; then
  echo "$engine_environment" >&2
  fail "Engine use cases must remain independent of process and persistence adapters"
fi

cli_sources="$(find crates/neoengram/src -type f -name '*.rs' -print | sort)"
expected_cli_sources=$'crates/neoengram/src/cli/mod.rs\ncrates/neoengram/src/main.rs'
if [[ "$cli_sources" != "$expected_cli_sources" ]]; then
  echo "$cli_sources" >&2
  fail "neoengram must contain only the CLI adapter"
fi

bash deploy/kubernetes/agent/check-manifests.sh

echo "architecture checks passed"
