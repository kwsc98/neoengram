#!/usr/bin/env bash
set -Eeuo pipefail

# Coverage-gap module: checks that the base modules deliberately do not cover.
#
#   gaps-all-features-tests      workspace tests with --all-features (base
#                                modules only run default-feature tests)
#   gaps-doc-tests               Rust doc tests (--all-targets excludes them)
#   gaps-schema-determinism      committed JSON schemas regenerate byte-identically
#   gaps-package-domain          crates.io package verification for neoengram-domain
#   gaps-package-cli             CLI archive assembly verification
#   gaps-web-local-gateway-build the third web build mode not exercised elsewhere
#   gaps-runner-self-test        the project test runner validates itself
#   gaps-bundle-collision-audit  bundled OpenAPI must not contain renamed components
#
# This module is intentionally opt-in (`scripts/project-test.sh module gaps`) and
# is not part of the default `modules`/`all` run yet: gaps-bundle-collision-audit
# currently fails on nine known schema name collisions between the agent OpenAPI
# component wrappers and the external schema $defs (redocly renames them to `X-2`
# in the bundle). Promote it into `run_modules` in scripts/project-test.sh once
# those collisions are resolved.
#
# Focused suites are available through PROJECT_TEST_GAPS_SUITE (all,
# all-features, doc, schema, package, web-build, self-test, bundle-audit).

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd bash
project_test_require_cmd cargo
project_test_require_cmd jq
project_test_require_cmd node
project_test_require_cmd npm

web_npm() {
  (cd "${PROJECT_TEST_ROOT}/apps/neoengram-web" && npm "$@")
}

# Deterministic aggregate digest over every committed schema file, independent of
# filesystem ordering and hash-tool flavor.
schema_tree_digest() {
  local directory="$1"
  local manifest="${PROJECT_TEST_TEMP_ROOT}/schema-manifest.txt"
  local file
  : >"${manifest}"
  while IFS= read -r -d '' file; do
    printf '%s  %s\n' "$(project_test_sha256_file "${file}")" "${file#"${directory}"/}" \
      >>"${manifest}"
  done < <(find "${directory}" -type f -name '*.json' -print0 | LC_ALL=C sort -z)
  project_test_sha256_file "${manifest}"
}

run_schema_determinism() {
  local schema_dir="${PROJECT_TEST_ROOT}/crates/neoengram-domain/schemas"
  local before after
  before="$(schema_tree_digest "${schema_dir}")"
  cargo run --quiet --locked --offline \
    -p neoengram-domain --example generate_schemas || return $?
  after="$(schema_tree_digest "${schema_dir}")"
  if [[ "${before}" != "${after}" ]]; then
    printf 'schema tree is not deterministically regenerated\n' >&2
    printf 'before=%s\nafter=%s\n' "${before}" "${after}" >&2
    return 1
  fi
  printf 'schema tree regenerates byte-identically (digest %s)\n' "${after}"
}

run_bundle_collision_audit() {
  local bundle bundle_path collisions
  for bundle in neoengram-agent-api neoengram-api; do
    bundle_path="${PROJECT_TEST_ROOT}/target/openapi/${bundle}.json"
    if [[ ! -f "${bundle_path}" ]]; then
      printf 'bundle %s is missing; run the openapi module first\n' "${bundle_path}" >&2
      return 1
    fi
    collisions="$(
      jq -r '.components.schemas | keys[]' "${bundle_path}" \
        | grep -c -- '-2$' || true
    )"
    printf '%s: %s renamed components\n' "${bundle}" "${collisions}"
    if [[ "${collisions}" != "0" ]]; then
      jq -r '.components.schemas | keys[]' "${bundle_path}" | grep -- '-2$' >&2 || true
      return 1
    fi
  done
}

run_runner_self_test() {
  PROJECT_TEST_CHILD=1 \
    PROJECT_TEST_LOG_ROOT="${PROJECT_TEST_LOG_ROOT}/self-test" \
    bash "${PROJECT_TEST_ROOT}/tests/project/self-test.sh"
}

# Network timeout guard for steps that touch the crates.io index. The package
# verify builds in a fresh temporary CARGO_HOME and needs to re-download the
# dependency graph; without these bounds a stalled git-index fetch can hang
# indefinitely (cargo applies the same limits to libgit2 index fetches).
cargo_package() {
  CARGO_HTTP_TIMEOUT=120 \
    CARGO_HTTP_LOW_SPEED_LIMIT=1024 \
    CARGO_NET_RETRY=2 \
    cargo package "$@"
}

run_gaps_suite() {
  local suite="${PROJECT_TEST_GAPS_SUITE:-all}"
  case "${suite}" in
    all)
      project_test_run_step gaps-all-features-tests \
        cargo test --workspace --all-targets --all-features --locked || return $?
      project_test_run_step gaps-doc-tests \
        cargo test --workspace --doc --locked || return $?
      project_test_run_step gaps-schema-determinism run_schema_determinism || return $?
      project_test_run_step gaps-package-domain \
        cargo_package --locked --allow-dirty -p neoengram-domain || return $?
      project_test_run_step gaps-package-cli \
        cargo_package --locked --allow-dirty --no-verify --exclude-lockfile -p neoengram \
        || return $?
      project_test_run_step gaps-web-local-gateway-build \
        web_npm run build:local-gateway || return $?
      project_test_run_step gaps-runner-self-test run_runner_self_test || return $?
      project_test_run_step gaps-bundle-collision-audit run_bundle_collision_audit || return $?
      ;;
    all-features)
      project_test_run_step gaps-all-features-tests \
        cargo test --workspace --all-targets --all-features --locked || return $?
      ;;
    doc)
      project_test_run_step gaps-doc-tests \
        cargo test --workspace --doc --locked || return $?
      ;;
    schema)
      project_test_run_step gaps-schema-determinism run_schema_determinism || return $?
      ;;
    package)
      # --allow-dirty: local developers run this module on working trees with
      # uncommitted changes; on a clean CI checkout the flag is a no-op.
      project_test_run_step gaps-package-domain \
        cargo_package --locked --allow-dirty -p neoengram-domain || return $?
      project_test_run_step gaps-package-cli \
        cargo_package --locked --allow-dirty --no-verify --exclude-lockfile -p neoengram \
        || return $?
      ;;
    web-build)
      project_test_run_step gaps-web-local-gateway-build \
        web_npm run build:local-gateway || return $?
      ;;
    self-test)
      project_test_run_step gaps-runner-self-test run_runner_self_test || return $?
      ;;
    bundle-audit)
      project_test_run_step gaps-bundle-collision-audit run_bundle_collision_audit || return $?
      ;;
    *)
      project_test_die "unknown gaps suite '${suite}' (expected all, all-features, doc, schema, package, web-build, self-test, or bundle-audit)"
      ;;
  esac
}

main() {
  run_gaps_suite
}

main "$@"
