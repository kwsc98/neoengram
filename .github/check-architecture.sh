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

expected_packages=$'neoengram\nneoengram-agent\nneoengram-core\nneoengram-engine\nneoengram-fs\nneoengram-protocol\nneoengram-standalone\nneoengramd'
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
assert_internal_dependencies neoengramd $'neoengram-core\nneoengram-protocol' ''

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
  'neoengram-(engine|fs|standalone|agent)|rusqlite|reqwest|hyper|axum|sqlx|diesel|aws-sdk'
assert_manifest_excludes \
  crates/neoengram-agent/Cargo.toml \
  'neoengram-standalone'
assert_manifest_excludes \
  services/neoengramd/Cargo.toml \
  'neoengram-(engine|fs|standalone)|rusqlite|reqwest|hyper|axum|sqlx|diesel|aws-sdk'
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
  services/neoengramd/Cargo.toml; do
  rg -q '^publish = false$' "$manifest" || fail "$manifest must remain workspace-private"
done

if [[ -e crates/neoengram-client ]]; then
  fail "neoengram-client is intentionally deferred until a user transport exists"
fi

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

echo "architecture checks passed"
