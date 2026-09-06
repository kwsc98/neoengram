# Project Test Runner

The project test runner is the single entry point for the repository's existing
Rust, OpenAPI, web, manifest, and end-to-end tests. It orchestrates tests; it does
not replace the package-level test suites or the fixtures under `test/`.

Run it from the repository root:

```text
scripts/project-test.sh bootstrap
scripts/project-test.sh module <domain|runtime|agent|central|gateway|dev-stack|cli|openapi|web|web-e2e|manifests|quality|gaps>
scripts/project-test.sh modules
scripts/project-test.sh full-flow
scripts/project-test.sh all
scripts/project-test.sh mount-probe
scripts/project-test.sh self-test
tests/project/self-test.sh
```

Options can appear before the command:

```text
--no-install   skip `npm ci` (the existing node_modules must already be usable)
--keep-temp    retain the isolated temporary directory after a successful run
--verbose      print each step log while it runs
```

## Default iteration policy

For routine AI or automation iterations, run the full project suite once after
the change:

```bash
bash scripts/project-test.sh --no-install all
```

Use `bash scripts/project-test.sh all` on the first run, when dependencies are
missing, or when a lockfile changes so the runner can install its pinned npm
dependencies. `--no-install` assumes that the existing `node_modules` trees
are usable; it does not hide missing dependencies. A non-zero exit or an
unavailable platform/dependency must be reported as a failure or an explicit
not-run reason, together with the preserved log path. Do not rerun the same
suite solely for confirmation; rerun a focused check only to diagnose a
failure or when the user asks for it. Manual testing may be performed by the
user separately.

## Prerequisites

Rust tests require the pinned toolchain from `rust-toolchain.toml`, Cargo, `jq`,
and `bash`. Full-flow fixture generation additionally requires `openssl`.
OpenAPI and web modules require Node.js/npm matching their package
engine declarations and their lockfiles. The web E2E module additionally needs
`npx` and a Playwright Chromium installation; the normal runner installs the
browser when installs are enabled. Manifest checks require `rg` (and use
`kubectl`/Ruby only when those tools are available).

`bootstrap` installs both lockfile-pinned JavaScript dependency sets. `modules`
does not bootstrap implicitly, while `all` runs bootstrap before the modules and
isolated full flow. `--no-install` skips the JavaScript install steps in all of
these commands; it does not hide a missing dependency.

## Reports and temporary data

Each invocation receives a unique run ID. Reports and step logs are written to:

```text
target/project-test/<run-id>/summary.json
target/project-test/<run-id>/steps.json
target/project-test/<run-id>/logs/
target/project-test/<run-id>/central/
target/project-test/<run-id>/gateway-source/
target/project-test/<run-id>/gateway-target/
target/project-test/<run-id>/agent-source/
target/project-test/<run-id>/agent-target/
```

The full-flow report includes generated Agent YAML summaries under the two
Agent directories. Enrollment token files, private keys, SQLite state, and
payload volumes stay under the per-run temporary root and are never copied into
the report.

Runtime state is created below `${TMPDIR:-/tmp}/neoengram-project-test.*`. It
contains only data for that run. Successful runs remove it unless `--keep-temp`
is supplied; failed runs preserve it and print its path. The runner stops child
processes on exit. A cleanup failure is reported as a non-zero result and keeps
the temporary state for diagnosis. Failed-run retention scrubs generated token,
private-key, Agent-state, repository, and payload files while keeping logs and
configuration summaries.

Exit status is stable across commands:

* `0`: every requested step passed;
* `1`: a test, assertion, process, or cleanup failed;
* `2`: a required command, platform, configuration, or fixture is unavailable.

## Module suites

The Rust wrappers invoke the package's real Cargo targets. Central supports
focused suites through `PROJECT_TEST_CENTRAL_SUITE` (`all`, `http`,
`permissions`, `lifecycle`, `placement`, `replication`, or `workspace`); Agent
supports `PROJECT_TEST_AGENT_SUITE` (`all`, `state_machine`,
`persistent_adapters`, `central_managed_add`, or `mount-probe`). Domain and the
other modules have analogous package-specific suite selectors documented in the
script source.

## Coverage-gap module (`gaps`)

The `gaps` module runs the checks the base modules deliberately do not cover:
all-features workspace tests, doc tests, schema regeneration determinism,
crates.io package verification, the `local-gateway` web build mode, runner
self-test, and a bundled-OpenAPI collision audit. It is registered but **not**
part of the default `modules`/`all` run yet: the bundle-collision audit
currently fails on nine known schema name collisions between the agent OpenAPI
component wrappers and the external schema `$defs` (redocly renames the
conflicting components to `X-2` in `target/openapi/neoengram-agent-api.json`).
Run it explicitly with `scripts/project-test.sh module gaps`, and promote it
into `run_modules` in `scripts/project-test.sh` once the collisions are
resolved. Focused suites are available through `PROJECT_TEST_GAPS_SUITE`
(`all`, `all-features`, `doc`, `schema`, `package`, `web-build`, `self-test`,
or `bundle-audit`).

## Mount probe boundary

`mount-probe` is never part of the default modules/all run. It is a privileged,
Linux-only check and must be given a prepared real mount root through
`NEOENGRAM_REAL_MOUNT_PROBE_ROOT` (the CI workflow provisions a temporary
tmpfs). It does not use or remove the desktop `mount`/`mount2` directories.

## Full-flow boundary

`full-flow` provisions an isolated loopback Central, two Gateway processes, and
temporary source/target volumes. It builds a real CLI commit, exercises the
Central placement/replication contract tests, and verifies copied volume
contents without touching repository fixtures or an already-running local
stack. It also generates per-run keyring, CA, and Gateway leaf certificate
fixtures; private keys and payloads remain under the temporary root and are
removed after successful runs. The production Agent enrollment and
cross-Gateway transfer path requires
a separately provisioned fixture; provide its command as
`PROJECT_TEST_REAL_FLOW_COMMAND` to run that explicit step. Without it, the
runner records a clear skip rather than claiming that a real Agent transfer was
verified.
Set `PROJECT_TEST_REQUIRE_REAL_FLOW=1` in CI acceptance jobs so a missing real-flow
fixture is a configuration failure instead of a skip. Generated local Agent fixtures
explicitly set `replication.enabled: false`; a real-flow fixture must provision the
Central command trust bundle, both QUIC endpoints, and mutual-TLS material before
enabling replication.

The default full-flow does not start production Agents because enrollment needs
deployment-specific Gateway trust, bootstrap tokens, and Central command-key
material. A supplied real-flow command owns those prerequisites and is streamed
through the runner's secret redaction helper. The shell digest in the local
contract is SHA-256; Agent replication tests cover the production BLAKE3 object
verification contract.
