#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
deployment="$root/deployment.yaml"
config="$root/configmap.yaml"
secret="$root/secret.example.yaml"
state_pvc="$root/agent-state-pvc.yaml"

fail() {
  echo "Agent manifest check failed: $*" >&2
  exit 1
}

command -v rg >/dev/null 2>&1 || fail "ripgrep is required"

for file in "$deployment" "$config" "$secret" "$state_pvc"; do
  [[ -f "$file" ]] || fail "missing $file"
  rg -q '^apiVersion: ' "$file" || fail "$file has no apiVersion"
  rg -q '^kind: ' "$file" || fail "$file has no kind"
done

rg -q '^  replicas: 1$' "$deployment" || fail "Deployment must have exactly one replica"
rg -q '^    type: Recreate$' "$deployment" || fail "Deployment strategy must be Recreate"
rg -q '^      automountServiceAccountToken: false$' "$deployment" || \
  fail "ServiceAccount token automount must be disabled"
rg -q '^        runAsNonRoot: true$' "$deployment" || fail "Pod must run as non-root"
rg -q '^            readOnlyRootFilesystem: true$' "$deployment" || \
  fail "Agent root filesystem must be read-only"
rg -q '^            allowPrivilegeEscalation: false$' "$deployment" || \
  fail "privilege escalation must be disabled"
rg -q '^                - ALL$' "$deployment" || fail "all Linux capabilities must be dropped"
rg -q 'image: .+@sha256:[0-9a-f]{64}$' "$deployment" || \
  fail "Agent image must use a sha256 digest"
rg -q '^              mountPath: /volume$' "$deployment" || \
  fail "business PVC must mount at /volume"
rg -q '^              mountPath: /var/lib/neoengram-agent$' "$deployment" || \
  fail "state PVC must mount at /var/lib/neoengram-agent"
rg -q '^            optional: true$' "$deployment" || \
  fail "consumed bootstrap Secret must be removable"

if rg -n '^[[:space:]]+(fsGroup|hostPath|privileged|serviceAccountName):' "$deployment"; then
  fail "Deployment contains a forbidden broad privilege or PVC-mutating setting"
fi
if rg -n '^kind: (DaemonSet|StatefulSet|Service|Ingress|HorizontalPodAutoscaler|Role|RoleBinding|ServiceAccount)$' \
  "$root"/*.yaml; then
  fail "0.0.1 must remain one manually deployed volume-bound Deployment"
fi

for expected in \
  'name: neoengram-agent-volume-safe-slug' \
  'claimName: neoengram-agent-state-volume-safe-slug' \
  'name: neoengram-agent-config-volume-safe-slug' \
  'secretName: neoengram-agent-bootstrap-volume-safe-slug'; do
  rg -q "$expected" "$deployment" || fail "Deployment is missing Volume-specific reference: $expected"
done

for expected in \
  'storage_volume_id: volume-example' \
  'region: cn-example-1' \
  'backend_type: pvc' \
  'access_mode: read_write_many' \
  'mount_path: /volume' \
  'state_dir: /var/lib/neoengram-agent' \
  'marker_file: /volume/.neoengram-volume-marker' \
  'expected_volume_marker: volume-example' \
  'token_id: replace-with-storage-enrollment-token-id'; do
  rg -q "$expected" "$config" || fail "Agent config is missing scope field: $expected"
done

rg -q '^    - ReadWriteOnce$' "$state_pvc" || fail "Agent state PVC must be RWO"
rg -q '^immutable: true$' "$secret" || fail "bootstrap Secret must be immutable"

echo "Agent manifest policy checks passed"
