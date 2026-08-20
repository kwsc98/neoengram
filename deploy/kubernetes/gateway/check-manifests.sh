#!/usr/bin/env bash
set -euo pipefail

manifest_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
deployments="$manifest_dir/deployment.yaml"
services="$manifest_dir/service.yaml"
config="$manifest_dir/configmap.yaml"
pdb="$manifest_dir/pdb.yaml"
network_policy="$manifest_dir/networkpolicy.yaml"
identity_secrets="$manifest_dir/secret.example.yaml"
activation_secrets="$manifest_dir/activation-secret.example.yaml"
public_tls_secret="$manifest_dir/public-tls-secret.example.yaml"
kustomization="$manifest_dir/kustomization.yaml"
readme="$manifest_dir/README.md"

fail() {
  echo "Gateway manifest check failed: $*" >&2
  exit 1
}

expect_count() {
  local expected="$1"
  local pattern="$2"
  local file="$3"
  local actual
  actual="$(rg -c "$pattern" "$file" || true)"
  [[ "$actual" == "$expected" ]] || \
    fail "$file: expected $expected matches for '$pattern', found ${actual:-0}"
}

command -v rg >/dev/null 2>&1 || fail "ripgrep is required"

for file in \
  "$deployments" \
  "$services" \
  "$config" \
  "$pdb" \
  "$network_policy" \
  "$identity_secrets" \
  "$activation_secrets" \
  "$public_tls_secret" \
  "$kustomization"; do
  [[ -f "$file" ]] || fail "missing $file"
  rg -q '^apiVersion: ' "$file" || fail "$file has no apiVersion"
  rg -q '^kind: ' "$file" || fail "$file has no kind"
done

# Parse all YAML documents when the standard Ruby YAML parser is available. This catches
# indentation errors such as a selector accidentally escaping matchLabels without adding a yq
# dependency to minimal build images.
if command -v ruby >/dev/null 2>&1; then
  ruby -e '
    require "yaml"
    ARGV.each do |path|
      YAML.load_stream(File.read(path)).each_with_index do |document, index|
        unless document.is_a?(Hash) && document["apiVersion"] && document["kind"]
          warn "#{path}: YAML document #{index + 1} is not a Kubernetes object"
          exit 1
        end
      end
    end
  ' \
    "$deployments" "$services" "$config" "$pdb" "$network_policy" \
    "$identity_secrets" "$activation_secrets" "$public_tls_secret" "$kustomization" || \
      fail "YAML parsing failed"
fi

for resource in configmap.yaml public-tls-secret.example.yaml secret.example.yaml \
  activation-secret.example.yaml \
  deployment.yaml service.yaml pdb.yaml networkpolicy.yaml; do
  rg -q "^[[:space:]]+- ${resource}$" "$kustomization" || \
    fail "kustomization.yaml is missing ${resource}"
done
if command -v kubectl >/dev/null 2>&1; then
  kubectl kustomize "$manifest_dir" >/dev/null || fail "Kustomize rendering failed"
fi

# Replica identity is explicit and stable. A Deployment template cannot safely derive both its
# Registry identity and Secret name from a transient Pod name, and scaling either document would
# duplicate one credential.
expect_count 2 '^kind: Deployment$' "$deployments"
expect_count 2 '^  replicas: 1$' "$deployments"
expect_count 2 '^    type: Recreate$' "$deployments"
if rg -n 'fieldPath: metadata\.(name|uid)|SYNAPSE_GATEWAY_REPLICA_ID[^\n]*metadata' "$deployments"; then
  fail "GatewayReplicaId must not be derived from Pod metadata"
fi
for replica in r0 r1; do
  replica_id="gateway-pool-example-${replica}"
  rg -Uq -- "- name: SYNAPSE_GATEWAY_REPLICA_ID\n[[:space:]]+value: ${replica_id}$" \
    "$deployments" || fail "missing fixed GatewayReplicaId ${replica_id}"
  rg -q "secretName: synapse-gateway-identity-${replica_id}$" "$deployments" || \
    fail "${replica_id} must use its own listener identity Secret"
  rg -q "secretName: synapse-gateway-activation-${replica_id}$" "$deployments" || \
    fail "${replica_id} must use its own activation Secret"
  rg -q "name: synapse-gateway-identity-${replica_id}$" "$identity_secrets" || \
    fail "missing identity Secret example for ${replica_id}"
  rg -q "name: synapse-gateway-activation-${replica_id}$" "$activation_secrets" || \
    fail "missing activation Secret example for ${replica_id}"
  rg -q "name: synapse-gateway-${replica_id}$" "$services" || \
    fail "missing stable per-Replica Service for ${replica_id}"
done
expect_count 2 '^            secretName: synapse-gateway-identity-' "$deployments"
expect_count 2 '^            secretName: synapse-gateway-activation-' "$deployments"

expect_count 2 '^      automountServiceAccountToken: false$' "$deployments"
expect_count 2 '^        runAsNonRoot: true$' "$deployments"
expect_count 2 '^            readOnlyRootFilesystem: true$' "$deployments"
expect_count 2 '^            allowPrivilegeEscalation: false$' "$deployments"
expect_count 2 '^                - ALL$' "$deployments"
expect_count 2 'image: .+@sha256:[0-9a-f]{64}$' "$deployments"
expect_count 4 '^              scheme: HTTPS$' "$deployments"
expect_count 2 '^          lifecycle:$' "$deployments"
expect_count 2 '^            preStop:$' "$deployments"
expect_count 2 '^              exec:$' "$deployments"
expect_count 2 '^                  - /proc/1/exe$' "$deployments"
expect_count 2 '^                  - --pre-stop-drain$' "$deployments"
expect_count 2 '^      terminationGracePeriodSeconds: 60$' "$deployments"
expect_count 2 '^      affinity:$' "$deployments"
expect_count 2 '^        podAntiAffinity:$' "$deployments"
expect_count 2 '^          requiredDuringSchedulingIgnoredDuringExecution:$' "$deployments"
expect_count 2 '^              topologyKey: kubernetes\.io/hostname$' "$deployments"
expect_count 2 '^                  neoengram\.io/gateway-pool: gateway-pool-example$' "$deployments"
if rg -q '^          preferredDuringSchedulingIgnoredDuringExecution:$' "$deployments"; then
  fail "Gateway replicas require hard hostname anti-affinity within one GatewayPool"
fi
rg -q '^  SYNAPSE_GATEWAY_PRE_STOP_DRAIN_SECONDS: "20"$' "$config" || \
  fail "Gateway preStop drain interval must leave time for SIGTERM shutdown"

for setting in \
  SYNAPSE_GATEWAY_BOOTSTRAP_PRIVATE_KEY_FILE \
  SYNAPSE_GATEWAY_BOOTSTRAP_ACTIVATION_TOKEN_FILE \
  SYNAPSE_GATEWAY_BOOTSTRAP_CERTIFICATE_CHAIN_FILE; do
  rg -q "$setting" "$deployments" || fail "Gateway bootstrap setting $setting is missing"
  if rg -q "$setting" "$config"; then
    fail "short-lived Gateway bootstrap setting $setting must not be stored in the shared ConfigMap"
  fi
done
rg -q '^  SYNAPSE_GATEWAY_WORKLOAD_TRUST_DOMAIN: ' "$config" || \
  fail "long-lived Gateway workload trust domain is missing from the ConfigMap"
rg -q '^  SYNAPSE_GATEWAY_TLS_CERTIFICATE_FILE: /var/run/secrets/synapse-gateway/listener/tls\.crt$' \
  "$config" || fail "Gateway listener certificate path is missing or inconsistent"
rg -q '^  SYNAPSE_GATEWAY_TLS_PRIVATE_KEY_FILE: /var/run/secrets/synapse-gateway/listener/tls\.key$' \
  "$config" || fail "Gateway listener private-key path is missing or inconsistent"
rg -q '^  SYNAPSE_GATEWAY_TLS_CLIENT_CA_FILE: /var/run/secrets/synapse-gateway/listener/ca\.crt$' \
  "$config" || fail "Gateway workload CA path is missing or inconsistent"
rg -q '^  SYNAPSE_GATEWAY_PUBLIC_LISTEN: 0\.0\.0\.0:8080$' "$config" || \
  fail "Gateway public listener must be explicitly enabled on port 8080"
rg -q '^  SYNAPSE_GATEWAY_PUBLIC_TLS_CERTIFICATE_FILE: /var/run/secrets/synapse-gateway/public/tls\.crt$' \
  "$config" || fail "Gateway public certificate path is missing or inconsistent"
rg -q '^  SYNAPSE_GATEWAY_PUBLIC_TLS_PRIVATE_KEY_FILE: /var/run/secrets/synapse-gateway/public/tls\.key$' \
  "$config" || fail "Gateway public private-key path is missing or inconsistent"
for setting in SYNAPSE_GATEWAY_CONSOLE_HOST SYNAPSE_GATEWAY_S3_HOST \
  SYNAPSE_GATEWAY_WEB_ROOT SYNAPSE_GATEWAY_CENTRAL_API_UPSTREAM SYNAPSE_GATEWAY_S3_MAX_STREAMS; do
  rg -q "^  ${setting}: " "$config" || fail "Gateway public setting ${setting} is missing"
done
rg -q '^  SYNAPSE_GATEWAY_CENTRAL_API_UPSTREAM: https://[^/]+:8080$' "$config" || \
  fail "Gateway Central upstream must be a private origin-form HTTPS URL"

expect_count 2 '^type: kubernetes\.io/tls$' "$identity_secrets"
expect_count 2 '^  tls\.crt: \|$' "$identity_secrets"
expect_count 2 '^  tls\.key: \|$' "$identity_secrets"
expect_count 2 '^  ca\.crt: \|$' "$identity_secrets"
expect_count 2 '^type: Opaque$' "$activation_secrets"
expect_count 2 '^  activation-token: \|$' "$activation_secrets"
expect_count 2 '^immutable: true$' "$identity_secrets"
expect_count 2 '^immutable: true$' "$activation_secrets"
expect_count 1 '^kind: Secret$' "$public_tls_secret"
expect_count 1 '^  name: synapse-gateway-public-tls-gateway-pool-example$' "$public_tls_secret"
expect_count 1 '^type: kubernetes\.io/tls$' "$public_tls_secret"
expect_count 1 '^  tls\.crt: \|$' "$public_tls_secret"
expect_count 1 '^  tls\.key: \|$' "$public_tls_secret"
expect_count 1 '^immutable: true$' "$public_tls_secret"

expect_count 2 '^            - name: public$' "$deployments"
expect_count 2 '^              containerPort: 8080$' "$deployments"
expect_count 2 '^              mountPath: /var/run/secrets/synapse-gateway/public$' "$deployments"
expect_count 2 '^            secretName: synapse-gateway-public-tls-gateway-pool-example$' "$deployments"

# Only a bounded, memory-backed certificate delivery directory is writable. Gateway must never
# receive a business Volume, durable PVC, host path, storage adapter, or object payload mount.
expect_count 2 '^          emptyDir:$' "$deployments"
expect_count 2 '^            medium: Memory$' "$deployments"
expect_count 2 '^            sizeLimit: 2Mi$' "$deployments"
if rg -n 'persistentVolumeClaim:|hostPath:|nfs:|csi:|claimName:|mountPath: /(volume|data|cas)(/|$)' \
  "$deployments"; then
  fail "Gateway must not mount a business Volume, PVC, host path, NFS export, or CAS path"
fi
if rg -n 'neoengram-(engine|fs|standalone|agentd|server)|neoengram-central|fusen|sql' "$config"; then
  fail "Gateway runtime configuration contains a forbidden architecture dependency"
fi

# Agents use the Pool Service, while Central uses stable Replica Services. Bootstrap Services must
# publish the Pending Pods or activation deadlocks on a readiness check that requires control mTLS.
expect_count 3 '^kind: Service$' "$services"
expect_count 2 '^  publishNotReadyAddresses: true$' "$services"
for port in agent control peer; do
  rg -q "^    - name: ${port}$" "$services" || fail "Gateway Services are missing ${port}"
done
expect_count 1 '^    - name: public$' "$services"
expect_count 1 '^      port: 443$' "$services"
expect_count 1 '^      targetPort: public$' "$services"
rg -q '^  minAvailable: 1$' "$pdb" || fail "Gateway PDB must preserve one replica"
rg -Uq '^  selector:\n    matchLabels:\n      app\.kubernetes\.io/name: synapse-gateway$' "$pdb" || \
  fail "Gateway PDB selector indentation is invalid"

rg -q '^kind: NetworkPolicy$' "$network_policy" || fail "Gateway NetworkPolicy is required"
rg -q '^    - Ingress$' "$network_policy" || fail "Gateway NetworkPolicy must restrict ingress"
rg -q '^    - Egress$' "$network_policy" || fail "Gateway NetworkPolicy must restrict egress"
rg -q 'neoengram\.io/control-plane: "true"' "$network_policy" || \
  fail "Central ingress must be namespace-scoped"
rg -q 'neoengram\.io/gateway-plane: "true"' "$network_policy" || \
  fail "peer traffic must be namespace-scoped"
rg -q 'neoengram\.io/public-ingress: "true"' "$network_policy" || \
  fail "public ingress must be namespace-scoped"
rg -q 'neoengram\.io/gateway-public-ingress: "true"' "$network_policy" || \
  fail "public ingress must be pod-scoped"
rg -q 'app\.kubernetes\.io/name: neoengram-central' "$network_policy" || \
  fail "Central egress must be pod-scoped"
for port in 8080 8081 8082 8083; do
  rg -q "port: ${port}$" "$network_policy" || \
    fail "NetworkPolicy is missing listener port ${port}"
done
if rg -n 'central_endpoint|neoengram-central|neoengram-central' "$config" "$deployments"; then
  fail "Gateway manifests must not configure a static Agent-to-Central fallback"
fi

# These files are intentionally unusable as a production activation bundle until a renderer and
# external provisioner replace all placeholders and implement Secret rotation.
for file in "$deployments" "$services" "$config" "$pdb" "$network_policy" \
  "$identity_secrets" "$activation_secrets" "$public_tls_secret"; do
  rg -q 'neoengram.io/example-only: "external-provisioner-required"' "$file" || \
    fail "$file must remain marked example-only"
done
rg -qi 'not a production activation bundle' "$readme" || \
  fail "README must state that the manifests are not a production activation bundle"
rg -qi 'external provisioner' "$readme" || \
  fail "README must document the required external provisioner"

echo "Gateway example manifest checks passed (external credential provisioner still required)"
