#!/usr/bin/env bash
set -Eeuo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/common.sh"

project_test_generate_ca() {
  local output_dir="$1" name="${2:-project-test-ca}"
  project_test_require_cmd openssl
  mkdir -p "${output_dir}"
  openssl genpkey -algorithm ED25519 -out "${output_dir}/${name}-key.pem" >/dev/null 2>&1
  openssl req -x509 -new -key "${output_dir}/${name}-key.pem" \
    -out "${output_dir}/${name}.pem" -days 2 -subj "/CN=${name}" >/dev/null 2>&1
  chmod 600 "${output_dir}/${name}-key.pem"
}

project_test_generate_leaf() {
  local output_dir="$1" name="$2" san="$3" ca_name="${4:-project-test-ca}"
  project_test_require_cmd openssl
  openssl genpkey -algorithm ED25519 -out "${output_dir}/${name}-key.pem" >/dev/null 2>&1
  openssl req -new -key "${output_dir}/${name}-key.pem" \
    -out "${output_dir}/${name}.csr.pem" -subj "/CN=${name}" \
    -addext "subjectAltName=${san}" \
    -addext 'basicConstraints=critical,CA:false' \
    -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
    -addext 'extendedKeyUsage=serverAuth,clientAuth' >/dev/null 2>&1
  openssl x509 -req -in "${output_dir}/${name}.csr.pem" \
    -CA "${output_dir}/${ca_name}.pem" -CAkey "${output_dir}/${ca_name}-key.pem" \
    -CAcreateserial -out "${output_dir}/${name}.pem" -days 2 \
    -copy_extensions copy >/dev/null 2>&1
  cat "${output_dir}/${name}.pem" "${output_dir}/${ca_name}.pem" >"${output_dir}/${name}-chain.pem"
  chmod 600 "${output_dir}/${name}-key.pem"
  rm -f "${output_dir}/${name}.csr.pem" "${output_dir}/${ca_name}.srl"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  project_test_require_base_tools
  output_dir="${1:-${PROJECT_TEST_TEMP_ROOT}/certificates}"
  mkdir -p "${output_dir}"
  project_test_generate_ca "${output_dir}" gateway-bootstrap-ca
  project_test_generate_leaf "${output_dir}" gateway-source 'IP:127.0.0.1' gateway-bootstrap-ca
  project_test_generate_leaf "${output_dir}" gateway-target 'IP:127.0.0.1' gateway-bootstrap-ca
  project_test_generate_ca "${output_dir}" transfer-ca
  project_test_generate_leaf "${output_dir}" transfer-source 'IP:127.0.0.1' transfer-ca
  project_test_generate_leaf "${output_dir}" transfer-target 'IP:127.0.0.1' transfer-ca
  project_test_log "generated test certificates under ${output_dir}"
fi
