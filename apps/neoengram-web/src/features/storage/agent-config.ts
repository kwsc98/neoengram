import { dump } from 'js-yaml';

import type {
  CreateStorageEnrollmentTokenRequest,
  CreateStorageEnrollmentTokenResponse,
} from '@/api/types';

const RESOURCE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

export function canonicalGatewayEndpoint(value: string): string {
  const endpoint = new URL(value);
  if (
    endpoint.username ||
    endpoint.password ||
    endpoint.pathname !== '/' ||
    endpoint.search ||
    endpoint.hash
  ) {
    throw new Error('Gateway endpoint 必须是不含凭据、路径、查询或片段的 origin URL');
  }
  const loopbackHttp =
    endpoint.protocol === 'http:' &&
    ['127.0.0.1', '[::1]', 'localhost'].includes(endpoint.hostname);
  if (endpoint.protocol !== 'https:' && !loopbackHttp) {
    throw new Error('Gateway endpoint 必须使用 HTTPS；仅 loopback 开发环境允许 HTTP');
  }
  return endpoint.origin;
}

export function canonicalGatewayWorkloadTrustDomain(value: string): string {
  const valid =
    value.length > 0 &&
    value.length <= 253 &&
    value === value.toLowerCase() &&
    value.split('.').every((label) => {
      return (
        label.length > 0 && label.length <= 63 && /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(label)
      );
    });
  if (!valid) {
    throw new Error('Gateway workload trust domain 必须是小写 DNS 名称');
  }
  return value;
}

/**
 * Canonicalizes the build-time EdgeCluster binding. The binding is deployment
 * configuration, so surrounding whitespace is harmless, but the resulting
 * value must still use the protocol ResourceId grammar.
 */
export function canonicalGatewayEdgeClusterId(value: string): string {
  const clusterId = value.trim();
  if (!RESOURCE_ID_PATTERN.test(clusterId)) {
    throw new Error('Gateway EdgeCluster binding 必须是合法的 EdgeCluster ID');
  }
  return clusterId;
}

/**
 * A Web build has one configured GatewayPool origin. On a networked Gateway
 * that origin is only safe for the one EdgeCluster it was provisioned for.
 * Loopback HTTP remains intentionally unbound for local development.
 */
export function validateGatewayClusterBinding(
  gatewayEndpoint: string,
  configuredEdgeClusterId: string,
  descriptorEdgeClusterId: string,
): void {
  const canonicalEndpoint = canonicalGatewayEndpoint(gatewayEndpoint);
  const requiresBinding = new URL(canonicalEndpoint).protocol === 'https:';
  const configured = configuredEdgeClusterId.trim();

  if (!RESOURCE_ID_PATTERN.test(descriptorEdgeClusterId)) {
    throw new Error('请求中的 EdgeCluster ID 不合法');
  }

  if (!configured) {
    if (requiresBinding) {
      throw new Error(
        'HTTPS Gateway 必须配置 VITE_GATEWAY_EDGE_CLUSTER_ID，才能生成该集群的 Agent 配置',
      );
    }
    return;
  }

  const expected = canonicalGatewayEdgeClusterId(configured);
  if (expected !== descriptorEdgeClusterId) {
    throw new Error(
      `EdgeCluster ${descriptorEdgeClusterId} 与 Web 预配置的 Gateway 集群 ${expected} 不匹配`,
    );
  }
}

export function buildAgentConfig(
  gatewayEndpoint: string,
  gatewayWorkloadTrustDomain: string,
  descriptor: CreateStorageEnrollmentTokenRequest,
  token: CreateStorageEnrollmentTokenResponse,
  gatewayEdgeClusterId = '',
): string {
  if (!/^[0-9a-f]{64}$/.test(token.volume_descriptor_digest)) {
    throw new Error('服务端未返回合法的 Volume descriptor digest');
  }

  const canonicalEndpoint = canonicalGatewayEndpoint(gatewayEndpoint);
  const gatewayIdentity =
    new URL(canonicalEndpoint).protocol === 'https:'
      ? {
          gateway_workload_trust_domain: canonicalGatewayWorkloadTrustDomain(
            gatewayWorkloadTrustDomain,
          ),
          central_command_trust_bundle_file: '/etc/neoengram/central-command-trust.json',
        }
      : {};
  validateGatewayClusterBinding(
    canonicalEndpoint,
    gatewayEdgeClusterId,
    descriptor.edge_cluster_id,
  );

  return dump(
    {
      schema_version: 1,
      protocol_version: 1,
      gateway_endpoint: canonicalEndpoint,
      trust_bundle_file: '/etc/neoengram/gateway-ca.pem',
      ...gatewayIdentity,
      tenant_id: descriptor.tenant_id,
      edge_cluster_id: descriptor.edge_cluster_id,
      storage_volume_id: descriptor.storage_volume_id,
      volume_descriptor_digest: token.volume_descriptor_digest,
      region: descriptor.region,
      storage: {
        backend_type: 'pvc',
        access_mode: descriptor.access_mode,
        mount_path: '/volume',
        state_dir: '/var/lib/neoengram-agent',
        marker_file: '/volume/.neoengram-volume-marker',
        expected_volume_marker: descriptor.storage_volume_id,
        pvc_reference: {
          namespace: descriptor.pvc_reference.namespace,
          claim_name: descriptor.pvc_reference.claim_name,
        },
      },
      registration: {
        approval_required: true,
        token_id: token.token_id,
        bootstrap_token_file: '/var/run/secrets/neoengram/bootstrap-token',
      },
      session: {
        heartbeat_interval_seconds: 10,
        reconnect_max_delay_seconds: 30,
      },
      logging: {
        format: 'json',
        level: 'info',
      },
    },
    {
      lineWidth: -1,
      noCompatMode: true,
      noRefs: true,
      sortKeys: false,
    },
  );
}
