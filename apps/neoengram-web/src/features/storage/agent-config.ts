import { dump } from 'js-yaml';

import type {
  CreateStorageEnrollmentTokenRequest,
  CreateStorageEnrollmentTokenResponse,
} from '@/api/types';

export function canonicalAgentEndpoint(value: string): string {
  const endpoint = new URL(value);
  if (
    endpoint.username ||
    endpoint.password ||
    endpoint.pathname !== '/' ||
    endpoint.search ||
    endpoint.hash
  ) {
    throw new Error('Agent endpoint 必须是不含凭据、路径、查询或片段的 origin URL');
  }
  const loopbackHttp =
    endpoint.protocol === 'http:' &&
    ['127.0.0.1', '[::1]', 'localhost'].includes(endpoint.hostname);
  if (endpoint.protocol !== 'https:' && !loopbackHttp) {
    throw new Error('Agent endpoint 必须使用 HTTPS；仅 loopback 开发环境允许 HTTP');
  }
  return endpoint.origin;
}

export function buildAgentConfig(
  agentEndpoint: string,
  descriptor: CreateStorageEnrollmentTokenRequest,
  token: CreateStorageEnrollmentTokenResponse,
): string {
  if (!/^[0-9a-f]{64}$/.test(token.volume_descriptor_digest)) {
    throw new Error('服务端未返回合法的 Volume descriptor digest');
  }

  return dump(
    {
      schema_version: 1,
      protocol_version: 1,
      central_endpoint: canonicalAgentEndpoint(agentEndpoint),
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
