import { load } from 'js-yaml';
import { describe, expect, it } from 'vitest';

import {
  buildAgentConfig,
  canonicalGatewayEdgeClusterId,
  validateGatewayClusterBinding,
} from '@/features/storage/agent-config';

const descriptor = {
  tenant_id: 'tenant-a',
  token_request_id: 'token-request-a',
  storage_volume_id: 'volume-a',
  display_name: 'Volume A',
  edge_cluster_id: 'cluster-a',
  region: 'cn-shanghai',
  access_mode: 'read_write_many' as const,
  pvc_reference: { namespace: 'neoengram-data', claim_name: 'volume-a' },
};

const token = {
  token_id: 'token-a',
  bootstrap_token: 'ngenr_v1_do-not-serialize',
  volume_descriptor_digest: 'a'.repeat(64),
  expires_at_unix_ms: '1785168500000',
  request_replayed: false,
  execution_reused: false,
};

describe('Agent YAML configuration', () => {
  it('serializes the frozen descriptor digest without embedding the bootstrap secret', () => {
    const yaml = buildAgentConfig(
      'https://gateway.example.com',
      'mesh.example.test',
      descriptor,
      token,
      'cluster-a',
    );
    const config = load(yaml) as Record<string, unknown>;

    expect(config).toMatchObject({
      gateway_endpoint: 'https://gateway.example.com',
      trust_bundle_file: '/etc/neoengram/gateway-ca.pem',
      gateway_workload_trust_domain: 'mesh.example.test',
      central_command_trust_bundle_file: '/etc/neoengram/central-command-trust.json',
      storage_volume_id: 'volume-a',
      volume_descriptor_digest: 'a'.repeat(64),
      registration: {
        token_id: 'token-a',
        bootstrap_token_file: '/var/run/secrets/neoengram/bootstrap-token',
      },
    });
    expect(yaml).not.toContain(token.bootstrap_token);
  });

  it('rejects endpoints that the Agent daemon cannot safely consume', () => {
    expect(() => buildAgentConfig('http://gateway.example.com', '', descriptor, token)).toThrow(
      /HTTPS/,
    );
    expect(() =>
      buildAgentConfig('https://gateway.example.com/agent', 'mesh.example.test', descriptor, token),
    ).toThrow(/origin URL/);
  });

  it('requires a valid workload trust domain for HTTPS', () => {
    expect(() => buildAgentConfig('https://gateway.example.com', '', descriptor, token)).toThrow(
      /trust domain/,
    );
    expect(() =>
      buildAgentConfig('https://gateway.example.com', 'Mesh.example.test', descriptor, token),
    ).toThrow(/小写 DNS/);
  });

  it('requires the configured Gateway EdgeCluster binding for networked endpoints', () => {
    expect(() =>
      buildAgentConfig('https://gateway.example.com', 'mesh.example.test', descriptor, token),
    ).toThrow(/VITE_GATEWAY_EDGE_CLUSTER_ID/);
    expect(() =>
      buildAgentConfig(
        'https://gateway.example.com',
        'mesh.example.test',
        descriptor,
        token,
        'cluster-b',
      ),
    ).toThrow(/不匹配/);
    expect(() =>
      buildAgentConfig(
        'https://gateway.example.com',
        'mesh.example.test',
        descriptor,
        token,
        'cluster-a',
      ),
    ).not.toThrow();
  });

  it('validates the deployment binding as a ResourceId', () => {
    expect(canonicalGatewayEdgeClusterId(' cluster-a ')).toBe('cluster-a');
    expect(() => canonicalGatewayEdgeClusterId('cluster/a')).toThrow(/EdgeCluster ID/);
    expect(() =>
      validateGatewayClusterBinding('https://gateway.example.com', 'cluster-a', 'cluster/a'),
    ).toThrow(/请求中的 EdgeCluster ID/);
  });

  it('keeps loopback HTTP development configuration free of HTTPS-only trust fields', () => {
    const yaml = buildAgentConfig('http://127.0.0.1:8081', '', descriptor, token);
    const config = load(yaml) as Record<string, unknown>;

    expect(config).not.toHaveProperty('gateway_workload_trust_domain');
    expect(config).not.toHaveProperty('central_command_trust_bundle_file');
  });
});
