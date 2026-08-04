import { load } from 'js-yaml';
import { describe, expect, it } from 'vitest';

import { buildAgentConfig } from '@/features/storage/agent-config';

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
  replayed: false,
};

describe('Agent YAML configuration', () => {
  it('serializes the frozen descriptor digest without embedding the bootstrap secret', () => {
    const yaml = buildAgentConfig('https://control.example.com', descriptor, token);
    const config = load(yaml) as Record<string, unknown>;

    expect(config).toMatchObject({
      central_endpoint: 'https://control.example.com',
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
    expect(() => buildAgentConfig('http://control.example.com', descriptor, token)).toThrow(
      /HTTPS/,
    );
    expect(() => buildAgentConfig('https://control.example.com/agent', descriptor, token)).toThrow(
      /origin URL/,
    );
  });
});
