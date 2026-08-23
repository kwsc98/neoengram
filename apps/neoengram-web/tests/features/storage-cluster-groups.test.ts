import { describe, expect, it } from 'vitest';

import type { GatewayPoolView, StorageVolumeView } from '@/api/types';
import {
  groupStorageVolumesByCluster,
  isReplicationTargetSelectable,
  selectableReplicationVolumes,
} from '@/features/storage/cluster-groups';

function volume(
  storageVolumeId: string,
  edgeClusterId: string,
  region: string,
  state: StorageVolumeView['state'] = 'ready',
): StorageVolumeView {
  return {
    tenant_id: 'tenant-a',
    storage_volume_id: storageVolumeId,
    display_name: storageVolumeId,
    edge_cluster_id: edgeClusterId,
    region,
    backend_type: 'pvc',
    access_mode: 'read_write_once',
    allowed_delivery_modes: ['copy'],
    hardlink_policy: 'disabled',
    max_whole_file_bytes: '1024',
    copy_reserve_bytes: '0',
    state,
    resource_version: '1',
    lifecycle: { state: 'active', generation: '1' },
    created_at_unix_ms: '1',
    updated_at_unix_ms: '1',
  };
}

function gatewayPool(
  gatewayPoolId: string,
  edgeClusterId: string,
  displayName: string,
): GatewayPoolView {
  return {
    gateway_pool_id: gatewayPoolId,
    edge_cluster_id: edgeClusterId,
    display_name: displayName,
    agent_endpoint: 'https://gateway.example.test',
    desired_replicas: 1,
    minimum_ready_replicas: 1,
    state: 'ready',
    config_generation: '1',
    resource_version: '1',
    created_at_unix_ms: '1',
    updated_at_unix_ms: '1',
  };
}

describe('storage cluster groups', () => {
  it('places disks under their gateway cluster and exposes cluster health totals', () => {
    const groups = groupStorageVolumesByCluster(
      [
        volume('volume-b', 'cluster-b', 'tokyo', 'degraded'),
        volume('volume-a-1', 'cluster-a', 'osaka'),
        volume('volume-a-2', 'cluster-a', 'tokyo'),
      ],
      [
        gatewayPool('pool-a', 'cluster-a', 'Gateway A'),
        gatewayPool('pool-b', 'cluster-b', 'Gateway B'),
      ],
    );

    expect(groups.map((group) => group.gatewayPool?.gateway_pool_id)).toEqual(['pool-a', 'pool-b']);
    expect(groups[0]).toMatchObject({
      edgeClusterId: 'cluster-a',
      regions: ['osaka', 'tokyo'],
      readyVolumeCount: 2,
      routeState: 'ready',
      selectableVolumeCount: 2,
    });
    expect(groups[0]?.volumes.map((item) => item.storage_volume_id)).toEqual([
      'volume-a-1',
      'volume-a-2',
    ]);
    expect(groups[1]?.readyVolumeCount).toBe(0);
  });

  it('still groups volumes when gateway inventory is unavailable', () => {
    const groups = groupStorageVolumesByCluster([volume('volume-local', 'edge-local', 'local')]);

    expect(groups).toHaveLength(1);
    expect(groups[0]).toMatchObject({
      edgeClusterId: 'edge-local',
      gatewayPool: undefined,
      readyVolumeCount: 1,
      routeState: 'unknown',
      selectableVolumeCount: 1,
    });
  });

  it('disables disks below a known unhealthy or missing GatewayPool', () => {
    const drainingPool: GatewayPoolView = {
      ...gatewayPool('pool-a', 'cluster-a', 'Gateway A'),
      state: 'draining',
    };
    const groups = groupStorageVolumesByCluster(
      [volume('volume-a', 'cluster-a', 'tokyo'), volume('volume-b', 'cluster-b', 'osaka')],
      [drainingPool],
      { gatewayInventoryAvailable: true },
    );

    expect(groups.map((group) => group.routeState)).toEqual(['unavailable', 'unavailable']);
    expect(selectableReplicationVolumes(groups)).toEqual([]);
    expect(isReplicationTargetSelectable(groups[0]!, groups[0]!.volumes[0]!)).toBe(false);
  });

  it('allows Central to validate the route when gateway inventory is not visible', () => {
    const groups = groupStorageVolumesByCluster(
      [
        volume('volume-ready', 'cluster-a', 'tokyo'),
        volume('volume-degraded', 'cluster-a', 'tokyo', 'degraded'),
      ],
      [],
      { gatewayInventoryAvailable: false },
    );

    expect(selectableReplicationVolumes(groups).map((item) => item.storage_volume_id)).toEqual([
      'volume-ready',
    ]);
  });
});
