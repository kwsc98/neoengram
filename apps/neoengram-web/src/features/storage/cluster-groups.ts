import type { GatewayPoolView, StorageVolumeView } from '@/api/types';

export interface StorageClusterGroup {
  edgeClusterId: string;
  gatewayPool: GatewayPoolView | undefined;
  volumes: StorageVolumeView[];
  regions: string[];
  readyVolumeCount: number;
  routeState: 'ready' | 'unavailable' | 'unknown';
  selectableVolumeCount: number;
}

export function groupStorageVolumesByCluster(
  volumes: readonly StorageVolumeView[],
  gatewayPools: readonly GatewayPoolView[] = [],
  options: { gatewayInventoryAvailable?: boolean } = {},
): StorageClusterGroup[] {
  const poolsByCluster = new Map(gatewayPools.map((pool) => [pool.edge_cluster_id, pool] as const));
  const groups = new Map<string, StorageVolumeView[]>();

  for (const volume of volumes) {
    const clusterVolumes = groups.get(volume.edge_cluster_id) ?? [];
    clusterVolumes.push(volume);
    groups.set(volume.edge_cluster_id, clusterVolumes);
  }

  return [...groups.entries()]
    .map(([edgeClusterId, clusterVolumes]) => {
      const gatewayPool = poolsByCluster.get(edgeClusterId);
      const routeState = gatewayPool
        ? gatewayPool.state === 'ready'
          ? 'ready'
          : 'unavailable'
        : options.gatewayInventoryAvailable
          ? 'unavailable'
          : 'unknown';
      const readyVolumeCount = clusterVolumes.filter((volume) => volume.state === 'ready').length;
      return {
        edgeClusterId,
        gatewayPool,
        volumes: clusterVolumes,
        regions: [...new Set(clusterVolumes.map((volume) => volume.region))].sort(),
        readyVolumeCount,
        routeState,
        selectableVolumeCount: routeState === 'unavailable' ? 0 : readyVolumeCount,
      } satisfies StorageClusterGroup;
    })
    .sort((left, right) => {
      const leftName = left.gatewayPool?.display_name ?? left.edgeClusterId;
      const rightName = right.gatewayPool?.display_name ?? right.edgeClusterId;
      return leftName.localeCompare(rightName, 'zh-CN');
    });
}

export function isReplicationTargetSelectable(
  group: StorageClusterGroup,
  volume: StorageVolumeView,
): boolean {
  return group.routeState !== 'unavailable' && volume.state === 'ready';
}

export function selectableReplicationVolumes(
  groups: readonly StorageClusterGroup[],
): StorageVolumeView[] {
  return groups.flatMap((group) =>
    group.volumes.filter((volume) => isReplicationTargetSelectable(group, volume)),
  );
}
