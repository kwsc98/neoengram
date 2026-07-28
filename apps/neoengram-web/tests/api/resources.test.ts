import { describe, expect, it } from 'vitest';

import {
  commitPlayground,
  createArtifact,
  createPlayground,
  createSnapshot,
  createStorageVolume,
  createTenant,
  queryArtifact,
  queryArtifactCommitDiff,
  queryArtifactCommitGraph,
  queryArtifactList,
  queryPlayground,
  queryPlaygroundList,
  queryProjectList,
  querySnapshot,
  querySnapshotList,
  queryStorageVolume,
  queryStorageVolumeList,
  queryTenant,
  queryTenantList,
} from '@/api/operations';

describe('tenant-scoped public resource operations', () => {
  it('queries authorized tenants and creates a replayable Tenant', async () => {
    const list = await queryTenantList();
    expect(list.data.items.map((tenant) => tenant.tenant_id)).toEqual(['tenant-a', 'tenant-b']);
    expect(list.data.can_create_tenant).toBe(true);

    const request = {
      tenant_id: 'tenant-test',
      display_name: '测试租户',
      description: 'Vitest tenant',
      extension_mode: 'future',
    };
    expect((await createTenant(request)).data.replayed).toBe(false);
    expect((await createTenant(request)).data.replayed).toBe(true);
    expect((await queryTenant('tenant-test')).data.tenant.display_name).toBe('测试租户');
    await expect(createTenant({ ...request, display_name: '另一租户' })).rejects.toMatchObject({
      status: 409,
      code: 'TENANT_ID_REUSED',
    });
  });

  it('keeps Project, Artifact, Playground and Snapshot queries tenant-scoped', async () => {
    const projects = await queryProjectList({ tenant_id: 'tenant-a', page_size: 100 });
    expect(projects.data.items).toHaveLength(2);

    const storageVolumes = await queryStorageVolumeList({
      tenant_id: 'tenant-a',
      page_size: 100,
    });
    expect(storageVolumes.data.items).toHaveLength(3);
    expect(storageVolumes.data.items.every((item) => item.tenant_id === 'tenant-a')).toBe(true);

    const artifacts = await queryArtifactList({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      page_size: 100,
    });
    expect(artifacts.data.items.map((artifact) => artifact.artifact_id)).toEqual([
      'road-scenes',
      'quality-reports',
    ]);
    expect(
      (await queryArtifact('tenant-a', 'project-vision', 'road-scenes')).data.artifact.default_ref,
    ).toBe('refs/heads/main');

    const playgroundPage = await queryPlaygroundList({ tenant_id: 'tenant-a', page_size: 100 });
    const playground = playgroundPage.data.items[0]!;
    expect(
      (
        await queryPlayground(
          playground.tenant_id,
          playground.project_id,
          playground.artifact_id,
          playground.playground_id,
        )
      ).data.playground.index_version.revision,
    ).toBeTruthy();

    const snapshotPage = await querySnapshotList({ tenant_id: 'tenant-a', page_size: 100 });
    const snapshot = snapshotPage.data.items[0]!;
    expect(
      (
        await querySnapshot(
          snapshot.tenant_id,
          snapshot.project_id,
          snapshot.artifact_id,
          snapshot.commit_id,
        )
      ).data.snapshot,
    ).not.toHaveProperty('snapshot_id');
  });

  it('returns a single-parent Commit graph and rejects a cursor bound to another filter', async () => {
    const graph = await queryArtifactCommitGraph('tenant-a', 'project-vision', 'road-scenes');
    expect(graph.data.graph.refs.map((ref) => ref.name)).toContain('refs/heads/experiment');
    expect(graph.data.graph.nodes.every((node) => !Array.isArray(node.parent_commit_id))).toBe(
      true,
    );
    const diff = await queryArtifactCommitDiff(
      'tenant-a',
      'project-vision',
      'road-scenes',
      'commit-main-3',
    );
    expect(diff.data.diff.base_commit?.commit_id).toBe('commit-main-2');
    expect(diff.data.diff.base_commit?.description).toContain('质量抽检');
    expect(diff.data.diff.changes.map((change) => change.change_type)).toEqual([
      'modified',
      'added',
      'deleted',
    ]);

    await expect(
      queryArtifactList({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        cursor: 'mock:artifacts:1:wrong-filter',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'CURSOR_INVALID' });
  });

  it('creates an Artifact and Playground, then commits and snapshots the result', async () => {
    const storageRequest = {
      tenant_id: 'tenant-a',
      storage_volume_id: 'volume-test-evaluation',
      display_name: '评测数据 PVC',
      edge_cluster_id: 'cluster-cn-south-1',
      region: 'cn-guangzhou',
      backend_type: 'pvc' as const,
      access_mode: 'read_write_many' as const,
      pvc_reference: { namespace: 'neoengram-test', claim_name: 'evaluation-data' },
    };
    expect((await createStorageVolume(storageRequest)).data.replayed).toBe(false);
    expect((await createStorageVolume(storageRequest)).data.replayed).toBe(true);
    expect(
      (await queryStorageVolume(storageRequest.tenant_id, storageRequest.storage_volume_id)).data
        .storage_volume.region,
    ).toBe('cn-guangzhou');

    const artifactRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'evaluation-set',
      storage_volume_id: storageRequest.storage_volume_id,
      display_name: '评测数据集',
      default_ref: 'refs/heads/main',
    };
    expect((await createArtifact(artifactRequest)).data.replayed).toBe(false);
    expect((await createArtifact(artifactRequest)).data.replayed).toBe(true);

    const playgroundRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'evaluation-set',
      playground_id: 'review',
      storage_volume_id: storageRequest.storage_volume_id,
      display_name: '发布前复核',
    };
    const createdPlayground = await createPlayground(playgroundRequest);
    expect(createdPlayground.data.playground.region).toBe('cn-guangzhou');
    expect(createdPlayground.data.playground.head_commit_id).toBeUndefined();

    const commitRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'evaluation-set',
      playground_id: 'review',
      commit_request_id: 'commit-request-test',
      expected_index_version: createdPlayground.data.playground.index_version,
      message: '建立评测基线',
      description: '记录评测集初始导入范围和质量检查结果。',
      tag_names: ['baseline', 'evaluation/v1'],
    };
    const committed = await commitPlayground(commitRequest);
    expect(committed.data.replayed).toBe(false);
    expect(committed.data.commit.description).toContain('初始导入范围');
    expect(committed.data.commit.ref_names).toContain('refs/tags/baseline');
    expect((await commitPlayground(commitRequest)).data.replayed).toBe(true);
    expect(
      (await queryArtifactCommitGraph('tenant-a', 'project-vision', 'evaluation-set')).data.graph
        .refs[0]?.commit_id,
    ).toBe(committed.data.commit.commit_id);
    const rootDiff = await queryArtifactCommitDiff(
      'tenant-a',
      'project-vision',
      'evaluation-set',
      committed.data.commit.commit_id,
    );
    expect(rootDiff.data.diff.base_commit).toBeUndefined();
    expect(rootDiff.data.diff.summary.files_added).toBe('2');

    await expect(
      commitPlayground({
        ...commitRequest,
        commit_request_id: 'commit-request-duplicate-tag',
        message: '重复使用 Tag',
        tag_names: ['baseline'],
      }),
    ).rejects.toMatchObject({ status: 409, code: 'TAG_ALREADY_EXISTS' });

    const snapshotRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'evaluation-set',
      commit_id: committed.data.commit.commit_id,
      storage_volume_id: storageRequest.storage_volume_id,
    };
    expect((await createSnapshot(snapshotRequest)).data.replayed).toBe(false);
    expect((await createSnapshot(snapshotRequest)).data.replayed).toBe(true);
    expect(
      (
        await querySnapshot(
          snapshotRequest.tenant_id,
          snapshotRequest.project_id,
          snapshotRequest.artifact_id,
          snapshotRequest.commit_id,
        )
      ).data.snapshot.message,
    ).toBe('建立评测基线');
    expect(
      (
        await querySnapshot(
          snapshotRequest.tenant_id,
          snapshotRequest.project_id,
          snapshotRequest.artifact_id,
          snapshotRequest.commit_id,
        )
      ).data.snapshot.region,
    ).toBe('cn-guangzhou');
  });

  it('uses opaque cursors to continue the same filtered resource query', async () => {
    const first = await queryArtifactList({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      page_size: 1,
    });
    expect(first.data.items).toHaveLength(1);
    const nextCursor = first.data.next_cursor;
    expect(nextCursor).toBeTruthy();
    if (!nextCursor) throw new Error('expected a cursor for the second Artifact page');

    const second = await queryArtifactList({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      page_size: 1,
      cursor: nextCursor,
    });
    expect(second.data.items).toHaveLength(1);
    expect(second.data.items[0]?.artifact_id).not.toBe(first.data.items[0]?.artifact_id);
  });

  it('hides resources outside the visible tenant set', async () => {
    await expect(queryTenant('tenant-secret')).rejects.toMatchObject({
      status: 404,
      code: 'TENANT_NOT_FOUND',
    });
    await expect(queryArtifact('tenant-a', 'project-vision', 'missing')).rejects.toMatchObject({
      status: 404,
      code: 'ARTIFACT_NOT_FOUND',
    });
  });
});
