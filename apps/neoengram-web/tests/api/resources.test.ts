import { describe, expect, it } from 'vitest';

import {
  createTenant,
  queryArtifact,
  queryArtifactCommitGraph,
  queryArtifactList,
  queryPlayground,
  queryPlaygroundList,
  queryProjectList,
  querySnapshot,
  querySnapshotList,
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

    await expect(
      queryArtifactList({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        cursor: 'mock:artifacts:1:wrong-filter',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'CURSOR_INVALID' });
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
