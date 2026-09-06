import { describe, expect, it } from 'vitest';

import {
  cancelTask,
  cancelWorkspacePreCommit,
  commitWorkspace,
  createArtifact,
  createWorkspace,
  createSnapshot,
  createStorageVolume,
  createTenant,
  queryArtifact,
  queryArtifactCommitDiff,
  queryArtifactCommitGraph,
  queryArtifactList,
  queryCommitAvailabilityV2,
  queryCommitCoverage,
  queryGatewayPoolList,
  queryWorkspace,
  queryWorkspaceChangeList,
  queryWorkspaceDatasetProfile,
  queryWorkspaceFileList,
  queryWorkspaceFileMetadata,
  queryWorkspaceList,
  queryWorkspacePreCommit,
  queryProjectList,
  querySnapshot,
  querySnapshotDelivery,
  querySnapshotActivityList,
  querySnapshotDatasetProfile,
  querySnapshotFileList,
  querySnapshotList,
  queryStorageVolume,
  queryStorageVolumeList,
  queryTenant,
  queryTenantList,
  materializeCommit,
  queryTaskList,
  restartWorkspacePreCommit,
  retryTask,
  retrySnapshotDelivery,
  startWorkspacePreCommit,
} from '@/api/operations';
import type { PreCommitView } from '@/api/types';
import {
  artifacts,
  commitGraphs,
  gatewayPools,
  mockCommitIds,
  workspaces,
  snapshots,
  storageVolumes,
} from '@/mocks/data';

async function waitForPreCommitTerminal(
  tenantId: string,
  precommitId: string,
): Promise<PreCommitView> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const precommit = (await queryWorkspacePreCommit(tenantId, precommitId)).data.precommit;
    if (precommit.state !== 'running') return precommit;
  }
  throw new Error(`Pre-commit ${precommitId} did not reach a terminal state`);
}

describe('tenant-scoped public resource operations', () => {
  it('keeps every resource-browser Commit identity canonical', () => {
    const commitIds = [
      ...Object.values(mockCommitIds),
      ...artifacts.flatMap((artifact) => [
        artifact.head_commit_id,
        artifact.initialization.mode === 'derived'
          ? artifact.initialization.source_commit_id
          : undefined,
      ]),
      ...[...commitGraphs.values()].flatMap((graph) => [
        graph.head_commit_id,
        ...graph.nodes.flatMap((node) => [node.commit_id, node.parent_commit_id]),
      ]),
      ...workspaces.flatMap((workspace) => [workspace.base_commit_id, workspace.head_commit_id]),
      ...snapshots.map((snapshot) => snapshot.commit_id),
    ].filter((commitId): commitId is string => typeof commitId === 'string');

    expect(commitIds.length).toBeGreaterThan(0);
    expect(commitIds.every((commitId) => /^[0-9a-f]{64}$/.test(commitId))).toBe(true);
  });

  it('queries authorized tenants and creates a replayable Tenant', async () => {
    const list = await queryTenantList();
    expect(list.data.items.map((tenant) => tenant.tenant_id)).toEqual(['tenant-a', 'tenant-b']);
    expect(list.data.can_create_tenant).toBe(true);

    const request = {
      tenant_id: 'tenant-test',
      display_name: '测试租户',
      description: 'Vitest tenant',
    };
    expect((await createTenant(request)).data.request_replayed).toBe(false);
    expect((await createTenant(request)).data.request_replayed).toBe(true);
    expect((await queryTenant('tenant-test')).data.tenant.display_name).toBe('测试租户');
    await expect(createTenant({ ...request, display_name: '另一租户' })).rejects.toMatchObject({
      status: 409,
      code: 'TENANT_ID_REUSED',
    });
  });

  it('creates empty Artifacts without a Project catalog and rejects derived initialization', async () => {
    const emptyRequest = {
      tenant_id: 'tenant-a',
      project_id: 'typed-project-without-catalog',
      artifact_id: 'authoritative-empty',
      display_name: '权威空数据集',
      initialization: { mode: 'empty' as const },
    };
    const created = await createArtifact(emptyRequest);
    expect(created.data).toMatchObject({
      request_replayed: false,
      execution_reused: false,
      artifact: {
        project_id: 'typed-project-without-catalog',
        artifact_id: 'authoritative-empty',
        initialization: { mode: 'empty' },
      },
    });
    expect(created.data.artifact).not.toHaveProperty('head_commit_id');
    expect((await createArtifact(emptyRequest)).data.request_replayed).toBe(true);
    await expect(
      createArtifact({ ...emptyRequest, project_id: 'another-project' }),
    ).rejects.toMatchObject({ status: 409, code: 'ARTIFACT_ID_REUSED' });

    await expect(
      createArtifact({
        ...emptyRequest,
        artifact_id: 'unsupported-derived',
        initialization: {
          mode: 'derived' as const,
          source_project_id: 'project-vision',
          source_artifact_id: 'road-scenes',
          source_commit_id: 'b'.repeat(64),
        },
      }),
    ).rejects.toMatchObject({
      status: 409,
      code: 'ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED',
    });
  });

  it('queries the GatewayPool inventory used by the storage hierarchy', async () => {
    const pools = await queryGatewayPoolList({ page_size: 255 });
    expect(pools.data.items).toEqual(gatewayPools);
  });

  it('materializes a Commit through the mock cluster route and publishes its Coverage', async () => {
    const request = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      commit_id: mockCommitIds.roadMain3,
      target_storage_volume_id: 'volume-beijing-language',
      purpose: 'copy' as const,
      object_namespace_id: 'road-scenes',
      coverage_goal: 'complete' as const,
      request_id: 'materialize-road-main-to-beijing',
    };
    const created = await materializeCommit(request);
    expect(created.data).toMatchObject({
      request_replayed: false,
      execution_reused: false,
      materialization: {
        state: 'queued',
        target_storage_volume_id: request.target_storage_volume_id,
      },
      task: { intent_kind: 'commit.materialize', state: 'queued' },
    });
    expect((await materializeCommit(request)).data.request_replayed).toBe(true);

    let state = created.data.task!.state;
    for (let query = 0; query < 4 && state !== 'succeeded'; query += 1) {
      const tasks = await queryTaskList({
        tenant_id: request.tenant_id,
        commit_id: request.commit_id,
        object_namespace_id: request.artifact_id,
        intent_kind: ['commit.materialize'],
      });
      state = tasks.data.items[0]!.state;
    }
    expect(state).toBe('succeeded');

    const placements = await queryCommitCoverage({
      tenant_id: request.tenant_id,
      commit_id: request.commit_id,
      object_namespace_id: request.artifact_id,
    });
    expect(placements.data.coverage).toContainEqual(
      expect.objectContaining({
        storage_volume_id: request.target_storage_volume_id,
        state: 'complete',
      }),
    );
    const availability = await queryCommitAvailabilityV2({
      tenant_id: request.tenant_id,
      commit_id: request.commit_id,
      object_namespace_id: request.artifact_id,
    });
    expect(availability.data.availability.verified_storage_volume_ids).toContain(
      request.target_storage_volume_id,
    );

    await expect(
      materializeCommit({
        ...request,
        project_id: 'project-language',
        request_id: 'materialize-commit-from-wrong-artifact',
      }),
    ).rejects.toMatchObject({ status: 404 });
  });

  it('cancels one immutable Commit task and preserves terminal cancellation', async () => {
    const created = await materializeCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      commit_id: mockCommitIds.roadMain2,
      target_storage_volume_id: 'volume-guangzhou-delivery',
      purpose: 'copy' as const,
      object_namespace_id: 'road-scenes',
      coverage_goal: 'complete',
      request_id: 'materialize-road-main-to-guangzhou',
    });
    const cancelled = await cancelTask({
      tenant_id: 'tenant-a',
      task_id: created.data.task!.task_id,
    });
    expect(cancelled.data.task.state).toBe('cancelling');

    await expect(
      retryTask({
        tenant_id: 'tenant-a',
        task_id: created.data.task!.task_id,
      }),
    ).rejects.toMatchObject({ status: 409, code: 'TASK_NOT_RETRYABLE' });
  });

  it('keeps Project, Artifact, Workspace and Snapshot queries tenant-scoped', async () => {
    const projects = await queryProjectList({ tenant_id: 'tenant-a', page_size: 100 });
    expect(projects.data.items).toHaveLength(2);

    const storageVolumes = await queryStorageVolumeList({
      tenant_id: 'tenant-a',
      page_size: 100,
    });
    expect(storageVolumes.data.items).toHaveLength(4);
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
      (await queryArtifact('tenant-a', 'project-vision', 'road-scenes')).data.artifact,
    ).toMatchObject({
      initialization: { mode: 'empty' },
      head_commit_id: mockCommitIds.roadMain3,
    });

    const workspacePage = await queryWorkspaceList({ tenant_id: 'tenant-a', page_size: 100 });
    const workspace = workspacePage.data.items[0]!;
    expect(
      (
        await queryWorkspace(
          workspace.tenant_id,
          workspace.project_id,
          workspace.artifact_id,
          workspace.workspace_id,
        )
      ).data.workspace.index_version.revision,
    ).toBeTruthy();

    const snapshotPage = await querySnapshotList({ tenant_id: 'tenant-a', page_size: 100 });
    const snapshot = snapshotPage.data.items[0]!;
    expect(
      (await querySnapshot(snapshot.tenant_id, snapshot.snapshot_id)).data.snapshot,
    ).toMatchObject({ snapshot_id: snapshot.snapshot_id, commit_id: snapshot.commit_id });
    const logicalSnapshots = snapshotPage.data.items.filter(
      (item) => item.artifact_id === 'road-scenes' && item.commit_id === mockCommitIds.roadMain3,
    );
    expect(logicalSnapshots).toHaveLength(2);
    expect(
      logicalSnapshots.every(
        (item) =>
          item.delivery_id && item.edge_cluster_id && item.storage_volume_id && item.delivery_mode,
      ),
    ).toBe(true);
  });

  it('rejects new placement on a non-Ready StorageVolume', async () => {
    await expect(
      createWorkspace({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'road-scenes',
        workspace_id: 'degraded-placement',
        storage_volume_id: 'volume-shanghai-archive',
        display_name: '不可用放置测试',
        base_commit_id: mockCommitIds.roadMain3,
      }),
    ).rejects.toMatchObject({ status: 409, code: 'STORAGE_VOLUME_UNAVAILABLE' });

    const unavailableVolume = storageVolumes.find(
      (volume) => volume.storage_volume_id === 'volume-shanghai-archive',
    );
    if (!unavailableVolume) throw new Error('expected mock StorageVolume');
    unavailableVolume.state = 'unavailable';

    await expect(
      createWorkspace({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'road-scenes',
        workspace_id: 'unavailable-placement',
        storage_volume_id: unavailableVolume.storage_volume_id,
        display_name: '不可用放置测试',
        base_commit_id: mockCommitIds.roadMain3,
      }),
    ).rejects.toMatchObject({ status: 409, code: 'STORAGE_VOLUME_UNAVAILABLE' });
  });

  it('derives a Workspace from a selected historical Commit or the current Head', async () => {
    const artifact = artifacts.find(
      (item) =>
        item.tenant_id === 'tenant-a' &&
        item.project_id === 'project-vision' &&
        item.artifact_id === 'road-scenes',
    );
    if (!artifact) throw new Error('expected mock Artifact');
    const originalHeadCommitId = artifact.head_commit_id;

    await expect(
      createWorkspace({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'road-scenes',
        workspace_id: 'unknown-base',
        storage_volume_id: 'volume-shanghai-vision',
        display_name: '未知基线',
        base_commit_id: 'f'.repeat(64),
      }),
    ).rejects.toMatchObject({
      status: 404,
      code: 'COMMIT_NOT_FOUND',
    });

    const historical = await createWorkspace({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'historical-base',
      storage_volume_id: 'volume-shanghai-vision',
      display_name: '历史基线',
      base_commit_id: mockCommitIds.roadMain2,
    });
    expect(historical.data.workspace).toMatchObject({
      base_commit_id: mockCommitIds.roadMain2,
      head_commit_id: mockCommitIds.roadMain2,
      index_version: { revision: '18', digest: mockCommitIds.roadMain2 },
    });

    const inheritedRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'current-head',
      storage_volume_id: 'volume-shanghai-vision',
      display_name: '当前基线',
    };
    const inherited = await createWorkspace(inheritedRequest);
    expect(inherited.data.workspace).toMatchObject({
      base_commit_id: originalHeadCommitId,
      head_commit_id: originalHeadCommitId,
      index_version: { revision: '18', digest: originalHeadCommitId },
    });

    artifact.head_commit_id = mockCommitIds.roadMain2;
    const request_replayed = await createWorkspace(inheritedRequest);
    expect(request_replayed.data.request_replayed).toBe(true);
    expect(request_replayed.data.workspace.base_commit_id).toBe(originalHeadCommitId);
  });

  it('rejects Commit when the Workspace Head changed after Pre-commit start', async () => {
    const workspace = (
      await queryWorkspace('tenant-a', 'project-vision', 'road-scenes', 'labeling')
    ).data.workspace;
    const started = await startWorkspacePreCommit({
      tenant_id: workspace.tenant_id,
      project_id: workspace.project_id,
      artifact_id: workspace.artifact_id,
      workspace_id: workspace.workspace_id,
      precommit_request_id: 'precommit-head-conflict',
      expected_index_version: workspace.index_version,
      data_layout: 'fast_cdc' as const,
    });
    const ready = await waitForPreCommitTerminal(
      workspace.tenant_id,
      started.data.precommit.precommit_id,
    );
    if (!ready.candidate_index_version) throw new Error('expected candidate IndexVersion');
    const stored = workspaces.find(
      (item) =>
        item.tenant_id === workspace.tenant_id &&
        item.project_id === workspace.project_id &&
        item.artifact_id === workspace.artifact_id &&
        item.workspace_id === workspace.workspace_id,
    );
    if (!stored) throw new Error('expected mock Workspace');
    stored.head_commit_id = mockCommitIds.roadMain2;

    await expect(
      commitWorkspace({
        tenant_id: workspace.tenant_id,
        project_id: workspace.project_id,
        artifact_id: workspace.artifact_id,
        workspace_id: workspace.workspace_id,
        commit_request_id: 'commit-head-conflict',
        precommit_id: ready.precommit_id,
        expected_candidate_index_version: ready.candidate_index_version,
        data_layout: 'fast_cdc',
        message: '此提交必须被 CAS 拒绝',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'HEAD_COMMIT_CONFLICT' });
  });

  it('returns a single-parent Commit graph and rejects a cursor bound to another filter', async () => {
    const graph = await queryArtifactCommitGraph('tenant-a', 'project-vision', 'road-scenes');
    expect(graph.data.graph.head_commit_id).toBe(mockCommitIds.roadMain3);
    expect(graph.data.graph.nodes.flatMap((node) => node.tag_names)).toContain(
      'occlusion-experiment',
    );
    expect(graph.data.graph.nodes.every((node) => !Array.isArray(node.parent_commit_id))).toBe(
      true,
    );
    const diff = await queryArtifactCommitDiff(
      'tenant-a',
      'project-vision',
      'road-scenes',
      mockCommitIds.roadMain3,
    );
    expect(diff.data.diff.base_commit?.commit_id).toBe(mockCommitIds.roadMain2);
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

  it('creates empty Artifacts and fixed-target Snapshots while enforcing Workspace readiness', async () => {
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
    expect((await createStorageVolume(storageRequest)).data).toMatchObject({
      request_replayed: false,
      execution_reused: false,
      storage_volume: { state: 'unavailable' },
    });
    expect((await createStorageVolume(storageRequest)).data.request_replayed).toBe(true);
    expect(
      (await queryStorageVolume(storageRequest.tenant_id, storageRequest.storage_volume_id)).data
        .storage_volume.region,
    ).toBe('cn-guangzhou');

    const artifactRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'evaluation-set',
      display_name: '评测数据集',
      initialization: { mode: 'empty' as const },
    };
    const createdArtifact = await createArtifact(artifactRequest);
    expect(createdArtifact.data.request_replayed).toBe(false);
    expect(createdArtifact.data.artifact).toMatchObject({ initialization: { mode: 'empty' } });
    expect(createdArtifact.data.artifact).not.toHaveProperty('storage_volume_id');
    expect(createdArtifact.data.artifact).not.toHaveProperty('head_commit_id');
    expect((await createArtifact(artifactRequest)).data.request_replayed).toBe(true);
    const emptyGraph = await queryArtifactCommitGraph(
      'tenant-a',
      'project-vision',
      'evaluation-set',
    );
    expect(emptyGraph.data.graph.nodes).toHaveLength(0);
    expect(emptyGraph.data.graph.head_commit_id).toBeUndefined();

    const workspaceRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'evaluation-set',
      workspace_id: 'review',
      storage_volume_id: 'volume-guangzhou-delivery',
      display_name: '发布前复核',
    };
    const createdWorkspace = await createWorkspace(workspaceRequest);
    expect(createdWorkspace.data.workspace.region).toBe('cn-guangzhou');
    expect(createdWorkspace.data.workspace.state).toBe('creating');
    expect(createdWorkspace.data.workspace).not.toHaveProperty('base_commit_id');
    expect(createdWorkspace.data.workspace).not.toHaveProperty('head_commit_id');

    await expect(
      startWorkspacePreCommit({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'evaluation-set',
        workspace_id: 'review',
        precommit_request_id: 'precommit-evaluation-not-ready',
        expected_index_version: createdWorkspace.data.workspace.index_version,
        data_layout: 'fast_cdc',
      }),
    ).rejects.toMatchObject({
      status: 409,
      code: 'WORKSPACE_NOT_READY',
    });
    expect(
      (
        await queryWorkspace(
          workspaceRequest.tenant_id,
          workspaceRequest.project_id,
          workspaceRequest.artifact_id,
          workspaceRequest.workspace_id,
        )
      ).data.workspace.state,
    ).toBe('creating');
    expect(
      (
        await queryWorkspace(
          workspaceRequest.tenant_id,
          workspaceRequest.project_id,
          workspaceRequest.artifact_id,
          workspaceRequest.workspace_id,
        )
      ).data.workspace.state,
    ).toBe('ready');

    const readyWorkspace = (
      await queryWorkspace('tenant-a', 'project-vision', 'road-scenes', 'labeling')
    ).data.workspace;
    const startedPreCommit = await startWorkspacePreCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'labeling',
      precommit_request_id: 'precommit-request-test',
      expected_index_version: readyWorkspace.index_version,
      data_layout: 'fast_cdc',
    });
    expect(startedPreCommit.data.precommit.state).toBe('running');
    const readyPreCommit = await waitForPreCommitTerminal(
      'tenant-a',
      startedPreCommit.data.precommit.precommit_id,
    );
    expect(readyPreCommit.state).toBe('ready');
    expect(readyPreCommit.phase).toBe('idle');
    if (!readyPreCommit.candidate_index_version) throw new Error('expected candidate IndexVersion');
    const readyCommitRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'labeling',
      commit_request_id: 'commit-request-test',
      precommit_id: readyPreCommit.precommit_id,
      expected_candidate_index_version: readyPreCommit.candidate_index_version,
      data_layout: 'fast_cdc' as const,
      message: '建立跨区域评测基线',
      description: '记录评测集初始导入范围和质量检查结果。',
      tag_names: ['test-baseline', 'test-evaluation/v1'],
    };
    const committed = await commitWorkspace(readyCommitRequest);
    expect(committed.data.request_replayed).toBe(false);
    expect(committed.data.commit.commit_id).toMatch(/^[0-9a-f]{64}$/);
    expect(committed.data.commit.description).toContain('初始导入范围');
    expect(committed.data.commit.tag_names).toContain('test-baseline');
    expect(committed.data.consumed_precommit.state).toBe('committed');
    expect((await commitWorkspace(readyCommitRequest)).data.request_replayed).toBe(true);
    expect(
      (await queryArtifactCommitGraph('tenant-a', 'project-vision', 'road-scenes')).data.graph
        .head_commit_id,
    ).toBe(committed.data.commit.commit_id);
    const commitDiff = await queryArtifactCommitDiff(
      'tenant-a',
      'project-vision',
      'road-scenes',
      committed.data.commit.commit_id,
    );
    expect(commitDiff.data.diff.base_commit?.commit_id).toBe(mockCommitIds.roadMain3);
    expect(commitDiff.data.diff.summary.files_added).toBe('1');

    const duplicateStarted = await startWorkspacePreCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'labeling',
      precommit_request_id: 'precommit-duplicate-tag',
      expected_index_version: readyWorkspace.index_version,
      data_layout: 'fast_cdc',
    });
    const duplicateReady = await waitForPreCommitTerminal(
      'tenant-a',
      duplicateStarted.data.precommit.precommit_id,
    );
    if (!duplicateReady.candidate_index_version) throw new Error('expected duplicate candidate');
    await expect(
      commitWorkspace({
        ...readyCommitRequest,
        commit_request_id: 'commit-request-duplicate-tag',
        precommit_id: duplicateReady.precommit_id,
        expected_candidate_index_version: duplicateReady.candidate_index_version,
        data_layout: 'fast_cdc' as const,
        message: '重复使用 Tag',
        tag_names: ['test-baseline'],
      }),
    ).rejects.toMatchObject({ status: 409, code: 'TAG_ALREADY_EXISTS' });

    const snapshotRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      commit_id: committed.data.commit.commit_id,
      target_edge_cluster_id: 'cluster-cn-south-1',
      target_storage_volume_id: 'volume-guangzhou-delivery',
      delivery_mode: 'copy' as const,
      request_id: 'snapshot-request-evaluation-guangzhou',
    };
    const firstSnapshot = await createSnapshot(snapshotRequest);
    expect(firstSnapshot.data.request_replayed).toBe(false);
    expect(firstSnapshot.data.snapshot).toMatchObject({
      state: 'creating',
      edge_cluster_id: 'cluster-cn-south-1',
      storage_volume_id: 'volume-guangzhou-delivery',
      delivery_mode: 'copy',
      integrity: { state: 'pending' },
    });
    expect(typeof firstSnapshot.data.snapshot.delivery_id).toBe('string');
    expect((await createSnapshot(snapshotRequest)).data.request_replayed).toBe(true);
    await expect(
      createSnapshot({
        ...snapshotRequest,
        target_edge_cluster_id: 'cluster-cn-east-1',
        target_storage_volume_id: 'volume-shanghai-vision',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'SNAPSHOT_REQUEST_ID_REUSED' });
    const sameTargetSnapshot = await createSnapshot({
      ...snapshotRequest,
      request_id: 'snapshot-request-evaluation-guangzhou-reused',
    });
    expect(sameTargetSnapshot.data.request_replayed).toBe(false);
    expect(sameTargetSnapshot.data.snapshot.snapshot_id).not.toBe(
      firstSnapshot.data.snapshot.snapshot_id,
    );
    expect(sameTargetSnapshot.data.snapshot.delivery_id).not.toBe(
      firstSnapshot.data.snapshot.delivery_id,
    );
    expect(sameTargetSnapshot.data.snapshot.storage_volume_id).toBe(
      snapshotRequest.target_storage_volume_id,
    );
    await querySnapshot(snapshotRequest.tenant_id, firstSnapshot.data.snapshot.snapshot_id);
    const readySnapshot = (
      await querySnapshot(snapshotRequest.tenant_id, firstSnapshot.data.snapshot.snapshot_id)
    ).data.snapshot;
    expect(readySnapshot).toMatchObject({ state: 'ready', integrity: { state: 'verified' } });

    const otherTargetSnapshot = await createSnapshot({
      ...snapshotRequest,
      target_edge_cluster_id: 'cluster-cn-east-1',
      target_storage_volume_id: 'volume-shanghai-vision',
      request_id: 'snapshot-request-evaluation-shanghai',
    });
    expect(otherTargetSnapshot.data.request_replayed).toBe(false);
    expect(otherTargetSnapshot.data.snapshot.snapshot_id).not.toBe(
      firstSnapshot.data.snapshot.snapshot_id,
    );
    expect(otherTargetSnapshot.data.snapshot.storage_volume_id).toBe('volume-shanghai-vision');
    expect(otherTargetSnapshot.data.snapshot.edge_cluster_id).toBe('cluster-cn-east-1');
    expect(otherTargetSnapshot.data.snapshot.delivery_id).not.toBe(
      firstSnapshot.data.snapshot.delivery_id,
    );
  });

  it('drives Pre-commit states and returns paginated logical metadata', async () => {
    const abnormalWorkspace = (
      await queryWorkspace('tenant-a', 'project-vision', 'road-scenes', 'occlusion-audit')
    ).data.workspace;
    const failing = await startWorkspacePreCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'occlusion-audit',
      precommit_request_id: 'precommit-fail-validation',
      expected_index_version: abnormalWorkspace.index_version,
      data_layout: 'fast_cdc',
    });
    expect(failing.data.precommit.state).toBe('running');
    const abnormal = (
      await queryWorkspacePreCommit('tenant-a', failing.data.precommit.precommit_id)
    ).data.precommit;
    expect(abnormal.state).toBe('abnormal');
    expect(abnormal.phase).toBe('idle');
    expect(abnormal.blockers).toHaveLength(1);

    const restarted = await restartWorkspacePreCommit({
      tenant_id: 'tenant-a',
      precommit_id: abnormal.precommit_id,
      restart_request_id: 'restart-precommit-failure-01',
      expected_index_version: abnormalWorkspace.index_version,
    });
    expect(restarted.data.precommit).toMatchObject({ state: 'running', attempt: 2 });
    const cancelled = await cancelWorkspacePreCommit({
      tenant_id: 'tenant-a',
      precommit_id: abnormal.precommit_id,
      cancel_request_id: 'cancel-precommit-failure-01',
    });
    expect(cancelled.data.precommit.state).toBe('cancelled');

    const restartedCancelled = await restartWorkspacePreCommit({
      tenant_id: 'tenant-a',
      precommit_id: cancelled.data.precommit.precommit_id,
      restart_request_id: 'restart-precommit-cancelled-01',
      expected_index_version: abnormalWorkspace.index_version,
    });
    expect(restartedCancelled.data.precommit).toMatchObject({
      precommit_id: cancelled.data.precommit.precommit_id,
      state: 'running',
      phase: 'queued',
      attempt: 3,
    });

    const workspace = (
      await queryWorkspace('tenant-a', 'project-vision', 'road-scenes', 'labeling')
    ).data.workspace;
    const started = await startWorkspacePreCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'labeling',
      precommit_request_id: 'precommit-metadata-ready',
      expected_index_version: workspace.index_version,
      data_layout: 'fast_cdc',
    });
    const observedPhases = [started.data.precommit.phase];
    let ready = started.data.precommit;
    while (ready.state === 'running') {
      ready = (await queryWorkspacePreCommit('tenant-a', started.data.precommit.precommit_id)).data
        .precommit;
      observedPhases.push(ready.phase);
    }
    expect(observedPhases).toEqual([
      'queued',
      'scanning',
      'hashing',
      'uploading',
      'validating',
      'idle',
    ]);
    expect(ready.state).toBe('ready');
    expect(ready.phase).toBe('idle');

    const files = await queryWorkspaceFileList({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'labeling',
      page_size: 1,
    });
    expect(files.data.items).toHaveLength(1);
    expect(files.data.next_cursor).toBeTruthy();
    const changes = await queryWorkspaceChangeList({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'labeling',
      precommit_id: ready.precommit_id,
      page_size: 100,
    });
    expect(changes.data.source).toBe('precommit');
    expect(changes.data.items.map((item) => item.change_type)).toEqual([
      'modified',
      'added',
      'renamed',
      'deleted',
    ]);
    const metadata = await queryWorkspaceFileMetadata({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'labeling',
      path: 'dataset/night-rain/part-0042.parquet',
    });
    expect(metadata.data.metadata).toMatchObject({ format: 'parquet', row_count: '12842731' });
    const profile = await queryWorkspaceDatasetProfile({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      workspace_id: 'labeling',
    });
    expect(profile.data.profile.state).toBe('ready');
  });

  it("retries the Snapshot's unique Delivery and gates file browsing on delivery readiness", async () => {
    const retry = await retrySnapshotDelivery({
      tenant_id: 'tenant-a',
      delivery_id: 'delivery-road-main2-sha-01',
      request_id: 'retry-snapshot-main3-01',
    });
    expect(retry.data.delivery).toMatchObject({ state: 'requested' });
    expect(
      (
        await retrySnapshotDelivery({
          tenant_id: 'tenant-a',
          delivery_id: 'delivery-road-main2-sha-01',
          request_id: 'retry-snapshot-main3-01',
        })
      ).data.request_replayed,
    ).toBe(true);

    const firstRetryQuery = await querySnapshotDelivery({
      tenant_id: 'tenant-a',
      delivery_id: 'delivery-road-main2-sha-01',
    });
    expect(firstRetryQuery.data.delivery).toMatchObject({ state: 'requested' });
    const retriedSnapshot = await querySnapshot('tenant-a', 'snap-road-main2-sha-01');
    expect(retriedSnapshot.data.snapshot).toMatchObject({
      state: 'creating',
      integrity: { state: 'pending' },
    });
    await expect(
      querySnapshotFileList({
        tenant_id: 'tenant-a',
        snapshot_id: 'snap-road-main2-sha-01',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'SNAPSHOT_UNAVAILABLE' });
    const unchangedSnapshot = await querySnapshot('tenant-a', 'snap-road-main3-sha-01');
    expect(unchangedSnapshot.data.snapshot).toMatchObject({
      state: 'ready',
      integrity: { state: 'verified' },
    });

    const readyFiles = await querySnapshotFileList({
      tenant_id: 'tenant-a',
      snapshot_id: 'snap-road-main3-sha-01',
      page_size: 100,
    });
    expect(readyFiles.data.items[0]?.path).toBe('dataset/night-rain/part-0042.parquet');
    await expect(
      querySnapshotFileList({
        tenant_id: 'tenant-a',
        snapshot_id: 'snap-road-main3-gz-01',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'SNAPSHOT_UNAVAILABLE' });
    const activities = await querySnapshotActivityList({
      tenant_id: 'tenant-a',
      snapshot_id: 'snap-road-main3-sha-01',
      page_size: 100,
    });
    expect(activities.data.items.map((item) => item.activity_type)).toContain('ready');
    const profile = await querySnapshotDatasetProfile({
      tenant_id: 'tenant-a',
      snapshot_id: 'snap-road-main3-sha-01',
    });
    expect(profile.data.profile.summary?.logical_file_count).toBe('18554');
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
