import { describe, expect, it } from 'vitest';

import {
  cancelPlaygroundPreCommit,
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
  queryPlaygroundChangeList,
  queryPlaygroundDatasetProfile,
  queryPlaygroundFileList,
  queryPlaygroundFileMetadata,
  queryPlaygroundList,
  queryPlaygroundPreCommit,
  queryProjectList,
  querySnapshot,
  querySnapshotActivityList,
  querySnapshotDatasetProfile,
  querySnapshotFileList,
  querySnapshotList,
  queryStorageVolume,
  queryStorageVolumeList,
  queryTenant,
  queryTenantList,
  restartPlaygroundPreCommit,
  retrySnapshotDelivery,
  startPlaygroundPreCommit,
} from '@/api/operations';
import type { PreCommitView } from '@/api/types';
import { playgrounds, storageVolumes } from '@/mocks/data';

async function waitForPreCommitTerminal(
  tenantId: string,
  precommitId: string,
): Promise<PreCommitView> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const precommit = (await queryPlaygroundPreCommit(tenantId, precommitId)).data.precommit;
    if (precommit.state !== 'running') return precommit;
  }
  throw new Error(`Pre-commit ${precommitId} did not reach a terminal state`);
}

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
    ).toMatchObject({ initialization: { mode: 'empty' }, head_commit_id: 'commit-main-3' });

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
      (await querySnapshot(snapshot.tenant_id, snapshot.snapshot_id)).data.snapshot,
    ).toMatchObject({ snapshot_id: snapshot.snapshot_id, commit_id: snapshot.commit_id });
    const regionalCopies = snapshotPage.data.items.filter(
      (item) => item.artifact_id === 'road-scenes' && item.commit_id === 'commit-main-3',
    );
    expect(regionalCopies).toHaveLength(2);
    expect(new Set(regionalCopies.map((item) => item.region))).toEqual(
      new Set(['cn-shanghai', 'cn-guangzhou']),
    );
  });

  it('rejects new placement on a non-Ready StorageVolume', async () => {
    await expect(
      createPlayground({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'road-scenes',
        playground_id: 'degraded-placement',
        storage_volume_id: 'volume-shanghai-archive',
        display_name: '不可用放置测试',
        base_commit_id: 'commit-main-3',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'STORAGE_VOLUME_UNAVAILABLE' });

    await expect(
      createSnapshot({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'road-scenes',
        commit_id: 'commit-main-2',
        storage_volume_id: 'volume-shanghai-archive',
        snapshot_request_id: 'snapshot-degraded-placement',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'STORAGE_VOLUME_UNAVAILABLE' });

    const unavailableVolume = storageVolumes.find(
      (volume) => volume.storage_volume_id === 'volume-shanghai-archive',
    );
    if (!unavailableVolume) throw new Error('expected mock StorageVolume');
    unavailableVolume.state = 'unavailable';

    await expect(
      createPlayground({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'road-scenes',
        playground_id: 'unavailable-placement',
        storage_volume_id: unavailableVolume.storage_volume_id,
        display_name: '不可用放置测试',
        base_commit_id: 'commit-main-3',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'STORAGE_VOLUME_UNAVAILABLE' });

    await expect(
      createSnapshot({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'road-scenes',
        commit_id: 'commit-main-2',
        storage_volume_id: unavailableVolume.storage_volume_id,
        snapshot_request_id: 'snapshot-unavailable-placement',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'STORAGE_VOLUME_UNAVAILABLE' });
  });

  it('rejects Commit when the Playground Head changed after Pre-commit start', async () => {
    const playground = (
      await queryPlayground('tenant-a', 'project-vision', 'road-scenes', 'labeling')
    ).data.playground;
    const started = await startPlaygroundPreCommit({
      tenant_id: playground.tenant_id,
      project_id: playground.project_id,
      artifact_id: playground.artifact_id,
      playground_id: playground.playground_id,
      precommit_request_id: 'precommit-head-conflict',
      expected_index_version: playground.index_version,
    });
    const ready = await waitForPreCommitTerminal(
      playground.tenant_id,
      started.data.precommit.precommit_id,
    );
    if (!ready.candidate_index_version) throw new Error('expected candidate IndexVersion');
    const stored = playgrounds.find(
      (item) =>
        item.tenant_id === playground.tenant_id &&
        item.project_id === playground.project_id &&
        item.artifact_id === playground.artifact_id &&
        item.playground_id === playground.playground_id,
    );
    if (!stored) throw new Error('expected mock Playground');
    stored.head_commit_id = 'commit-main-2';

    await expect(
      commitPlayground({
        tenant_id: playground.tenant_id,
        project_id: playground.project_id,
        artifact_id: playground.artifact_id,
        playground_id: playground.playground_id,
        commit_request_id: 'commit-head-conflict',
        precommit_id: ready.precommit_id,
        expected_candidate_index_version: ready.candidate_index_version,
        message: '此提交必须被 CAS 拒绝',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'HEAD_COMMIT_CONFLICT' });
  });

  it('returns a single-parent Commit graph and rejects a cursor bound to another filter', async () => {
    const graph = await queryArtifactCommitGraph('tenant-a', 'project-vision', 'road-scenes');
    expect(graph.data.graph.head_commit_id).toBe('commit-main-3');
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

  it('creates derived Artifacts and regional Snapshots while enforcing Playground readiness', async () => {
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
      replayed: false,
      storage_volume: { state: 'unavailable' },
    });
    expect((await createStorageVolume(storageRequest)).data.replayed).toBe(true);
    expect(
      (await queryStorageVolume(storageRequest.tenant_id, storageRequest.storage_volume_id)).data
        .storage_volume.region,
    ).toBe('cn-guangzhou');

    const artifactRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'evaluation-set',
      display_name: '评测数据集',
      initialization: {
        mode: 'derived' as const,
        source_project_id: 'project-vision',
        source_artifact_id: 'road-scenes',
        source_commit_id: 'commit-main-2',
      },
    };
    const createdArtifact = await createArtifact(artifactRequest);
    expect(createdArtifact.data.replayed).toBe(false);
    expect(createdArtifact.data.artifact).toMatchObject({
      initialization: {
        mode: 'derived',
        source_project_id: 'project-vision',
        source_artifact_id: 'road-scenes',
        source_commit_id: 'commit-main-2',
      },
    });
    expect(createdArtifact.data.artifact).not.toHaveProperty('storage_volume_id');
    expect((await createArtifact(artifactRequest)).data.replayed).toBe(true);
    const derivedGraph = await queryArtifactCommitGraph(
      'tenant-a',
      'project-vision',
      'evaluation-set',
    );
    expect(derivedGraph.data.graph.nodes).toHaveLength(1);
    expect(derivedGraph.data.graph.nodes[0]?.parent_commit_id).toBeUndefined();
    expect(derivedGraph.data.graph.nodes[0]?.description).toContain('road-scenes@commit-main-2');

    const playgroundRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'evaluation-set',
      playground_id: 'review',
      storage_volume_id: 'volume-guangzhou-delivery',
      display_name: '发布前复核',
      base_commit_id: derivedGraph.data.graph.nodes[0]!.commit_id,
    };
    const createdPlayground = await createPlayground(playgroundRequest);
    expect(createdPlayground.data.playground.region).toBe('cn-guangzhou');
    expect(createdPlayground.data.playground.state).toBe('creating');

    await expect(
      startPlaygroundPreCommit({
        tenant_id: 'tenant-a',
        project_id: 'project-vision',
        artifact_id: 'evaluation-set',
        playground_id: 'review',
        precommit_request_id: 'precommit-evaluation-not-ready',
        expected_index_version: createdPlayground.data.playground.index_version,
      }),
    ).rejects.toMatchObject({
      status: 409,
      code: 'PLAYGROUND_NOT_READY',
    });
    expect(
      (
        await queryPlayground(
          playgroundRequest.tenant_id,
          playgroundRequest.project_id,
          playgroundRequest.artifact_id,
          playgroundRequest.playground_id,
        )
      ).data.playground.state,
    ).toBe('creating');
    expect(
      (
        await queryPlayground(
          playgroundRequest.tenant_id,
          playgroundRequest.project_id,
          playgroundRequest.artifact_id,
          playgroundRequest.playground_id,
        )
      ).data.playground.state,
    ).toBe('ready');

    const readyPlayground = (
      await queryPlayground('tenant-a', 'project-vision', 'road-scenes', 'labeling')
    ).data.playground;
    const startedPreCommit = await startPlaygroundPreCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      playground_id: 'labeling',
      precommit_request_id: 'precommit-request-test',
      expected_index_version: readyPlayground.index_version,
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
      playground_id: 'labeling',
      commit_request_id: 'commit-request-test',
      precommit_id: readyPreCommit.precommit_id,
      expected_candidate_index_version: readyPreCommit.candidate_index_version,
      message: '建立跨区域评测基线',
      description: '记录评测集初始导入范围和质量检查结果。',
      tag_names: ['test-baseline', 'test-evaluation/v1'],
    };
    const committed = await commitPlayground(readyCommitRequest);
    expect(committed.data.replayed).toBe(false);
    expect(committed.data.commit.description).toContain('初始导入范围');
    expect(committed.data.commit.tag_names).toContain('test-baseline');
    expect(committed.data.consumed_precommit.state).toBe('committed');
    expect((await commitPlayground(readyCommitRequest)).data.replayed).toBe(true);
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
    expect(commitDiff.data.diff.base_commit?.commit_id).toBe('commit-main-3');
    expect(commitDiff.data.diff.summary.files_added).toBe('1');

    const duplicateStarted = await startPlaygroundPreCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      playground_id: 'labeling',
      precommit_request_id: 'precommit-duplicate-tag',
      expected_index_version: readyPlayground.index_version,
    });
    const duplicateReady = await waitForPreCommitTerminal(
      'tenant-a',
      duplicateStarted.data.precommit.precommit_id,
    );
    if (!duplicateReady.candidate_index_version) throw new Error('expected duplicate candidate');
    await expect(
      commitPlayground({
        ...readyCommitRequest,
        commit_request_id: 'commit-request-duplicate-tag',
        precommit_id: duplicateReady.precommit_id,
        expected_candidate_index_version: duplicateReady.candidate_index_version,
        message: '重复使用 Tag',
        tag_names: ['test-baseline'],
      }),
    ).rejects.toMatchObject({ status: 409, code: 'TAG_ALREADY_EXISTS' });

    const snapshotRequest = {
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      commit_id: committed.data.commit.commit_id,
      storage_volume_id: 'volume-guangzhou-delivery',
      snapshot_request_id: 'snapshot-request-evaluation-guangzhou',
    };
    const firstSnapshot = await createSnapshot(snapshotRequest);
    expect(firstSnapshot.data.replayed).toBe(false);
    expect(firstSnapshot.data.placement_reused).toBe(false);
    expect(firstSnapshot.data.snapshot.state).toBe('creating');
    expect((await createSnapshot(snapshotRequest)).data.replayed).toBe(true);
    const reusedPlacement = await createSnapshot({
      ...snapshotRequest,
      snapshot_request_id: 'snapshot-request-evaluation-guangzhou-reused',
    });
    expect(reusedPlacement.data.replayed).toBe(false);
    expect(reusedPlacement.data.placement_reused).toBe(true);
    expect(reusedPlacement.data.snapshot.snapshot_id).toBe(firstSnapshot.data.snapshot.snapshot_id);
    const creatingSnapshot = (
      await querySnapshot(snapshotRequest.tenant_id, firstSnapshot.data.snapshot.snapshot_id)
    ).data.snapshot;
    expect(creatingSnapshot).toMatchObject({
      region: 'cn-guangzhou',
      state: 'creating',
      phase: 'materializing',
      integrity: { state: 'pending' },
    });
    const readySnapshot = (
      await querySnapshot(snapshotRequest.tenant_id, firstSnapshot.data.snapshot.snapshot_id)
    ).data.snapshot;
    expect(readySnapshot).toMatchObject({
      state: 'ready',
      phase: 'idle',
      integrity: { state: 'verified' },
    });

    const secondSnapshot = await createSnapshot({
      ...snapshotRequest,
      storage_volume_id: 'volume-shanghai-vision',
      snapshot_request_id: 'snapshot-request-evaluation-shanghai',
    });
    expect(secondSnapshot.data.replayed).toBe(false);
    expect(secondSnapshot.data.snapshot.snapshot_id).not.toBe(
      firstSnapshot.data.snapshot.snapshot_id,
    );
    expect(secondSnapshot.data.snapshot.commit_id).toBe(firstSnapshot.data.snapshot.commit_id);
  });

  it('drives Pre-commit states and returns paginated logical metadata', async () => {
    const abnormalPlayground = (
      await queryPlayground('tenant-a', 'project-vision', 'road-scenes', 'occlusion-audit')
    ).data.playground;
    const failing = await startPlaygroundPreCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      playground_id: 'occlusion-audit',
      precommit_request_id: 'precommit-fail-validation',
      expected_index_version: abnormalPlayground.index_version,
    });
    expect(failing.data.precommit.state).toBe('running');
    const abnormal = (
      await queryPlaygroundPreCommit('tenant-a', failing.data.precommit.precommit_id)
    ).data.precommit;
    expect(abnormal.state).toBe('abnormal');
    expect(abnormal.phase).toBe('idle');
    expect(abnormal.blockers).toHaveLength(1);

    const restarted = await restartPlaygroundPreCommit({
      tenant_id: 'tenant-a',
      precommit_id: abnormal.precommit_id,
      restart_request_id: 'restart-precommit-failure-01',
      expected_index_version: abnormalPlayground.index_version,
    });
    expect(restarted.data.precommit).toMatchObject({ state: 'running', attempt: 2 });
    const cancelled = await cancelPlaygroundPreCommit({
      tenant_id: 'tenant-a',
      precommit_id: abnormal.precommit_id,
      cancel_request_id: 'cancel-precommit-failure-01',
    });
    expect(cancelled.data.precommit.state).toBe('cancelled');

    const restartedCancelled = await restartPlaygroundPreCommit({
      tenant_id: 'tenant-a',
      precommit_id: cancelled.data.precommit.precommit_id,
      restart_request_id: 'restart-precommit-cancelled-01',
      expected_index_version: abnormalPlayground.index_version,
    });
    expect(restartedCancelled.data.precommit).toMatchObject({
      precommit_id: cancelled.data.precommit.precommit_id,
      state: 'running',
      phase: 'queued',
      attempt: 3,
    });

    const playground = (
      await queryPlayground('tenant-a', 'project-vision', 'road-scenes', 'labeling')
    ).data.playground;
    const started = await startPlaygroundPreCommit({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      playground_id: 'labeling',
      precommit_request_id: 'precommit-metadata-ready',
      expected_index_version: playground.index_version,
    });
    const observedPhases = [started.data.precommit.phase];
    let ready = started.data.precommit;
    while (ready.state === 'running') {
      ready = (await queryPlaygroundPreCommit('tenant-a', started.data.precommit.precommit_id)).data
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

    const files = await queryPlaygroundFileList({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      playground_id: 'labeling',
      page_size: 1,
    });
    expect(files.data.items).toHaveLength(1);
    expect(files.data.next_cursor).toBeTruthy();
    const changes = await queryPlaygroundChangeList({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      playground_id: 'labeling',
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
    const metadata = await queryPlaygroundFileMetadata({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      playground_id: 'labeling',
      path: 'dataset/night-rain/part-0042.parquet',
    });
    expect(metadata.data.metadata).toMatchObject({ format: 'parquet', row_count: '12842731' });
    const profile = await queryPlaygroundDatasetProfile({
      tenant_id: 'tenant-a',
      project_id: 'project-vision',
      artifact_id: 'road-scenes',
      playground_id: 'labeling',
    });
    expect(profile.data.profile.state).toBe('ready');
  });

  it('retries Snapshot delivery and gates file browsing on Ready state', async () => {
    const retry = await retrySnapshotDelivery({
      tenant_id: 'tenant-a',
      snapshot_id: 'snap-road-main2-sha-01',
      retry_request_id: 'retry-snapshot-main2-01',
    });
    expect(retry.data.snapshot).toMatchObject({ state: 'creating', phase: 'materializing' });
    expect(
      (
        await retrySnapshotDelivery({
          tenant_id: 'tenant-a',
          snapshot_id: 'snap-road-main2-sha-01',
          retry_request_id: 'retry-snapshot-main2-01',
        })
      ).data.replayed,
    ).toBe(true);

    const firstRetryQuery = await querySnapshot('tenant-a', 'snap-road-main2-sha-01');
    expect(firstRetryQuery.data.snapshot).toMatchObject({
      state: 'creating',
      phase: 'materializing',
      integrity: { state: 'pending' },
    });
    await expect(
      querySnapshotFileList({
        tenant_id: 'tenant-a',
        snapshot_id: 'snap-road-main2-sha-01',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'SNAPSHOT_NOT_READY' });
    const completedRetry = await querySnapshot('tenant-a', 'snap-road-main2-sha-01');
    expect(completedRetry.data.snapshot).toMatchObject({
      state: 'ready',
      phase: 'idle',
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
    ).rejects.toMatchObject({ status: 409, code: 'SNAPSHOT_NOT_READY' });
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
