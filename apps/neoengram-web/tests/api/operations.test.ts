import { describe, expect, it } from 'vitest';

import {
  cancelTask,
  materializeCommit,
  queryTask,
  queryTaskEventList,
  queryTaskList,
  queryTaskSummary,
  retryTask,
} from '@/api/operations';

const materializationRequest = {
  tenant_id: 'tenant-a',
  project_id: 'project-vision',
  artifact_id: 'road-scenes',
  object_namespace_id: 'road-scenes',
  commit_id: 'b'.repeat(64),
  target_storage_volume_id: 'volume-guangzhou-delivery',
  request_id: 'task-materialize-1',
  coverage_goal: 'complete' as const,
};

describe('public operation task API', () => {
  it('creates an idempotent materialization task and exposes it through task queries', async () => {
    const first = await materializeCommit(materializationRequest);
    const replay = await materializeCommit(materializationRequest);
    expect(first.data.replayed).toBe(false);
    expect(replay.data.replayed).toBe(true);
    expect(first.data.task?.task_kind).toBe('commit.materialize');

    const taskId = first.data.task?.task_id;
    expect(taskId).toBeTruthy();
    const detail = await queryTask({ tenant_id: 'tenant-a', task_id: taskId! });
    expect(detail.data.task.task_id).toBe(taskId);
    expect(detail.data.events[0]?.kind).toBe('created');

    const list = await queryTaskList({ tenant_id: 'tenant-a', task_kind: ['commit.materialize'] });
    expect(list.data.items.some((item) => item.task_id === taskId)).toBe(true);
    const summary = await queryTaskSummary({
      tenant_id: 'tenant-a',
      task_kind: ['commit.materialize'],
    });
    expect(Number(summary.data.summary.total)).toBeGreaterThanOrEqual(1);
  });

  it('uses task control endpoints and keeps the same task identity', async () => {
    const created = await materializeCommit({
      ...materializationRequest,
      request_id: 'task-materialize-2',
    });
    const taskId = created.data.task!.task_id;
    const cancelled = await cancelTask({ tenant_id: 'tenant-a', task_id: taskId });
    expect(cancelled.data.task.task_id).toBe(taskId);
    expect(cancelled.data.task.state).toBe('cancelled');
    await expect(retryTask({ tenant_id: 'tenant-a', task_id: taskId })).rejects.toMatchObject({
      status: 409,
      code: 'TASK_NOT_RETRYABLE',
    });
    const events = await queryTaskEventList({ tenant_id: 'tenant-a', task_id: taskId });
    expect(events.data.items.some((event) => event.kind === 'cancelled')).toBe(true);
  });
});
