import type {
  ArtifactView,
  CommitGraphView,
  PlaygroundView,
  ProjectSummary,
  SnapshotView,
  TenantView,
} from '@/api/types';

const created = '1785167000000';
const updated = '1785167600000';

export const tenants: TenantView[] = [
  {
    tenant_id: 'tenant-a',
    display_name: '研究数据平台',
    description: '模型训练与评估数据的受管空间',
    resource_version: '3',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
    permissions: ['tenant.admin', 'tenant.read', 'artifact.read', 'job.create'],
  },
  {
    tenant_id: 'tenant-b',
    display_name: '交付资料库',
    description: '发布物和交付快照的只读归档',
    resource_version: '7',
    created_at_unix_ms: '1784167000000',
    updated_at_unix_ms: '1785067600000',
    permissions: ['tenant.read', 'artifact.read'],
  },
];

export const projects: ProjectSummary[] = [
  {
    tenant_id: 'tenant-a',
    project_id: 'project-vision',
    display_name: '视觉数据',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
  {
    tenant_id: 'tenant-a',
    project_id: 'project-language',
    display_name: '语言模型数据',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
  {
    tenant_id: 'tenant-b',
    project_id: 'project-release',
    display_name: '版本交付',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
];

export const artifacts: ArtifactView[] = [
  {
    tenant_id: 'tenant-a',
    project_id: 'project-vision',
    artifact_id: 'road-scenes',
    display_name: '道路场景数据集',
    description: '覆盖白天、夜间和雨雪天气的训练样本',
    default_ref: 'refs/heads/main',
    resource_version: '18',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
  {
    tenant_id: 'tenant-a',
    project_id: 'project-vision',
    artifact_id: 'quality-reports',
    display_name: '视觉质量报告',
    description: '数据回归检查与人工复核结果',
    default_ref: 'refs/heads/main',
    resource_version: '9',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
  {
    tenant_id: 'tenant-a',
    project_id: 'project-language',
    artifact_id: 'dialog-corpus',
    display_name: '对话语料',
    description: '脱敏后的多轮中文对话语料',
    default_ref: 'refs/heads/main',
    resource_version: '12',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
  {
    tenant_id: 'tenant-b',
    project_id: 'project-release',
    artifact_id: 'release-assets',
    display_name: '发布资源',
    description: '已签发版本的固定交付内容',
    default_ref: 'refs/heads/stable',
    resource_version: '6',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
];

export const commitGraphs = new Map<string, CommitGraphView>([
  [
    'tenant-a\u0000project-vision\u0000road-scenes',
    {
      graph_version: '18',
      refs: [
        { name: 'refs/heads/main', commit_id: 'commit-main-3' },
        { name: 'refs/heads/experiment', commit_id: 'commit-exp-2' },
        { name: 'refs/tags/v1.0', commit_id: 'commit-main-2' },
      ],
      nodes: [
        {
          commit_id: 'commit-main-3',
          parent_commit_id: 'commit-main-2',
          message: '补充夜间道路场景',
          ref_names: ['refs/heads/main'],
          created_at_unix_ms: '1785167600000',
        },
        {
          commit_id: 'commit-exp-2',
          parent_commit_id: 'commit-main-2',
          message: '实验性标注规则',
          ref_names: ['refs/heads/experiment'],
          created_at_unix_ms: '1785167500000',
        },
        {
          commit_id: 'commit-main-2',
          parent_commit_id: 'commit-root-1',
          message: '完成首轮质量复核',
          ref_names: ['refs/tags/v1.0'],
          created_at_unix_ms: '1785067400000',
        },
        {
          commit_id: 'commit-root-1',
          message: '导入初始道路场景',
          ref_names: [],
          created_at_unix_ms: '1784967000000',
        },
      ],
    },
  ],
  [
    'tenant-a\u0000project-vision\u0000quality-reports',
    {
      graph_version: '9',
      refs: [{ name: 'refs/heads/main', commit_id: 'report-commit-2' }],
      nodes: [
        {
          commit_id: 'report-commit-2',
          parent_commit_id: 'report-commit-1',
          message: '补充夜间场景评估',
          ref_names: ['refs/heads/main'],
          created_at_unix_ms: updated,
        },
        {
          commit_id: 'report-commit-1',
          message: '建立质量基线',
          ref_names: [],
          created_at_unix_ms: created,
        },
      ],
    },
  ],
  [
    'tenant-a\u0000project-language\u0000dialog-corpus',
    {
      graph_version: '12',
      refs: [{ name: 'refs/heads/main', commit_id: 'dialog-commit-2' }],
      nodes: [
        {
          commit_id: 'dialog-commit-2',
          parent_commit_id: 'dialog-commit-1',
          message: '增加安全标注',
          ref_names: ['refs/heads/main'],
          created_at_unix_ms: updated,
        },
        {
          commit_id: 'dialog-commit-1',
          message: '导入脱敏对话',
          ref_names: [],
          created_at_unix_ms: created,
        },
      ],
    },
  ],
  [
    'tenant-b\u0000project-release\u0000release-assets',
    {
      graph_version: '6',
      refs: [{ name: 'refs/heads/stable', commit_id: 'release-commit-1' }],
      nodes: [
        {
          commit_id: 'release-commit-1',
          message: '发布 2026.07',
          ref_names: ['refs/heads/stable'],
          created_at_unix_ms: updated,
        },
      ],
    },
  ],
]);

export const playgrounds: PlaygroundView[] = [
  {
    tenant_id: 'tenant-a',
    project_id: 'project-vision',
    artifact_id: 'road-scenes',
    playground_id: 'labeling',
    display_name: '标注工作区',
    base_commit_id: 'commit-main-2',
    head_commit_id: 'commit-main-3',
    index_version: { revision: '31', digest: 'a'.repeat(64) },
    state: 'ready',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
  {
    tenant_id: 'tenant-a',
    project_id: 'project-vision',
    artifact_id: 'quality-reports',
    playground_id: 'nightly-review',
    display_name: '夜间回归检查',
    base_commit_id: 'report-commit-1',
    head_commit_id: 'report-commit-2',
    index_version: { revision: '9', digest: 'b'.repeat(64) },
    state: 'scanning',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
  {
    tenant_id: 'tenant-a',
    project_id: 'project-language',
    artifact_id: 'dialog-corpus',
    playground_id: 'safety-review',
    display_name: '安全标注复核',
    base_commit_id: 'dialog-commit-1',
    head_commit_id: 'dialog-commit-2',
    index_version: { revision: '14', digest: 'c'.repeat(64) },
    state: 'ready',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
  {
    tenant_id: 'tenant-b',
    project_id: 'project-release',
    artifact_id: 'release-assets',
    playground_id: 'release-candidate',
    display_name: '交付候选区',
    base_commit_id: 'release-commit-1',
    head_commit_id: 'release-commit-1',
    index_version: { revision: '6', digest: 'd'.repeat(64) },
    state: 'unavailable',
    created_at_unix_ms: created,
    updated_at_unix_ms: updated,
  },
];

export const snapshots: SnapshotView[] = [
  {
    tenant_id: 'tenant-a',
    project_id: 'project-vision',
    artifact_id: 'road-scenes',
    commit_id: 'commit-main-3',
    message: '补充夜间道路场景',
    ref_names: ['refs/heads/main'],
    created_at_unix_ms: '1785167600000',
    logical_file_count: '864',
    logical_size_bytes: '12884901888',
  },
  {
    tenant_id: 'tenant-a',
    project_id: 'project-vision',
    artifact_id: 'road-scenes',
    commit_id: 'commit-main-2',
    message: '完成首轮质量复核',
    ref_names: ['refs/tags/v1.0'],
    created_at_unix_ms: '1785067400000',
    logical_file_count: '820',
    logical_size_bytes: '11884901888',
  },
  {
    tenant_id: 'tenant-a',
    project_id: 'project-language',
    artifact_id: 'dialog-corpus',
    commit_id: 'dialog-commit-2',
    message: '增加安全标注',
    ref_names: ['refs/heads/main'],
    created_at_unix_ms: updated,
    logical_file_count: '120',
    logical_size_bytes: '2147483648',
  },
  {
    tenant_id: 'tenant-b',
    project_id: 'project-release',
    artifact_id: 'release-assets',
    commit_id: 'release-commit-1',
    message: '发布 2026.07',
    ref_names: ['refs/heads/stable'],
    created_at_unix_ms: updated,
    logical_file_count: '42',
    logical_size_bytes: '734003200',
  },
];

export function resourceKey(...parts: string[]): string {
  return parts.join('\u0000');
}
