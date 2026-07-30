import { expect, test, type Page } from '@playwright/test';

async function expectHealthyLayout(page: Page) {
  const dimensions = await page.evaluate(() => ({
    bodyHeight: document.body.getBoundingClientRect().height,
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
  }));
  expect(dimensions.bodyHeight).toBeGreaterThan(300);
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth + 1);
}

async function navigateFromSidebar(page: Page, label: string) {
  const mobileMenu = page.getByRole('button', { name: '打开导航' });
  if (await mobileMenu.isVisible()) {
    await mobileMenu.click();
    await page.locator('.mobile-drawer').getByRole('button', { name: label, exact: true }).click();
    return;
  }
  await page.locator('.sidebar').getByRole('button', { name: label, exact: true }).click();
}

test.beforeEach(({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error' && !message.text().startsWith('Failed to load resource:')) {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => errors.push(error.message));
  (page as typeof page & { consoleErrors?: string[] }).consoleErrors = errors;
});

test.afterEach(({ page }) => {
  expect((page as typeof page & { consoleErrors?: string[] }).consoleErrors ?? []).toEqual([]);
});

test('selects the preferred Tenant and creates a new Tenant', async ({ page }, testInfo) => {
  await page.goto('/');
  await expect(page).toHaveURL(/\/tenants\/tenant-a\/overview$/);
  await expect(page.getByRole('heading', { name: '研究数据平台' })).toBeVisible();
  await expect(page.getByText('服务存活')).toBeVisible();

  await page.getByRole('combobox', { name: '当前租户' }).click();
  await page.getByRole('option', { name: /交付资料库/ }).click();
  await expect(page).toHaveURL(/\/tenants\/tenant-b\/overview$/);
  await expect(page.getByRole('heading', { name: '交付资料库' })).toBeVisible();

  await page.getByRole('button', { name: '创建租户' }).click();
  const dialog = page.getByRole('dialog', { name: '创建租户' });
  await dialog.getByLabel('Tenant ID').fill('tenant-playwright');
  await dialog.getByLabel('租户名称').fill('端到端测试租户');
  await dialog.getByLabel('描述').fill('Playwright 创建的空租户');
  await dialog.getByRole('button', { name: '创建租户' }).click();
  await expect(page).toHaveURL(/\/tenants\/tenant-playwright\/overview$/);
  await expect(page.getByRole('heading', { name: '端到端测试租户' })).toBeVisible();
  await expectHealthyLayout(page);
  await page.screenshot({
    path: testInfo.outputPath('tenant-overview.png'),
    animations: 'disabled',
    fullPage: true,
  });
});

test('browses Artifact Commit graph and related resources', async ({ page }, testInfo) => {
  await page.goto('/tenants/tenant-a/artifacts');
  await expect(page.getByRole('heading', { name: '数据资产' })).toBeVisible();
  await expect(page.getByText('Default ref', { exact: true })).toHaveCount(0);
  await expect(
    page.locator('.resource-table').getByRole('columnheader', { name: '放置' }),
  ).toHaveCount(0);
  await page.getByRole('button', { name: /道路场景数据集/ }).click();
  await expect(page).toHaveURL(/\/projects\/project-vision\/artifacts\/road-scenes$/);
  const artifactOverview = page.getByRole('tabpanel', { name: '概览' });
  await expect(artifactOverview.getByText('Region', { exact: true })).toHaveCount(0);
  await expect(artifactOverview.getByText('StorageVolume', { exact: true })).toHaveCount(0);
  await expect(page.getByText('当前 Commit', { exact: true })).toBeVisible();
  await expect(page.getByText('commit-main-3', { exact: true }).first()).toBeVisible();
  await expect(page.getByText('dataset/v4', { exact: true }).first()).toBeVisible();
  await expect(page.getByText(/refs\/heads/)).toHaveCount(0);
  await page.screenshot({
    path: testInfo.outputPath('artifact-overview.png'),
    animations: 'disabled',
    fullPage: true,
  });

  await page.getByRole('tab', { name: '版本' }).click();
  await expect(page).toHaveURL(/tab=commits/);
  const latestCommit = page.locator('.commit-node').filter({ hasText: '补充夜间道路场景' });
  await expect(latestCommit.getByText('补充夜间道路场景', { exact: true })).toBeVisible();
  await expect(page.getByText('occlusion-experiment', { exact: true }).first()).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath('artifact-versions.png'),
    animations: 'disabled',
    fullPage: true,
  });
  await latestCommit.getByRole('button', { name: '详情与 Diff' }).click();
  const commitDrawer = page.getByRole('dialog', { name: 'Commit 详情' });
  await expect(commitDrawer.getByText('完成首轮质量复核', { exact: true })).toBeVisible();
  await expect(commitDrawer.getByText('dataset/index.json', { exact: true })).toBeVisible();
  await expect(commitDrawer.getByText('v1.0', { exact: true })).toBeVisible();
  await page.keyboard.press('Escape');

  await page.getByRole('tab', { name: '工作区' }).click();
  await page.getByText('标注工作区', { exact: true }).click();
  await expect(page.getByText('Index revision')).toBeVisible();
  await expect(page.getByText('commit-main-3', { exact: true }).first()).toBeVisible();
  await expect(page.getByRole('heading', { name: '工作区控制台' })).toBeVisible();
  await expect(page.getByText('变化文件', { exact: true })).toBeVisible();
  const parquetDiff = page
    .locator('.workspace-diff-table__row:visible, .mobile-workspace-diff button:visible')
    .filter({ hasText: 'dataset/night-rain/part-0042.parquet' });
  if (await parquetDiff.getByRole('button', { name: '元数据' }).isVisible()) {
    await parquetDiff.getByRole('button', { name: '元数据' }).click();
  } else {
    await parquetDiff.click();
  }
  const metadataDrawer = page.getByRole('dialog', { name: '文件元数据' });
  await expect(metadataDrawer.getByText('Chunk / Object 分布')).toBeVisible();
  await expect(metadataDrawer.getByText('Schema Diff')).toBeVisible();
  await page.keyboard.press('Escape');
  await page.getByRole('tab', { name: '文件' }).click();
  await expect(page.getByText('中心 Index 文件清单')).toBeVisible();
  await page.getByRole('tab', { name: '元数据' }).click();
  await expect(page.getByRole('heading', { name: 'Dataset Profile' })).toBeVisible();
  await page.getByRole('tab', { name: '活动' }).click();
  await expect(page.getByText('Index 扫描完成')).toBeVisible();
  await page.getByRole('tab', { name: '变化' }).click();
  await expectHealthyLayout(page);
  await page.screenshot({
    path: testInfo.outputPath('playground-detail.png'),
    animations: 'disabled',
    fullPage: true,
  });
});

test('creates an Artifact, Playground, Commit and Snapshot from resource pages', async ({
  page,
}, testInfo) => {
  const suffix = testInfo.project.name;
  const storageVolumeId = `volume-${suffix}`;
  const artifactId = `evaluation-${suffix}`;
  const playgroundId = `review-${suffix}`;

  await page.goto('/tenants/tenant-a/storage-volumes');
  await page.getByRole('button', { name: '登记 StorageVolume' }).click();
  const storageDialog = page.getByRole('dialog', { name: '登记 StorageVolume' });
  await storageDialog.getByLabel('StorageVolume ID').fill(storageVolumeId);
  await storageDialog.getByLabel('名称').fill('自动化评测 PVC');
  await storageDialog.getByLabel('EdgeCluster ID').fill('cluster-cn-south-1');
  await storageDialog.getByLabel('Region').fill('cn-guangzhou');
  await storageDialog.getByLabel('PVC Namespace').fill('neoengram-e2e');
  await storageDialog.getByLabel('PVC Claim name').fill(`evaluation-${suffix}`);
  await storageDialog.getByRole('button', { name: '登记 StorageVolume' }).click();
  await expect(
    page.locator('code:visible').filter({ hasText: storageVolumeId }).first(),
  ).toBeVisible();

  await navigateFromSidebar(page, '数据资产');
  await page.getByRole('button', { name: '创建 Artifact' }).click();
  const artifactDialog = page.getByRole('dialog', { name: '创建 Artifact' });
  await artifactDialog.getByRole('combobox', { name: 'Project 筛选' }).click();
  await page.getByRole('option', { name: /视觉数据/ }).click();
  await artifactDialog.getByLabel('Artifact ID').fill(artifactId);
  await expect(artifactDialog.getByRole('combobox', { name: 'StorageVolume 选择' })).toHaveCount(0);
  await artifactDialog.getByLabel('名称').fill('自动驾驶评测集');
  await artifactDialog.getByLabel('描述').fill('从资源页创建的端到端测试数据集');
  await expect(artifactDialog.getByText('Default ref', { exact: true })).toHaveCount(0);
  await page.screenshot({
    path: testInfo.outputPath('artifact-create.png'),
    animations: 'disabled',
    fullPage: false,
  });
  await artifactDialog.getByRole('button', { name: '创建 Artifact' }).click();
  await expect(page).toHaveURL(new RegExp(`/artifacts/${artifactId}$`));
  await expect(page.getByRole('heading', { name: '自动驾驶评测集' })).toBeVisible();

  await page.getByRole('button', { name: '创建 Playground' }).click();
  const playgroundDialog = page.getByRole('dialog', { name: '创建 Playground' });
  await playgroundDialog.getByLabel('Playground ID').fill(playgroundId);
  await playgroundDialog.getByLabel('名称').fill('提交前复核');
  await playgroundDialog.getByRole('combobox', { name: 'StorageVolume 选择' }).click();
  await page.getByRole('option', { name: /自动化评测 PVC/ }).click();
  await expect(playgroundDialog.getByText('自动化评测 PVC · cn-guangzhou')).toBeVisible();
  await playgroundDialog.getByRole('button', { name: '创建 Playground' }).click();
  await expect(page).toHaveURL(new RegExp(`/playgrounds/${playgroundId}$`));

  await page.getByRole('button', { name: '发起 Pre-commit', exact: true }).click();
  await expect(page.getByRole('heading', { name: '提交 Playground' })).toBeVisible();
  await expect(page.getByText('预检测完成', { exact: true })).toBeVisible();
  await expect(page.getByText('0 项阻断')).toBeVisible();
  await page.locator('.workflow-actions').getByRole('button', { name: '填写 Commit 信息' }).click();
  const commitDialog = page.getByRole('dialog', { name: '创建 Commit' });
  await commitDialog.getByLabel('Commit 标题').fill('建立自动驾驶评测基线');
  await commitDialog.getByLabel('详细描述').fill('记录自动驾驶评测集的初始导入和检查范围');
  await commitDialog.getByLabel('Commit Tags').fill(`baseline-${suffix}`);
  await commitDialog.getByLabel('Commit Tags').press('Enter');
  await commitDialog.getByRole('button', { name: '确认 Commit' }).click();
  await expect(page.getByRole('heading', { name: 'Commit 已创建' })).toBeVisible();
  await expect(page.getByText(/^commit-/).last()).toBeVisible();
  await page.locator('.commit-result').getByRole('button', { name: '返回 Playground' }).click();

  await page.getByRole('button', { name: '查看 Head Commit' }).click();
  const createdCommitDrawer = page.getByRole('dialog', { name: 'Commit 详情' });
  await expect(
    createdCommitDrawer.getByText('记录自动驾驶评测集的初始导入和检查范围'),
  ).toBeVisible();
  await expect(createdCommitDrawer.getByText(`baseline-${suffix}`, { exact: true })).toBeVisible();
  await expect(createdCommitDrawer.getByText('根 Commit，无父版本')).toBeVisible();
  await expect(createdCommitDrawer.getByText(/dataset\/commits\/commit-/).first()).toBeVisible();
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: '创建 Snapshot' }).click();
  const snapshotDialog = page.getByRole('dialog', { name: '创建 Snapshot' });
  await snapshotDialog.getByRole('combobox', { name: 'StorageVolume 选择' }).click();
  await page.getByRole('option', { name: /自动化评测 PVC/ }).click();
  await expect(snapshotDialog.getByText('自动化评测 PVC · cn-guangzhou')).toBeVisible();
  await snapshotDialog.getByRole('button', { name: '创建 Snapshot' }).click();
  await expect(page).toHaveURL(/\/snapshots\/commit-/);
  await expect(page.getByRole('heading', { name: '建立自动驾驶评测基线' })).toBeVisible();
  await expect(page.getByText('cn-guangzhou', { exact: true }).first()).toBeVisible();
  await expectHealthyLayout(page);
});

test('browses Tenant-wide Playground and Snapshot details', async ({ page }, testInfo) => {
  await page.goto('/tenants/tenant-a/playgrounds');
  const visiblePlaygroundList = page.locator(
    '.resource-table:visible, .mobile-resource-list:visible',
  );
  await expect(visiblePlaygroundList.getByText('计算内容摘要')).toBeVisible();
  await expect(visiblePlaygroundList.getByText('中心一致性校验')).toBeVisible();

  await page.getByRole('button', { name: /夜间回归检查/ }).click();
  await expect(page.getByText('Pre-commit 正在计算内容摘要')).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath('active-precommit-controls.png'),
    animations: 'disabled',
    fullPage: true,
  });
  await page.getByRole('button', { name: '查看 Pre-commit' }).click();
  await expect(page).toHaveURL(/resume_phase=hashing/);
  await expect(page.getByText('precommit-nightly-0729')).toBeVisible();

  await page.getByRole('button', { name: '重新检测' }).click();
  const restartDialog = page.locator('.el-message-box').filter({ hasText: '重新检测' });
  await restartDialog.getByRole('button', { name: '重新检测', exact: true }).click();
  await expect(page.getByText('precommit-nightly-0729')).toHaveCount(0);
  await expect(page.locator('.preflight-status__body small')).toContainText(
    'precommit-nightly-review-',
  );
  await expect(page.getByText('预检测完成', { exact: true })).toBeVisible();
  await page.locator('.workflow-actions').getByRole('button', { name: '填写 Commit 信息' }).click();
  const restartedCommitDialog = page.getByRole('dialog', { name: '创建 Commit' });
  await expect(restartedCommitDialog.locator('.tag-editor__values .el-tag')).toHaveCount(0);
  await restartedCommitDialog.getByRole('button', { name: '取消' }).click();

  await page.getByRole('button', { name: '取消 Pre-commit' }).click();
  const cancelDialog = page.locator('.el-message-box').filter({ hasText: '取消 Pre-commit' });
  await cancelDialog.getByRole('button', { name: '确认取消', exact: true }).click();
  await expect(page).toHaveURL(/\/playgrounds\/nightly-review$/);
  await expect(page.getByText('工作区可用，当前没有运行中的 Pre-commit')).toBeVisible();
  await expect(page.getByRole('button', { name: '发起 Pre-commit' })).toBeVisible();

  await page.goto('/tenants/tenant-a/playgrounds');
  await page.getByRole('button', { name: /安全标注复核/ }).click();
  await expect(page).toHaveURL(/\/playgrounds\/safety-review$/);
  await expect(page.getByText(/project-language \/ dialog-corpus \/ safety-review/)).toBeVisible();

  await page.goto('/tenants/tenant-a/snapshots');
  await page.getByRole('button', { name: /补充夜间道路场景/ }).click();
  await expect(page).toHaveURL(/\/snapshots\/commit-main-3$/);
  await expect(page.getByText('只读 · Ready', { exact: true })).toBeVisible();
  await expect(page.getByText('固定 Commit', { exact: true })).toBeVisible();
  await expect(page.getByText('dataset/v4', { exact: true })).toBeVisible();
  await expect(page.getByText(/refs\/heads/)).toHaveCount(0);
  await expect(page.getByText('12 GiB', { exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: '区域可用性' })).toBeVisible();
  await expect(page.getByText('cn-guangzhou', { exact: true })).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath('snapshot-detail.png'),
    animations: 'disabled',
    fullPage: true,
  });

  await page.getByRole('tab', { name: '文件' }).click();
  await expect(page.getByRole('heading', { name: 'Snapshot 文件清单' })).toBeVisible();
  const visibleSnapshotFile = page
    .locator('code:visible')
    .filter({ hasText: 'dataset/night-rain/part-0042.parquet' });
  await expect(visibleSnapshotFile).toBeVisible();
  await page.getByPlaceholder('按路径搜索').fill('night-rain/part-0042');
  await expect(visibleSnapshotFile).toBeVisible();
  await expect(page.getByText('当前显示 1 个代表文件，共 864 个文件')).toBeVisible();

  await page.getByRole('tab', { name: '活动' }).click();
  await expect(page.getByText('Snapshot 创建', { exact: true })).toBeVisible();
  await expect(page.getByText('2 个区域均通过 Manifest 与对象完整性校验')).toBeVisible();
  await expectHealthyLayout(page);
});

test('commits a Playground and delivers a fixed Snapshot', async ({ page }, testInfo) => {
  await page.goto(
    '/tenants/tenant-a/projects/project-vision/artifacts/road-scenes/playgrounds/labeling',
  );
  await page.getByRole('button', { name: '发起 Pre-commit', exact: true }).click();
  await expect(page).toHaveURL(
    /\/tenants\/tenant-a\/projects\/project-vision\/artifacts\/road-scenes\/playgrounds\/labeling\/commit$/,
  );
  await expect(page.getByRole('heading', { name: '提交 Playground' })).toBeVisible();
  await expect(page.getByText('road-scenes / labeling', { exact: true })).toBeVisible();
  await expect(page.getByText('tenant-a / project-vision', { exact: true })).toBeVisible();
  await expect(page.getByText('预检测完成', { exact: true })).toBeVisible();
  await expect(page.getByText('Playground 保持 Ready')).toBeVisible();
  await expect(page.getByText('0 项阻断')).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath('precommit-ready.png'),
    animations: 'disabled',
    fullPage: true,
  });
  await page.locator('.workflow-actions').getByRole('button', { name: '填写 Commit 信息' }).click();
  const commitDialog = page.getByRole('dialog', { name: '创建 Commit' });
  await expect(commitDialog).toBeVisible();
  await expect(commitDialog.getByText('目标 Ref')).toHaveCount(0);
  await expect(commitDialog.getByText('refs/heads/main')).toHaveCount(0);
  await page.screenshot({
    path: testInfo.outputPath('commit-dialog.png'),
    animations: 'disabled',
    fullPage: false,
  });
  await commitDialog.getByLabel('Commit 标题').fill('发布自动驾驶夜间场景 v4');
  await commitDialog.getByLabel('Commit Tags').fill('dataset/v5');
  await commitDialog.getByLabel('Commit Tags').press('Enter');
  await commitDialog.getByRole('button', { name: '确认 Commit' }).click();

  await expect(page.getByRole('heading', { name: 'Commit 已创建' })).toBeVisible();
  await expect(page.getByText('尚未创建 Snapshot')).toBeVisible();
  await page.getByRole('button', { name: '创建并交付 Snapshot' }).click();
  await expect(page).toHaveURL(/\/snapshots\/new\?commit_id=/);
  await expect(page.getByRole('heading', { name: '交付只读 Snapshot' })).toBeVisible();
  await page.getByRole('button', { name: '选择交付区域' }).click();

  await expect(page.getByRole('heading', { name: '选择需要就绪的计算区域' })).toBeVisible();
  await expect(page.getByRole('checkbox', { name: '选择 cn-guangzhou 区域' })).toBeChecked();
  await expect(page.locator('.el-message')).toHaveCount(0);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: testInfo.outputPath('release-placement.png'),
    animations: 'disabled',
    fullPage: true,
  });
  await page.getByRole('button', { name: '创建 Snapshot 并开始交付' }).click();

  await expect(page.getByRole('heading', { name: '正在交付 Snapshot' })).toBeVisible();
  await expect(page.getByText('materializing · 64%')).toBeVisible();
  await page.getByRole('button', { name: '推进模拟' }).click();
  await expect(page.getByRole('heading', { name: 'Snapshot 已在目标区域就绪' })).toBeVisible();
  await expect(page.getByText('2 个区域已通过完整性校验')).toBeVisible();
  await expectHealthyLayout(page);
  await expect(page.locator('.el-message')).toHaveCount(0);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: testInfo.outputPath('snapshot-delivery.png'),
    animations: 'disabled',
    fullPage: true,
  });
});

test('creates, advances, finalizes and replays a Managed Add Job', async ({ page }, testInfo) => {
  await page.goto('/tenants/tenant-a/jobs/new');
  await expect(page.getByText('当前租户：tenant-a')).toBeVisible();
  await expect(page.getByLabel('Tenant ID')).toHaveCount(0);
  const jobId = await page
    .locator('.el-form-item')
    .filter({ hasText: 'Job ID' })
    .locator('input')
    .inputValue();
  await page.getByRole('button', { name: '开始扫描' }).click();
  await expect(page).toHaveURL(new RegExp(`/tenants/tenant-a/jobs/${jobId}$`));
  await expect(page.getByText('待发布').first()).toBeVisible({ timeout: 7000 });
  await page.getByRole('button', { name: 'Finalize', exact: true }).click();
  await page.getByRole('dialog').getByRole('button', { name: 'Finalize' }).click();
  await expect(page.getByText('已成功').first()).toBeVisible();
  await expect(page.getByText('publish', { exact: true })).toBeVisible();
  await expectHealthyLayout(page);
  await page.screenshot({
    path: testInfo.outputPath('finalized-job.png'),
    animations: 'disabled',
    fullPage: true,
  });

  const replayed = await page.evaluate(
    async ({ id }) => {
      const response = await fetch('/api/job/add/finalize', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer mock-access-token',
          'Content-Type': 'application/json',
          'NeoEngram-API-Version': '1',
          'X-Request-ID': 'req-playwright-replay',
        },
        body: JSON.stringify({ tenant_id: 'tenant-a', job_id: id }),
      });
      return (await response.json()) as { replayed: boolean };
    },
    { id: jobId },
  );
  expect(replayed.replayed).toBe(true);
});

test('handles missing, validation, unavailable and invisible Tenant routes', async ({ page }) => {
  await page.goto('/tenants/tenant-a/jobs/query');
  await page.getByLabel('Job ID').fill('job-missing');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await expect(page.getByText('未找到可见的 Job')).toBeVisible();

  await page.goto('/tenants/tenant-a/jobs/new');
  await page
    .locator('.el-form-item')
    .filter({ hasText: 'Project ID' })
    .locator('input')
    .fill('project-invalid');
  await page.getByRole('button', { name: '开始扫描' }).click();
  await expect(page.getByText('请求未通过校验')).toBeVisible();
  await expect(page.getByText('PROTOCOL_INVALID')).toBeVisible();

  await page.goto('/tenants/tenant-unavailable/jobs/new');
  await page.getByRole('button', { name: '开始扫描' }).click();
  await expect(page.getByText('中心 authority 暂不可用')).toBeVisible();
  await expect(page.getByRole('button', { name: '重试' })).toBeVisible();

  await page.goto('/tenants/tenant-secret/artifacts');
  await expect(page).toHaveURL(/\/tenants\/tenant-a\/overview$/);
  await expectHealthyLayout(page);
});
