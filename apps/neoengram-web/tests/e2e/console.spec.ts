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

async function expectNoOperatorDetails(page: Page) {
  await expect(page.locator('body')).not.toContainText(
    /\b(?:Manifest|Object|Chunk|Agent|Mount|Lease|Fencing)\b/i,
  );
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
  if (testInfo.project.name === 'mobile') {
    await expect(page.getByText('存活探针')).toBeVisible();
    await expect(page.getByText('就绪探针')).toBeVisible();
  } else {
    await expect(page.getByText('控制面正常')).toBeVisible();
  }
  await expect(
    page.getByRole('region', { name: '租户资源' }).getByRole('button', { name: /数据资产/ }),
  ).toBeVisible();
  await expect(page.getByText('服务存活')).toHaveCount(0);
  await expect(page.getByText('最近版本')).toHaveCount(0);

  await page.getByRole('combobox', { name: '当前租户' }).click();
  await page.getByRole('option', { name: /交付资料库/ }).click();
  await expect(page).toHaveURL(/\/tenants\/tenant-b\/overview$/);
  await expect(page.getByRole('heading', { name: '交付资料库' })).toBeVisible();
  await page.goto('/tenants/tenant-b/artifacts');
  await expect(page.getByRole('button', { name: '创建 Artifact' })).toHaveCount(0);
  await page.goto('/tenants/tenant-b/storage-volumes');
  await expect(page.getByRole('button', { name: '接入 PVC' })).toHaveCount(0);
  await expect(page.getByRole('button', { name: '登记 NFS' })).toHaveCount(0);
  await page.goto('/tenants/tenant-b/overview');

  await page.getByRole('button', { name: '创建租户' }).click();
  const dialog = page.getByRole('dialog', { name: '创建租户' });
  await dialog.getByLabel('Tenant ID').fill('tenant-playwright');
  await dialog.getByLabel('租户名称').fill('端到端测试租户');
  await dialog.getByLabel('描述').fill('Playwright 创建的空租户');
  await dialog.getByRole('button', { name: '创建租户' }).click();
  await expect(page).toHaveURL(/\/tenants\/tenant-playwright\/overview$/);
  await expect(page.getByRole('heading', { name: '端到端测试租户' })).toBeVisible();
  await expectNoOperatorDetails(page);
  await expectHealthyLayout(page);
  await page.screenshot({
    path: testInfo.outputPath('tenant-overview.png'),
    animations: 'disabled',
    fullPage: true,
  });
  await navigateFromSidebar(page, '数据资产');
  await expect(page.getByText('当前筛选下没有 Artifact')).toBeVisible();
});

test('creates a PVC token and independently approves a pending enrollment', async ({
  page,
}, testInfo) => {
  const suffix = testInfo.project.name;
  const tokenVolumeId = `volume-enrollment-${suffix}`;
  const pendingVolumeId = 'volume-review-pvc';

  await page.goto('/tenants/tenant-a/storage-volumes');
  await page.getByRole('button', { name: '接入 PVC' }).click();
  const enrollmentDialog = page.getByRole('dialog', { name: '接入 PVC' });
  await enrollmentDialog.getByLabel('StorageVolume ID').fill(tokenVolumeId);
  await enrollmentDialog.getByLabel('名称').fill('自动化接入 PVC');
  await enrollmentDialog.getByLabel('EdgeCluster ID').fill('cluster-cn-south-1');
  await enrollmentDialog.getByLabel('Region').fill('cn-guangzhou');
  await enrollmentDialog.getByLabel('PVC Namespace').fill('neoengram-e2e');
  await enrollmentDialog.getByLabel('PVC Claim name').fill(`enrollment-${suffix}`);
  await enrollmentDialog.getByRole('button', { name: '生成接入凭证' }).click();
  await expect(enrollmentDialog.getByLabel('Bootstrap token', { exact: true })).toContainText(
    'ngenr_v1_',
  );
  const agentConfig = enrollmentDialog.locator('.deployment-config');
  await expect(agentConfig).toContainText('schema_version: 1');
  await expect(agentConfig).toContainText('protocol_version: 1');
  await expect(agentConfig).toContainText('region: cn-guangzhou');
  await expect(agentConfig).toContainText('storage:');
  await expect(agentConfig).toContainText('backend_type: pvc');
  await expect(agentConfig).toContainText('access_mode: read_write_many');
  await expect(agentConfig).toContainText('mount_path: /volume');
  await expect(agentConfig).toContainText('state_dir: /var/lib/neoengram-agent');
  await expect(agentConfig).toContainText('marker_file: /volume/.neoengram-volume-marker');
  await expect(agentConfig).toContainText(`expected_volume_marker: ${tokenVolumeId}`);
  await expect(agentConfig).toContainText('pvc_reference:');
  await expect(agentConfig).toContainText('registration:');
  await expect(agentConfig).toContainText('approval_required: true');
  await expect(agentConfig).toContainText('token_id: storage-enrollment-token-');
  await expect(agentConfig).toContainText(
    'bootstrap_token_file: /var/run/secrets/neoengram/bootstrap-token',
  );
  await enrollmentDialog.getByRole('button', { name: '完成' }).click();
  await expect(page.getByLabel('Bootstrap token', { exact: true })).toHaveCount(0);

  await page.getByRole('tab', { name: '待审批' }).click();
  const pendingSection = page.locator('.enrollment-section');
  await expect(pendingSection.getByText(tokenVolumeId, { exact: true })).toHaveCount(0);
  const visiblePendingList = pendingSection.locator(
    testInfo.project.name === 'desktop'
      ? '.desktop-table:visible'
      : '.mobile-resource-list:visible',
  );
  const pendingItem = visiblePendingList
    .locator(testInfo.project.name === 'desktop' ? '.el-table__row' : '.mobile-resource-item')
    .filter({ hasText: pendingVolumeId })
    .first();
  await expect(pendingItem).toBeVisible();
  await expect(pendingItem.getByText(pendingVolumeId, { exact: true })).toBeVisible();
  await expect(pendingItem).toContainText('cn-shanghai');
  await expect(pendingItem).toContainText('cluster-cn-east-1');
  await expect(pendingItem).toContainText('read_write_many');
  await expect(pendingItem).toContainText('0.2.0');
  await expect(pendingItem).toContainText('aaaaaaaaaaaa...aaaaaaaa');
  await pendingItem.getByRole('button', { name: `批准 ${pendingVolumeId}`, exact: true }).click();
  const approvalDialog = page.getByRole('dialog', { name: '批准存储接入' });
  await approvalDialog.getByRole('button', { name: '批准', exact: true }).click();

  await page.getByRole('tab', { name: '已登记' }).click();
  const volumeRow = page
    .locator('.desktop-table .el-table__row')
    .filter({ hasText: pendingVolumeId });
  if (testInfo.project.name === 'desktop') {
    await expect(volumeRow.getByText('unavailable', { exact: true })).toBeVisible();
  } else {
    await expect(
      page
        .locator('.mobile-resource-item')
        .filter({ hasText: pendingVolumeId })
        .getByText('unavailable'),
    ).toBeVisible();
  }
  await expectHealthyLayout(page);
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
  await expect(page.getByText('Index revision', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: '查看 Head Commit' })).toBeVisible();
  await expect(page.getByRole('heading', { name: '工作区数据' })).toBeVisible();
  await expect(page.getByLabel('变化摘要')).toBeVisible();
  if (testInfo.project.name === 'desktop') {
    const parquetDiff = page
      .locator('.desktop-table .el-table__row')
      .filter({ hasText: 'dataset/night-rain/part-0042.parquet' });
    await parquetDiff.getByRole('button', { name: '元数据' }).click();
    const metadataDrawer = page.getByRole('dialog', { name: '文件元数据' });
    await expect(metadataDrawer.getByRole('heading', { name: 'Schema' })).toBeVisible();
    await expect(metadataDrawer.getByText('质量状态', { exact: true })).toBeVisible();
    await expect(metadataDrawer.getByText('观测时间', { exact: true })).toBeVisible();
    await expectNoOperatorDetails(page);
    await page.keyboard.press('Escape');
  }
  await page.getByRole('tab', { name: '文件' }).click();
  await page.getByLabel('文件路径前缀').fill('dataset/night-rain');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  if (testInfo.project.name === 'desktop') {
    await expect(page.getByRole('columnheader', { name: '逻辑路径' })).toBeVisible();
    await expect(
      page
        .getByRole('tabpanel', { name: '文件' })
        .getByText('dataset/night-rain/part-0042.parquet', { exact: true }),
    ).toBeVisible();
  }
  await page.getByRole('tab', { name: 'Dataset Profile' }).click();
  await expect(page.getByRole('heading', { name: '派生数据概览' })).toBeVisible();
  await expect(page.getByRole('heading', { name: '统计与质量' })).toBeVisible();
  await page.getByRole('tab', { name: '变化' }).click();
  await expectNoOperatorDetails(page);
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
  const artifactId = `evaluation-${suffix}`;
  const playgroundId = `review-${suffix}`;

  await page.goto('/tenants/tenant-a/artifacts');
  await page.getByRole('button', { name: '创建 Artifact' }).click();
  const artifactDialog = page.getByRole('dialog', { name: '创建 Artifact' });
  await artifactDialog.getByRole('combobox', { name: 'Project 筛选' }).click();
  await page.getByRole('option', { name: /视觉数据/ }).click();
  await artifactDialog.getByLabel('Artifact ID').fill(artifactId);
  await expect(artifactDialog.getByRole('combobox', { name: 'StorageVolume 选择' })).toHaveCount(0);
  await artifactDialog.getByLabel('名称').fill('自动驾驶评测集');
  await artifactDialog.getByLabel('描述').fill('从资源页创建的端到端测试数据集');
  await expect(artifactDialog.getByText('创建空 Artifact', { exact: true })).toBeVisible();
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
  await page.getByRole('option', { name: /广州训练集交付 PVC/ }).click();
  await expect(playgroundDialog.getByText('广州训练集交付 PVC · cn-guangzhou')).toBeVisible();
  await playgroundDialog.getByRole('button', { name: '创建 Playground' }).click();
  await expect(page).toHaveURL(new RegExp(`/playgrounds/${playgroundId}$`));

  await page.getByRole('button', { name: '发起 Pre-commit', exact: true }).click();
  await expect(page.getByRole('heading', { name: '提交 Playground' })).toBeVisible();
  await expect(page.locator('.preflight-status__body strong')).toContainText('可提交 · 处理完成');
  await expect(page.getByText('0 项阻断')).toBeVisible();
  await page.getByRole('button', { name: '填写 Commit 信息' }).click();
  const commitDialog = page.getByRole('dialog', { name: '创建 Commit' });
  await commitDialog.getByLabel('Commit 标题').fill('建立自动驾驶评测基线');
  await commitDialog.getByLabel('详细描述').fill('记录自动驾驶评测集的初始导入和检查范围');
  await commitDialog.getByLabel('Commit Tags').fill(`baseline-${suffix}`);
  await commitDialog.getByLabel('Commit Tags').press('Enter');
  await commitDialog.getByRole('button', { name: '确认 Commit' }).click();
  const commitResult = page.locator('.commit-result');
  await expect(commitResult.getByRole('heading', { name: '建立自动驾驶评测基线' })).toBeVisible();
  await expect(commitResult.getByText(`baseline-${suffix}`, { exact: true })).toBeVisible();
  await commitResult.getByRole('button', { name: '创建 Snapshot' }).click();

  await expect(page).toHaveURL(/\/snapshots\/new\?commit_id=/);
  await expect(page.getByRole('heading', { name: '创建 Snapshot' })).toBeVisible();
  await expect(page.getByRole('heading', { name: '建立自动驾驶评测基线' })).toBeVisible();
  await page.getByRole('button', { name: '选择存储位置' }).click();
  await page.getByRole('button', { name: /广州训练集交付 PVC/ }).click();
  await expect(page.getByText('cn-guangzhou', { exact: true }).first()).toBeVisible();
  await page.getByRole('button', { name: '创建 Snapshot', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Snapshot 已可用' })).toBeVisible();
  await expect(page.getByText('新请求', { exact: true })).toBeVisible();
  await expect(page.getByText('新建交付位置', { exact: true })).toBeVisible();
  await expectNoOperatorDetails(page);
  await page.getByRole('button', { name: '查看 Snapshot' }).click();
  await expect(page).toHaveURL(/\/snapshots\/snap-/);
  await expect(page.getByRole('heading', { name: '建立自动驾驶评测基线' })).toBeVisible();
  await expect(page.getByText('只读 · 可用', { exact: true })).toBeVisible();
  await expect(page.getByText('cn-guangzhou', { exact: true }).first()).toBeVisible();
  await expectNoOperatorDetails(page);
  await expectHealthyLayout(page);
});

test('creates an Artifact derived from an explicit source Commit', async ({ page }, testInfo) => {
  const artifactId = `derived-${testInfo.project.name}`;
  await page.goto('/tenants/tenant-a/artifacts?project_id=project-language');
  await page.getByRole('button', { name: '创建 Artifact' }).click();
  const dialog = page.getByRole('dialog', { name: '创建 Artifact' });
  await dialog.getByLabel('Artifact ID').fill(artifactId);
  await dialog.getByLabel('名称').fill('道路场景派生评测集');
  await dialog.getByText('从 Commit 派生', { exact: true }).click();
  await dialog.getByRole('combobox', { name: '来源 Artifact' }).click();
  await page.getByRole('option', { name: /道路场景数据集/ }).click();
  await dialog.getByRole('combobox', { name: '来源 Commit' }).click();
  await page.getByRole('option', { name: /完成首轮质量复核 · commit-main-2/ }).click();
  await dialog.getByRole('button', { name: '创建 Artifact' }).click();

  await expect(page).toHaveURL(new RegExp(`/artifacts/${artifactId}$`));
  await expect(page.getByText('从 Commit 派生', { exact: true })).toBeVisible();
  await expect(page.getByText('road-scenes', { exact: true })).toBeVisible();
  await expect(page.getByText('commit-main-2', { exact: true })).toBeVisible();
  await expect(page.getByText('尚无 Commit')).toHaveCount(0);
});

test('renders resource loading, pagination, empty, forbidden and missing states', async ({
  page,
}) => {
  await page.addInitScript(() => {
    const nativeFetch = window.fetch.bind(window);
    let artifactItems: unknown[] = [];
    let delayFirstPage = true;

    window.fetch = async (input, init) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const url = new URL(request.url);
      if (!url.pathname.endsWith('/api/artifact/list/query')) return nativeFetch(request);

      const body = JSON.parse(await request.clone().text()) as {
        tenant_id?: string;
        page_size?: number;
        cursor?: string;
        query?: string;
      };
      if (body.tenant_id === 'tenant-b' && body.page_size === 50) {
        return new Response(
          JSON.stringify({
            type: 'urn:neoengram:problem:authorization-denied',
            title: 'Authorization denied',
            status: 403,
            detail: 'The principal cannot list these Artifacts',
            instance: url.pathname,
            code: 'AUTHORIZATION_DENIED',
            request_id: 'req-e2e-artifact-forbidden',
            retryable: false,
          }),
          {
            status: 403,
            headers: {
              'Content-Type': 'application/problem+json',
              'X-Request-ID': 'req-e2e-artifact-forbidden',
            },
          },
        );
      }
      if (body.tenant_id !== 'tenant-a' || body.page_size !== 50 || body.query) {
        return nativeFetch(request);
      }
      if (body.cursor === 'e2e-artifact-next') {
        return new Response(JSON.stringify({ items: artifactItems.slice(1, 2) }), {
          status: 200,
          headers: { 'Content-Type': 'application/json', 'X-Request-ID': 'req-e2e-page-2' },
        });
      }

      const response = await nativeFetch(request);
      const payload = (await response.clone().json()) as { items: unknown[] };
      artifactItems = payload.items;
      if (delayFirstPage) {
        delayFirstPage = false;
        await new Promise((resolve) => setTimeout(resolve, 600));
      }
      return new Response(
        JSON.stringify({ items: artifactItems.slice(0, 1), next_cursor: 'e2e-artifact-next' }),
        {
          status: response.status,
          headers: response.headers,
        },
      );
    };
  });

  await page.goto('/tenants/tenant-a/artifacts');
  await expect(page.locator('.resource-section .el-skeleton')).toBeVisible();
  await expect(page.getByRole('button', { name: /道路场景数据集/ })).toBeVisible();
  const nextPage = page.getByRole('button', { name: '下一页' });
  await expect(nextPage).toBeEnabled();
  await nextPage.click();
  await expect(page.getByRole('button', { name: /视觉质量报告/ })).toBeVisible();
  await expect(page.getByRole('button', { name: '上一页' })).toBeEnabled();

  await page.getByPlaceholder('搜索名称或 Artifact ID').fill('道路');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await expect(page.getByRole('button', { name: /道路场景数据集/ })).toBeVisible();
  await expect(page.getByRole('button', { name: '上一页' })).toBeDisabled();
  await page.getByPlaceholder('搜索名称或 Artifact ID').fill('artifact-does-not-exist');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await expect(page.getByText('当前筛选下没有 Artifact')).toBeVisible();

  await page.goto('/tenants/tenant-b/artifacts');
  await expect(page.getByText('当前身份无权执行此操作')).toBeVisible();
  await expect(page.getByText('AUTHORIZATION_DENIED', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: '创建 Artifact' })).toHaveCount(0);

  await page.goto(
    '/tenants/tenant-a/projects/project-vision/artifacts/road-scenes/snapshots/snapshot-does-not-exist',
  );
  await expect(page.getByText('SNAPSHOT_NOT_FOUND', { exact: true })).toBeVisible();
  await expectHealthyLayout(page);
});

test('browses Tenant-wide Playground and Snapshot details', async ({ page }, testInfo) => {
  testInfo.setTimeout(45_000);
  await page.goto('/tenants/tenant-a/playgrounds');
  const visiblePlaygroundList = page.locator(
    '.resource-table:visible, .mobile-resource-list:visible',
  );
  await expect(visiblePlaygroundList.getByText('创建中', { exact: true })).toBeVisible();
  await expect(visiblePlaygroundList.getByText('可用', { exact: true }).first()).toBeVisible();
  await expect(visiblePlaygroundList.getByText('异常', { exact: true })).toBeVisible();
  await expect(visiblePlaygroundList.getByText(/活动 Pre-commit/).first()).toBeVisible();
  await expect(visiblePlaygroundList.getByText('计算内容摘要')).toHaveCount(0);
  await expect(visiblePlaygroundList.getByText('一致性校验')).toHaveCount(0);

  await page.getByRole('button', { name: /夜间回归检查/ }).click();
  await expect(page.getByText('存在活动 Pre-commit', { exact: true })).toBeVisible();
  await expect(page.getByText('precommit-nightly-0729', { exact: true })).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath('active-precommit-controls.png'),
    animations: 'disabled',
    fullPage: true,
  });
  await page.getByRole('button', { name: '查看 Pre-commit' }).click();
  await expect(page).toHaveURL(
    /\/playgrounds\/nightly-review\/commit\?precommit_id=precommit-nightly-0729$/,
  );
  await expect(page.getByText('precommit-nightly-0729', { exact: true })).toBeVisible();
  await expect(page.locator('.preflight-status__body strong')).toContainText('可提交 · 处理完成');
  await expect(page.getByText('0 项阻断')).toBeVisible();

  await page.getByRole('button', { name: '重新检测' }).click();
  const restartDialog = page.locator('.el-message-box').filter({ hasText: '重新检测' });
  await restartDialog.getByRole('button', { name: '重新检测', exact: true }).click();
  await expect(page.getByText('precommit-nightly-0729')).toHaveCount(0);
  await expect(page.locator('.preflight-status__body small')).toContainText('precommit-');
  await expect(page.locator('.preflight-status__body strong')).toContainText('可提交 · 处理完成');
  const redetectedPreCommitId =
    (await page.locator('.preflight-status__body small').textContent())?.split(' · ')[0] ?? '';
  expect(redetectedPreCommitId).toMatch(/^precommit-/);
  await page.getByRole('button', { name: '填写 Commit 信息' }).click();
  const restartedCommitDialog = page.getByRole('dialog', { name: '创建 Commit' });
  await expect(restartedCommitDialog.locator('.tag-editor__values .el-tag')).toHaveCount(0);
  await restartedCommitDialog.getByRole('button', { name: '取消' }).click();

  await page.getByRole('button', { name: '取消 Pre-commit' }).click();
  const cancelDialog = page.locator('.el-message-box').filter({ hasText: '取消 Pre-commit' });
  await cancelDialog.getByRole('button', { name: '确认取消', exact: true }).click();
  await expect(page).toHaveURL(
    new RegExp(`/playgrounds/nightly-review/commit\\?precommit_id=${redetectedPreCommitId}$`),
  );
  await expect(page.locator('.preflight-status__body strong')).toContainText('已取消 · 处理完成');
  await expect(page.locator('.preflight-status__body small')).toContainText(
    `${redetectedPreCommitId} · attempt 1`,
  );
  await page.getByRole('button', { name: '失败重试' }).click();
  await expect(page.locator('.preflight-status__body small')).toContainText(
    `${redetectedPreCommitId} · attempt 2`,
  );
  await expect(page.locator('.preflight-status__body strong')).toContainText('可提交 · 处理完成');

  await page.goto(
    '/tenants/tenant-a/projects/project-language/artifacts/dialog-corpus/playgrounds/safety-review/commit',
  );
  await expect(page.getByRole('heading', { name: '没有活动 Pre-commit' })).toBeVisible();
  await expect(page.getByText('刷新此页面不会创建任务。')).toBeVisible();
  await expect(page.getByText(/precommit-/)).toHaveCount(0);

  await page.goto('/tenants/tenant-a/snapshots');
  const visibleSnapshotList = page.locator(
    '.resource-table:visible, .mobile-resource-list:visible',
  );
  await expect(visibleSnapshotList.getByText('创建中', { exact: true })).toBeVisible();
  await expect(visibleSnapshotList.getByText('可用', { exact: true }).first()).toBeVisible();
  await expect(visibleSnapshotList.getByText('异常', { exact: true })).toBeVisible();
  const regionalSnapshots = page
    .locator('.desktop-table .el-table__row')
    .filter({ hasText: 'commit-main-3' });
  await expect(regionalSnapshots).toHaveCount(2);
  await page.getByRole('button', { name: /snap-road-main3-sha-01/ }).click();
  await expect(page).toHaveURL(/\/snapshots\/snap-road-main3-sha-01$/);
  await expect(page.getByText('只读 · 可用', { exact: true })).toBeVisible();
  await expect(page.getByText('固定 Commit', { exact: true })).toBeVisible();
  await expect(page.getByText('dataset/v4', { exact: true })).toBeVisible();
  await expect(page.getByText(/refs\/heads/)).toHaveCount(0);
  await expect(page.getByText('12 GiB', { exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: '存储位置' })).toBeVisible();
  await expect(page.getByText('cn-shanghai', { exact: true }).first()).toBeVisible();
  await expect(page.getByText('cn-guangzhou', { exact: true })).toHaveCount(0);
  await page.screenshot({
    path: testInfo.outputPath('snapshot-detail.png'),
    animations: 'disabled',
    fullPage: true,
  });

  await page.getByRole('tab', { name: '文件' }).click();
  await expect(page.getByRole('heading', { name: 'Snapshot 文件' })).toBeVisible();
  const visibleSnapshotFile = page
    .locator('code:visible')
    .filter({ hasText: 'dataset/night-rain/part-0042.parquet' });
  await expect(visibleSnapshotFile).toBeVisible();
  await page.getByLabel('文件路径前缀').fill('dataset/night-rain/part-0042');
  await page.getByRole('button', { name: '查询', exact: true }).click();
  await expect(visibleSnapshotFile).toBeVisible();
  await expect(page.getByText('当前页 1 项')).toBeVisible();

  await page.getByRole('tab', { name: '活动' }).click();
  await expect(page.getByText('Snapshot 已创建', { exact: true })).toBeVisible();
  await expect(page.getByText('Snapshot 完整性校验通过并可读取')).toBeVisible();
  await page.getByRole('tab', { name: 'Dataset Profile' }).click();
  await expect(page.getByRole('heading', { name: 'Dataset Profile' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Schema' })).toBeVisible();
  await expect(page.getByRole('heading', { name: '统计与质量' })).toBeVisible();
  await expectNoOperatorDetails(page);
  await expectHealthyLayout(page);

  await page.goto(
    '/tenants/tenant-a/projects/project-vision/artifacts/road-scenes/snapshots/snap-road-main2-sha-01',
  );
  await expect(page.getByText('只读 · 异常', { exact: true })).toBeVisible();
  await expect(page.getByRole('tab', { name: '文件' })).toHaveClass(/is-disabled/);
  await page.getByRole('button', { name: '重试交付' }).click();
  await expect(page).toHaveURL(/\/snapshots\/snap-road-main2-sha-01$/);
  await expect(page.getByText('只读 · 可用', { exact: true })).toBeVisible();
  await expect(page.getByRole('tab', { name: '文件' })).not.toHaveClass(/is-disabled/);
  await expectNoOperatorDetails(page);
});

test('commits a Playground and delivers a fixed Snapshot', async ({ page }, testInfo) => {
  testInfo.setTimeout(45_000);
  await page.addInitScript(() => {
    const nativeFetch = window.fetch.bind(window);
    const trackedWindow = window as typeof window & {
      __snapshotCreateBodies?: string[];
    };
    trackedWindow.__snapshotCreateBodies = [];
    let rejectFirstCommit = true;
    let failFirstSnapshotCreate = true;

    window.fetch = async (input, init) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const url = new URL(request.url);
      if (url.pathname.endsWith('/api/playground/commit/create') && rejectFirstCommit) {
        rejectFirstCommit = false;
        return new Response(
          JSON.stringify({
            type: 'urn:neoengram:problem:head-commit-conflict',
            title: 'Head Commit conflict',
            status: 409,
            detail: 'The Playground Head changed after Pre-commit froze it',
            instance: url.pathname,
            code: 'HEAD_COMMIT_CONFLICT',
            request_id: 'req-e2e-head-conflict',
            retryable: false,
          }),
          {
            status: 409,
            headers: {
              'Content-Type': 'application/problem+json',
              'X-Request-ID': 'req-e2e-head-conflict',
            },
          },
        );
      }
      if (url.pathname.endsWith('/api/storage/volume/list/query')) {
        const response = await nativeFetch(request);
        const payload = (await response.clone().json()) as { items?: unknown[] };
        return new Response(
          JSON.stringify({
            ...payload,
            items: [
              ...(payload.items ?? []),
              {
                tenant_id: 'tenant-a',
                storage_volume_id: 'volume-e2e-unavailable',
                display_name: '离线交付 Volume',
                edge_cluster_id: 'cluster-e2e-offline',
                region: 'cn-hangzhou',
                backend_type: 'nfs',
                access_mode: 'read_only_many',
                state: 'unavailable',
                resource_version: '1',
                created_at_unix_ms: '1785167000000',
                updated_at_unix_ms: '1785167600000',
              },
            ],
          }),
          { status: response.status, headers: response.headers },
        );
      }
      if (url.pathname.endsWith('/api/snapshot/create')) {
        trackedWindow.__snapshotCreateBodies?.push(await request.clone().text());
        if (failFirstSnapshotCreate) {
          failFirstSnapshotCreate = false;
          return new Response(
            JSON.stringify({
              type: 'urn:neoengram:problem:authority-unavailable',
              title: 'Authority unavailable',
              status: 503,
              detail: 'The authority is temporarily unavailable',
              instance: url.pathname,
              code: 'AUTHORITY_UNAVAILABLE',
              request_id: 'req-e2e-snapshot-unavailable',
              retryable: true,
              retry_after_ms: '100',
            }),
            {
              status: 503,
              headers: {
                'Content-Type': 'application/problem+json',
                'X-Request-ID': 'req-e2e-snapshot-unavailable',
              },
            },
          );
        }
      }
      return nativeFetch(request);
    };
  });

  await page.goto(
    '/tenants/tenant-a/projects/project-vision/artifacts/road-scenes/playgrounds/labeling',
  );
  await page.getByRole('button', { name: '发起 Pre-commit', exact: true }).click();
  await expect(page).toHaveURL(
    /\/tenants\/tenant-a\/projects\/project-vision\/artifacts\/road-scenes\/playgrounds\/labeling\/commit\?precommit_id=precommit-[^&]+$/,
  );
  await expect(page.getByRole('heading', { name: '提交 Playground' })).toBeVisible();
  await expect(page.getByText('road-scenes / labeling', { exact: true })).toBeVisible();
  await expect(page.getByText('tenant-a / project-vision', { exact: true })).toBeVisible();
  await expect(page.locator('.preflight-status__body strong')).toContainText('可提交 · 处理完成');
  await expect(page.getByText('0 项阻断')).toBeVisible();
  await expect(page.getByText('Metadata 和逻辑路径检查通过')).toBeVisible();
  await expect(
    page.getByText('dataset/night-rain/part-0042.parquet', { exact: true }),
  ).toBeVisible();
  await expectNoOperatorDetails(page);
  await page.screenshot({
    path: testInfo.outputPath('precommit-ready.png'),
    animations: 'disabled',
    fullPage: true,
  });
  await page.getByRole('button', { name: '填写 Commit 信息' }).click();
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

  await expect(commitDialog.getByText('请求与当前状态冲突')).toBeVisible();
  await expect(commitDialog.getByText('HEAD_COMMIT_CONFLICT', { exact: true })).toBeVisible();
  await expect(commitDialog.getByLabel('Commit 标题')).toHaveValue('发布自动驾驶夜间场景 v4');
  await expect(commitDialog.getByText('dataset/v5', { exact: true })).toBeVisible();
  await commitDialog.getByRole('button', { name: '取消' }).click();
  await page.getByRole('button', { name: '重新检测' }).click();
  const redetectDialog = page.locator('.el-message-box').filter({ hasText: '重新检测' });
  await redetectDialog.getByRole('button', { name: '重新检测', exact: true }).click();
  await expect(page.locator('.preflight-status__body strong')).toContainText('可提交 · 处理完成');
  await page.getByRole('button', { name: '填写 Commit 信息' }).click();
  await expect(commitDialog.getByLabel('Commit 标题')).toHaveValue('发布自动驾驶夜间场景 v4');
  await expect(commitDialog.getByText('dataset/v5', { exact: true })).toBeVisible();
  await commitDialog.getByRole('button', { name: '确认 Commit' }).click();

  const commitResult = page.locator('.commit-result');
  await expect(
    commitResult.getByRole('heading', { name: '发布自动驾驶夜间场景 v4' }),
  ).toBeVisible();
  await expect(commitResult.getByText('dataset/v5', { exact: true })).toBeVisible();
  await commitResult.getByRole('button', { name: '创建 Snapshot' }).click();
  await expect(page).toHaveURL(/\/snapshots\/new\?commit_id=/);
  await expect(page.getByRole('heading', { name: '创建 Snapshot' })).toBeVisible();
  await expect(page.getByRole('heading', { name: '发布自动驾驶夜间场景 v4' })).toBeVisible();
  await expect(
    page.locator('.fixed-commit').getByText('dataset/v5', { exact: true }),
  ).toBeVisible();
  await page.getByRole('button', { name: '选择存储位置' }).click();

  await expect(page.getByRole('heading', { name: '选择存储位置' })).toBeVisible();
  const degradedVolume = page.getByRole('button', { name: /上海共享归档/ });
  await expect(degradedVolume).toBeVisible();
  await expect(degradedVolume).toBeDisabled();
  const unavailableVolume = page.getByRole('button', { name: /离线交付 Volume/ });
  await expect(unavailableVolume).toBeVisible();
  await expect(unavailableVolume).toBeDisabled();
  await page.getByRole('button', { name: /广州训练集交付 PVC/ }).click();
  await expect(
    page.getByLabel('Snapshot 创建摘要').getByText('volume-guangzhou-delivery', { exact: true }),
  ).toBeVisible();
  await expect(page.locator('.el-message')).toHaveCount(0);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: testInfo.outputPath('release-placement.png'),
    animations: 'disabled',
    fullPage: true,
  });
  await page.getByRole('button', { name: '创建 Snapshot', exact: true }).click();

  await expect(page.getByText('中心 authority 暂不可用')).toBeVisible();
  await expect(page.getByText('AUTHORITY_UNAVAILABLE', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: '重试', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Snapshot 已可用' })).toBeVisible();
  await expect(page.getByText('新请求', { exact: true })).toBeVisible();
  await expect(page.getByText('新建交付位置', { exact: true })).toBeVisible();
  await expect(page.getByText('cn-guangzhou', { exact: true })).toBeVisible();
  const snapshotCreateBodies = await page.evaluate(
    () =>
      (window as typeof window & { __snapshotCreateBodies?: string[] }).__snapshotCreateBodies ??
      [],
  );
  expect(snapshotCreateBodies).toHaveLength(2);
  expect(JSON.parse(snapshotCreateBodies[1] ?? '{}')).toEqual(
    JSON.parse(snapshotCreateBodies[0] ?? '{}'),
  );
  await expectNoOperatorDetails(page);
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
