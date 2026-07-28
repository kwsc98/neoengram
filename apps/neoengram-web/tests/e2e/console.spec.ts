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
  await expect(page.getByText('可接收请求')).toBeVisible();

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
  await expect(page.getByRole('heading', { name: 'Artifacts' })).toBeVisible();
  await page.getByRole('button', { name: /道路场景数据集/ }).click();
  await expect(page).toHaveURL(/\/projects\/project-vision\/artifacts\/road-scenes$/);

  await page.getByRole('tab', { name: 'Commits' }).click();
  await expect(page).toHaveURL(/tab=commits/);
  await expect(page.getByText('补充夜间道路场景', { exact: true })).toBeVisible();
  await expect(page.getByText(/experiment/).first()).toBeVisible();

  await page.getByRole('tab', { name: 'Playgrounds' }).click();
  await page.getByText('标注工作区', { exact: true }).click();
  await expect(page.getByText('Index revision')).toBeVisible();
  await expect(page.getByText('commit-main-3', { exact: true })).toBeVisible();
  await expectHealthyLayout(page);
  await page.screenshot({
    path: testInfo.outputPath('playground-detail.png'),
    animations: 'disabled',
    fullPage: true,
  });
});

test('browses Tenant-wide Playground and Snapshot details', async ({ page }) => {
  await page.goto('/tenants/tenant-a/playgrounds');
  await page.getByRole('button', { name: /安全标注复核/ }).click();
  await expect(page).toHaveURL(/\/playgrounds\/safety-review$/);
  await expect(page.getByText('dialog-corpus', { exact: true })).toBeVisible();

  await page.goto('/tenants/tenant-a/snapshots');
  await page.getByRole('button', { name: /补充夜间道路场景/ }).click();
  await expect(page).toHaveURL(/\/snapshots\/commit-main-3$/);
  await expect(page.getByText('Snapshot 没有独立 snapshot_id')).toBeVisible();
  await expect(page.getByText('12 GiB', { exact: true })).toBeVisible();
  await expectHealthyLayout(page);
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
  await page.getByRole('button', { name: '创建 Job' }).click();
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
  await page.getByRole('button', { name: '创建 Job' }).click();
  await expect(page.getByText('请求未通过校验')).toBeVisible();
  await expect(page.getByText('PROTOCOL_INVALID')).toBeVisible();

  await page.goto('/tenants/tenant-unavailable/jobs/new');
  await page.getByRole('button', { name: '创建 Job' }).click();
  await expect(page.getByText('中心 authority 暂不可用')).toBeVisible();
  await expect(page.getByRole('button', { name: '重试' })).toBeVisible();

  await page.goto('/tenants/tenant-secret/artifacts');
  await expect(page).toHaveURL(/\/tenants\/tenant-a\/overview$/);
  await expectHealthyLayout(page);
});
