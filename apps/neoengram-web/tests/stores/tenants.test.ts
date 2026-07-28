import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it } from 'vitest';

import { useTenantsStore } from '@/stores/tenants';

describe('Tenant context store', () => {
  beforeEach(() => setActivePinia(createPinia()));

  it('loads visible tenants and persists only the selected Tenant ID', async () => {
    const store = useTenantsStore();
    await store.load();
    store.remember('tenant-b');

    expect(store.items).toHaveLength(2);
    expect(store.canCreateTenant).toBe(true);
    expect(store.lastTenantId).toBe('tenant-b');
    expect(window.localStorage.getItem('neoengram.last-tenant.v1')).toBe('tenant-b');
    expect(window.localStorage.getItem('neoengram.last-tenant.v1')).not.toContain('permissions');
  });

  it('adds a newly created empty Tenant to the switcher state', async () => {
    const store = useTenantsStore();
    const result = await store.create({ tenant_id: 'tenant-new', display_name: '新租户' });

    expect(result.data.replayed).toBe(false);
    expect(store.byId('tenant-new')?.display_name).toBe('新租户');
    expect(store.lastTenantId).toBe('tenant-new');
  });
});
