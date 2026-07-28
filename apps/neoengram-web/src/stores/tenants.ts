import { defineStore } from 'pinia';

import { createTenant, queryTenant, queryTenantList } from '@/api/operations';
import type { CreateTenantRequest, TenantView } from '@/api/types';

const STORAGE_KEY = 'neoengram.last-tenant.v1';

function loadLastTenantId(): string | null {
  const value = window.localStorage.getItem(STORAGE_KEY);
  return value?.trim() || null;
}

export const useTenantsStore = defineStore('tenants', {
  state: () => ({
    items: [] as TenantView[],
    canCreateTenant: false,
    nextCursor: undefined as string | undefined,
    searchQuery: '',
    lastTenantId: loadLastTenantId(),
    loaded: false,
    loading: false,
  }),
  getters: {
    byId: (state) => (tenantId: string) =>
      state.items.find((tenant) => tenant.tenant_id === tenantId),
  },
  actions: {
    remember(tenantId: string) {
      this.lastTenantId = tenantId;
      window.localStorage.setItem(STORAGE_KEY, tenantId);
    },
    upsert(tenant: TenantView) {
      const index = this.items.findIndex((item) => item.tenant_id === tenant.tenant_id);
      if (index === -1) this.items.push(tenant);
      else this.items[index] = tenant;
      this.items.sort((left, right) =>
        left.display_name.localeCompare(right.display_name, 'zh-CN'),
      );
    },
    async load(query = '', append = false) {
      this.loading = true;
      try {
        const result = await queryTenantList({
          page_size: 50,
          ...(query ? { query } : {}),
          ...(append && this.nextCursor ? { cursor: this.nextCursor } : {}),
        });
        const retained = query
          ? this.items.filter((tenant) => tenant.tenant_id === this.lastTenantId)
          : [];
        const nextItems = append
          ? [...this.items, ...result.data.items]
          : [...retained, ...result.data.items];
        this.items = [...new Map(nextItems.map((tenant) => [tenant.tenant_id, tenant])).values()];
        this.canCreateTenant = result.data.can_create_tenant;
        this.nextCursor = result.data.next_cursor;
        this.searchQuery = query;
        this.loaded = true;
        return result.data;
      } finally {
        this.loading = false;
      }
    },
    async loadMore() {
      if (!this.nextCursor || this.loading) return;
      await this.load(this.searchQuery, true);
    },
    async ensure(tenantId: string) {
      const cached = this.byId(tenantId);
      if (cached) return cached;
      const result = await queryTenant(tenantId);
      this.upsert(result.data.tenant);
      return result.data.tenant;
    },
    async create(request: CreateTenantRequest) {
      const result = await createTenant(request);
      this.upsert(result.data.tenant);
      this.remember(result.data.tenant.tenant_id);
      return result;
    },
  },
});
