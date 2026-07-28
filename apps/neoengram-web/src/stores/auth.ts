import { defineStore } from 'pinia';

import { getAuthService } from '@/auth/runtime';
import type { AuthUser } from '@/auth/types';

export const useAuthStore = defineStore('auth', {
  state: () => ({
    initialized: false,
    loading: false,
    user: null as AuthUser | null,
  }),
  getters: {
    authenticated: (state) => state.user !== null,
    displayName: (state) => state.user?.displayName ?? '未登录',
    mode: () => getAuthService().mode,
  },
  actions: {
    async initialize() {
      if (this.initialized) return;
      this.loading = true;
      try {
        this.user = await getAuthService().initialize();
        this.initialized = true;
      } finally {
        this.loading = false;
      }
    },
    async login(returnTo = window.location.pathname + window.location.search) {
      await getAuthService().login(returnTo);
      if (getAuthService().mode === 'mock') this.user = await getAuthService().initialize();
    },
    async handleCallback() {
      const result = await getAuthService().handleCallback();
      this.user = result.user;
      this.initialized = true;
      return result.returnTo;
    },
    async logout() {
      await getAuthService().logout();
      this.user = null;
    },
  },
});
