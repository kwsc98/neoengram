import { defineStore } from 'pinia';

const STORAGE_KEY = 'neoengram.recent-jobs.v1';
const MAX_RECENT_JOBS = 50;

export interface RecentJob {
  tenantId: string;
  jobId: string;
  lastSeen: string;
}

function loadRecentJobs(): RecentJob[] {
  try {
    const parsed: unknown = JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? '[]');
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(
        (item): item is RecentJob =>
          typeof item === 'object' &&
          item !== null &&
          typeof (item as RecentJob).tenantId === 'string' &&
          typeof (item as RecentJob).jobId === 'string' &&
          typeof (item as RecentJob).lastSeen === 'string',
      )
      .slice(0, MAX_RECENT_JOBS);
  } catch {
    return [];
  }
}

export const useRecentJobsStore = defineStore('recent-jobs', {
  state: () => ({ jobs: loadRecentJobs() }),
  actions: {
    remember(tenantId: string, jobId: string) {
      const next = this.jobs.filter((item) => item.tenantId !== tenantId || item.jobId !== jobId);
      next.unshift({ tenantId, jobId, lastSeen: new Date().toISOString() });
      this.jobs = next.slice(0, MAX_RECENT_JOBS);
      this.persist();
    },
    clear() {
      this.jobs = [];
      this.persist();
    },
    clearTenant(tenantId: string) {
      this.jobs = this.jobs.filter((item) => item.tenantId !== tenantId);
      this.persist();
    },
    persist() {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(this.jobs));
    },
  },
});
