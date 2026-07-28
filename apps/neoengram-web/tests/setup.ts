import '@testing-library/jest-dom/vitest';

import { afterAll, afterEach } from 'vitest';

import { resetMockState } from '@/mocks/handlers';

import { server } from './support/server';

function createStorage(): Storage {
  const values = new Map<string, string>();
  return {
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => [...values.keys()][index] ?? null,
    removeItem: (key) => values.delete(key),
    setItem: (key, value) => values.set(key, value),
  };
}

Object.defineProperties(window, {
  localStorage: { configurable: true, value: createStorage() },
  sessionStorage: { configurable: true, value: createStorage() },
});

server.listen({ onUnhandledRequest: 'error' });

afterEach(() => {
  server.resetHandlers();
  resetMockState();
  window.localStorage.clear();
  window.sessionStorage.clear();
});

afterAll(() => server.close());
