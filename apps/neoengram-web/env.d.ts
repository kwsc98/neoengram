/// <reference types="vite/client" />

declare module '*.vue' {
  import type { DefineComponent } from 'vue';

  const component: DefineComponent;
  export default component;
}

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_S3_ENDPOINT?: string;
  readonly VITE_GATEWAY_ENDPOINT?: string;
  readonly VITE_GATEWAY_WORKLOAD_TRUST_DOMAIN?: string;
  readonly VITE_GATEWAY_EDGE_CLUSTER_ID?: string;
  readonly VITE_API_MODE?: 'mock' | 'real';
  readonly VITE_AUTH_MODE?: 'mock' | 'development' | 'oidc';
  readonly VITE_E2E_GATEWAY_MOCKS?: 'true';
  readonly VITE_LOCAL_GATEWAY_PROFILE?: 'true';
  readonly VITE_DEVELOPMENT_TOKEN?: string;
  readonly VITE_DEVELOPMENT_PRINCIPAL?: string;
  readonly VITE_OIDC_AUTHORITY?: string;
  readonly VITE_OIDC_CLIENT_ID?: string;
  readonly VITE_OIDC_SCOPE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
