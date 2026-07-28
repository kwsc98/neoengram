import createClient from 'openapi-fetch';

import { getAuthService } from '@/auth/runtime';
import { runtimeConfig } from '@/config';

import type { paths } from './generated/openapi';

export const apiClient = createClient<paths>({ baseUrl: runtimeConfig.apiBaseUrl });

apiClient.use({
  async onRequest({ request }) {
    const requestUrl = new URL(request.url);
    request.headers.set('X-Request-ID', `req-${crypto.randomUUID()}`);
    if (
      requestUrl.pathname.startsWith('/api/') &&
      requestUrl.pathname !== '/api/system/version/query'
    ) {
      request.headers.set('NeoEngram-API-Version', '1');
      const token = await getAuthService().getAccessToken();
      if (token) request.headers.set('Authorization', `Bearer ${token}`);
    }
    return request;
  },
});
