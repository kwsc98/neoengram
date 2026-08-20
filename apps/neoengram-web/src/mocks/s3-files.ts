import type { S3ObjectEntryView } from '../api/types';

export interface MockS3File extends S3ObjectEntryView {
  access_point_id: string;
  bucket_name: string;
  content_type: string;
  body: string;
}

// Keep the mock object catalog in one place so the control API and the local S3 listener cannot
// disagree about which bucket/key combinations are downloadable.
export const mockS3Files: MockS3File[] = [
  {
    access_point_id: 'ap-road-main3',
    bucket_name: 'road-scenes-snapshot',
    key: 'images/day/scene-001.jpg',
    entry_type: 'object',
    size_bytes: '5242880',
    etag: '"a1b2c3d4"',
    last_modified_unix_ms: '1785167600000',
    content_type: 'application/octet-stream',
    body: 'NeoEngram mock object: images/day/scene-001.jpg\n',
  },
  {
    access_point_id: 'ap-road-main3',
    bucket_name: 'road-scenes-snapshot',
    key: 'images/night/scene-001.jpg',
    entry_type: 'object',
    size_bytes: '7340032',
    etag: '"d4c3b2a1"',
    last_modified_unix_ms: '1785167600000',
    content_type: 'application/octet-stream',
    body: 'NeoEngram mock object: images/night/scene-001.jpg\n',
  },
  {
    access_point_id: 'ap-road-main3',
    bucket_name: 'road-scenes-snapshot',
    key: 'manifests/index.json',
    entry_type: 'object',
    size_bytes: '8192',
    etag: '"manifest-road-main3"',
    last_modified_unix_ms: '1785167600000',
    content_type: 'application/json',
    body: '{"snapshot":"snap-road-main3-sha-01","state":"ready"}\n',
  },
  {
    access_point_id: 'ap-road-main3',
    bucket_name: 'road-scenes-snapshot',
    key: 'README.md',
    entry_type: 'object',
    size_bytes: '1640',
    etag: '"readme-road-main3"',
    last_modified_unix_ms: '1785167600000',
    content_type: 'text/markdown; charset=utf-8',
    body: '# Road scenes\n\nThis is a mock S3 object served by the local Gateway profile.\n',
  },
];

export function mockS3Entries(accessPointId: string): S3ObjectEntryView[] {
  return mockS3Files
    .filter((file) => file.access_point_id === accessPointId)
    .map((file) => ({
      key: file.key,
      entry_type: file.entry_type,
      ...(file.size_bytes === undefined ? {} : { size_bytes: file.size_bytes }),
      ...(file.etag === undefined ? {} : { etag: file.etag }),
      ...(file.last_modified_unix_ms === undefined
        ? {}
        : { last_modified_unix_ms: file.last_modified_unix_ms }),
    }));
}

export function resolveMockS3Path(pathname: string): {
  bucketName: string;
  key: string;
  file?: MockS3File;
} | null {
  const segments = pathname.split('/').filter(Boolean);
  if (segments.length === 0) return null;

  let bucketName: string;
  let key: string;
  try {
    bucketName = decodeURIComponent(segments[0]!);
    key = decodeURIComponent(segments.slice(1).join('/'));
  } catch {
    return null;
  }
  if (!mockS3Files.some((file) => file.bucket_name === bucketName)) return null;
  if (!key || key.includes('\\') || key.split('/').some((part) => part === '.' || part === '..')) {
    return { bucketName, key };
  }
  const file = mockS3Files.find(
    (candidate) => candidate.bucket_name === bucketName && candidate.key === key,
  );
  return file ? { bucketName, key, file } : { bucketName, key };
}
