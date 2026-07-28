export function formatTime(value?: string): string {
  if (!value) return '—';
  return new Intl.DateTimeFormat('zh-CN', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(Number(value)));
}

export function formatCount(value?: string): string {
  if (!value) return '—';
  return BigInt(value).toLocaleString('zh-CN');
}

export function formatBytes(value?: string): string {
  if (!value) return '—';
  const bytes = BigInt(value);
  const units = [
    ['TiB', 1024n ** 4n],
    ['GiB', 1024n ** 3n],
    ['MiB', 1024n ** 2n],
    ['KiB', 1024n],
  ] as const;
  for (const [unit, divisor] of units) {
    if (bytes >= divisor) return `${Number((bytes * 10n) / divisor) / 10} ${unit}`;
  }
  return `${bytes.toLocaleString('zh-CN')} B`;
}

export function shortId(value: string, width = 18): string {
  if (value.length <= width) return value;
  return `${value.slice(0, width - 3)}...`;
}
