export function commitTagNames(tagNames: readonly string[]): string[] {
  return [...tagNames];
}

export function commitDataLayoutLabel(dataLayout: 'fast_cdc' | 'whole_file'): string {
  return dataLayout === 'whole_file' ? '全文件' : '分块';
}
