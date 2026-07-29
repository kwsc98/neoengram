const INTERNAL_TAG_PREFIX = 'refs/tags/';

export function commitTagNames(referenceNames: readonly string[]): string[] {
  return referenceNames
    .filter((name) => name.startsWith(INTERNAL_TAG_PREFIX))
    .map((name) => name.slice(INTERNAL_TAG_PREFIX.length));
}
