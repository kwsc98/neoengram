export function supportsResourceBrowser(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('resource_browser') ?? false;
}
