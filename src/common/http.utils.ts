/**
 * Build a safe Content-Disposition header value.
 *
 * Produces both an ASCII-safe `filename` parameter and an RFC 5987
 * `filename*` parameter so browsers can display the original name
 * regardless of special characters.
 */
export function contentDisposition(
  filename: string,
  disposition: 'inline' | 'attachment' = 'inline',
): string {
  const ascii = filename.replace(/[^\x20-\x7E]/g, '_').replace(/"/g, '\\"');
  const encoded = encodeURIComponent(filename);
  return `${disposition}; filename="${ascii}"; filename*=UTF-8''${encoded}`;
}
