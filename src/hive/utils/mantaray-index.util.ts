/**
 * Chooses a default document for mantaray `website-index-document` when the user
 * has no `index.html` at the collection root (common for apps using `views/home.html`).
 *
 * Root `index.html` must win over deeper nested `index.html` paths — sorted order alone
 * would wrongly prefer e.g. `admin/index.html` before root `index.html`.
 */
export function resolveWebsiteIndexDocument(relPaths: string[]): string | null {
  const sorted = [...new Set(relPaths.filter(Boolean))].sort((a, b) =>
    a.localeCompare(b),
  );

  const rootFile = (name: string): string | undefined =>
    sorted.find((p) => {
      const parts = p.split('/');
      return (
        parts.length === 1 &&
        parts[0].toLowerCase() === name.toLowerCase()
      );
    });

  const rHtml = rootFile('index.html');
  if (rHtml) {
    return rHtml;
  }
  const rHtm = rootFile('index.htm');
  if (rHtm) {
    return rHtm;
  }

  const shallowest = (
    pred: (p: string) => boolean,
  ): string | null => {
    const c = sorted.filter(pred);
    if (!c.length) {
      return null;
    }
    c.sort((a, b) => {
      const d = a.split('/').length - b.split('/').length;
      return d !== 0 ? d : a.localeCompare(b);
    });
    return c[0] ?? null;
  };

  const nestedIdx = shallowest((p) => /(^|\/)index\.html$/i.test(p));
  if (nestedIdx) {
    return nestedIdx;
  }
  const nestedHtm = shallowest((p) => /(^|\/)index\.htm$/i.test(p));
  if (nestedHtm) {
    return nestedHtm;
  }

  const htmlPaths = sorted.filter((p) => /\.html?$/i.test(p));
  if (!htmlPaths.length) {
    return null;
  }
  const homeHtml = htmlPaths.find((p) => /(^|\/)home\.html$/i.test(p));
  if (homeHtml) {
    return homeHtml;
  }
  return htmlPaths[0] ?? null;
}
