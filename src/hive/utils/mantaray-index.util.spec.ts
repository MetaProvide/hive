import { describe, expect, it } from 'vitest';
import { resolveWebsiteIndexDocument } from './mantaray-index.util';

describe('resolveWebsiteIndexDocument', () => {
  it('prefers index.html when present', () => {
    expect(
      resolveWebsiteIndexDocument(['views/home.html', 'index.html', 'a.css']),
    ).toBe('index.html');
  });

  it('prefers root index.html over deeper paths that sort first alphabetically', () => {
    expect(
      resolveWebsiteIndexDocument([
        'admin/index.html',
        'index.html',
        'views/home.html',
      ]),
    ).toBe('index.html');
  });

  it('prefers shallowest nested index.html when no root index', () => {
    expect(
      resolveWebsiteIndexDocument([
        'views/deep/index.html',
        'views/index.html',
        'views/home.html',
      ]),
    ).toBe('views/index.html');
  });

  it('uses views/home.html when no root index', () => {
    expect(
      resolveWebsiteIndexDocument([
        'css/theme.css',
        'views/home.html',
        'js/app.js',
      ]),
    ).toBe('views/home.html');
  });

  it('returns null when no html paths', () => {
    expect(resolveWebsiteIndexDocument(['a.css', 'b.js'])).toBeNull();
  });
});
