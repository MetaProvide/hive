import {
  parseKuboUnixfsTypeFromFilesStat,
  kuboUnixfsTypeForCid,
} from './kubo-unixfs.util';
import {
  BadRequestException,
  ServiceUnavailableException,
} from '@nestjs/common';

describe('parseKuboUnixfsTypeFromFilesStat', () => {
  it('maps directory and file (case-insensitive)', () => {
    expect(parseKuboUnixfsTypeFromFilesStat({ Type: 'directory' })).toBe(
      'directory',
    );
    expect(parseKuboUnixfsTypeFromFilesStat({ Type: 'FILE' })).toBe('file');
  });

  it('returns null for unknown or missing Type', () => {
    expect(parseKuboUnixfsTypeFromFilesStat({ Type: 'symlink' })).toBeNull();
    expect(parseKuboUnixfsTypeFromFilesStat({})).toBeNull();
    expect(parseKuboUnixfsTypeFromFilesStat(null)).toBeNull();
  });
});

describe('kuboUnixfsTypeForCid', () => {
  it('parses successful Kubo JSON', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      text: async () => JSON.stringify({ Type: 'directory' }),
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(
      kuboUnixfsTypeForCid('http://127.0.0.1:5001', 5000, 'QmTest'),
    ).resolves.toBe('directory');

    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/api/v0/files/stat'),
      expect.objectContaining({ method: 'POST' }),
    );
    vi.unstubAllGlobals();
  });

  it('throws BadRequest on HTTP error', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        text: async () => 'boom',
      }),
    );

    await expect(
      kuboUnixfsTypeForCid('http://127.0.0.1:5001', 5000, 'QmX'),
    ).rejects.toBeInstanceOf(BadRequestException);

    vi.unstubAllGlobals();
  });

  it('throws ServiceUnavailable when fetch fails (e.g. Kubo down)', async () => {
    const cause = new Error('connect ECONNREFUSED');
    const err = new TypeError('fetch failed');
    (err as Error & { cause?: Error }).cause = cause;
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(err));

    await expect(
      kuboUnixfsTypeForCid('http://127.0.0.1:5001', 5000, 'QmX'),
    ).rejects.toBeInstanceOf(ServiceUnavailableException);

    vi.unstubAllGlobals();
  });
});
