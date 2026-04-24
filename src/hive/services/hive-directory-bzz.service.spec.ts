import { ConfigService } from '../../config/config.service';
import { DriveService } from './drive.service';
import { FileIndexService } from './file-index.service';
import {
  HiveDirectoryBzzService,
  parseIpfsDirectoryJson,
  parseLegacyDirectoryMarker,
} from './hive-directory-bzz.service';
import { SwarmBridgeService } from './swarm-bridge.service';

describe('parseLegacyDirectoryMarker', () => {
  it('extracts root CID after hive marker prefix', () => {
    const root = 'QmdDbLyNKCwtBVFhEVQfdBTCnEWmWRJGnm93qa3kzBjVym';
    const buf = Buffer.concat([
      Buffer.from('hive-ipfs-directory\0', 'utf8'),
      Buffer.from(root, 'utf8'),
    ]);
    expect(buf.length).toBe(66);
    expect(parseLegacyDirectoryMarker(buf)).toBe(root);
    expect(parseLegacyDirectoryMarker(Buffer.from('{}'))).toBeNull();
  });
});

describe('parseIpfsDirectoryJson', () => {
  it('parses valid path -> cid map', () => {
    const buf = Buffer.from(
      JSON.stringify({
        '/': {
          cid: 'bafybeiddnr2jz65byk67sjt6jsu6g7tueddr7odhzzpzli3rgudlbnc6iq',
        },
        '/file1.txt': { cid: 'QmFoo' },
        '/subdir/file2.png': { cid: 'QmBar' },
      }),
    );
    const parsed = parseIpfsDirectoryJson(buf);
    expect(parsed).not.toBeNull();
    expect(parsed?.['/file1.txt']?.cid).toBe('QmFoo');
    expect(parsed?.['/subdir/file2.png']?.cid).toBe('QmBar');
  });

  it('rejects non-objects and bad cid', () => {
    expect(parseIpfsDirectoryJson(Buffer.from('[]'))).toBeNull();
    expect(parseIpfsDirectoryJson(Buffer.from('{}'))).toBeNull();
    expect(
      parseIpfsDirectoryJson(
        Buffer.from(JSON.stringify({ '/a': { cid: '' } })),
      ),
    ).toBeNull();
    expect(
      parseIpfsDirectoryJson(
        Buffer.from(JSON.stringify({ '/a': { notcid: 'x' } })),
      ),
    ).toBeNull();
  });
});

describe('HiveDirectoryBzzService', () => {
  it('isDirectoryMapContent is true only for directory type and valid JSON', () => {
    const service = new HiveDirectoryBzzService(
      {} as ConfigService,
      {} as DriveService,
      {} as FileIndexService,
      {} as SwarmBridgeService,
    );
    const map = Buffer.from(JSON.stringify({ '/a': { cid: 'QmX' } }));
    expect(
      service.isDirectoryMapContent(
        'application/vnd.hive.ipfs-directory+json',
        map,
      ),
    ).toBe(true);
    expect(service.isDirectoryMapContent('application/json', map)).toBe(false);
    expect(
      service.isDirectoryMapContent(
        'application/vnd.hive.ipfs-directory+json',
        Buffer.from('not json'),
      ),
    ).toBe(false);
    const legacy = Buffer.concat([
      Buffer.from('hive-ipfs-directory\0', 'utf8'),
      Buffer.from('QmX', 'utf8'),
    ]);
    expect(
      service.isDirectoryMapContent(
        'application/vnd.hive.ipfs-directory+json',
        legacy,
      ),
    ).toBe(true);
  });

  it('uploads raw leaf bytes for manual mantaray forks instead of reusing file-level bzz refs', async () => {
    const driveService = {
      get: vi.fn().mockResolvedValue({ content: Buffer.from('index html') }),
    } as unknown as DriveService;
    const service = new HiveDirectoryBzzService(
      {
        beePostageStamp: 'batch123',
      } as ConfigService,
      driveService,
      {} as FileIndexService,
      {} as SwarmBridgeService,
    );
    const bee = {
      uploadData: vi.fn().mockResolvedValue({
        reference: { toHex: () => 'bytesRef123' },
      }),
    };

    const ref = await (service as any).ensureLeafFileOnBee(bee, {
      checksum: 'abc123',
      size: 10,
      contentType: 'text/html',
      timestamp: 1,
      ipfsCid: 'QmIndex',
      bzzHash: 'singleFileManifestRef',
    });

    expect(ref).toBe('bytesRef123');
    expect((driveService.get as any).mock.calls[0][0]).toBe('abc123');
    expect(bee.uploadData).toHaveBeenCalledWith(
      'batch123',
      Buffer.from('index html'),
      { pin: true },
    );
  });
});
