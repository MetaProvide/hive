import { Test, type TestingModule } from '@nestjs/testing';
import type { ContentMetadataDto } from '../dto/content';
import { DriveService } from './drive.service';
import { FileIndexService } from './file-index.service';
import { IdentityService } from './identity.service';

describe('FileIndexService', () => {
  let service: FileIndexService;
  let mockDrive: any;
  const mockDriveService = {
    updateBridgeMetadata: vi.fn(),
    delete: vi.fn(),
  };

  const mockIdentityService = {
    getDrive: vi.fn(),
  };

  beforeEach(async () => {
    mockDrive = {
      put: vi.fn().mockResolvedValue(undefined),
      get: vi.fn().mockResolvedValue(null),
    };

    mockIdentityService.getDrive.mockReturnValue(mockDrive);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        FileIndexService,
        { provide: IdentityService, useValue: mockIdentityService },
        { provide: DriveService, useValue: mockDriveService },
      ],
    }).compile();

    service = module.get<FileIndexService>(FileIndexService);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('addOrUpdate', () => {
    it('should add new entry', async () => {
      const metadata: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
      };

      await service.addOrUpdate(metadata);

      expect(service.has('abc123')).toBe(true);
      expect(service.getByChecksum('abc123')).toEqual(metadata);
    });

    it('should update existing entry', async () => {
      const original: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
      };

      const updated: ContentMetadataDto = {
        ...original,
        ipfsCid: 'QmNewCid',
      };

      await service.addOrUpdate(original);
      await service.addOrUpdate(updated);

      const result = service.getByChecksum('abc123');
      expect(result?.ipfsCid).toBe('QmNewCid');
    });

    it('should merge ipfsCid from existing entry', async () => {
      const original: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
        ipfsCid: 'QmOriginal',
      };

      const updated: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
        bzzHash: 'bzzNew',
      };

      await service.addOrUpdate(original);
      await service.addOrUpdate(updated);

      const result = service.getByChecksum('abc123');
      expect(result?.ipfsCid).toBe('QmOriginal');
      expect(result?.bzzHash).toBe('bzzNew');
    });

    it('should merge manifestBzzHash and prefer it for getBzzHashByIpfsCid', async () => {
      await service.addOrUpdate({
        checksum: 'a1',
        size: 10,
        contentType: 'text/html',
        timestamp: 1,
        ipfsCid: 'QmRoot',
        bzzHash: 'leafChunk',
      });
      await service.addOrUpdate({
        checksum: 'a1',
        size: 10,
        contentType: 'text/html',
        timestamp: 1,
        manifestBzzHash: 'manifestRoot',
      });
      expect(service.getBzzHashByIpfsCid('QmRoot')).toBe('manifestRoot');
      expect(service.getByBzzHash('leafChunk')?.checksum).toBe('a1');
      expect(service.getByBzzHash('manifestRoot')?.checksum).toBe('a1');
    });

    it('should save to drive after update', async () => {
      const metadata: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
      };

      await service.addOrUpdate(metadata);

      expect(mockDrive.put).toHaveBeenCalledWith(
        '/index/files.json',
        expect.any(Buffer),
      );
    });

    it('should index bare root ipfsCid on anchor file row', async () => {
      await service.addOrUpdate({
        checksum: 'anch1',
        size: 1800,
        contentType: 'text/html',
        timestamp: Date.now(),
        ipfsCid: 'QmDirRoot',
        bzzHash: 'manifest',
      });

      expect(service.getByIpfsCid('QmDirRoot')?.checksum).toBe('anch1');
      expect(service.getDirectoryChecksumByRootCid('QmDirRoot')).toBe('anch1');
      expect(service.getBzzHashByIpfsCid('QmDirRoot')).toBe('manifest');
    });
  });

  describe('remove', () => {
    it('should remove existing entry', async () => {
      const metadata: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
      };

      await service.addOrUpdate(metadata);
      const result = await service.remove('abc123');

      expect(result).toBe(true);
      expect(service.has('abc123')).toBe(false);
    });

    it('should return false for non-existent entry', async () => {
      const result = await service.remove('nonexistent');

      expect(result).toBe(false);
    });

    it('should remove from all indexes', async () => {
      const metadata: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
        ipfsCid: 'QmTest',
        bzzHash: 'bzzTest',
      };

      await service.addOrUpdate(metadata);
      await service.remove('abc123');

      expect(service.hasIpfsCid('QmTest')).toBe(false);
      expect(service.hasBzzHash('bzzTest')).toBe(false);
    });
  });

  describe('getByIpfsCid', () => {
    it('should return metadata by IPFS CID', async () => {
      const metadata: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
        ipfsCid: 'QmTest123',
      };

      await service.addOrUpdate(metadata);

      const result = service.getByIpfsCid('QmTest123');
      expect(result).toEqual(metadata);
    });

    it('should return undefined for non-existent CID', () => {
      const result = service.getByIpfsCid('QmNonExistent');
      expect(result).toBeUndefined();
    });
  });

  describe('getByBzzHash', () => {
    it('should return metadata by Swarm hash', async () => {
      const metadata: ContentMetadataDto = {
        checksum: 'abc123',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
        bzzHash: 'bzzHash123',
      };

      await service.addOrUpdate(metadata);

      const result = service.getByBzzHash('bzzHash123');
      expect(result).toEqual(metadata);
    });
  });

  describe('getAll', () => {
    it('should return all entries', async () => {
      const items: ContentMetadataDto[] = [
        {
          checksum: 'abc1',
          size: 100,
          contentType: 'text/plain',
          timestamp: Date.now(),
        },
        {
          checksum: 'abc2',
          size: 200,
          contentType: 'text/html',
          timestamp: Date.now(),
        },
      ];

      for (const item of items) {
        await service.addOrUpdate(item);
      }

      const result = service.getAll();
      expect(result.length).toBe(2);
    });
  });

  describe('getCount', () => {
    it('should return correct count', async () => {
      expect(service.getCount()).toBe(0);

      await service.addOrUpdate({
        checksum: 'abc1',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
      });

      expect(service.getCount()).toBe(1);

      await service.addOrUpdate({
        checksum: 'abc2',
        size: 200,
        contentType: 'text/html',
        timestamp: Date.now(),
      });

      expect(service.getCount()).toBe(2);
    });
  });

  describe('clear', () => {
    it('should clear all entries', async () => {
      await service.addOrUpdate({
        checksum: 'abc1',
        size: 100,
        contentType: 'text/plain',
        timestamp: Date.now(),
        ipfsCid: 'QmTest',
      });

      await service.clear();

      expect(service.getCount()).toBe(0);
      expect(service.hasIpfsCid('QmTest')).toBe(false);
    });
  });

  describe('load', () => {
    it('should load index from drive', async () => {
      const items: ContentMetadataDto[] = [
        {
          checksum: 'abc1',
          size: 100,
          contentType: 'text/plain',
          timestamp: Date.now(),
          ipfsCid: 'QmTest1',
        },
      ];

      mockDrive.get.mockResolvedValueOnce(Buffer.from(JSON.stringify(items)));

      await service.load();

      expect(service.getCount()).toBe(1);
      expect(service.has('abc1')).toBe(true);
      expect(service.hasIpfsCid('QmTest1')).toBe(true);
    });

    it('should migrate legacy synthetic directory row onto anchor', async () => {
      const items: ContentMetadataDto[] = [
        {
          checksum: 'dirchk',
          size: 66,
          contentType: 'application/vnd.hive.ipfs-directory+json',
          timestamp: 1,
          ipfsCid: 'QmDirRoot',
          contentKind: 'directory',
          bzzHash: 'manifestFromDir',
        },
        {
          checksum: 'anchchk',
          size: 1800,
          contentType: 'text/html',
          timestamp: 2,
          ipfsCid: 'QmDirRoot',
        },
      ];

      mockDriveService.updateBridgeMetadata.mockResolvedValue({
        ...items[1],
        bzzHash: 'manifestFromDir',
      });
      mockDrive.get.mockResolvedValueOnce(Buffer.from(JSON.stringify(items)));

      await service.load();

      expect(mockDriveService.updateBridgeMetadata).toHaveBeenCalledWith(
        'anchchk',
        expect.objectContaining({
          ipfsCid: 'QmDirRoot',
          bzzHash: 'manifestFromDir',
        }),
      );
      expect(mockDriveService.delete).toHaveBeenCalledWith('dirchk', {
        preserveIndexedRefs: true,
      });
      expect(service.has('dirchk')).toBe(false);
      expect(service.getByIpfsCid('QmDirRoot')?.checksum).toBe('anchchk');
      expect(service.getByIpfsCid('QmDirRoot')?.bzzHash).toBe(
        'manifestFromDir',
      );
      expect(service.getByIpfsCid('QmDirRoot')?.manifestBzzHash).toBeUndefined();
    });

    it('should handle missing index file', async () => {
      mockDrive.get.mockRejectedValueOnce(new Error('Not found'));

      await service.load();

      expect(service.getCount()).toBe(0);
    });
  });
});
