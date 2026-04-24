import { Test, type TestingModule } from '@nestjs/testing';
import { DriveService } from './drive.service';
import { IdentityService } from './identity.service';

describe('DriveService', () => {
  let service: DriveService;
  let mockDrive: any;

  const mockIdentityService = {
    getDrive: vi.fn(),
  };

  beforeEach(async () => {
    // Create mock drive
    mockDrive = {
      put: vi.fn().mockResolvedValue(undefined),
      get: vi.fn().mockResolvedValue(null),
      del: vi.fn().mockResolvedValue(undefined),
      entry: vi.fn().mockResolvedValue(null),
      list: vi.fn().mockReturnValue({
        [Symbol.asyncIterator]: async function* () {},
      }),
    };

    mockIdentityService.getDrive.mockReturnValue(mockDrive);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DriveService,
        { provide: IdentityService, useValue: mockIdentityService },
      ],
    }).compile();

    service = module.get<DriveService>(DriveService);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('calculateChecksum', () => {
    it('should calculate SHA256 checksum', () => {
      const content = Buffer.from('Hello World');
      const checksum = service.calculateChecksum(content);

      expect(checksum).toBe(
        'a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e',
      );
    });

    it('should return same checksum for same content', () => {
      const content = Buffer.from('Test content');
      const checksum1 = service.calculateChecksum(content);
      const checksum2 = service.calculateChecksum(content);

      expect(checksum1).toBe(checksum2);
    });

    it('should return different checksum for different content', () => {
      const content1 = Buffer.from('Content A');
      const content2 = Buffer.from('Content B');

      expect(service.calculateChecksum(content1)).not.toBe(
        service.calculateChecksum(content2),
      );
    });
  });

  describe('put', () => {
    it('should store content and metadata', async () => {
      const content = Buffer.from('Test content');
      const dto = {
        content,
        contentType: 'text/plain',
      };

      const result = await service.put(dto);

      expect(result.checksum).toBeDefined();
      expect(result.size).toBe(content.length);
      expect(result.contentType).toBe('text/plain');
      expect(result.timestamp).toBeDefined();
      expect(mockDrive.put).toHaveBeenCalled();
    });

    it('should not re-store existing content', async () => {
      mockDrive.entry.mockResolvedValue({ value: {} }); // Content exists

      const content = Buffer.from('Existing content');
      await service.put({ content, contentType: 'text/plain' });

      // Should only be called for metadata, not content
      const putCalls = mockDrive.put.mock.calls;
      const contentPutCalls = putCalls.filter((call: string[]) =>
        call[0].startsWith('/content/'),
      );
      expect(contentPutCalls.length).toBe(0);
    });

    it('should create IPFS reference when cid provided', async () => {
      const content = Buffer.from('IPFS content');
      const ipfsCid = 'QmTest123';

      await service.put({ content, ipfsCid });

      const putCalls = mockDrive.put.mock.calls;
      const refPut = putCalls.find((call: string[]) =>
        call[0].includes('/refs/ipfs/'),
      );
      expect(refPut).toBeDefined();
      expect(refPut[0]).toContain(ipfsCid);
    });

    it('should create Swarm reference when hash provided', async () => {
      const content = Buffer.from('Swarm content');
      const bzzHash = 'abc123hash';

      await service.put({ content, bzzHash });

      const putCalls = mockDrive.put.mock.calls;
      const refPut = putCalls.find((call: string[]) =>
        call[0].includes('/refs/bzz/'),
      );
      expect(refPut).toBeDefined();
      expect(refPut[0]).toContain(bzzHash);
    });
  });

  describe('get', () => {
    it('should return null for non-existent content', async () => {
      mockDrive.get.mockResolvedValue(null);

      const result = await service.get('nonexistent');

      expect(result).toBeNull();
    });

    it('should return content and metadata', async () => {
      const content = Buffer.from('Retrieved content');
      const metadata = {
        checksum: 'abc123',
        size: content.length,
        contentType: 'text/plain',
        timestamp: Date.now(),
      };

      mockDrive.get
        .mockResolvedValueOnce(content) // Content
        .mockResolvedValueOnce(Buffer.from(JSON.stringify(metadata))); // Metadata

      const result = await service.get('abc123');

      expect(result).not.toBeNull();
      expect(result?.content).toEqual(content);
      expect(result?.metadata.checksum).toBe('abc123');
    });
  });

  describe('delete', () => {
    it('should delete content and metadata', async () => {
      mockDrive.get.mockResolvedValueOnce(
        Buffer.from(
          JSON.stringify({
            checksum: 'abc123',
            size: 10,
            contentType: 'text/plain',
            timestamp: Date.now(),
          }),
        ),
      );

      const result = await service.delete('abc123');

      expect(result).toBe(true);
      expect(mockDrive.del).toHaveBeenCalledTimes(2); // Content + metadata
    });

    it('should delete refs if they exist', async () => {
      mockDrive.get.mockResolvedValueOnce(
        Buffer.from(
          JSON.stringify({
            checksum: 'abc123',
            ipfsCid: 'QmTest',
            bzzHash: 'bzzTest',
          }),
        ),
      );

      await service.delete('abc123');

      expect(mockDrive.del).toHaveBeenCalledTimes(5); // Content + metadata + 2 refs + bridge ref
    });
  });

  describe('exists', () => {
    it('should return true if entry exists', async () => {
      mockDrive.entry.mockResolvedValue({ value: {} });

      const result = await service.exists('/some/path');

      expect(result).toBe(true);
    });

    it('should return false if entry does not exist', async () => {
      mockDrive.entry.mockResolvedValue(null);

      const result = await service.exists('/nonexistent');

      expect(result).toBe(false);
    });
  });

  describe('putIpfs', () => {
    it('should store content with IPFS CID', async () => {
      const content = Buffer.from('IPFS content');
      const cid = 'QmTestCid123';

      const result = await service.putIpfs(cid, content, 'text/plain');

      expect(result.ipfsCid).toBe(cid);
      expect(result.contentType).toBe('text/plain');
    });
  });

  describe('putBzz', () => {
    it('should store content with Swarm hash', async () => {
      const content = Buffer.from('Swarm content');
      const hash = 'swarmHash123';

      const result = await service.putBzz(hash, content, 'text/plain');

      expect(result.bzzHash).toBe(hash);
      expect(result.contentType).toBe('text/plain');
    });
  });

  describe('publishIpfsDirectoryManifest', () => {
    it('should preserve bzzHash and store the manifest root on manifestBzzHash', async () => {
      const existing = {
        checksum: 'abc123',
        size: 10,
        contentType: 'text/html',
        timestamp: Date.now(),
        ipfsCid: 'QmRoot',
        bzzHash: 'leafChunk',
      };
      mockDrive.get.mockResolvedValueOnce(Buffer.from(JSON.stringify(existing)));

      const result = await service.publishIpfsDirectoryManifest(
        'QmRoot',
        'manifestRoot',
        'abc123',
      );

      expect(result).toMatchObject({
        ipfsCid: 'QmRoot',
        bzzHash: 'leafChunk',
        manifestBzzHash: 'manifestRoot',
      });
      expect(mockDrive.put).toHaveBeenCalledWith(
        '/refs/bridge/ipfs/QmRoot',
        Buffer.from('manifestRoot'),
      );
    });

    it('should ignore callers that try to overwrite bzzHash with the manifest ref', async () => {
      const existing = {
        checksum: 'abc123',
        size: 10,
        contentType: 'text/html',
        timestamp: Date.now(),
        ipfsCid: 'QmRoot',
        bzzHash: 'leafChunk',
      };
      mockDrive.get.mockResolvedValueOnce(Buffer.from(JSON.stringify(existing)));

      const result = await service.updateBridgeMetadata('abc123', {
        ipfsCid: 'QmRoot',
        bzzHash: 'manifestRoot',
        manifestBzzHash: 'manifestRoot',
      });

      expect(result).toMatchObject({
        bzzHash: 'leafChunk',
        manifestBzzHash: 'manifestRoot',
      });
    });
  });
});
