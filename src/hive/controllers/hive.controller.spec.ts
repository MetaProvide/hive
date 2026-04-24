import { Test, type TestingModule } from '@nestjs/testing';
import { ConfigService } from '../../config/config.service';
import { DriveService } from '../services/drive.service';
import { FileIndexService } from '../services/file-index.service';
import { HiveDirectoryBzzService } from '../services/hive-directory-bzz.service';
import { IdentityService } from '../services/identity.service';
import { HiveController } from './hive.controller';

describe('HiveController', () => {
  let controller: HiveController;
  let mockResponse: any;

  const mockDriveService = {
    put: vi.fn(),
    get: vi.fn(),
    getMetadata: vi.fn().mockResolvedValue(null),
    getContentCount: vi.fn().mockResolvedValue(0),
    getBridgedCount: vi.fn().mockResolvedValue(0),
    delete: vi.fn(),
    list: vi.fn().mockResolvedValue([]),
    listContent: vi.fn().mockResolvedValue([]),
  };

  const mockFileIndexService = {
    getByChecksum: vi.fn(),
    addOrUpdate: vi.fn(),
    remove: vi.fn(),
    load: vi.fn().mockResolvedValue(undefined),
  };

  const mockIdentityService = {
    purgeStorageFolder: vi.fn().mockResolvedValue(undefined),
  };

  const mockConfigService = {
    nodeId: 'TestNode',
    storagePath: '/tmp/hive-test-storage',
  };

  const mockHiveDirectoryBzzService = {
    isDirectoryMapContent: vi.fn().mockReturnValue(false),
    uploadDirectoryTreeToBzz: vi.fn(),
  };

  beforeEach(async () => {
    mockResponse = {
      status: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      headers: vi.fn().mockReturnThis(),
      header: vi.fn().mockReturnThis(),
    };

    const module: TestingModule = await Test.createTestingModule({
      controllers: [HiveController],
      providers: [
        { provide: DriveService, useValue: mockDriveService },
        { provide: FileIndexService, useValue: mockFileIndexService },
        { provide: IdentityService, useValue: mockIdentityService },
        { provide: ConfigService, useValue: mockConfigService },
        {
          provide: HiveDirectoryBzzService,
          useValue: mockHiveDirectoryBzzService,
        },
      ],
    }).compile();

    controller = module.get<HiveController>(HiveController);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('should delegate publish', async () => {
    mockHiveDirectoryBzzService.uploadDirectoryTreeToBzz.mockResolvedValue({
      checksum: 'abc',
      size: 1,
      contentType: 'application/vnd.hive.ipfs-directory+json',
      timestamp: 1,
      bzzHash: 'm1',
    });

    await expect(controller.uploadDirToBzz(' abc ')).resolves.toMatchObject({
      checksum: 'abc',
      bzzHash: 'm1',
    });
    expect(
      mockHiveDirectoryBzzService.uploadDirectoryTreeToBzz,
    ).toHaveBeenCalledWith('abc');
  });

  it('should purge storage and reload index', async () => {
    await expect(controller.purgeStorage()).resolves.toEqual({
      purged: true,
      storagePath: '/tmp/hive-test-storage',
    });
    expect(mockIdentityService.purgeStorageFolder).toHaveBeenCalledTimes(1);
    expect(mockFileIndexService.load).toHaveBeenCalledTimes(1);
  });

  it('should return node status', async () => {
    mockDriveService.getContentCount.mockResolvedValue(5);
    mockDriveService.getBridgedCount.mockResolvedValue(3);

    await expect(controller.getStatus()).resolves.toEqual({
      nodeId: 'TestNode',
      contentCount: 5,
      bridgedCount: 3,
    });
  });

  it('should return content list', async () => {
    const items = [
      { checksum: 'abc1', size: 100, contentType: 'text/plain' },
      { checksum: 'abc2', size: 200, contentType: 'text/html' },
    ];
    mockDriveService.listContent.mockResolvedValue(items);

    await expect(controller.getContentList()).resolves.toEqual({
      items,
      count: 2,
    });
  });

  it('should list drive paths', async () => {
    const entries = [{ key: '/content', size: 0, isDirectory: true }];
    mockDriveService.list.mockResolvedValue(entries);

    await expect(controller.listRoot()).resolves.toEqual({
      path: '/',
      entries,
      count: 1,
    });
    await expect(controller.listPath('content')).resolves.toEqual({
      path: '/content',
      entries,
      count: 1,
    });
  });

  it('should return local content with headers', async () => {
    const content = Buffer.from('Test content');
    mockDriveService.get.mockResolvedValue({
      content,
      metadata: {
        checksum: 'abc123',
        contentType: 'text/plain',
        filename: 'test.txt',
      },
    });

    await controller.getContent('abc123', mockResponse);

    expect(mockResponse.headers).toHaveBeenCalledWith({
      'Content-Type': 'text/plain',
      'Content-Length': content.length,
      'X-Checksum': 'abc123',
    });
    expect(mockResponse.send).toHaveBeenCalledWith(content);
  });

  it('should return 404 when local content is missing', async () => {
    mockDriveService.get.mockResolvedValue(null);

    await controller.getContent('missing', mockResponse);

    expect(mockResponse.status).toHaveBeenCalledWith(404);
    expect(mockResponse.send).toHaveBeenCalledWith({
      error: 'Content not found',
      checksum: 'missing',
    });
  });

  it('should return metadata from index or drive', async () => {
    const metadata = {
      checksum: 'abc123',
      size: 100,
      contentType: 'text/plain',
    };
    mockFileIndexService.getByChecksum.mockReturnValue(metadata);

    await controller.getMetadata('abc123', mockResponse);

    expect(mockResponse.send).toHaveBeenCalledWith(metadata);
  });

  it('should build Swarm manifest when storing directory map with bzzHash', async () => {
    const dirJson = { '/a.txt': { cid: 'QmChild1' } };
    const content = Buffer.from(JSON.stringify(dirJson));
    mockHiveDirectoryBzzService.isDirectoryMapContent.mockReturnValue(true);
    mockDriveService.put.mockResolvedValue({
      checksum: 'dirsum',
      size: content.length,
      contentType: 'application/vnd.hive.ipfs-directory+json',
      timestamp: Date.now(),
    });
    mockHiveDirectoryBzzService.uploadDirectoryTreeToBzz.mockResolvedValue({
      checksum: 'dirsum',
      size: content.length,
      contentType: 'application/vnd.hive.ipfs-directory+json',
      timestamp: Date.now(),
      bzzHash: 'manifest1',
    });

    const result = await controller.storeContent({
      content: content.toString('base64'),
      contentType: 'application/vnd.hive.ipfs-directory+json',
      bzzHash: 'clientShouldBeIgnored',
    });

    expect(mockDriveService.put).toHaveBeenCalledWith(
      expect.objectContaining({
        content,
        contentType: 'application/vnd.hive.ipfs-directory+json',
      }),
    );
    expect(mockDriveService.put.mock.calls[0][0].bzzHash).toBeUndefined();
    expect(
      mockHiveDirectoryBzzService.uploadDirectoryTreeToBzz,
    ).toHaveBeenCalledWith('dirsum');
    expect(result.bzzHash).toBe('manifest1');
  });

  it('should store content and update the index', async () => {
    const metadata = {
      checksum: 'abc123',
      size: 12,
      contentType: 'text/plain',
      timestamp: Date.now(),
    };
    mockDriveService.put.mockResolvedValue(metadata);

    const result = await controller.storeContent({
      content: Buffer.from('Test content').toString('base64'),
      contentType: 'text/plain',
    });

    expect(result).toEqual(metadata);
    expect(mockFileIndexService.addOrUpdate).toHaveBeenCalledWith(metadata);
  });

  it('should delete content and update the index', async () => {
    mockDriveService.delete.mockResolvedValue(true);

    await controller.deleteContent('abc123', mockResponse);

    expect(mockFileIndexService.remove).toHaveBeenCalledWith('abc123');
    expect(mockResponse.send).toHaveBeenCalledWith({
      deleted: true,
      checksum: 'abc123',
    });
  });

  it('should return a local feed', async () => {
    mockDriveService.listContent.mockResolvedValue([
      { checksum: 'a1', size: 10, contentType: 'text/plain', timestamp: 2 },
      { checksum: 'a2', size: 20, contentType: 'text/html', timestamp: 1 },
    ]);

    await controller.getFeedLocal('10', mockResponse);

    expect(mockResponse.send).toHaveBeenCalledWith({
      items: [
        { checksum: 'a1', contentType: 'text/plain', size: 10, timestamp: 2 },
        { checksum: 'a2', contentType: 'text/html', size: 20, timestamp: 1 },
      ],
      count: 2,
      scope: 'local',
    });
  });
});
