import { HttpAdapterHost } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { ConfigService } from '../config/config.service';
import { DriveService } from '../hive/services/drive.service';
import { FileIndexService } from '../hive/services/file-index.service';
import { EthswarmService } from './ethswarm.service';

const mockGet = vi.fn();
const mockPost = vi.fn();
const mockRegister = vi.fn(async (plugin: (instance: any) => Promise<void>) => {
  await plugin({
    removeAllContentTypeParsers: vi.fn(),
    addContentTypeParser: vi.fn(),
    get: mockGet,
    post: mockPost,
  });
});

const mockHttpAdapterHost = {
  httpAdapter: {
    getInstance: vi.fn().mockReturnValue({
      get: mockGet,
      post: mockPost,
      register: mockRegister,
    }),
  },
};

const mockDriveService = {
  getByBzzHash: vi.fn(),
  putBzz: vi.fn(),
  put: vi.fn().mockResolvedValue({ checksum: 'a'.repeat(64) }),
  getByRef: vi.fn(),
  putWithRef: vi.fn(),
};

const mockFileIndexService = {
  getIpfsCidByBzzHash: vi.fn().mockReturnValue('QmFromIndex'),
};

const mockConfigService = {
  beeApiUrl: 'http://localhost:1633',
  upstreamTimeout: 100000,
};

describe('EthswarmService', () => {
  let service: EthswarmService;

  beforeEach(async () => {
    vi.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        EthswarmService,
        { provide: HttpAdapterHost, useValue: mockHttpAdapterHost },
        { provide: DriveService, useValue: mockDriveService },
        { provide: FileIndexService, useValue: mockFileIndexService },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    service = module.get<EthswarmService>(EthswarmService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('should register bzz routes on module init', async () => {
    await service.onModuleInit();

    expect(mockGet).toHaveBeenCalledWith('/bzz/*', expect.any(Function));
    expect(mockPost).toHaveBeenCalledWith('/bzz', expect.any(Function));
    expect(mockPost).toHaveBeenCalledWith('/bzz/*', expect.any(Function));
  });

  it('should fetch bzz sub-paths and cache them without overwriting ipfsCid', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/bzz/*',
    )![1];

    mockDriveService.getByRef.mockResolvedValue(null);
    const upstreamBody = Buffer.from('css data');
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        status: 200,
        arrayBuffer: () =>
          Promise.resolve(
            upstreamBody.buffer.slice(
              upstreamBody.byteOffset,
              upstreamBody.byteOffset + upstreamBody.byteLength,
            ),
          ),
        headers: new Headers({ 'content-type': 'text/css' }),
      }),
    );

    const req = { params: { '*': 'abc123/path/bootstrap.min.css' } };
    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler(req, reply);

    expect(mockDriveService.put).toHaveBeenCalledWith(
      expect.objectContaining({
        bzzRefKey: 'abc123/path/bootstrap.min.css',
        bzzHash: 'abc123',
        content: upstreamBody,
        contentType: 'text/css',
        filename: 'bootstrap.min.css',
      }),
    );
    expect(mockDriveService.put.mock.calls[0][0].ipfsCid).toBeUndefined();
    expect(reply.from).not.toHaveBeenCalled();

    vi.unstubAllGlobals();
  });

  it('should prefer Bee content over cached swarm content', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/bzz/*',
    )![1];

    mockDriveService.getByBzzHash.mockResolvedValue({
      content: Buffer.from('cached bzz data'),
      metadata: { contentType: 'application/json', checksum: 'bzzcheck' },
    });

    const upstreamBody = Buffer.from('swarm data');
    const fetchMock = vi.fn().mockResolvedValue({
      status: 200,
      arrayBuffer: () =>
        Promise.resolve(
          upstreamBody.buffer.slice(
            upstreamBody.byteOffset,
            upstreamBody.byteOffset + upstreamBody.byteLength,
          ),
        ),
      headers: new Headers({ 'content-type': 'text/plain' }),
    });
    vi.stubGlobal('fetch', fetchMock);

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler({ params: { '*': 'bzzHash456' } }, reply);
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(fetchMock.mock.calls[0][0]).toMatch(
      /\/bzz\/bzzHash456\/$/,
    );

    expect(reply.header).toHaveBeenCalledWith('content-type', 'text/plain');
    expect(reply.header).toHaveBeenCalledWith('x-content-origin', 'swarm');
    expect(reply.send).toHaveBeenCalledWith(upstreamBody);
    expect(mockDriveService.getByRef).not.toHaveBeenCalled();
    expect(mockDriveService.putBzz).toHaveBeenCalledWith(
      'bzzHash456',
      expect.any(Buffer),
      'text/plain',
      undefined,
    );

    vi.unstubAllGlobals();
  });

  it('should fall back to bare /bzz/<ref> when collection URL returns 404', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/bzz/*',
    )![1];

    mockDriveService.getByRef.mockResolvedValue(null);
    const singleFileBody = Buffer.from('single file');
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        status: 404,
        arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)),
        headers: new Headers({ 'content-type': 'application/json' }),
      })
      .mockResolvedValueOnce({
        status: 200,
        arrayBuffer: () =>
          Promise.resolve(
            singleFileBody.buffer.slice(
              singleFileBody.byteOffset,
              singleFileBody.byteOffset + singleFileBody.byteLength,
            ),
          ),
        headers: new Headers({ 'content-type': 'text/plain' }),
      });
    vi.stubGlobal('fetch', fetchMock);

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler({ params: { '*': 'deadbeef' } }, reply);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0][0]).toMatch(/\/bzz\/deadbeef\/$/);
    expect(fetchMock.mock.calls[1][0]).toMatch(/\/bzz\/deadbeef$/);
    expect(reply.send).toHaveBeenCalledWith(singleFileBody);

    vi.unstubAllGlobals();
  });

  it('should fall back to cached swarm content when Bee fails', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/bzz/*',
    )![1];

    const cached = {
      content: Buffer.from('cached bzz data'),
      metadata: { contentType: 'application/json', checksum: 'bzzcheck' },
    };
    mockDriveService.getByRef.mockResolvedValue(cached);

    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('bee down')));

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler({ params: { '*': 'abc123hash' } }, reply);

    expect(mockDriveService.getByRef).toHaveBeenCalledWith(
      'bzz',
      'abc123hash',
    );
    expect(reply.header).toHaveBeenCalledWith(
      'content-type',
      'application/json',
    );
    expect(reply.header).toHaveBeenCalledWith('x-content-origin', 'hive');
    expect(reply.send).toHaveBeenCalledWith(cached.content);

    vi.unstubAllGlobals();
  });

  it('should force swarm-pin on uploads', async () => {
    await service.onModuleInit();
    const handler = mockPost.mock.calls.find(
      (call: any[]) => call[0] === '/bzz',
    )![1];

    mockDriveService.putBzz.mockResolvedValue({
      checksum: 'sha256bzz',
      contentType: 'text/plain',
    });

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 201,
      text: () => Promise.resolve(JSON.stringify({ reference: 'bzzRef123' })),
    });
    vi.stubGlobal('fetch', fetchMock);

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler(
      {
        url: '/bzz?name=test.txt',
        raw: {
          rawHeaders: [
            'content-type',
            'text/plain',
            'swarm-postage-batch-id',
            'stamp123',
            'swarm-pin',
            'false',
            'swarm-encrypt',
            'true',
          ],
        },
        body: Buffer.from('upload data'),
        headers: {
          'content-type': 'text/plain',
          'swarm-postage-batch-id': 'stamp123',
          'swarm-pin': 'false',
        },
        query: { name: 'test.txt' },
      },
      reply,
    );
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(fetchMock).toHaveBeenCalledWith(
      'http://localhost:1633/bzz?name=test.txt',
      expect.objectContaining({ headers: expect.any(Headers) }),
    );

    const forwardedHeaders = fetchMock.mock.calls[0][1].headers as Headers;
    expect(forwardedHeaders.get('content-type')).toBe('text/plain');
    expect(forwardedHeaders.get('swarm-postage-batch-id')).toBe('stamp123');
    expect(forwardedHeaders.get('swarm-encrypt')).toBe('true');
    expect(forwardedHeaders.get('swarm-pin')).toBe('true');

    vi.unstubAllGlobals();
  });
});
