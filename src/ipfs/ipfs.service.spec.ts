import { HttpAdapterHost } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { ConfigService } from '../config/config.service';
import { DriveService } from '../hive/services/drive.service';
import { SwarmBridgeService } from '../hive/services/swarm-bridge.service';
import { IpfsService } from './ipfs.service';

const mockGet = vi.fn();
const mockAll = vi.fn();
const mockRoute = vi.fn();
const mockRegister = vi.fn(async (plugin: (instance: any) => Promise<void>) => {
  await plugin({
    removeAllContentTypeParsers: vi.fn(),
    addContentTypeParser: vi.fn(),
    route: mockRoute,
  });
});

const mockHttpAdapterHost = {
  httpAdapter: {
    getInstance: vi.fn().mockReturnValue({
      get: mockGet,
      all: mockAll,
      register: mockRegister,
    }),
  },
};

const mockDriveService = {
  getByIpfsCid: vi.fn(),
  getByRef: vi.fn(),
  putIpfs: vi.fn(),
  putWithRef: vi.fn().mockResolvedValue({ checksum: 'a'.repeat(64) }),
};

const mockSwarmBridgeService = {
  getBzzHashForCid: vi.fn().mockResolvedValue('bzzRoot1'),
  fetchFromBee: vi.fn(),
  bridgeIpfsContent: vi.fn().mockResolvedValue(undefined),
  saveIpfsSubpath: vi.fn().mockResolvedValue({ checksum: 'b'.repeat(64) }),
  recordBridgedSubpathInHive: vi.fn().mockResolvedValue({ checksum: 'b'.repeat(64) }),
  ensureIpfsDirectoryListEntry: vi.fn().mockResolvedValue(undefined),
};

const mockConfigService = {
  ipfsGatewayUrl: 'http://127.0.0.1:8080',
  ipfsApiUrl: 'http://127.0.0.1:5001',
  upstreamTimeout: 100000,
};

describe('IpfsService', () => {
  let service: IpfsService;

  beforeEach(async () => {
    vi.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        IpfsService,
        { provide: HttpAdapterHost, useValue: mockHttpAdapterHost },
        { provide: DriveService, useValue: mockDriveService },
        { provide: SwarmBridgeService, useValue: mockSwarmBridgeService },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    service = module.get<IpfsService>(IpfsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('should register gateway and RPC routes on module init', async () => {
    await service.onModuleInit();

    expect(mockGet).toHaveBeenCalledWith('/ipfs/*', expect.any(Function));
    expect(mockAll).toHaveBeenCalledWith('/ipns/*', expect.any(Function));
    expect(mockRoute).toHaveBeenCalledWith(
      expect.objectContaining({
        url: '/api/v0/*',
        handler: expect.any(Function),
      }),
    );
  });

  it('should fetch sub-paths from the gateway, serve them, upload to Swarm, and save full-path ipfsCid and bzzHash in Hive', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/ipfs/*',
    )![1];

    mockDriveService.getByRef.mockResolvedValue(null);

    const upstreamBody = Buffer.from('subpath data');
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

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
      redirect: vi.fn().mockReturnThis(),
    };

    await handler(
      { params: { '*': 'QmCID123/path/to/file' }, url: '/ipfs/QmCID123/path/to/file' },
      reply,
    );
    await new Promise((r) => setImmediate(r));

    expect(mockDriveService.getByRef).toHaveBeenCalledWith(
      'ipfs',
      'QmCID123/path/to/file',
    );
    expect(reply.header).toHaveBeenCalledWith('content-type', 'text/css');
    expect(mockSwarmBridgeService.saveIpfsSubpath).toHaveBeenCalledWith(
      'QmCID123/path/to/file',
      expect.any(Buffer),
      'text/css',
      'file',
    );
    expect(mockDriveService.putWithRef).not.toHaveBeenCalled();
    expect(mockSwarmBridgeService.bridgeIpfsContent).not.toHaveBeenCalled();
    expect(reply.from).not.toHaveBeenCalled();

    vi.unstubAllGlobals();
  });

  it('should redirect bare /ipfs/CID to /ipfs/CID/ when the gateway treats it as a directory', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/ipfs/*',
    )![1];

    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        status: 302,
        headers: {
          get: (name: string) =>
            name === 'location' ? 'http://127.0.0.1:8080/ipfs/QmDIR123/' : null,
        },
      }),
    );

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
      redirect: vi.fn().mockReturnThis(),
    };

    await handler(
      { params: { '*': 'QmDIR123' }, url: '/ipfs/QmDIR123' },
      reply,
    );

    expect(reply.redirect).toHaveBeenCalledWith('/ipfs/QmDIR123/', 302);
    expect(reply.from).not.toHaveBeenCalled();
    expect(reply.send).not.toHaveBeenCalled();

    vi.unstubAllGlobals();
  });

  it('should serve IPFS cache hits locally', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/ipfs/*',
    )![1];

    const cached = {
      content: Buffer.from('cached data'),
      metadata: { contentType: 'text/plain', checksum: 'abc123' },
    };
    mockSwarmBridgeService.getBzzHashForCid.mockResolvedValue(null);
    mockDriveService.getByRef.mockResolvedValue(cached);

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler({ params: { '*': 'QmCID123' } }, reply);

    expect(reply.header).toHaveBeenCalledWith('content-type', 'text/plain');
    expect(reply.header).toHaveBeenCalledWith('x-content-origin', 'hive');
    expect(reply.send).toHaveBeenCalledWith(cached.content);
  });

  it('should serve bridged swarm content before the local cache', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/ipfs/*',
    )![1];

    mockDriveService.getByRef.mockResolvedValue(null);
    mockSwarmBridgeService.getBzzHashForCid.mockResolvedValue('bzz123');
    mockSwarmBridgeService.fetchFromBee.mockResolvedValue({
      content: Buffer.from('swarm data'),
      contentType: 'text/plain',
    });

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler({ params: { '*': 'QmCID789' } }, reply);
    await Promise.resolve();

    expect(mockSwarmBridgeService.getBzzHashForCid).toHaveBeenCalledWith(
      'QmCID789',
    );
    expect(reply.header).toHaveBeenCalledWith('content-type', 'text/plain');
    expect(reply.header).toHaveBeenCalledWith('x-content-origin', 'swarm');
    expect(reply.send).toHaveBeenCalledWith(Buffer.from('swarm data'));
    expect(mockSwarmBridgeService.bridgeIpfsContent).toHaveBeenCalledWith(
      'QmCID789',
      Buffer.from('swarm data'),
      'text/plain',
      undefined,
    );
  });

  it('sub-path: should prefer Bee over a Hive ref when a full-path bridge exists', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/ipfs/*',
    )![1];

    const ref = 'QmdRoot/lib/angular-sanitize.min.js';
    const bzzPath =
      'b0a79312b6379b953bdf548255578273af5ab2ee39cc13e7a9bdd46d5eb6d43f/lib/angular-sanitize.min.js';

    mockSwarmBridgeService.getBzzHashForCid.mockResolvedValue(bzzPath);
    mockSwarmBridgeService.fetchFromBee.mockResolvedValue({
      content: Buffer.from('from bee'),
      contentType: 'text/javascript; charset=utf-8',
    });
    mockDriveService.getByRef.mockResolvedValue({
      content: Buffer.from('stale in hive'),
      metadata: { contentType: 'text/javascript; charset=utf-8' },
    });

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler({ params: { '*': ref } }, reply);
    await new Promise((r) => setImmediate(r));

    expect(mockSwarmBridgeService.getBzzHashForCid).toHaveBeenCalledWith(ref);
    expect(mockSwarmBridgeService.fetchFromBee).toHaveBeenCalledWith(bzzPath);
    expect(reply.header).toHaveBeenCalledWith('x-content-origin', 'swarm');
    expect(reply.send).toHaveBeenCalledWith(Buffer.from('from bee'));
  });

  it('should fetch uncached content from the IPFS gateway and bridge it', async () => {
    await service.onModuleInit();
    const handler = mockGet.mock.calls.find(
      (call: any[]) => call[0] === '/ipfs/*',
    )![1];

    mockSwarmBridgeService.getBzzHashForCid.mockResolvedValue(null);
    mockDriveService.getByRef.mockResolvedValue(null);

    const upstreamBody = Buffer.from('upstream data');
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
        headers: new Headers({ 'content-type': 'image/png' }),
      }),
    );

    const reply = {
      header: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
      status: vi.fn().mockReturnThis(),
      from: vi.fn().mockReturnThis(),
    };

    await handler({ params: { '*': 'QmCID456' } }, reply);
    await Promise.resolve();

    expect(reply.header).toHaveBeenCalledWith('content-type', 'image/png');
    expect(reply.header).toHaveBeenCalledWith('x-content-origin', 'ipfs');
    expect(mockSwarmBridgeService.bridgeIpfsContent).toHaveBeenCalledWith(
      'QmCID456',
      expect.any(Buffer),
      'image/png',
      undefined,
    );

    vi.unstubAllGlobals();
  });
});
