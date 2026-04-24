import { Injectable, type OnModuleInit } from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

import { BaseProxyService } from '../common/base-proxy.service';
import {
  CONTENT_ORIGIN_HEADER,
  type ContentOrigin,
} from '../common/content-origin';
import { ConfigService } from '../config/config.service';
import type {
  ContentMetadataDto,
  ContentResponseDto,
} from '../hive/dto/content';
import { DriveService } from '../hive/services/drive.service';
import {
  type BeeFetchResult,
  SwarmBridgeService,
} from '../hive/services/swarm-bridge.service';

interface RpcUpstreamResponse {
  body: Buffer;
  contentType: string;
  status: number;
}

@Injectable()
export class IpfsService extends BaseProxyService implements OnModuleInit {
  protected readonly protocol = 'ipfs';

  private readonly ipfsApiUrl: string;
  private readonly pendingBridges = new Map<string, Promise<void>>();

  constructor(
    httpAdapterHost: HttpAdapterHost,
    driveService: DriveService,
    private readonly swarmBridgeService: SwarmBridgeService,
    config: ConfigService,
  ) {
    super(
      httpAdapterHost,
      driveService,
      config,
      IpfsService.name,
      config.ipfsGatewayUrl,
    );
    this.ipfsApiUrl = config.ipfsApiUrl;
  }

  async onModuleInit() {
    const fastify: FastifyInstance =
      this.httpAdapterHost.httpAdapter.getInstance();

    fastify.get('/ipfs/*', (req: FastifyRequest, reply: FastifyReply) =>
      this.handleGatewayGet(req, reply),
    );

    fastify.all('/ipns/*', (req: FastifyRequest, reply: FastifyReply) => {
      const path = (req.params as Record<string, string>)['*'];
      return reply.from(`${this.target}/ipns/${path}`);
    });

    await fastify.register(async (instance) => {
      instance.removeAllContentTypeParsers();
      instance.addContentTypeParser(
        '*',
        { parseAs: 'buffer' },
        (_req, body, done) => done(null, body),
      );

      instance.route({
        method: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS'],
        url: '/api/v0/*',
        handler: async (req: FastifyRequest, reply: FastifyReply) =>
          this.handleRpc(req, reply),
      });
    });

    this.logger.log(
      `IPFS proxy registered -> gateway ${this.target}, rpc ${this.ipfsApiUrl} [GET /ipfs/*, ALL /ipns/*, ALL /api/v0/*]`,
    );
  }

  protected async lookupCache(cid: string): Promise<ContentResponseDto | null> {
    return this.driveService.getByRef('ipfs', this.ipfsGatewayRefKey(cid));
  }

  protected async cacheToDrive(
    cid: string,
    content: Buffer,
    contentType: string,
  ): Promise<ContentMetadataDto> {
    return this.driveService.putIpfs(cid, content, contentType);
  }

  private async handleGatewayGet(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    const fullPath = (req.params as Record<string, string>)['*'];
    const segments = fullPath.split('/').filter(Boolean);
    const cid = segments[0];
    const refKey = this.ipfsGatewayRefKey(fullPath);
    const isRootOnly = !refKey.includes('/');

    this.logger.log(`[${cid.slice(0, 16)}] IPFS gateway read requested`);

    if (!isRootOnly) {
      this.logger.debug(
        `[${cid.slice(0, 16)}] Sub-path request (Hive ref ${refKey})`,
      );
    }

    if (!fullPath.endsWith('/')) {
      const canonical = await this.getDirectoryTrailingSlashIfNeeded(
        fullPath,
        cid,
        req,
      );
      if (canonical) {
        this.logger.log(
          `[${cid.slice(0, 16)}] Redirecting to ${canonical} so relative asset URLs resolve under this CID`,
        );
        return reply.redirect(canonical, 302);
      }
    }

    // Prefer Bee whenever an IPFS ref (root CID or sub-path) is bridged, so
    // `x-content-origin` reflects Swarm, not a prior Hyperdrive copy.
    const bridged = await this.tryServeFromSwarm(refKey, reply);
    if (bridged) {
      return;
    }

    const cached = await this.driveService.getByRef('ipfs', refKey);
    if (cached) {
      this.logger.log(
        `[${cid.slice(0, 16)}] Hive cache served IPFS content (${cached.content.length} bytes, ${cached.metadata.contentType})`,
      );
      this.sendResponse(
        reply,
        cached.content,
        cached.metadata.contentType,
        'hive',
        cid,
        cached.metadata,
      );
      return;
    }

    this.logger.log(
      `[${cid.slice(0, 16)}] No bridge mapping or Hive cache hit, fetching from the IPFS gateway`,
    );
    const upstream = await this.fetchUpstream(
      `${this.target}/ipfs/${fullPath}`,
    );
    if (upstream && upstream.status >= 200 && upstream.status < 400) {
      this.logger.log(
        `[${cid.slice(0, 16)}] IPFS gateway served the content (${upstream.content.length} bytes, ${upstream.contentType})${isRootOnly ? ', starting bridge' : ', caching sub-path'}`,
      );
      this.sendResponse(
        reply,
        upstream.content,
        upstream.contentType,
        'ipfs',
        cid,
      );
      if (isRootOnly) {
        this.bridgeInBackground(cid, upstream.content, upstream.contentType);
      } else {
        this.cacheIpfsSubpathInBackground(
          refKey,
          upstream.content,
          upstream.contentType,
        );
      }
      return;
    }

    this.logger.error(
      `[${cid.slice(0, 16)}] IPFS gateway read failed and no Hive fallback was available`,
    );

    reply.status(502).send({
      statusCode: 502,
      error: 'Bad Gateway',
      message: 'IPFS gateway unavailable and no cached content exists',
    });
  }

  /**
   * Canonical ref id for `/ipfs/*` so `CID` and `CID/` share one cache entry.
   * Sub-paths use `CID/rel/path` (no trailing slash).
   */
  private ipfsGatewayRefKey(fullPath: string): string {
    return fullPath.replace(/\/+$/, '');
  }

  private cacheIpfsSubpathInBackground(
    refKey: string,
    content: Buffer,
    contentType: string,
  ): void {
    this.logger.debug(
      `[${refKey.slice(0, 16)}] Queueing Hive+Swarm persist for IPFS sub-path ref (${content.length} bytes, ${contentType})`,
    );
    void this.persistIpfsSubpathToHive(refKey, content, contentType).catch(
      (error) => {
        this.logger.warn(
          `[${refKey.slice(0, 16)}] Failed to cache IPFS sub-path: ${(error as Error).message}`,
        );
      },
    );
  }

  private async persistIpfsSubpathToHive(
    refKey: string,
    content: Buffer,
    contentType: string,
  ): Promise<void> {
    const segments = refKey.split('/').filter(Boolean);
    const filename =
      segments.length > 1 ? segments[segments.length - 1] : undefined;
    const rootCid = segments[0];
    let rootBzz = await this.swarmBridgeService.getBzzHashForCid(rootCid);
    if (!rootBzz) {
      this.logger.debug(
        `[${rootCid.slice(0, 16)}] No Swarm map for root, bridging then saving sub-path`,
      );
      await this.bridgeCidFromGateway(rootCid);
      rootBzz = await this.swarmBridgeService.getBzzHashForCid(rootCid);
    }
    if (!rootBzz) {
      this.logger.warn(
        `[${rootCid.slice(0, 16)}] Sub-path could not be mirrored to a Swarm path, falling back to Hive ref only`,
      );
      const metadata = await this.driveService.putWithRef(
        'ipfs',
        refKey,
        content,
        contentType,
        filename,
      );
      this.logger.debug(
        `[${refKey.slice(0, 16)}] Cached IPFS sub-path (Hive only) -> ${metadata.checksum.slice(0, 16)}...`,
      );
      if (segments.length >= 2) {
        await this.swarmBridgeService.ensureIpfsDirectoryListEntry(rootCid);
      }
      return;
    }
    const metadata = await this.swarmBridgeService.saveIpfsSubpath(
      refKey,
      content,
      contentType,
      filename,
    );
    this.logger.debug(
      `[${refKey.slice(0, 16)}] Cached IPFS sub-path -> ${metadata.checksum.slice(0, 16)}... (ipfsCid, Bee chunk bzzHash)`,
    );
  }

  private async handleRpc(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    const path = (req.params as Record<string, string>)['*'];
    this.logger.log(`[IPFS RPC] ${req.method} /api/v0/${path}`);

    if (req.method === 'OPTIONS') {
      this.logger.debug('[IPFS RPC] Handling local CORS preflight');
      return reply.status(204).send();
    }

    if (path === 'add' && req.method === 'POST') {
      return this.handleAdd(req, reply);
    }

    if (path === 'pin/add' && req.method === 'POST') {
      return this.handlePinAdd(req, reply);
    }

    if (path === 'cat') {
      return this.handleCat(req, reply);
    }

    if (path === 'get') {
      return this.handleGetRequest(req, reply);
    }

    return this.proxyRpcRequest(req, reply, path);
  }

  private async handleAdd(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    try {
      this.logger.log('[IPFS RPC] Forwarding add request to Kubo');
      const upstream = await this.forwardRpcRequest(req, 'add');
      this.sendRpcReply(reply, upstream);

      if (upstream.status >= 200 && upstream.status < 400) {
        const cids = this.collectAddCids(upstream.body);
        this.logger.log(
          `[IPFS RPC] add completed, extracted ${cids.length} CID${cids.length === 1 ? '' : 's'} for bridging`,
        );
        for (const cid of cids) {
          this.queueBridgeFromGateway(cid);
        }
      }
    } catch (error) {
      this.sendRpcError(reply, error as Error);
    }
  }

  private async handlePinAdd(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    try {
      this.logger.log('[IPFS RPC] Forwarding pin/add request to Kubo');
      const upstream = await this.forwardRpcRequest(req, 'pin/add');
      this.sendRpcReply(reply, upstream);

      if (upstream.status >= 200 && upstream.status < 400) {
        const cids = this.collectPinCids(upstream.body, req);
        this.logger.log(
          `[IPFS RPC] pin/add completed, extracted ${cids.length} CID${cids.length === 1 ? '' : 's'} for bridging`,
        );
        for (const cid of cids) {
          this.queueBridgeFromGateway(cid);
        }
      }
    } catch (error) {
      this.sendRpcError(reply, error as Error);
    }
  }

  private async handleCat(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    const cacheableCid = this.getCacheableCatCid(req);
    this.logger.log(
      `[IPFS RPC] cat requested${cacheableCid ? ` for cacheable CID ${cacheableCid}` : ' for a non-cacheable path/range request'}`,
    );
    if (cacheableCid) {
      const bridged = await this.tryServeFromSwarm(cacheableCid, reply);
      if (bridged) {
        return;
      }

      const cached = await this.lookupCache(cacheableCid);
      if (cached) {
        this.logger.log(
          `[IPFS RPC] cat served from Hive cache for ${cacheableCid} (${cached.content.length} bytes)`,
        );
        this.sendResponse(
          reply,
          cached.content,
          cached.metadata.contentType,
          'hive',
          cacheableCid,
          cached.metadata,
        );
        return;
      }
    }

    try {
      const upstream = await this.forwardRpcRequest(req, 'cat');
      this.logger.log(
        `[IPFS RPC] cat upstream response ${upstream.status} (${upstream.body.length} bytes, ${upstream.contentType})`,
      );
      this.sendRpcReply(
        reply,
        upstream,
        upstream.status >= 200 && upstream.status < 400 && cacheableCid
          ? 'ipfs'
          : undefined,
      );

      const cidToBridge = cacheableCid ?? this.getRootCid(req);
      if (upstream.status >= 200 && upstream.status < 400 && cidToBridge) {
        this.logger.debug(
          `[IPFS RPC] cat response will be bridged for CID ${cidToBridge}`,
        );
        this.bridgeInBackground(
          cidToBridge,
          upstream.body,
          upstream.contentType,
        );
      }
    } catch (error) {
      this.sendRpcError(reply, error as Error);
    }
  }

  private async handleGetRequest(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    try {
      this.logger.log('[IPFS RPC] Forwarding get request to Kubo');
      const upstream = await this.forwardRpcRequest(req, 'get');
      this.sendRpcReply(reply, upstream);

      const cid = this.getRootCid(req);
      if (upstream.status >= 200 && upstream.status < 400 && cid) {
        this.logger.debug(
          `[IPFS RPC] get completed, queueing bridge for ${cid}`,
        );
        this.queueBridgeFromGateway(cid);
      }
    } catch (error) {
      this.sendRpcError(reply, error as Error);
    }
  }

  private async tryServeFromSwarm(
    ipfsRef: string,
    reply: FastifyReply,
  ): Promise<boolean> {
    const bzzPath = await this.swarmBridgeService.getBzzHashForCid(ipfsRef);
    if (!bzzPath) {
      this.logger.debug(
        `[${ipfsRef.slice(0, 16)}] No Swarm bridge mapping for this IPFS ref yet`,
      );
      return false;
    }

    this.logger.log(
      `[${ipfsRef.slice(0, 16)}] Swarm bridge mapping found -> ${bzzPath.slice(0, 32)}${bzzPath.length > 32 ? '…' : ''}, fetching from Bee`,
    );

    const beeResult = await this.swarmBridgeService.fetchFromBee(bzzPath);
    if (!beeResult) {
      this.logger.warn(
        `[${ipfsRef.slice(0, 16)}] Swarm bridge fetch failed, falling back to Hive cache/IPFS gateway`,
      );
      return false;
    }

    this.logger.log(
      `[${ipfsRef.slice(0, 16)}] Bee served bridged IPFS content (${beeResult.content.length} bytes, ${beeResult.contentType})`,
    );
    this.sendBeeResponse(reply, ipfsRef, beeResult);

    if (!(await this.lookupCache(ipfsRef))) {
      this.logger.debug(
        `[${ipfsRef.slice(0, 16)}] Bee returned content that is not cached locally yet, backfilling Hive cache`,
      );
      if (ipfsRef.includes('/')) {
        const segments = ipfsRef.split('/').filter(Boolean);
        const filename =
          segments.length > 1 ? segments[segments.length - 1] : undefined;
        void this.swarmBridgeService
          .recordBridgedSubpathInHive(
            ipfsRef,
            beeResult.content,
            beeResult.contentType,
            filename,
            bzzPath,
          )
          .catch((e) => {
            this.logger.warn(
              `[${ipfsRef.slice(0, 16)}] Sub-path backfill from Bee failed: ${(e as Error).message}`,
            );
          });
      } else {
        this.bridgeInBackground(
          ipfsRef,
          beeResult.content,
          beeResult.contentType,
          beeResult.filename,
        );
      }
    }

    return true;
  }

  private sendBeeResponse(
    reply: FastifyReply,
    ipfsRef: string,
    beeResult: BeeFetchResult,
  ): void {
    this.sendResponse(
      reply,
      beeResult.content,
      beeResult.contentType,
      'swarm',
      ipfsRef,
    );
  }

  private bridgeInBackground(
    cid: string,
    content: Buffer,
    contentType: string,
    filename?: string,
  ): void {
    this.logger.debug(
      `[${cid.slice(0, 16)}] Queueing background bridge (${content.length} bytes, ${contentType}, filename=${filename ?? 'none'})`,
    );
    this.queueBridge(cid, () =>
      this.swarmBridgeService.bridgeIpfsContent(
        cid,
        content,
        contentType,
        filename,
      ),
    );
  }

  private queueBridgeFromGateway(cid: string): void {
    this.queueBridge(cid, () => this.bridgeCidFromGateway(cid));
  }

  private queueBridge(cid: string, action: () => Promise<unknown>): void {
    if (this.pendingBridges.has(cid)) {
      this.logger.debug(
        `[${cid.slice(0, 16)}] Bridge work already in flight, skipping duplicate queue`,
      );
      return;
    }

    this.logger.debug(`[${cid.slice(0, 16)}] Starting bridge worker`);

    const task = action()
      .then(() => {
        this.logger.debug(`[${cid.slice(0, 16)}] Bridge worker completed`);
      })
      .catch((error) => {
        this.logger.warn(
          `[${cid.slice(0, 16)}] Failed to bridge content: ${(error as Error).message}`,
        );
      })
      .finally(() => {
        this.pendingBridges.delete(cid);
      });

    this.pendingBridges.set(
      cid,
      task.then(() => undefined),
    );
  }

  private async bridgeCidFromGateway(cid: string): Promise<void> {
    const existingBzzHash = await this.swarmBridgeService.getBzzHashForCid(cid);
    if (existingBzzHash) {
      this.logger.debug(
        `[${cid.slice(0, 16)}] CID already has Swarm mapping ${existingBzzHash.slice(0, 16)}..., bridge fetch skipped`,
      );
      return;
    }

    const cached = await this.driveService.getByIpfsCid(cid);
    if (cached) {
      this.logger.debug(
        `[${cid.slice(0, 16)}] Bridging from Hive cache (${cached.content.length} bytes, ${cached.metadata.contentType})`,
      );
      await this.swarmBridgeService.bridgeIpfsContent(
        cid,
        cached.content,
        cached.metadata.contentType,
        cached.metadata.filename,
      );
      return;
    }

    this.logger.debug(
      `[${cid.slice(0, 16)}] No Hive cache for CID, fetching from IPFS gateway before bridging`,
    );
    const upstream = await this.fetchUpstream(`${this.target}/ipfs/${cid}`);
    if (!upstream || upstream.status < 200 || upstream.status >= 400) {
      throw new Error('Unable to fetch CID from IPFS gateway for bridging');
    }

    this.logger.debug(
      `[${cid.slice(0, 16)}] IPFS gateway fetch for bridge succeeded (${upstream.content.length} bytes, ${upstream.contentType})`,
    );

    await this.swarmBridgeService.bridgeIpfsContent(
      cid,
      upstream.content,
      upstream.contentType,
    );
  }

  private async proxyRpcRequest(
    req: FastifyRequest,
    reply: FastifyReply,
    path: string,
  ): Promise<void> {
    try {
      this.logger.debug(`[IPFS RPC] Proxying generic RPC path ${path}`);
      const upstream = await this.forwardRpcRequest(req, path);
      this.sendRpcReply(reply, upstream);
    } catch (error) {
      this.sendRpcError(reply, error as Error);
    }
  }

  private async forwardRpcRequest(
    req: FastifyRequest,
    path: string,
  ): Promise<RpcUpstreamResponse> {
    const queryString = this.getQueryString(req.url);
    const target = `${this.ipfsApiUrl}/api/v0/${path}${queryString}`;

    const headers: Record<string, string> = {};
    if (req.headers['content-type']) {
      headers['content-type'] = req.headers['content-type'];
    }

    const fetchOptions: RequestInit = {
      method: req.method,
      headers,
      signal: AbortSignal.timeout(this.upstreamTimeoutMs),
    };

    if (req.body && req.method !== 'GET' && req.method !== 'HEAD') {
      fetchOptions.body = new Uint8Array(req.body as Buffer);
    }

    this.logger.debug(
      `[IPFS RPC] Forwarding ${req.method} /api/v0/${path}${queryString} -> ${target} (${req.body ? (req.body as Buffer).length : 0} body bytes)`,
    );

    const upstream = await fetch(target, fetchOptions);
    const body = Buffer.from(await upstream.arrayBuffer());
    const contentType =
      upstream.headers.get('content-type') || 'application/json';
    this.logger.debug(
      `[IPFS RPC] Upstream replied ${upstream.status} (${body.length} bytes, ${contentType})`,
    );
    return {
      body,
      contentType,
      status: upstream.status,
    };
  }

  private sendRpcReply(
    reply: FastifyReply,
    upstream: RpcUpstreamResponse,
    contentOrigin?: ContentOrigin,
  ): void {
    this.logger.debug(
      `[IPFS RPC] Sending RPC response (${upstream.status}, ${upstream.body.length} bytes, ${upstream.contentType}${contentOrigin ? `, origin=${contentOrigin}` : ''})`,
    );
    reply.status(upstream.status);
    reply.header('content-type', upstream.contentType);
    if (contentOrigin) {
      reply.header(CONTENT_ORIGIN_HEADER, contentOrigin);
    }
    reply.send(upstream.body);
  }

  private sendRpcError(reply: FastifyReply, error: Error): void {
    this.logger.error(`[IPFS RPC] Request failed: ${error.message}`);
    reply.status(502).send({
      statusCode: 502,
      error: 'Bad Gateway',
      message: `IPFS RPC unreachable: ${error.message}`,
    });
  }

  /** Mirror Kubo's redirect to `/ipfs/{cid}/` for directory roots so relative URLs stay under the CID. */
  private async getDirectoryTrailingSlashIfNeeded(
    fullPath: string,
    rootCid: string,
    req: FastifyRequest,
  ): Promise<string | null> {
    if (fullPath !== rootCid) {
      return null;
    }

    const headUrl = `${this.target}/ipfs/${rootCid}`;

    try {
      const res = await fetch(headUrl, {
        method: 'HEAD',
        redirect: 'manual',
        signal: AbortSignal.timeout(this.upstreamTimeoutMs),
      });
      if (res.status < 300 || res.status >= 400) {
        return null;
      }
      const loc = res.headers.get('location');
      if (!loc) {
        return null;
      }
      const resolved = new URL(loc, this.target);
      const m = resolved.pathname.match(/^\/ipfs\/([^/]+)\/$/);
      if (!m || m[1] !== rootCid) {
        return null;
      }
    } catch (error) {
      this.logger.debug(
        `Directory trailing-slash probe failed: ${(error as Error).message}`,
      );
      return null;
    }

    return `/ipfs/${rootCid}/` + this.getQueryString(req.url);
  }

  private getQueryString(url: string): string {
    return url.includes('?') ? url.slice(url.indexOf('?')) : '';
  }

  private getRequestArgs(req: FastifyRequest): string[] {
    const url = new URL(req.url, 'http://127.0.0.1');
    return url.searchParams.getAll('arg');
  }

  private getRootCid(req: FastifyRequest): string | null {
    return this.getRootCidFromArg(this.getRequestArgs(req)[0]);
  }

  private getRootCidFromArg(arg?: string): string | null {
    if (!arg) {
      return null;
    }

    const segments = arg.split('/').filter(Boolean);
    return segments[0] || null;
  }

  private getCacheableCatCid(req: FastifyRequest): string | null {
    const url = new URL(req.url, 'http://127.0.0.1');
    if (url.searchParams.has('offset') || url.searchParams.has('length')) {
      return null;
    }

    const args = url.searchParams.getAll('arg');
    if (args.length !== 1) {
      return null;
    }

    const segments = args[0].split('/').filter(Boolean);
    if (segments.length !== 1) {
      return null;
    }

    return segments[0] || null;
  }

  private collectAddCids(body: Buffer): string[] {
    const hashes = new Set<string>();
    for (const line of body.toString('utf8').split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) {
        continue;
      }

      try {
        const parsed = JSON.parse(trimmed) as { Hash?: string };
        if (parsed.Hash) {
          hashes.add(parsed.Hash);
        }
      } catch {
        // Ignore non-JSON progress lines.
      }
    }
    const cids = Array.from(hashes);
    this.logger.debug(
      `[IPFS RPC] Parsed add response CIDs: ${cids.length > 0 ? cids.join(', ') : 'none'}`,
    );
    return cids;
  }

  private collectPinCids(body: Buffer, req: FastifyRequest): string[] {
    try {
      const parsed = JSON.parse(body.toString('utf8')) as { Pins?: string[] };
      if (Array.isArray(parsed.Pins) && parsed.Pins.length > 0) {
        this.logger.debug(
          `[IPFS RPC] Parsed pin response CIDs: ${parsed.Pins.join(', ')}`,
        );
        return parsed.Pins;
      }
    } catch {
      // Fall back to query args below.
    }

    const cids = this.getRequestArgs(req)
      .map((arg) => this.getRootCidFromArg(arg))
      .filter((cid): cid is string => Boolean(cid));
    this.logger.debug(
      `[IPFS RPC] Parsed pin request arg CIDs: ${cids.length > 0 ? cids.join(', ') : 'none'}`,
    );
    return cids;
  }
}
