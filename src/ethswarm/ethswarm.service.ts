import { Injectable, type OnModuleInit } from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';

type FastifyReplyWithFrom = FastifyReply & {
  from: (source: string) => FastifyReply;
};

import {
  BaseProxyService,
  type UpstreamResponse,
} from '../common/base-proxy.service';
import { contentDisposition } from '../common/http.utils';
import { ConfigService } from '../config/config.service';
import type {
  ContentMetadataDto,
  ContentResponseDto,
} from '../hive/dto/content';
import { DriveService } from '../hive/services/drive.service';
import { FileIndexService } from '../hive/services/file-index.service';

@Injectable()
export class EthswarmService extends BaseProxyService implements OnModuleInit {
  protected readonly protocol = 'bzz';

  constructor(
    httpAdapterHost: HttpAdapterHost,
    driveService: DriveService,
    private readonly fileIndexService: FileIndexService,
    config: ConfigService,
  ) {
    super(
      httpAdapterHost,
      driveService,
      config,
      EthswarmService.name,
      config.beeApiUrl,
    );
  }

  async onModuleInit() {
    const fastify: FastifyInstance =
      this.httpAdapterHost.httpAdapter.getInstance();

    await fastify.register(async (instance) => {
      instance.removeAllContentTypeParsers();
      instance.addContentTypeParser(
        '*',
        { parseAs: 'buffer' },
        (_req, body, done) => done(null, body),
      );

      instance.get('/bzz/*', (req: FastifyRequest, reply: FastifyReply) =>
        this.handleGet(req, reply),
      );

      instance.post('/bzz', (req: FastifyRequest, reply: FastifyReply) =>
        this.handleUpload(req, reply),
      );

      instance.post('/bzz/*', (req: FastifyRequest, reply: FastifyReply) =>
        this.handleUpload(req, reply),
      );

      instance.get('/chunks/*', (req: FastifyRequest, reply: FastifyReply) =>
        this.handleGetForProtocol(req, reply, 'chunks'),
      );

      instance.get('/bytes/*', (req: FastifyRequest, reply: FastifyReply) =>
        this.handleGetForProtocol(req, reply, 'bytes'),
      );

      instance.post('/bytes', (req: FastifyRequest, reply: FastifyReply) =>
        this.handleUpload(req, reply, 'bytes'),
      );

      instance.post('/chunks', (req: FastifyRequest, reply: FastifyReply) =>
        this.handleUpload(req, reply, 'chunks'),
      );
    });

    this.logger.log(
      `Ethswarm proxy registered -> ${this.target} [GET /bzz/*, /chunks/*, /bytes/*, POST /bzz, /bzz/*, /bytes, /chunks]`,
    );
  }

  protected async lookupCache(
    hash: string,
  ): Promise<ContentResponseDto | null> {
    return this.driveService.getByRef('bzz', this.bzzGatewayRefKey(hash));
  }

  protected async cacheToDrive(
    hash: string,
    content: Buffer,
    contentType: string,
    filename?: string,
  ): Promise<ContentMetadataDto> {
    return this.driveService.putBzz(hash, content, contentType, filename);
  }

  protected addProtocolHeaders(
    reply: FastifyReply,
    id: string,
    metadata?: ContentMetadataDto,
  ): void {
    const filename = metadata?.filename || id;
    reply.header('accept-ranges', 'bytes');
    reply.header('content-disposition', contentDisposition(filename));
    if (metadata?.checksum) {
      reply.header('etag', `"${metadata.checksum}"`);
    }
  }

  protected override async handleGet(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    await this.handleSwarmFirstGet(req, reply, this.protocol);
  }

  protected override async handleGetForProtocol(
    req: FastifyRequest,
    reply: FastifyReply,
    protocol: string,
  ): Promise<void> {
    await this.handleSwarmFirstGet(req, reply, protocol);
  }

  private async handleSwarmFirstGet(
    req: FastifyRequest,
    reply: FastifyReply,
    protocol: string,
  ): Promise<void> {
    const fullPath = (req.params as Record<string, string>)['*'];
    const segments = fullPath.split('/').filter(Boolean);
    const id = segments[0];
    const refKey = this.bzzGatewayRefKey(fullPath);
    const isRootOnly = !refKey.includes('/');
    const tag = `${protocol}:${id.slice(0, 12)}`;

    if (!isRootOnly) {
      this.logger.debug(
        `[${tag}] Sub-path request (ref ${refKey.slice(0, 48)}${refKey.length > 48 ? '…' : ''})`,
      );
    }

    this.logger.log(`[${tag}] Swarm-first read: trying Bee before Hive cache`);

    const beePath = refKey;
    let upstream: UpstreamResponse | null = null;
    if (protocol === this.protocol && isRootOnly && beePath.length > 0) {
      // Bee often returns 200 + raw mantaray bytes for GET /bzz/<ref> while
      // GET /bzz/<ref>/ resolves website-index-document. Prefer the collection URL.
      const withSlash = `${this.target}/${protocol}/${beePath}/`;
      const noSlash = `${this.target}/${protocol}/${beePath}`;
      this.logger.debug(
        `[${tag}] Root bzz: trying collection URL first: ${withSlash}`,
      );
      upstream = await this.fetchUpstream(withSlash);
      if (!upstream || upstream.status < 200 || upstream.status >= 400) {
        this.logger.debug(
          `[${tag}] Root bzz: collection URL miss; trying bare reference: ${noSlash}`,
        );
        upstream = await this.fetchUpstream(noSlash);
      }
    } else {
      upstream = await this.fetchUpstream(
        `${this.target}/${protocol}/${beePath}`,
      );
    }
    if (upstream && upstream.status >= 200 && upstream.status < 400) {
      this.logger.log(
        `[${tag}] Bee served the content (${upstream.content.length} bytes, ${upstream.contentType})`,
      );

      let responseMetadata: ContentMetadataDto | undefined;
      if (protocol === this.protocol) {
        if (isRootOnly) {
          this.cacheInBackground(id, upstream.content, upstream.contentType);
        } else {
          try {
            // Await so /meta and bridge fields (bzzHash, bzzRefKey) exist before the next list/read
            responseMetadata = await this.cacheBzzSubpath(
              refKey,
              id,
              upstream.content,
              upstream.contentType,
            );
          } catch (e) {
            this.logger.warn(
              `[${tag}] Storing bzz sub-path in Hive failed: ${(e as Error).message}`,
            );
          }
        }
      } else {
        this.cacheWithRefInBackground(
          protocol,
          isRootOnly ? id : refKey,
          upstream.content,
          upstream.contentType,
        );
      }

      this.sendResponse(
        reply,
        upstream.content,
        upstream.contentType,
        'swarm',
        id,
        responseMetadata,
      );

      return;
    }

    this.logger.warn(`[${tag}] Bee unavailable, falling back to Hive cache`);

    const cached = await this.driveService.getByRef(protocol, refKey);

    if (cached) {
      this.logger.log(
        `[${tag}] Hive cache served the content (${cached.content.length} bytes, ${cached.metadata.contentType})`,
      );
      let metadata = cached.metadata;
      if (
        protocol === this.protocol &&
        !isRootOnly &&
        (!metadata.bzzHash || !metadata.bzzRefKey)
      ) {
        const segments = refKey.split('/').filter(Boolean);
        const filename =
          segments.length > 1 ? segments[segments.length - 1] : undefined;
        try {
          metadata = await this.driveService.put({
            content: cached.content,
            contentType: cached.metadata.contentType,
            filename: metadata.filename ?? filename,
            bzzHash: id,
            bzzRefKey: refKey,
          });
        } catch (e) {
          this.logger.warn(
            `[${tag}] Could not backfill bzz metadata for sub-path: ${(e as Error).message}`,
          );
        }
      }
      this.sendResponse(
        reply,
        cached.content,
        metadata.contentType,
        'hive',
        id,
        metadata,
      );
      return;
    }

    this.logger.error(
      `[${tag}] Bee buffered fetch failed (status ${upstream?.status ?? 'n/a'}) and Hive cache does not have the content`,
    );

    if (protocol === this.protocol && isRootOnly && beePath.length > 0) {
      const streamUrl = `${this.target}/${protocol}/${beePath}/`;
      this.logger.warn(
        `[${tag}] Streaming fallback to Bee (handles mantaray / collection responses): ${streamUrl}`,
      );
      return (reply as FastifyReplyWithFrom).from(streamUrl);
    }

    reply.status(502).send({
      statusCode: 502,
      error: 'Bad Gateway',
      message: `${protocol.toUpperCase()} gateway unavailable and no cached content exists`,
    });
  }

  private bzzGatewayRefKey(fullPath: string): string {
    return fullPath.replace(/\/+$/, '');
  }

  private async cacheBzzSubpath(
    refKey: string,
    rootBzz: string,
    content: Buffer,
    contentType: string,
  ): Promise<ContentMetadataDto> {
    const segments = refKey.split('/').filter(Boolean);
    const filename =
      segments.length > 1 ? segments[segments.length - 1] : undefined;
    this.logger.debug(
      `[${rootBzz.slice(0, 12)}] Storing bzz sub-path in Hive (preserving any existing ipfsCid on this checksum)`,
    );
    const metadata = await this.driveService.put({
      content,
      contentType,
      filename,
      bzzHash: rootBzz,
      bzzRefKey: refKey,
    });
    this.logger.debug(
      `[${rootBzz.slice(0, 12)}] Cached bzz sub-path -> ${metadata.checksum.slice(0, 16)}...`,
    );
    return metadata;
  }

  private async handleUpload(
    req: FastifyRequest,
    reply: FastifyReply,
    protocol = 'bzz',
  ) {
    const target = `${this.target}${req.url}`;
    const body = this.normalizeUploadBody(req.body, protocol);
    if (!body) {
      return reply.status(400).send({
        statusCode: 400,
        error: 'Bad Request',
        message: 'Swarm uploads must provide a raw binary or text request body',
      });
    }

    const headers = this.buildForwardHeaders(req);
    headers.set('swarm-pin', 'true');
    const contentType =
      headers.get('content-type') || 'application/octet-stream';

    this.logger.log(
      `[${protocol}:upload] Intercepting Swarm upload (${body.length} bytes, ${contentType})`,
    );

    this.logger.debug(
      `[${protocol}:upload] Forwarding upload to Bee with original request headers and swarm-pin=true`,
    );

    try {
      const upstream = await fetch(target, {
        method: 'POST',
        headers,
        body: body as BodyInit,
        signal: AbortSignal.timeout(this.upstreamTimeoutMs),
      });

      const responseBody = await upstream.text();
      this.logger.debug(
        `[${protocol}:upload] Bee responded ${upstream.status} with ${responseBody.length} characters`,
      );
      if (upstream.ok) {
        try {
          const reference = JSON.parse(responseBody).reference as
            | string
            | undefined;
          if (reference) {
            this.logger.log(
              `[${protocol}:upload] Bee created reference ${reference.slice(0, 16)}..., caching locally`,
            );
            const filename =
              protocol === 'bzz'
                ? (req.query as Record<string, string>)?.name || 'untitled'
                : undefined;

            if (protocol === 'bzz') {
              this.cacheInBackground(reference, body, contentType, filename);
            } else {
              this.cacheWithRefInBackground(
                protocol,
                reference,
                body,
                contentType,
                filename,
              );
            }
          }
        } catch {
          this.logger.warn(
            `[${protocol}:upload] Bee upload succeeded but did not return a cacheable reference payload`,
          );
        }
      }

      reply
        .status(upstream.status)
        .header('content-type', 'application/json')
        .send(responseBody);
    } catch (error) {
      reply.status(502).send({
        statusCode: 502,
        error: 'Bad Gateway',
        message: `Bee upload failed: ${(error as Error).message}`,
      });
    }
  }

  private buildForwardHeaders(req: FastifyRequest): Headers {
    const headers = new Headers();
    const rawHeaders = req.raw.rawHeaders;

    for (let index = 0; index < rawHeaders.length; index += 2) {
      const name = rawHeaders[index];
      const value = rawHeaders[index + 1];
      const lowerName = name.toLowerCase();

      if (
        lowerName === 'host' ||
        lowerName === 'content-length' ||
        lowerName === 'connection' ||
        lowerName === 'transfer-encoding'
      ) {
        continue;
      }

      headers.append(name, value);
    }

    return headers;
  }

  private normalizeUploadBody(
    body: unknown,
    protocol: string,
  ): Buffer | undefined {
    if (Buffer.isBuffer(body)) {
      return body;
    }

    if (typeof body === 'string') {
      return Buffer.from(body);
    }

    if (body instanceof Uint8Array) {
      return Buffer.from(body);
    }

    if (body === undefined || body === null) {
      this.logger.warn(`[${protocol}:upload] Upload request had no body`);
      return undefined;
    }

    this.logger.warn(
      `[${protocol}:upload] Unsupported upload body type: ${typeof body}`,
    );
    return undefined;
  }
}
