import { Logger } from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import type { FastifyReply, FastifyRequest } from 'fastify';
import {
  CONTENT_ORIGIN_HEADER,
  type ContentOrigin,
} from './content-origin';
import type { ConfigService } from '../config/config.service';
import type {
  ContentMetadataDto,
  ContentResponseDto,
} from '../hive/dto/content';
import type { DriveService } from '../hive/services/drive.service';

export interface UpstreamResponse {
  content: Buffer;
  contentType: string;
  status: number;
}

export abstract class BaseProxyService {
  protected readonly logger: Logger;
  protected readonly target: string;
  protected readonly upstreamTimeoutMs: number;

  constructor(
    protected readonly httpAdapterHost: HttpAdapterHost,
    protected readonly driveService: DriveService,
    config: ConfigService,
    loggerContext: string,
    target: string,
  ) {
    this.logger = new Logger(loggerContext);
    this.target = target;
    this.upstreamTimeoutMs = config.upstreamTimeout;
  }

  protected abstract readonly protocol: string;

  protected abstract lookupCache(
    id: string,
  ): Promise<ContentResponseDto | null>;

  protected abstract cacheToDrive(
    id: string,
    content: Buffer,
    contentType: string,
    filename?: string,
  ): Promise<ContentMetadataDto>;

  protected async handleGet(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    const fullPath = (req.params as Record<string, string>)['*'];
    const segments = fullPath.split('/').filter(Boolean);
    const id = segments[0];
    const hasSubPath = segments.length > 1;
    const tag = id.slice(0, 16);

    if (hasSubPath) {
      this.logger.debug(`[${tag}] Sub-path request, proxying directly`);
      return reply.from(`${this.target}/${this.protocol}/${fullPath}`);
    }

    this.logger.debug(
      `[${tag}] Looking up ${this.protocol} content in local cache`,
    );
    const cached = await this.lookupCache(id);
    if (cached) {
      this.logger.debug(
        `[${tag}] Local cache hit (${cached.content.length} bytes, ${cached.metadata.contentType})`,
      );
      this.sendResponse(
        reply,
        cached.content,
        cached.metadata.contentType,
        'hive',
        id,
        cached.metadata,
      );
      return;
    }

    const upstreamUrl = `${this.target}/${this.protocol}/${fullPath}`;
    this.logger.debug(
      `[${tag}] Local cache miss, fetching upstream ${upstreamUrl}`,
    );
    const upstream = await this.fetchUpstream(upstreamUrl);

    if (upstream && upstream.status >= 200 && upstream.status < 400) {
      this.logger.debug(
        `[${tag}] Upstream ${this.protocol} fetch succeeded (${upstream.status}, ${upstream.content.length} bytes, ${upstream.contentType})`,
      );
      this.sendResponse(
        reply,
        upstream.content,
        upstream.contentType,
        this.getContentOriginForProtocol(this.protocol),
        id,
      );
      this.cacheInBackground(id, upstream.content, upstream.contentType);
      return;
    }

    this.logger.warn(
      `[${tag}] Upstream ${this.protocol} fetch failed and no cached content is available`,
    );

    reply.status(502).send({
      statusCode: 502,
      error: 'Bad Gateway',
      message: `${this.protocol.toUpperCase()} gateway unavailable and no cached content exists`,
    });
  }

  protected async handleGetForProtocol(
    req: FastifyRequest,
    reply: FastifyReply,
    protocol: string,
  ): Promise<void> {
    const fullPath = (req.params as Record<string, string>)['*'];
    const segments = fullPath.split('/').filter(Boolean);
    const id = segments[0];
    const hasSubPath = segments.length > 1;

    if (hasSubPath) {
      this.logger.debug(
        `[${protocol}:${id.slice(0, 12)}] Sub-path request, proxying directly`,
      );
      return reply.from(`${this.target}/${protocol}/${fullPath}`);
    }

    this.logger.debug(
      `[${protocol}:${id.slice(0, 12)}] Looking up protocol ref in local cache`,
    );
    const cached = await this.driveService.getByRef(protocol, id);
    if (cached) {
      this.logger.debug(
        `[${protocol}:${id.slice(0, 12)}] Local cache hit (${cached.content.length} bytes, ${cached.metadata.contentType})`,
      );
      this.sendResponse(
        reply,
        cached.content,
        cached.metadata.contentType,
        'hive',
        id,
        cached.metadata,
      );
      return;
    }

    const upstreamUrl = `${this.target}/${protocol}/${fullPath}`;
    this.logger.debug(
      `[${protocol}:${id.slice(0, 12)}] Local cache miss, fetching upstream ${upstreamUrl}`,
    );
    const upstream = await this.fetchUpstream(upstreamUrl);

    if (upstream && upstream.status >= 200 && upstream.status < 400) {
      this.logger.debug(
        `[${protocol}:${id.slice(0, 12)}] Upstream fetch succeeded (${upstream.status}, ${upstream.content.length} bytes, ${upstream.contentType})`,
      );
      this.sendResponse(
        reply,
        upstream.content,
        upstream.contentType,
        this.getContentOriginForProtocol(protocol),
        id,
      );
      this.cacheWithRefInBackground(
        protocol,
        id,
        upstream.content,
        upstream.contentType,
      );
      return;
    }

    this.logger.warn(
      `[${protocol}:${id.slice(0, 12)}] Upstream fetch failed and no cached content is available`,
    );

    reply.status(502).send({
      statusCode: 502,
      error: 'Bad Gateway',
      message: `${protocol.toUpperCase()} gateway unavailable and no cached content exists`,
    });
  }

  protected cacheWithRefInBackground(
    protocol: string,
    id: string,
    content: Buffer,
    contentType: string,
    filename?: string,
  ): void {
    this.logger.debug(
      `[${protocol}:${id.slice(0, 12)}] Caching upstream response in background`,
    );
    this.driveService
      .putWithRef(protocol, id, content, contentType, filename)
      .then((metadata) => {
        this.logger.debug(
          `[${protocol}:${id.slice(0, 12)}] Cached as checksum ${metadata.checksum.slice(0, 16)}...`,
        );
      })
      .catch((error) => {
        this.logger.error(
          `[${protocol}:${id.slice(0, 12)}] Failed to cache: ${(error as Error).message}`,
        );
      });
  }

  protected sendResponse(
    reply: FastifyReply,
    content: Buffer,
    contentType: string,
    contentOrigin: ContentOrigin,
    id: string,
    metadata?: ContentMetadataDto,
  ): void {
    this.logger.debug(
      `[${id.slice(0, 16)}] Sending response (${contentOrigin}, ${content.length} bytes, ${contentType})`,
    );
    reply.header('content-type', contentType);
    reply.header(CONTENT_ORIGIN_HEADER, contentOrigin);
    this.addProtocolHeaders(reply, id, metadata);
    reply.send(content);
  }

  protected getContentOriginForProtocol(
    protocol: string,
  ): Extract<ContentOrigin, 'ipfs' | 'swarm'> {
    return protocol === 'ipfs' || protocol === 'ipns' ? 'ipfs' : 'swarm';
  }

  protected addProtocolHeaders(
    _reply: FastifyReply,
    _id: string,
    _metadata?: ContentMetadataDto,
  ): void {}

  protected cacheInBackground(
    id: string,
    content: Buffer,
    contentType: string,
    filename?: string,
  ): void {
    this.logger.debug(
      `[${id.slice(0, 16)}] Caching response in background (${content.length} bytes, ${contentType})`,
    );
    this.cacheToDrive(id, content, contentType, filename).catch((error) => {
      this.logger.error(
        `[${id.slice(0, 16)}] Failed to cache: ${(error as Error).message}`,
      );
    });
  }

  protected async fetchUpstream(url: string): Promise<UpstreamResponse | null> {
    try {
      this.logger.debug(`Fetching upstream URL ${url}`);
      const response = await fetch(url, {
        signal: AbortSignal.timeout(this.upstreamTimeoutMs),
      });
      const content = Buffer.from(await response.arrayBuffer());
      const contentType =
        response.headers.get('content-type') || 'application/octet-stream';
      this.logger.debug(
        `Fetched upstream URL ${url} -> ${response.status} (${content.length} bytes, ${contentType})`,
      );
      return {
        content,
        contentType: contentType,
        status: response.status,
      };
    } catch (error) {
      this.logger.warn(`Upstream fetch error: ${(error as Error).message}`);
      return null;
    }
  }
}
