import {
  Body,
  Controller,
  Delete,
  Get,
  HttpStatus,
  Logger,
  Param,
  Post,
  Query,
  Res,
} from '@nestjs/common';
import {
  ApiBody,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import type { FastifyReply } from 'fastify';
import { contentDisposition } from '../../common/http.utils';
import { ConfigService } from '../../config/config.service';
import {
  ContentListResponseDto,
  ContentMetadataDto,
  StoreContentRequestDto,
} from '../dto/content';

import {
  DriveListResponseDto,
  FeedResponseDto,
  NodeStatusDto,
  PurgeStorageResponseDto,
} from '../dto/status';
import { DriveService } from '../services/drive.service';
import { FileIndexService } from '../services/file-index.service';
import { HiveDirectoryBzzService } from '../services/hive-directory-bzz.service';
import { IdentityService } from '../services/identity.service';


@ApiTags('hive')
@Controller('hive')
export class HiveController {
  private readonly logger = new Logger(HiveController.name);

  constructor(
    private readonly driveService: DriveService,
    private readonly fileIndexService: FileIndexService,
    private readonly identityService: IdentityService,
    private readonly config: ConfigService,
    private readonly hiveDirectoryBzzService: HiveDirectoryBzzService,
  ) {}

  @Get('status')
  @ApiOperation({
    summary: 'Get node status',
    description: 'Returns bridge cache counts for this local node',
  })
  @ApiResponse({ status: 200, description: 'Node status', type: NodeStatusDto })
  async getStatus(): Promise<NodeStatusDto> {
    const [contentCount, bridgedCount] = await Promise.all([
      this.driveService.getContentCount(),
      this.driveService.getBridgedCount(),
    ]);

    return {
      nodeId: this.config.nodeId,
      contentCount,
      bridgedCount,
    };
  }

  @Post('storage/purge')
  @ApiOperation({
    summary: 'Purge Hyperdrive storage folder',
    description:
      'Closes Corestore and Hyperdrive, deletes everything under STORAGE_PATH, recreates an empty store, and reloads the in-memory file index. All cached content, refs, and index data are lost.',
  })
  @ApiResponse({
    status: 200,
    description: 'Storage wiped and reopened',
    type: PurgeStorageResponseDto,
  })
  async purgeStorage(): Promise<PurgeStorageResponseDto> {
    await this.identityService.purgeStorageFolder();
    await this.fileIndexService.load();
    this.logger.warn(`Hyperdrive storage purged at ${this.config.storagePath}`);
    return {
      purged: true,
      storagePath: this.config.storagePath,
    };
  }

  @Get('list')
  @ApiOperation({
    summary: 'List all content',
    description: 'Returns metadata for all stored content',
  })
  @ApiResponse({
    status: 200,
    description: 'Content list',
    type: ContentListResponseDto,
  })
  async getContentList(): Promise<ContentListResponseDto> {
    const items = await this.driveService.listContent();
    return {
      items,
      count: items.length,
    };
  }

  @Get('ls')
  @ApiOperation({
    summary: 'List drive root',
    description: 'List entries at drive root',
  })
  @ApiResponse({
    status: 200,
    description: 'Drive listing',
    type: DriveListResponseDto,
  })
  async listRoot(): Promise<DriveListResponseDto> {
    const entries = await this.driveService.list('/');
    return {
      path: '/',
      entries,
      count: entries.length,
    };
  }

  @Get('ls/:path')
  @ApiOperation({
    summary: 'List drive path',
    description: 'List entries at specified drive path',
  })
  @ApiParam({
    name: 'path',
    description: 'Path to list (without leading slash)',
    example: 'content',
  })
  @ApiResponse({
    status: 200,
    description: 'Drive listing',
    type: DriveListResponseDto,
  })
  async listPath(@Param('path') path: string): Promise<DriveListResponseDto> {
    const fullPath = `/${path || ''}`;
    const entries = await this.driveService.list(fullPath);
    return {
      path: fullPath,
      entries,
      count: entries.length,
    };
  }

  @Get('content/:checksum')
  @ApiOperation({
    summary: 'Get content',
    description: 'Retrieve local cached content by SHA256 checksum',
  })
  @ApiParam({
    name: 'checksum',
    description: 'SHA256 checksum',
    example: '7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069',
  })
  @ApiResponse({
    status: 200,
    description: 'Raw content with appropriate Content-Type header',
  })
  @ApiResponse({ status: 404, description: 'Content not found' })
  async getContent(
    @Param('checksum') checksum: string,
    @Res() reply: FastifyReply,
  ): Promise<void> {
    const result = await this.driveService.get(checksum);

    if (!result) {
      reply.status(HttpStatus.NOT_FOUND).send({
        error: 'Content not found',
        checksum,
      });
      return;
    }

    reply.headers({
      'Content-Type': result.metadata.contentType,
      'Content-Length': result.content.length,
      'X-Checksum': result.metadata.checksum,
    });
    if (result.metadata.filename) {
      reply.header(
        'Content-Disposition',
        contentDisposition(result.metadata.filename),
      );
    }
    reply.send(result.content);
  }

  @Get('meta/:checksum')
  @ApiOperation({
    summary: 'Get metadata',
    description: 'Retrieve metadata only by checksum',
  })
  @ApiParam({
    name: 'checksum',
    description: 'SHA256 checksum',
    example: '7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069',
  })
  @ApiResponse({
    status: 200,
    description: 'Content metadata',
    type: ContentMetadataDto,
  })
  @ApiResponse({ status: 404, description: 'Metadata not found' })
  async getMetadata(
    @Param('checksum') checksum: string,
    @Res() reply: FastifyReply,
  ): Promise<void> {
    const metadata =
      this.fileIndexService.getByChecksum(checksum) ??
      (await this.driveService.getMetadata(checksum));

    if (!metadata) {
      reply.status(HttpStatus.NOT_FOUND).send({
        error: 'Metadata not found',
        checksum,
      });
      return;
    }

    reply.send(metadata);
  }

  @Get('feed/local')
  @ApiOperation({
    summary: 'Local content feed',
    description: 'Latest content stored on this node',
  })
  @ApiQuery({
    name: 'limit',
    required: false,
    type: Number,
    description: 'Max entries to return (default 10)',
  })
  @ApiResponse({
    status: 200,
    description: 'Local feed entries',
    type: FeedResponseDto,
  })
  async getFeedLocal(
    @Query('limit') limit: string,
    @Res() reply: FastifyReply,
  ): Promise<void> {
    const max = Math.min(parseInt(limit || '10', 10) || 10, 100);
    const items = (await this.driveService.listContent())
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(0, max)
      .map((metadata) => ({
        checksum: metadata.checksum,
        contentType: metadata.contentType,
        size: metadata.size,
        timestamp: metadata.timestamp,
      }));

    reply.send({ items, count: items.length, scope: 'local' });
  }

  @Post('publish/:checksum')
  @ApiOperation({
    summary: 'Build Swarm collection manifest for an IPFS directory',
    description:
      'Kubo must report ipfsCid as a UnixFS directory. Resolves child files from a JSON path→cid map or from indexed sub-paths, uploads the tree to Bee, and replaces bzzHash on the anchor row with the Swarm collection/manifest root (so GET /bzz/<manifest>/ serves the site). Rows that only bridged index.html keep a raw file chunk until this runs. Legacy directory-marker checksums redirect to the anchor.',
  })
  @ApiParam({
    name: 'checksum',
    description:
      'SHA-256 of the directory JSON map row, legacy marker row, or anchor file row (bare root ipfsCid)',
  })
  @ApiResponse({
    status: 200,
    description: 'Metadata after manifest upload',
    type: ContentMetadataDto,
  })
  @ApiResponse({ status: 404, description: 'Content not found' })
  @ApiResponse({ status: 400, description: 'Invalid directory JSON or config' })
  async uploadDirToBzz(
    @Param('checksum') checksum: string,
  ): Promise<ContentMetadataDto> {
    return this.hiveDirectoryBzzService.uploadDirectoryTreeToBzz(
      checksum.trim(),
    );
  }

  @Post('content')
  @ApiOperation({
    summary: 'Store content',
    description: 'Store new content with optional metadata',
  })
  @ApiBody({ type: StoreContentRequestDto })
  @ApiResponse({
    status: 201,
    description: 'Content stored',
    type: ContentMetadataDto,
  })
  async storeContent(
    @Body() body: StoreContentRequestDto,
  ): Promise<ContentMetadataDto> {
    const content = Buffer.from(body.content, 'base64');
    const isDirBzz =
      this.hiveDirectoryBzzService.isDirectoryMapContent(
        body.contentType,
        content,
      ) && Boolean(body.bzzHash);

    if (isDirBzz) {
      const metadata = await this.driveService.put({
        content,
        contentType: body.contentType,
        filename: body.filename,
        lastModified: body.lastModified,
        sourcePath: body.sourcePath,
        ipfsCid: body.ipfsCid,
      });
      await this.fileIndexService.addOrUpdate(metadata);
      const published =
        await this.hiveDirectoryBzzService.uploadDirectoryTreeToBzz(
          metadata.checksum,
        );
      this.logger.log(
        `Stored directory + Swarm manifest: ${published.checksum.slice(0, 16)}…`,
      );
      return published;
    }

    const metadata = await this.driveService.put({
      content,
      contentType: body.contentType,
      filename: body.filename,
      lastModified: body.lastModified,
      sourcePath: body.sourcePath,
      ipfsCid: body.ipfsCid,
      bzzHash: body.bzzHash,
    });

    await this.fileIndexService.addOrUpdate(metadata);

    this.logger.log(
      `Stored content: ${metadata.checksum.slice(0, 16)}... (${metadata.size} bytes)`,
    );

    return metadata;
  }

  @Post('drive')
  @ApiOperation({
    summary: 'Store content privately',
    description:
      'Store content to the drive without publishing to the content index',
  })
  @ApiBody({ type: StoreContentRequestDto })
  @ApiResponse({
    status: 201,
    description: 'Content stored privately',
    type: ContentMetadataDto,
  })
  async storePrivate(
    @Body() body: StoreContentRequestDto,
  ): Promise<ContentMetadataDto> {
    const metadata = await this.driveService.put({
      content: Buffer.from(body.content, 'base64'),
      contentType: body.contentType,
      filename: body.filename,
      lastModified: body.lastModified,
      sourcePath: body.sourcePath,
      ipfsCid: body.ipfsCid,
      bzzHash: body.bzzHash,
    });

    this.logger.log(
      `Stored privately: ${metadata.checksum.slice(0, 16)}... (${metadata.size} bytes)`,
    );

    return metadata;
  }

  @Delete('content/:checksum')
  @ApiOperation({
    summary: 'Delete content',
    description: 'Delete content by checksum',
  })
  @ApiParam({
    name: 'checksum',
    description: 'SHA256 checksum',
    example: '7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069',
  })
  @ApiResponse({ status: 200, description: 'Content deleted' })
  @ApiResponse({ status: 404, description: 'Content not found' })
  async deleteContent(
    @Param('checksum') checksum: string,
    @Res() reply: FastifyReply,
  ): Promise<void> {
    const deleted = await this.driveService.delete(checksum);

    if (!deleted) {
      reply.status(HttpStatus.NOT_FOUND).send({
        error: 'Content not found',
        checksum,
      });
      return;
    }

    await this.fileIndexService.remove(checksum);
    reply.send({ deleted: true, checksum });
  }
}
