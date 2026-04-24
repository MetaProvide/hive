import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '../../config/config.service';
import type { ContentMetadataDto } from '../dto/content';
import { DriveService } from './drive.service';
import { FileIndexService } from './file-index.service';

export interface BeeFetchResult {
  content: Buffer;
  contentType: string;
  filename?: string;
}

@Injectable()
export class SwarmBridgeService {
  private readonly logger = new Logger(SwarmBridgeService.name);

  constructor(
    private readonly config: ConfigService,
    private readonly driveService: DriveService,
    private readonly fileIndexService: FileIndexService,
  ) {}

  /**
   * Legacy no-op: the anchor file row (`ipfsCid` = bare directory root) holds the Swarm manifest
   * `bzzHash`. Synthetic `contentKind: directory` marker rows are migrated away on index load.
   */
  async ensureIpfsDirectoryListEntry(_rootCid: string): Promise<void> {
    return;
  }

  async getBzzHashForCid(cid: string): Promise<string | null> {
    const bzzHash =
      this.fileIndexService.getBzzHashByIpfsCid(cid) ??
      (await this.driveService.resolveIpfsToBzz(cid));
    this.logger.debug(
      `[${cid.slice(0, 16)}] CID->bzz lookup ${bzzHash ? `hit ${bzzHash.slice(0, 16)}...` : 'miss'}`,
    );
    return bzzHash;
  }

  async bridgeIpfsContent(
    cid: string,
    content: Buffer,
    contentType = 'application/octet-stream',
    filename?: string,
  ): Promise<ContentMetadataDto> {
    this.logger.debug(
      `[${cid.slice(0, 16)}] Starting IPFS->Swarm bridge (${content.length} bytes, ${contentType}, filename=${filename ?? 'none'})`,
    );
    const existingBzzHash = await this.getBzzHashForCid(cid);

    let metadata = await this.driveService.put({
      content,
      contentType,
      filename,
      ipfsCid: cid,
      bzzHash: existingBzzHash ?? undefined,
    });
    await this.fileIndexService.addOrUpdate(metadata);
    this.logger.debug(
      `[${cid.slice(0, 16)}] Stored local metadata under checksum ${metadata.checksum.slice(0, 16)}...`,
    );

    if (existingBzzHash) {
      this.logger.debug(
        `[${cid.slice(0, 16)}] Reusing existing Swarm hash ${existingBzzHash.slice(0, 16)}...`,
      );
      const linked = await this.driveService.linkIpfsCidToBzz(
        cid,
        existingBzzHash,
        metadata.checksum,
      );
      metadata = linked ?? { ...metadata, bzzHash: existingBzzHash };
      await this.fileIndexService.addOrUpdate(metadata);
      this.logger.debug(
        `[${cid.slice(0, 16)}] Bridge complete using existing Swarm hash ${existingBzzHash.slice(0, 16)}...`,
      );
      return metadata;
    }

    this.logger.debug(
      `[${cid.slice(0, 16)}] No Swarm hash exists yet, uploading to Bee`,
    );
    const bzzHash = await this.uploadToBee(content, contentType);
    const linked = await this.driveService.linkIpfsCidToBzz(
      cid,
      bzzHash,
      metadata.checksum,
    );
    metadata = linked ?? { ...metadata, bzzHash };
    await this.fileIndexService.addOrUpdate(metadata);
    this.logger.debug(
      `[${cid.slice(0, 16)}] Bridge complete with new Swarm hash ${bzzHash.slice(0, 16)}...`,
    );

    return metadata;
  }

  /**
   * Pins sub-path file bytes in Bee, stores Hive content with the IPFS ref path as `ipfsCid`.
   * `bzzHash` is only the Bee content reference for this chunk (GET /bzz/{bzzHash}), not a path.
   */
  async saveIpfsSubpath(
    refKey: string,
    content: Buffer,
    contentType: string,
    filename: string | undefined,
  ): Promise<ContentMetadataDto> {
    const segments = refKey.split('/').filter(Boolean);
    if (segments.length < 2) {
      throw new Error(
        `saveIpfsSubpath expects a sub-path ref (root plus path), got: ${refKey}`,
      );
    }
    this.logger.debug(
      `[${refKey.slice(0, 32)}] Uploading IPFS sub-path to Bee (${content.length} bytes)`,
    );
    const chunkRef = await this.uploadToBee(content, contentType);
    let metadata = await this.driveService.put({
      content,
      contentType,
      filename,
      ipfsCid: refKey,
      bzzHash: chunkRef,
    });
    await this.fileIndexService.addOrUpdate(metadata);
    const linked = await this.driveService.linkIpfsCidToBzz(
      refKey,
      chunkRef,
      metadata.checksum,
    );
    if (linked) {
      await this.fileIndexService.addOrUpdate(linked);
      metadata = linked;
    }
    this.logger.debug(
      `[${refKey.slice(0, 32)}] IPFS sub-path in Hive+Swarm (Bee chunk + path ref)`,
    );
    await this.ensureIpfsDirectoryListEntry(segments[0]!);
    return metadata;
  }

  /**
   * Backfills Hyperdrive from a successful Bee read without re-uploading the chunk.
   * @param bzzChunkRef The Bee `GET /bzz/{ref}` content reference (no path suffixes).
   */
  async recordBridgedSubpathInHive(
    refKey: string,
    content: Buffer,
    contentType: string,
    filename: string | undefined,
    bzzChunkRef: string,
  ): Promise<ContentMetadataDto> {
    const segments = refKey.split('/').filter(Boolean);
    if (segments.length < 2) {
      throw new Error(
        `recordBridgedSubpathInHive expects a sub-path ref (root plus path), got: ${refKey}`,
      );
    }
    let metadata = await this.driveService.put({
      content,
      contentType,
      filename,
      ipfsCid: refKey,
      bzzHash: bzzChunkRef,
    });
    await this.fileIndexService.addOrUpdate(metadata);
    const linked = await this.driveService.linkIpfsCidToBzz(
      refKey,
      bzzChunkRef,
      metadata.checksum,
    );
    if (linked) {
      await this.fileIndexService.addOrUpdate(linked);
      metadata = linked;
    }
    await this.ensureIpfsDirectoryListEntry(segments[0]!);
    return metadata;
  }

  async uploadToBee(
    content: Buffer,
    contentType = 'application/octet-stream',
  ): Promise<string> {
    if (!this.config.beePostageStamp) {
      throw new Error(
        'BEE_POSTAGE_STAMP is required for IPFS-to-Swarm bridging',
      );
    }

    this.logger.debug(
      `Uploading ${content.length} bytes to Bee ${this.config.beeApiUrl}/bzz with swarm-pin=true`,
    );

    const response = await fetch(`${this.config.beeApiUrl}/bzz`, {
      method: 'POST',
      headers: {
        'content-type': contentType,
        'swarm-pin': 'true',
        'swarm-postage-batch-id': this.config.beePostageStamp,
      },
      body: content as BodyInit,
      signal: AbortSignal.timeout(this.config.upstreamTimeout),
    });

    const responseBody = await response.text();
    if (!response.ok) {
      throw new Error(
        `Bee upload failed with ${response.status}: ${responseBody}`,
      );
    }

    const reference = JSON.parse(responseBody).reference as string | undefined;
    if (!reference) {
      throw new Error('Bee upload response did not include a reference');
    }

    this.logger.debug(
      `Bee upload succeeded with reference ${reference.slice(0, 16)}...`,
    );

    return reference;
  }

  async fetchFromBee(bzzHash: string): Promise<BeeFetchResult | null> {
    try {
      this.logger.debug(
        `[${bzzHash.slice(0, 16)}] Fetching content from Bee ${this.config.beeApiUrl}`,
      );
      const response = await fetch(`${this.config.beeApiUrl}/bzz/${bzzHash}`, {
        signal: AbortSignal.timeout(this.config.upstreamTimeout),
      });

      if (!response.ok) {
        this.logger.warn(
          `[${bzzHash.slice(0, 12)}] Bee fetch returned ${response.status}`,
        );
        return null;
      }

      const content = Buffer.from(await response.arrayBuffer());
      const contentType =
        response.headers.get('content-type') || 'application/octet-stream';
      this.logger.debug(
        `[${bzzHash.slice(0, 16)}] Bee fetch succeeded (${content.length} bytes, ${contentType})`,
      );

      return {
        content,
        contentType,
        filename: this.parseFilename(
          response.headers.get('content-disposition') ?? undefined,
        ),
      };
    } catch (error) {
      this.logger.warn(
        `[${bzzHash.slice(0, 12)}] Bee fetch failed: ${(error as Error).message}`,
      );
      return null;
    }
  }

  private parseFilename(contentDisposition?: string): string | undefined {
    if (!contentDisposition) {
      return undefined;
    }

    const encodedMatch = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
    if (encodedMatch) {
      return decodeURIComponent(encodedMatch[1]);
    }

    const plainMatch = contentDisposition.match(/filename="?([^";]+)"?/i);
    return plainMatch?.[1];
  }
}
