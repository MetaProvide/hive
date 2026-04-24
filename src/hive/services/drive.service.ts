import * as crypto from 'node:crypto';
import { Injectable, Logger } from '@nestjs/common';
import type {
  ContentMetadataDto,
  ContentResponseDto,
  StoreContentDto,
} from '../dto/content';
import type { DriveEntryDto } from '../dto/status';
import { IdentityService } from './identity.service';

@Injectable()
export class DriveService {
  private readonly logger = new Logger(DriveService.name);

  constructor(private readonly identityService: IdentityService) {}

  calculateChecksum(content: Buffer): string {
    return crypto.createHash('sha256').update(content).digest('hex');
  }

  private getContentPath(checksum: string): string {
    return `/content/${checksum}`;
  }

  private getMetadataPath(checksum: string): string {
    return `/meta/${checksum}.json`;
  }

  private getIpfsRefPath(cid: string): string {
    return `/refs/ipfs/${cid}`;
  }

  private getBzzRefPath(hash: string): string {
    return `/refs/bzz/${hash}`;
  }

  private getIpfsBridgePath(cid: string): string {
    return `/refs/bridge/ipfs/${cid}`;
  }

  private async writeMetadata(metadata: ContentMetadataDto): Promise<void> {
    const drive = this.identityService.getDrive();
    this.logger.debug(
      `Writing metadata for ${metadata.checksum.slice(0, 16)}... to ${this.getMetadataPath(metadata.checksum)}`,
    );
    await drive.put(
      this.getMetadataPath(metadata.checksum),
      Buffer.from(JSON.stringify(metadata)),
    );
  }

  private async syncRefs(
    metadata: Pick<
      ContentMetadataDto,
      | 'checksum'
      | 'ipfsCid'
      | 'bzzHash'
      | 'manifestBzzHash'
      | 'bzzRefKey'
      | 'contentKind'
    >,
  ): Promise<void> {
    if (metadata.contentKind === 'directory') {
      return;
    }
    const drive = this.identityService.getDrive();
    const isBzzSubPath = Boolean(metadata.bzzRefKey);

    if (metadata.ipfsCid && !isBzzSubPath) {
      await drive.put(
        this.getIpfsRefPath(metadata.ipfsCid),
        Buffer.from(metadata.checksum),
      );
      this.logger.debug(
        `Created IPFS ref: ${metadata.ipfsCid} -> ${metadata.checksum.slice(0, 16)}...`,
      );
    }

    if (metadata.bzzRefKey) {
      await drive.put(
        this.getBzzRefPath(metadata.bzzRefKey),
        Buffer.from(metadata.checksum),
      );
      this.logger.debug(
        `Created Bzz path ref: ${metadata.bzzRefKey.slice(0, 24)}... -> ${metadata.checksum.slice(0, 16)}...`,
      );
    } else {
      const bzzRefs = new Set<string>();
      if (metadata.manifestBzzHash) {
        bzzRefs.add(metadata.manifestBzzHash);
      }
      if (metadata.bzzHash) {
        bzzRefs.add(metadata.bzzHash);
      }
      for (const ref of bzzRefs) {
        await drive.put(
          this.getBzzRefPath(ref),
          Buffer.from(metadata.checksum),
        );
        this.logger.debug(
          `Created Bzz ref: ${ref.slice(0, 16)}… -> ${metadata.checksum.slice(0, 16)}...`,
        );
      }
    }
  }

  async exists(path: string): Promise<boolean> {
    const drive = this.identityService.getDrive();
    try {
      const entry = await drive.entry(path);
      return entry !== null;
    } catch {
      return false;
    }
  }

  async put(dto: StoreContentDto): Promise<ContentMetadataDto> {
    const drive = this.identityService.getDrive();
    const checksum = this.calculateChecksum(dto.content);
    const contentPath = this.getContentPath(checksum);
    const metadataPath = this.getMetadataPath(checksum);

    this.logger.debug(
      `Persisting content ${checksum.slice(0, 16)}... (${dto.content.length} bytes, ${dto.contentType || 'application/octet-stream'}, ipfs=${dto.ipfsCid ?? 'none'}, bzz=${dto.bzzHash ?? 'none'})`,
    );

    if (!(await this.exists(contentPath))) {
      await drive.put(contentPath, dto.content);
      this.logger.log(`Stored content: ${checksum.slice(0, 16)}...`);
    } else {
      this.logger.debug(`Content exists: ${checksum.slice(0, 16)}...`);
    }

    let metadata: ContentMetadataDto;
    try {
      const existingMeta = await drive.get(metadataPath);
      metadata = existingMeta
        ? JSON.parse(existingMeta.toString())
        : this.createMetadata(checksum, dto);
      if (existingMeta) {
        this.logger.debug(
          `Loaded existing metadata for ${checksum.slice(0, 16)}... before merge`,
        );
      }
    } catch {
      metadata = this.createMetadata(checksum, dto);
      this.logger.debug(
        `No existing metadata for ${checksum.slice(0, 16)}..., creating a new record`,
      );
    }

    metadata = this.mergeMetadata(metadata, dto);
    await this.writeMetadata(metadata);
    await this.syncRefs(metadata);

    return metadata;
  }

  private createMetadata(
    checksum: string,
    dto: StoreContentDto,
  ): ContentMetadataDto {
    return {
      checksum,
      size: dto.content.length,
      contentType: dto.contentType || 'application/octet-stream',
      filename: dto.filename,
      timestamp: Date.now(),
      lastModified: dto.lastModified,
      sourcePath: dto.sourcePath,
      ipfsCid: dto.ipfsCid,
      bzzHash: dto.bzzHash,
      bzzRefKey: dto.bzzRefKey,
      contentKind: dto.contentKind,
    };
  }

  private mergeMetadata(
    existing: ContentMetadataDto,
    dto: StoreContentDto,
  ): ContentMetadataDto {
    return {
      ...existing,
      contentType: dto.contentType || existing.contentType,
      filename: dto.filename || existing.filename,
      lastModified: dto.lastModified || existing.lastModified,
      sourcePath: dto.sourcePath || existing.sourcePath,
      // Use nullish so an explicit bzz / IPFS / path ref in dto wins over the prior row (e.g. IPFS-only, then bzz)
      ipfsCid: dto.ipfsCid ?? existing.ipfsCid,
      bzzHash: dto.bzzHash ?? existing.bzzHash,
      bzzRefKey: dto.bzzRefKey ?? existing.bzzRefKey,
      contentKind: dto.contentKind ?? existing.contentKind,
    };
  }

  async updateBridgeMetadata(
    checksum: string,
    patch: { ipfsCid?: string; bzzHash?: string; manifestBzzHash?: string },
  ): Promise<ContentMetadataDto | null> {
    this.logger.debug(
      `Updating bridge metadata for ${checksum.slice(0, 16)}... (ipfs=${patch.ipfsCid ?? 'unchanged'}, bzz=${patch.bzzHash ?? 'unchanged'}, manifestBzz=${patch.manifestBzzHash ?? 'unchanged'})`,
    );
    const existing = await this.getMetadata(checksum);
    if (!existing) {
      this.logger.warn(
        `Cannot update bridge metadata for ${checksum.slice(0, 16)}... because no metadata exists`,
      );
      return null;
    }

    const nextBzzHash =
      patch.manifestBzzHash &&
      patch.bzzHash === patch.manifestBzzHash &&
      existing.bzzHash &&
      existing.bzzHash !== patch.manifestBzzHash
        ? existing.bzzHash
        : (patch.bzzHash ?? existing.bzzHash);

    const metadata: ContentMetadataDto = {
      ...existing,
      ipfsCid: patch.ipfsCid ?? existing.ipfsCid,
      bzzHash: nextBzzHash,
      manifestBzzHash: patch.manifestBzzHash ?? existing.manifestBzzHash,
    };

    await this.writeMetadata(metadata);
    await this.syncRefs(metadata);
    return metadata;
  }

  async linkIpfsCidToBzz(
    cid: string,
    bzzHash: string,
    checksum?: string,
  ): Promise<ContentMetadataDto | null> {
    const drive = this.identityService.getDrive();
    this.logger.debug(
      `Linking IPFS CID ${cid} to Swarm hash ${bzzHash.slice(0, 16)}...`,
    );
    await drive.put(this.getIpfsBridgePath(cid), Buffer.from(bzzHash));

    const resolvedChecksum =
      checksum ??
      (await this.resolveIpfsCid(cid)) ??
      (await this.resolveBzzHash(bzzHash));
    if (!resolvedChecksum) {
      this.logger.warn(
        `Stored CID->bzz mapping for ${cid}, but no checksum could be resolved yet`,
      );
      return null;
    }

    this.logger.debug(
      `Resolved checksum ${resolvedChecksum.slice(0, 16)}... for CID ${cid} -> bzz ${bzzHash.slice(0, 16)}...`,
    );

    return this.updateBridgeMetadata(resolvedChecksum, {
      ipfsCid: cid,
      bzzHash,
    });
  }

  /**
   * Writes the IPFS→Swarm bridge to a directory mantaray root and stores that root only on
   * `manifestBzzHash`. The original `bzzHash` stays as the file-level bridge for the anchor row.
   */
  async publishIpfsDirectoryManifest(
    cid: string,
    manifestRef: string,
    checksum?: string,
  ): Promise<ContentMetadataDto | null> {
    const drive = this.identityService.getDrive();
    this.logger.debug(
      `Publishing IPFS directory manifest ${cid.slice(0, 12)}… → ${manifestRef.slice(0, 16)}…`,
    );
    await drive.put(this.getIpfsBridgePath(cid), Buffer.from(manifestRef));

    const resolvedChecksum =
      checksum ??
      (await this.resolveIpfsCid(cid)) ??
      (await this.resolveBzzHash(manifestRef));
    if (!resolvedChecksum) {
      this.logger.warn(
        `Stored CID→manifest mapping for ${cid}, but no checksum could be resolved yet`,
      );
      return null;
    }

    return this.updateBridgeMetadata(resolvedChecksum, {
      ipfsCid: cid,
      manifestBzzHash: manifestRef,
    });
  }

  async resolveIpfsToBzz(cid: string): Promise<string | null> {
    const drive = this.identityService.getDrive();
    try {
      const ref = await drive.get(this.getIpfsBridgePath(cid));
      const resolved = ref ? ref.toString() : null;
      this.logger.debug(
        `Resolved CID->bzz mapping for ${cid}: ${resolved ? `${resolved.slice(0, 16)}...` : 'miss'}`,
      );
      return resolved;
    } catch {
      this.logger.debug(`CID->bzz mapping lookup failed for ${cid}`);
      return null;
    }
  }

  async putIpfs(
    cid: string,
    content: Buffer,
    contentType?: string,
  ): Promise<ContentMetadataDto> {
    return this.put({
      content,
      contentType,
      ipfsCid: cid,
    });
  }

  async putBzz(
    hash: string,
    content: Buffer,
    contentType?: string,
    filename?: string,
  ): Promise<ContentMetadataDto> {
    return this.put({
      content,
      contentType,
      filename,
      bzzHash: hash,
    });
  }

  async get(checksum: string): Promise<ContentResponseDto | null> {
    const drive = this.identityService.getDrive();
    const contentPath = this.getContentPath(checksum);
    const metadataPath = this.getMetadataPath(checksum);

    try {
      const content = await drive.get(contentPath);
      if (!content) return null;

      let metadata: ContentMetadataDto;
      try {
        const metaBuffer = await drive.get(metadataPath);
        metadata = metaBuffer
          ? JSON.parse(metaBuffer.toString())
          : {
              checksum,
              size: content.length,
              contentType: 'application/octet-stream',
              timestamp: Date.now(),
            };
      } catch {
        metadata = {
          checksum,
          size: content.length,
          contentType: 'application/octet-stream',
          timestamp: Date.now(),
        };
      }

      return { content, metadata };
    } catch {
      return null;
    }
  }

  async getContent(checksum: string): Promise<Buffer | null> {
    const drive = this.identityService.getDrive();
    try {
      return await drive.get(this.getContentPath(checksum));
    } catch {
      return null;
    }
  }

  async getMetadata(checksum: string): Promise<ContentMetadataDto | null> {
    const drive = this.identityService.getDrive();
    try {
      const path = this.getMetadataPath(checksum);
      this.logger.debug(`Reading metadata from ${path}`);
      const metaBuffer = await drive.get(path);
      if (!metaBuffer) {
        this.logger.debug(`No metadata found at ${path}`);
        return null;
      }
      return JSON.parse(metaBuffer.toString());
    } catch (err) {
      this.logger.error(
        `Failed to read metadata for ${checksum}: ${(err as Error).message}`,
      );
      return null;
    }
  }

  async resolveIpfsCid(cid: string): Promise<string | null> {
    const drive = this.identityService.getDrive();
    try {
      const ref = await drive.get(this.getIpfsRefPath(cid));
      const resolved = ref ? ref.toString() : null;
      this.logger.debug(
        `Resolved IPFS ref ${cid} -> ${resolved ? `${resolved.slice(0, 16)}...` : 'miss'}`,
      );
      return resolved;
    } catch {
      this.logger.debug(`IPFS ref lookup failed for ${cid}`);
      return null;
    }
  }

  async resolveBzzHash(hash: string): Promise<string | null> {
    const drive = this.identityService.getDrive();
    try {
      const ref = await drive.get(this.getBzzRefPath(hash));
      const resolved = ref ? ref.toString() : null;
      this.logger.debug(
        `Resolved Bzz ref ${hash.slice(0, 16)}... -> ${resolved ? `${resolved.slice(0, 16)}...` : 'miss'}`,
      );
      return resolved;
    } catch {
      this.logger.debug(`Bzz ref lookup failed for ${hash.slice(0, 16)}...`);
      return null;
    }
  }

  async getByIpfsCid(cid: string): Promise<ContentResponseDto | null> {
    const checksum = await this.resolveIpfsCid(cid);
    if (!checksum) {
      this.logger.debug(`No local content found for IPFS CID ${cid}`);
      return null;
    }
    this.logger.debug(
      `Loading local content for IPFS CID ${cid} via checksum ${checksum.slice(0, 16)}...`,
    );
    return this.get(checksum);
  }

  async getByBzzHash(hash: string): Promise<ContentResponseDto | null> {
    const checksum = await this.resolveBzzHash(hash);
    if (!checksum) {
      this.logger.debug(
        `No local content found for Bzz hash ${hash.slice(0, 16)}...`,
      );
      return null;
    }
    this.logger.debug(
      `Loading local content for Bzz hash ${hash.slice(0, 16)}... via checksum ${checksum.slice(0, 16)}...`,
    );
    return this.get(checksum);
  }

  async resolveByRef(protocol: string, id: string): Promise<string | null> {
    const drive = this.identityService.getDrive();
    try {
      const ref = await drive.get(`/refs/${protocol}/${id}`);
      const resolved = ref ? ref.toString() : null;
      this.logger.debug(
        `Resolved ${protocol} ref ${id} -> ${resolved ? `${resolved.slice(0, 16)}...` : 'miss'}`,
      );
      return resolved;
    } catch {
      this.logger.debug(`Ref lookup failed for ${protocol}:${id}`);
      return null;
    }
  }

  async getByRef(
    protocol: string,
    id: string,
  ): Promise<ContentResponseDto | null> {
    const checksum = await this.resolveByRef(protocol, id);
    if (!checksum) {
      this.logger.debug(`No local content found for ${protocol} ref ${id}`);
      return null;
    }
    this.logger.debug(
      `Loading local content for ${protocol} ref ${id} via checksum ${checksum.slice(0, 16)}...`,
    );
    return this.get(checksum);
  }

  async putWithRef(
    protocol: string,
    id: string,
    content: Buffer,
    contentType?: string,
    filename?: string,
  ): Promise<ContentMetadataDto> {
    this.logger.debug(
      `Persisting ${protocol} ref ${id} (${content.length} bytes, ${contentType || 'application/octet-stream'})`,
    );
    const metadata = await this.put({ content, contentType, filename });
    const drive = this.identityService.getDrive();
    await drive.put(`/refs/${protocol}/${id}`, Buffer.from(metadata.checksum));
    this.logger.debug(
      `Created ${protocol} ref: ${id} -> ${metadata.checksum.slice(0, 16)}...`,
    );
    return metadata;
  }

  async delete(
    checksum: string,
    options?: { preserveIndexedRefs?: boolean },
  ): Promise<boolean> {
    const drive = this.identityService.getDrive();
    const contentPath = this.getContentPath(checksum);
    const metadataPath = this.getMetadataPath(checksum);

    try {
      const metadata = await this.getMetadata(checksum);

      await drive.del(contentPath);
      await drive.del(metadataPath);

      if (
        metadata?.ipfsCid &&
        !metadata?.bzzRefKey &&
        !options?.preserveIndexedRefs
      ) {
        await drive.del(this.getIpfsRefPath(metadata.ipfsCid)).catch(() => {});
        await drive
          .del(this.getIpfsBridgePath(metadata.ipfsCid))
          .catch(() => {});
      }
      if (metadata?.bzzRefKey) {
        await drive.del(this.getBzzRefPath(metadata.bzzRefKey)).catch(() => {});
      } else {
        if (metadata?.manifestBzzHash) {
          await drive
            .del(this.getBzzRefPath(metadata.manifestBzzHash))
            .catch(() => {});
        }
        if (metadata?.bzzHash) {
          await drive.del(this.getBzzRefPath(metadata.bzzHash)).catch(() => {});
        }
      }

      this.logger.log(`Deleted content: ${checksum.slice(0, 16)}...`);
      return true;
    } catch {
      return false;
    }
  }

  async list(path: string = '/'): Promise<DriveEntryDto[]> {
    const drive = this.identityService.getDrive();
    const entries: DriveEntryDto[] = [];

    try {
      for await (const entry of drive.list(path)) {
        entries.push({
          key: entry.key,
          size: entry.value?.blob?.byteLength || 0,
          isDirectory: !entry.value?.blob,
        });
      }
    } catch (err) {
      this.logger.error(`Error listing path ${path}`, err);
    }

    return entries;
  }

  async listContent(): Promise<ContentMetadataDto[]> {
    const drive = this.identityService.getDrive();
    const allMetadata: ContentMetadataDto[] = [];

    try {
      for await (const entry of drive.list('/meta')) {
        if (entry.key.endsWith('.json')) {
          try {
            const metaBuffer = await drive.get(entry.key);
            if (metaBuffer) {
              allMetadata.push(JSON.parse(metaBuffer.toString()));
            }
          } catch {
            // Skip invalid metadata
          }
        }
      }
    } catch {
      // /meta directory might not exist yet
    }

    return allMetadata;
  }

  async getContentCount(): Promise<number> {
    const metadata = await this.listContent();
    return metadata.length;
  }

  async getBridgedCount(): Promise<number> {
    const metadata = await this.listContent();
    return metadata.filter(
      (item) => item.ipfsCid && (item.bzzHash || item.manifestBzzHash),
    ).length;
  }
}
