import { Injectable, Logger, type OnModuleInit } from '@nestjs/common';
import type { ContentMetadataDto } from '../dto/content';
import { DriveService } from './drive.service';
import { IdentityService } from './identity.service';

@Injectable()
export class FileIndexService implements OnModuleInit {
  private readonly logger = new Logger(FileIndexService.name);
  private readonly INDEX_PATH = '/index/files.json';

  private byChecksum: Map<string, ContentMetadataDto> = new Map();
  private byIpfsCid: Map<string, string> = new Map();
  private byBzzHash: Map<string, string> = new Map();
  private bridgedIpfsToBzz: Map<string, string> = new Map();
  private bridgedBzzToIpfs: Map<string, string> = new Map();

  constructor(
    private readonly identityService: IdentityService,
    private readonly driveService: DriveService,
  ) {}

  async onModuleInit() {
    await this.identityService.whenReady();
    await this.load();
  }

  async load(): Promise<void> {
    const drive = this.identityService.getDrive();
    let items: ContentMetadataDto[] = [];

    try {
      const data = await drive.get(this.INDEX_PATH);
      if (data) {
        items = JSON.parse(data.toString()) as ContentMetadataDto[];
        items = await this.migrateLegacySyntheticDirectoryRows(items);
        this.logger.log(`Loaded ${items.length} items from index`);
      } else {
        this.logger.log('No existing index found, starting fresh');
      }
    } catch {
      this.logger.log('No existing index found, starting fresh');
    }

    this.rebuildMaps(items);
  }

  /**
   * Legacy: synthetic `contentKind: directory` + bare `ipfsCid` marker row alongside the
   * real anchor file (same root CID). Merge manifest onto the anchor and drop the marker.
   */
  private async migrateLegacySyntheticDirectoryRows(
    items: ContentMetadataDto[],
  ): Promise<ContentMetadataDto[]> {
    const byBareCid = new Map<string, ContentMetadataDto[]>();
    for (const i of items) {
      if (!i.ipfsCid || i.ipfsCid.includes('/')) {
        continue;
      }
      const list = byBareCid.get(i.ipfsCid) ?? [];
      list.push(i);
      byBareCid.set(i.ipfsCid, list);
    }

    const toRemove = new Set<string>();
    const mergedAnchors = new Map<string, ContentMetadataDto>();

    for (const [cid, group] of byBareCid) {
      const legacyDir = group.find((x) => x.contentKind === 'directory');
      if (!legacyDir) {
        continue;
      }
      const anchor = group.find(
        (x) =>
          x.checksum !== legacyDir.checksum && x.contentKind !== 'directory',
      );
      if (!anchor) {
        this.logger.debug(
          `Legacy directory row for ${cid.slice(0, 12)}… has no anchor file row; skipping migration`,
        );
        continue;
      }

      const merged: ContentMetadataDto = {
        ...anchor,
        bzzHash: anchor.bzzHash ?? legacyDir.bzzHash,
        manifestBzzHash:
          legacyDir.manifestBzzHash ?? anchor.manifestBzzHash,
        contentKind: undefined,
      };
      mergedAnchors.set(anchor.checksum, merged);
      toRemove.add(legacyDir.checksum);
    }

    if (!toRemove.size) {
      return items;
    }

    for (const merged of mergedAnchors.values()) {
      await this.driveService.updateBridgeMetadata(merged.checksum, {
        ipfsCid: merged.ipfsCid,
        bzzHash: merged.bzzHash,
        ...(merged.manifestBzzHash != null
          ? { manifestBzzHash: merged.manifestBzzHash }
          : {}),
      });
    }

    for (const checksum of toRemove) {
      await this.driveService.delete(checksum, { preserveIndexedRefs: true });
    }

    const filtered = items
      .filter((i) => !toRemove.has(i.checksum))
      .map((i) => mergedAnchors.get(i.checksum) ?? i);

    const drive = this.identityService.getDrive();
    await drive.put(this.INDEX_PATH, Buffer.from(JSON.stringify(filtered)));

    this.logger.log(
      `Migrated ${toRemove.size} synthetic IPFS directory row(s) onto anchor file rows`,
    );
    return filtered;
  }

  /** Swarm ref used for IPFS CID ↔ website bridge (manifest root when set). */
  private bridgeBzzForItem(item: ContentMetadataDto): string | undefined {
    return item.manifestBzzHash ?? item.bzzHash;
  }

  private rebuildMaps(items: ContentMetadataDto[]): void {
    this.byChecksum.clear();
    this.byIpfsCid.clear();
    this.byBzzHash.clear();
    this.bridgedIpfsToBzz.clear();
    this.bridgedBzzToIpfs.clear();

    for (const item of items) {
      this.byChecksum.set(item.checksum, item);

      if (item.ipfsCid) {
        this.byIpfsCid.set(item.ipfsCid, item.checksum);
      }

      if (item.bzzHash) {
        this.byBzzHash.set(item.bzzHash, item.checksum);
      }
      if (item.manifestBzzHash) {
        this.byBzzHash.set(item.manifestBzzHash, item.checksum);
      }

      const bridgeBzz = this.bridgeBzzForItem(item);
      if (item.ipfsCid && bridgeBzz) {
        this.bridgedIpfsToBzz.set(item.ipfsCid, bridgeBzz);
        this.bridgedBzzToIpfs.set(bridgeBzz, item.ipfsCid);
      }
      if (
        item.ipfsCid &&
        item.bzzHash &&
        item.manifestBzzHash &&
        item.bzzHash !== item.manifestBzzHash
      ) {
        this.bridgedBzzToIpfs.set(item.bzzHash, item.ipfsCid);
      }
    }
  }

  async save(): Promise<void> {
    const drive = this.identityService.getDrive();
    const items = Array.from(this.byChecksum.values());

    await drive.put(this.INDEX_PATH, Buffer.from(JSON.stringify(items)));
    this.logger.debug(`Saved ${items.length} items to index`);
  }

  async addOrUpdate(metadata: ContentMetadataDto): Promise<void> {
    const existing = this.byChecksum.get(metadata.checksum);

    const merged: ContentMetadataDto = existing
      ? {
          ...existing,
          ...metadata,
          ipfsCid: metadata.ipfsCid || existing.ipfsCid,
          bzzHash: metadata.bzzHash || existing.bzzHash,
          manifestBzzHash:
            metadata.manifestBzzHash ?? existing.manifestBzzHash,
          contentKind: metadata.contentKind ?? existing.contentKind,
        }
      : metadata;

    this.byChecksum.set(merged.checksum, merged);

    if (existing?.ipfsCid) {
      this.byIpfsCid.delete(existing.ipfsCid);
    }
    if (existing?.bzzHash) {
      this.byBzzHash.delete(existing.bzzHash);
    }
    if (existing?.manifestBzzHash) {
      this.byBzzHash.delete(existing.manifestBzzHash);
    }
    if (existing?.ipfsCid) {
      const prevBridge = this.bridgeBzzForItem(existing);
      if (prevBridge) {
        this.bridgedIpfsToBzz.delete(existing.ipfsCid);
        this.bridgedBzzToIpfs.delete(prevBridge);
      }
      if (
        existing.bzzHash &&
        existing.manifestBzzHash &&
        existing.bzzHash !== existing.manifestBzzHash
      ) {
        this.bridgedBzzToIpfs.delete(existing.bzzHash);
      }
    }

    if (merged.ipfsCid) {
      this.byIpfsCid.set(merged.ipfsCid, merged.checksum);
    }
    if (merged.bzzHash) {
      this.byBzzHash.set(merged.bzzHash, merged.checksum);
    }
    if (merged.manifestBzzHash) {
      this.byBzzHash.set(merged.manifestBzzHash, merged.checksum);
    }
    const mergedBridge = this.bridgeBzzForItem(merged);
    if (merged.ipfsCid && mergedBridge) {
      this.bridgedIpfsToBzz.set(merged.ipfsCid, mergedBridge);
      this.bridgedBzzToIpfs.set(mergedBridge, merged.ipfsCid);
    }
    if (
      merged.ipfsCid &&
      merged.bzzHash &&
      merged.manifestBzzHash &&
      merged.bzzHash !== merged.manifestBzzHash
    ) {
      this.bridgedBzzToIpfs.set(merged.bzzHash, merged.ipfsCid);
    }

    await this.save();
  }

  async remove(checksum: string): Promise<boolean> {
    const existing = this.byChecksum.get(checksum);
    if (!existing) return false;

    this.byChecksum.delete(checksum);

    if (existing.ipfsCid) {
      this.byIpfsCid.delete(existing.ipfsCid);
    }

    if (existing.bzzHash) {
      this.byBzzHash.delete(existing.bzzHash);
    }
    if (existing.manifestBzzHash) {
      this.byBzzHash.delete(existing.manifestBzzHash);
    }

    if (existing.ipfsCid) {
      const bridge = this.bridgeBzzForItem(existing);
      if (bridge) {
        this.bridgedIpfsToBzz.delete(existing.ipfsCid);
        this.bridgedBzzToIpfs.delete(bridge);
      }
      if (
        existing.bzzHash &&
        existing.manifestBzzHash &&
        existing.bzzHash !== existing.manifestBzzHash
      ) {
        this.bridgedBzzToIpfs.delete(existing.bzzHash);
      }
    }

    await this.save();
    return true;
  }

  getByChecksum(checksum: string): ContentMetadataDto | undefined {
    return this.byChecksum.get(checksum);
  }

  getChecksumByIpfsCid(cid: string): string | undefined {
    return this.byIpfsCid.get(cid);
  }

  /** @deprecated Use {@link getChecksumByIpfsCid} — bare root CID maps to the anchor file row. */
  getDirectoryChecksumByRootCid(rootCid: string): string | undefined {
    return this.getChecksumByIpfsCid(rootCid);
  }

  getChecksumByBzzHash(hash: string): string | undefined {
    return this.byBzzHash.get(hash);
  }

  getBzzHashByIpfsCid(cid: string): string | undefined {
    return this.bridgedIpfsToBzz.get(cid);
  }

  getIpfsCidByBzzHash(hash: string): string | undefined {
    return this.bridgedBzzToIpfs.get(hash);
  }

  getByIpfsCid(cid: string): ContentMetadataDto | undefined {
    const checksum = this.byIpfsCid.get(cid);
    return checksum ? this.byChecksum.get(checksum) : undefined;
  }

  getByBzzHash(hash: string): ContentMetadataDto | undefined {
    const checksum = this.byBzzHash.get(hash);
    return checksum ? this.byChecksum.get(checksum) : undefined;
  }

  getAll(): ContentMetadataDto[] {
    return Array.from(this.byChecksum.values());
  }

  getCount(): number {
    return this.byChecksum.size;
  }

  getBridgedCount(): number {
    return this.bridgedIpfsToBzz.size;
  }

  has(checksum: string): boolean {
    return this.byChecksum.has(checksum);
  }

  hasIpfsCid(cid: string): boolean {
    return this.byIpfsCid.has(cid);
  }

  hasBzzHash(hash: string): boolean {
    return this.byBzzHash.has(hash);
  }

  async clear(): Promise<void> {
    this.byChecksum.clear();
    this.byIpfsCid.clear();
    this.byBzzHash.clear();
    this.bridgedIpfsToBzz.clear();
    this.bridgedBzzToIpfs.clear();
    await this.save();
  }
}
