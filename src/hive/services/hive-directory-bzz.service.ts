import { Bee, MantarayNode, NULL_ADDRESS } from '@ethersphere/bee-js';
import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { mkdir, mkdtemp, rm, writeFile } from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import { ConfigService } from '../../config/config.service';
import type { ContentMetadataDto } from '../dto/content';
import { DriveService } from './drive.service';
import { FileIndexService } from './file-index.service';
import { SwarmBridgeService } from './swarm-bridge.service';
import { resolveWebsiteIndexDocument } from '../utils/mantaray-index.util';
import { kuboUnixfsTypeForCid } from '../utils/kubo-unixfs.util';

/** Path -> IPFS CID entry as stored by the Drive UI for directory listings. */
export type IpfsDirectoryJsonEntry = { cid: string };

export type IpfsDirectoryJson = Record<string, IpfsDirectoryJsonEntry>;

/** Binary marker written for synthetic directory rows (not the JSON map from the dashboard). */
const LEGACY_DIRECTORY_MARKER_PREFIX = Buffer.from(
  'hive-ipfs-directory\0',
  'utf8',
);

/**
 * Returns the root IPFS CID after the `hive-ipfs-directory\\0` prefix, or null.
 */
export function parseLegacyDirectoryMarker(buf: Buffer): string | null {
  if (buf.length <= LEGACY_DIRECTORY_MARKER_PREFIX.length) {
    return null;
  }
  if (
    !buf
      .subarray(0, LEGACY_DIRECTORY_MARKER_PREFIX.length)
      .equals(LEGACY_DIRECTORY_MARKER_PREFIX)
  ) {
    return null;
  }
  const root = buf
    .subarray(LEGACY_DIRECTORY_MARKER_PREFIX.length)
    .toString('utf8')
    .trim();
  return root || null;
}

export function parseIpfsDirectoryJson(buf: Buffer): IpfsDirectoryJson | null {
  try {
    const o: unknown = JSON.parse(buf.toString());
    if (typeof o !== 'object' || o === null || Array.isArray(o)) {
      return null;
    }
    const out: IpfsDirectoryJson = {};
    for (const [k, v] of Object.entries(o)) {
      if (
        !v ||
        typeof v !== 'object' ||
        typeof (v as IpfsDirectoryJsonEntry).cid !== 'string' ||
        !(v as IpfsDirectoryJsonEntry).cid.trim()
      ) {
        return null;
      }
      out[k] = { cid: (v as IpfsDirectoryJsonEntry).cid.trim() };
    }
    return Object.keys(out).length ? out : null;
  } catch {
    return null;
  }
}

function normalizeManifestPath(key: string): string {
  return key.replace(/^\/+/, '').replace(/\/+$/g, '');
}

type DirectoryForkEntry = { rel: string; child: ContentMetadataDto };

@Injectable()
export class HiveDirectoryBzzService {
  private readonly logger = new Logger(HiveDirectoryBzzService.name);

  constructor(
    private readonly config: ConfigService,
    private readonly driveService: DriveService,
    private readonly fileIndexService: FileIndexService,
    private readonly swarmBridgeService: SwarmBridgeService,
  ) {}

  private async verifyManifestOnBee(
    bee: Bee,
    manifestRef: string,
    logTag: string,
    indexDocument?: string | null,
  ): Promise<void> {
    let rootErr: string | undefined;
    try {
      const file = await bee.downloadFile(manifestRef, '');
      this.logger.log(
        `[${logTag}] Bee /bzz verify OK for ${manifestRef.slice(0, 16)}… (${file.data.length} bytes, ${file.contentType ?? 'unknown'})`,
      );
      return;
    } catch (e) {
      rootErr = (e as Error).message;
    }
    if (indexDocument) {
      try {
        const file = await bee.downloadFile(manifestRef, indexDocument);
        this.logger.log(
          `[${logTag}] Bee /bzz verify OK at /${indexDocument} (${file.data.length} bytes)`,
        );
        return;
      } catch (e) {
        this.logger.debug(
          `[${logTag}] Bee /bzz verify at /${indexDocument}: ${(e as Error).message}`,
        );
      }
    }
    try {
      await bee.downloadData(manifestRef);
      this.logger.warn(
        `[${logTag}] Bee GET /bytes OK for ${manifestRef.slice(0, 16)}… but /bzz failed (${rootErr}). Chunk is on node; check website-index-document path and Bee /bzz routing.`,
      );
    } catch (e) {
      this.logger.warn(
        `[${logTag}] Bee /bzz verify failed for ${manifestRef.slice(0, 16)}…: ${rootErr ?? 'unknown'}. /bytes: ${(e as Error).message}`,
      );
    }
  }

  /**
   * Uploads the raw file bytes and returns a Bee data reference that is safe to embed in
   * a Mantaray fork. File-level `/bzz` refs can themselves be tiny manifests.
   */
  private async ensureLeafFileOnBee(
    bee: Bee,
    child: ContentMetadataDto,
  ): Promise<string> {
    const got = await this.driveService.get(child.checksum);
    if (!got?.content) {
      throw new NotFoundException(
        `No drive content for checksum ${child.checksum.slice(0, 16)}… (child CID ${child.ipfsCid ?? 'unknown'})`,
      );
    }
    this.logger.log(
      `Uploading leaf bytes for mantaray: ${child.ipfsCid ?? child.checksum.slice(0, 16)} (${got.content.length} bytes, ${child.contentType ?? 'octet-stream'})`,
    );
    const { reference } = await bee.uploadData(
      this.config.beePostageStamp,
      got.content,
      { pin: true },
    );
    return reference.toHex();
  }

  /**
   * POSIX-style relative path for Bee tar collections (matches {@link makeCollectionFromFS} layout).
   */
  private hasRootIndexInRelPaths(relPaths: string[]): boolean {
    return relPaths.some((p) => {
      const base = p.split('/').pop() ?? p;
      const b = base.toLowerCase();
      return b === 'index.html' || b === 'index.htm';
    });
  }

  /**
   * Default document for IPFS directories is often not opened in Hive, so it never
   * appears in the file index. Pull it from the local Kubo API so +bzz includes the SPA shell.
   */
  private async ipfsUnixfsType(cid: string): Promise<'file' | 'directory'> {
    return kuboUnixfsTypeForCid(
      this.config.ipfsApiUrl,
      this.config.upstreamTimeout,
      cid,
    );
  }

  private async assertIpfsUnixfsDirectory(cid: string, tag: string): Promise<void> {
    const t = await this.ipfsUnixfsType(cid);
    if (t !== 'directory') {
      throw new BadRequestException(
        `[${tag}] Expected UnixFS directory at /ipfs/${cid.slice(0, 12)}… for upload-dir-to-bzz; Kubo reported "${t}"`,
      );
    }
  }

  private async injectRootIndexFromIpfsIfMissing(
    forkRelPaths: string[],
    tmpRoot: string,
    directoryRootCid: string | undefined,
    tag: string,
  ): Promise<void> {
    if (
      !directoryRootCid ||
      directoryRootCid.includes('/') ||
      directoryRootCid.includes('\\') ||
      this.hasRootIndexInRelPaths(forkRelPaths)
    ) {
      return;
    }
    const api = this.config.ipfsApiUrl.replace(/\/$/, '');
    const attempts: { rel: string; arg: string }[] = [
      { rel: 'index.html', arg: `${directoryRootCid}/index.html` },
      { rel: 'index.htm', arg: `${directoryRootCid}/index.htm` },
    ];
    for (const { rel, arg } of attempts) {
      try {
        const url = `${api}/api/v0/cat?arg=${encodeURIComponent(arg)}`;
        const res = await fetch(url, {
          method: 'POST',
          signal: AbortSignal.timeout(this.config.upstreamTimeout),
        });
        if (!res.ok) {
          continue;
        }
        const buf = Buffer.from(await res.arrayBuffer());
        if (!buf.length) {
          continue;
        }
        await writeFile(path.join(tmpRoot, rel), buf);
        forkRelPaths.push(rel);
        this.logger.log(
          `[${tag}] added ${rel} from IPFS (${buf.length} bytes) — was not in Hive file index`,
        );
        return;
      } catch (e) {
        this.logger.debug(
          `[${tag}] IPFS cat ${arg}: ${(e as Error).message}`,
        );
      }
    }
  }

  private warnIfSpaShellMissing(
    tag: string,
    forkRelPaths: string[],
    indexDoc: string | null,
  ): void {
    if (
      !indexDoc ||
      !/(^|\/)home\.html$/i.test(indexDoc) ||
      forkRelPaths.some(
        (p) =>
          /(^|\/)index\.html$/i.test(p) || /(^|\/)index\.htm$/i.test(p),
      )
    ) {
      return;
    }
    this.logger.warn(
      `[${tag}] website-index-document=${indexDoc} points at an ng-view template only. ` +
        `Include root index.html in the directory (e.g. open /ipfs/<root>/index.html in Hive so it is indexed) for CSS/JS and the full HTML shell.`,
    );
  }

  private normalizeCollectionRel(rel: string): string {
    const p = rel.replace(/\\/g, '/').replace(/^\.\//, '');
    if (!p || p.includes('..') || path.posix.isAbsolute(p)) {
      throw new BadRequestException(
        `Invalid collection path "${rel}" (relative path, no .., use / as separator)`,
      );
    }
    return p;
  }

  private collectEntriesFromJsonMap(
    dirMap: IpfsDirectoryJson,
    metadata: ContentMetadataDto,
  ): DirectoryForkEntry[] {
    const sorted = Object.entries(dirMap)
      .map(([key, { cid }]) => ({
        key,
        cid,
        rel: normalizeManifestPath(key),
      }))
      .filter((e) => {
        if (e.rel === '' && metadata.ipfsCid && e.cid === metadata.ipfsCid) {
          return false;
        }
        return true;
      })
      .sort((a, b) => a.rel.localeCompare(b.rel));

    const out: DirectoryForkEntry[] = [];
    for (const { key, cid, rel } of sorted) {
      if (!rel) {
        throw new BadRequestException(
          `Directory key "${key}" maps to an empty path; remove it or use a non-root path`,
        );
      }
      const child = this.fileIndexService.getByIpfsCid(cid);
      if (!child) {
        throw new NotFoundException(
          `No indexed Hive item with ipfsCid ${cid} (manifest path "${rel}")`,
        );
      }
      out.push({ rel, child });
    }
    return out;
  }

  /** Indexed sub-path rows: `ipfsCid` is `{rootCid}/{relativePath}`. */
  private collectEntriesFromIndexRoot(rootCid: string): DirectoryForkEntry[] {
    const prefix = `${rootCid}/`;
    return this.fileIndexService
      .getAll()
      .filter((m) => m.ipfsCid?.startsWith(prefix))
      .map((m) => ({
        rel: m.ipfsCid!.slice(prefix.length),
        child: m,
      }))
      .filter((e) => e.rel.length > 0)
      .sort((a, b) => a.rel.localeCompare(b.rel));
  }

  private async childIsNestedDirectory(
    child: ContentMetadataDto,
  ): Promise<boolean> {
    if (child.contentKind === 'directory') {
      return true;
    }
    if (child.contentType !== 'application/vnd.hive.ipfs-directory+json') {
      return false;
    }
    const body = await this.driveService.getContent(child.checksum);
    if (!body) {
      return false;
    }
    return (
      parseIpfsDirectoryJson(body) !== null ||
      parseLegacyDirectoryMarker(body) !== null
    );
  }

  /**
   * Build a mantaray manifest mirroring the directory JSON, upload it, and set manifestBzzHash on this row.
   */
  async uploadDirectoryTreeToBzz(
    checksum: string,
    visiting: Set<string> = new Set(),
  ): Promise<ContentMetadataDto> {
    if (!this.config.beePostageStamp) {
      throw new BadRequestException('BEE_POSTAGE_STAMP is required');
    }
    if (visiting.has(checksum)) {
      throw new BadRequestException(
        `Circular directory reference (checksum ${checksum.slice(0, 16)}…)`,
      );
    }
    visiting.add(checksum);
    const tag = checksum.slice(0, 12);
    try {
      const stored = await this.driveService.get(checksum);
      if (!stored) {
        throw new NotFoundException(`No content for checksum ${checksum}`);
      }
      const { content, metadata } = stored;
      const bareRoot = metadata.ipfsCid?.trim();
      if (
        metadata.contentKind === 'directory' &&
        bareRoot &&
        !bareRoot.includes('/')
      ) {
        const anchor = this.fileIndexService.getChecksumByIpfsCid(bareRoot);
        if (anchor && anchor !== checksum) {
          this.logger.log(
            `[${tag}] redirect upload-dir-to-bzz from legacy directory marker → anchor ${anchor.slice(0, 12)}…`,
          );
          return this.uploadDirectoryTreeToBzz(anchor, visiting);
        }
      }
      const dirMap = parseIpfsDirectoryJson(content);
      const markerRoot = parseLegacyDirectoryMarker(content);

      this.logger.log(
        `[${tag}] upload-dir-to-bzz start (ipfsCid=${metadata.ipfsCid?.slice(0, 20) ?? 'none'}…, depth=${visiting.size})`,
      );

      let dirEntries: DirectoryForkEntry[];
      let resolvedRootCid: string | undefined;

      if (dirMap) {
        dirEntries = this.collectEntriesFromJsonMap(dirMap, metadata);
        if (!dirEntries.length) {
          throw new BadRequestException(
            'Directory JSON has no mappable paths after normalizing',
          );
        }
        resolvedRootCid = metadata.ipfsCid?.trim() || undefined;
        this.logger.log(
          `[${tag}] source=json-map, ${dirEntries.length} path(s) from JSON`,
        );
        if (resolvedRootCid && !resolvedRootCid.includes('/')) {
          await this.assertIpfsUnixfsDirectory(resolvedRootCid, tag);
        }
      } else {
        const rootCid = (metadata.ipfsCid?.trim() || markerRoot || '').trim();
        let treatsAsDirectory =
          metadata.contentKind === 'directory' ||
          markerRoot !== null ||
          metadata.contentType === 'application/vnd.hive.ipfs-directory+json';

        let unixfsDirectoryAlreadyVerified = false;

        if (!rootCid) {
          throw new BadRequestException(
            'Directory upload requires ipfsCid (bare root) or a legacy directory marker in the file body',
          );
        }

        if (!treatsAsDirectory && !rootCid.includes('/')) {
          const remoteType = await this.ipfsUnixfsType(rootCid);
          if (remoteType === 'directory') {
            treatsAsDirectory = true;
            unixfsDirectoryAlreadyVerified = true;
            this.logger.log(
              `[${tag}] Kubo reports UnixFS directory for ${rootCid.slice(0, 12)}… (e.g. index.html anchor); building Swarm website manifest`,
            );
          } else {
            throw new BadRequestException(
              `upload-dir-to-bzz applies only when ipfsCid is a UnixFS directory root. Kubo reports /ipfs/${rootCid.slice(0, 12)}… as type "${remoteType}". For a single file, use the normal Swarm bridge — this endpoint sets bzzHash to a collection manifest, not the raw file chunk.`,
            );
          }
        }

        if (!treatsAsDirectory) {
          throw new BadRequestException(
            'Directory content must be either a path→cid JSON map, an IPFS directory root with indexed sub-paths, or an anchor row whose ipfsCid is a UnixFS directory (verified via Kubo)',
          );
        }

        if (!rootCid.includes('/') && !unixfsDirectoryAlreadyVerified) {
          await this.assertIpfsUnixfsDirectory(rootCid, tag);
        }

        if (
          markerRoot &&
          metadata.ipfsCid?.trim() &&
          markerRoot !== metadata.ipfsCid.trim()
        ) {
          this.logger.warn(
            `Directory marker CID ${markerRoot.slice(0, 12)}… != metadata.ipfsCid ${metadata.ipfsCid.slice(0, 12)}…; using metadata.ipfsCid for sub-path index lookup`,
          );
        }

        dirEntries = this.collectEntriesFromIndexRoot(rootCid);
        if (!dirEntries.length) {
          throw new BadRequestException(
            `No indexed files under IPFS root ${rootCid.slice(0, 12)}… Open or sync /ipfs/${rootCid}/… paths so Hive caches sub-paths, then try +bzz again.`,
          );
        }
        resolvedRootCid = rootCid;
        this.logger.log(
          `[${tag}] source=index-subpaths, root=${rootCid.slice(0, 16)}…, ${dirEntries.length} file(s) under root`,
        );
      }

      const nestedFlags = await Promise.all(
        dirEntries.map((e) => this.childIsNestedDirectory(e.child)),
      );
      const hasNested = nestedFlags.some(Boolean);

      const forkRelPaths: string[] = [];
      const bee = new Bee(this.config.beeApiUrl.replace(/\/$/, ''));
      let manifestRef: string;
      let indexDoc: string | null = null;

      if (!hasNested) {
        const tmpRoot = await mkdtemp(path.join(os.tmpdir(), 'hive-bzz-col-'));
        try {
          for (const { rel, child } of dirEntries) {
            const safeRel = this.normalizeCollectionRel(rel);
            const got = await this.driveService.get(child.checksum);
            if (!got?.content) {
              throw new NotFoundException(
                `No drive content for checksum ${child.checksum.slice(0, 16)}… (path "${rel}")`,
              );
            }
            const absFile = path.join(tmpRoot, safeRel);
            await mkdir(path.dirname(absFile), { recursive: true });
            await writeFile(absFile, got.content);
            forkRelPaths.push(safeRel);
          }
          await this.injectRootIndexFromIpfsIfMissing(
            forkRelPaths,
            tmpRoot,
            resolvedRootCid,
            tag,
          );
          indexDoc = resolveWebsiteIndexDocument(forkRelPaths);
          if (indexDoc) {
            this.logger.log(`[${tag}] website-index-document: ${indexDoc}`);
          } else {
            this.logger.log(
              `[${tag}] no HTML entry for website-index-document; /bzz/<ref>/ may 404 until you add index.html or similar`,
            );
          }
          // Tar POST /bzz can fail on some Bee builds ("could not store directory");
          // streamDirectory uses per-chunk uploads + the same mantaray/index logic as bee-js.
          this.logger.log(
            `[${tag}] streamDirectory → Bee (${this.config.beeApiUrl}, pin=true, ${forkRelPaths.length} file(s))`,
          );
          const { reference } = await bee.streamDirectory(
            this.config.beePostageStamp,
            tmpRoot,
            undefined,
            {
              pin: true,
              ...(indexDoc ? { indexDocument: indexDoc } : {}),
            },
          );
          manifestRef = reference.toHex();
        } finally {
          await rm(tmpRoot, { recursive: true, force: true }).catch((err) => {
            this.logger.warn(
              `Could not remove temp dir ${tmpRoot}: ${(err as Error).message}`,
            );
          });
        }
      } else {
        const node = new MantarayNode();
        for (let i = 0; i < dirEntries.length; i++) {
          const { rel, child } = dirEntries[i];
          const childIsDir = nestedFlags[i];

          let childRef: string;
          if (childIsDir) {
            this.logger.log(
              `[${tag}]   fork ${rel}: nested directory (checksum ${child.checksum.slice(0, 12)}…)`,
            );
            const nested = await this.uploadDirectoryTreeToBzz(
              child.checksum,
              visiting,
            );
            childRef =
              nested.manifestBzzHash?.trim() || nested.bzzHash?.trim() || '';
            if (!childRef) {
              throw new BadRequestException(
                `Nested directory upload did not produce bzzHash for ${child.checksum.slice(0, 16)}…`,
              );
            }
          } else {
            this.logger.log(
              `[${tag}]   fork ${rel}: file (ipfsCid ${child.ipfsCid?.slice(0, 28) ?? child.checksum.slice(0, 12)}…)`,
            );
            childRef = await this.ensureLeafFileOnBee(bee, child);
          }

          this.logger.debug(
            `[${tag}]   fork ${rel}: mantaray → bzz ${childRef.slice(0, 16)}…`,
          );

          const filename = child.filename || rel.split('/').pop() || rel;
          forkRelPaths.push(rel);
          node.addFork(rel, childRef, {
            'Content-Type': child.contentType || 'application/octet-stream',
            Filename: filename,
          });
        }

        indexDoc = resolveWebsiteIndexDocument(forkRelPaths);
        if (indexDoc) {
          this.logger.log(`[${tag}] website-index-document: ${indexDoc}`);
          node.addFork('/', NULL_ADDRESS, {
            'website-index-document': indexDoc,
          });
        } else {
          this.logger.log(
            `[${tag}] no HTML entry for website-index-document; /bzz/<ref>/ may 404 until you add index.html or similar`,
          );
        }

        this.logger.log(
          `[${tag}] saveRecursively (mantaray) → Bee (${this.config.beeApiUrl}, pin=true, ${forkRelPaths.length} content fork(s))`,
        );
        const { reference } = await node.saveRecursively(
          bee,
          this.config.beePostageStamp,
          { pin: true },
        );
        manifestRef = reference.toHex();
      }

      this.warnIfSpaShellMissing(tag, forkRelPaths, indexDoc);

      await this.verifyManifestOnBee(bee, manifestRef, tag, indexDoc);

      const bridgeCid =
        resolvedRootCid?.trim() || metadata.ipfsCid?.trim() || '';
      if (!bridgeCid || bridgeCid.includes('/')) {
        throw new BadRequestException(
          'Cannot store manifest bridge without bare IPFS directory root CID on this item',
        );
      }
      const anchorChecksum =
        metadata.ipfsCid?.trim() === bridgeCid
          ? checksum
          : (this.fileIndexService.getChecksumByIpfsCid(bridgeCid) ?? checksum);

      let published: ContentMetadataDto | null =
        await this.driveService.publishIpfsDirectoryManifest(
          bridgeCid,
          manifestRef,
          anchorChecksum,
        );
      if (!published) {
        published = await this.driveService.updateBridgeMetadata(anchorChecksum, {
          ipfsCid: bridgeCid,
          manifestBzzHash: manifestRef,
        });
      }
      if (!published) {
        throw new BadRequestException(
          'Could not persist manifest ref on anchor row (file with ipfsCid = directory root)',
        );
      }
      await this.fileIndexService.addOrUpdate(published);
      this.logger.log(
        `[${tag}] done: manifest on anchor ${anchorChecksum.slice(0, 12)}… manifestBzzHash=${manifestRef.slice(0, 16)}… (GET /bzz/${manifestRef.slice(0, 16)}…/)`,
      );
      return published;
    } finally {
      visiting.delete(checksum);
    }
  }

  /** True if this is a directory map that should not use raw +bzz blob upload. */
  isDirectoryMapContent(
    contentType: string | undefined,
    content: Buffer,
  ): boolean {
    if (contentType !== 'application/vnd.hive.ipfs-directory+json') {
      return false;
    }
    return (
      parseIpfsDirectoryJson(content) !== null ||
      parseLegacyDirectoryMarker(content) !== null
    );
  }
}
