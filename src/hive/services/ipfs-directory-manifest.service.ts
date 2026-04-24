import { Bee } from '@ethersphere/bee-js';
import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { mkdir, mkdtemp, rm, writeFile } from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import { ConfigService } from '../../config/config.service';
import type { DirectoryManifestFileDto } from '../dto/ipfs-directory-manifest';
import { DriveService } from './drive.service';
import { FileIndexService } from './file-index.service';
import { resolveWebsiteIndexDocument } from '../utils/mantaray-index.util';
import { kuboUnixfsTypeForCid } from '../utils/kubo-unixfs.util';

/**
 * Build a Swarm mantaray from known sub-path → Bee chunk refs, set optional index
 * document, upload the manifest, and set the root as bzzHash for the IPFS directory CID.
 * Paths and refs come from the client (e.g. Drive UI); IPFS ls is not used.
 */
@Injectable()
export class IpfsDirectoryManifestService {
  private readonly logger = new Logger(IpfsDirectoryManifestService.name);

  constructor(
    private readonly config: ConfigService,
    private readonly driveService: DriveService,
    private readonly fileIndexService: FileIndexService,
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
        `[${logTag}] Bee /bzz verify OK for root ${manifestRef.slice(0, 16)}… (${file.data.length} bytes, ${file.contentType ?? 'unknown type'})`,
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
        `[${logTag}] Bee GET /bytes OK for ${manifestRef.slice(0, 16)}… but /bzz failed (${rootErr}). Same BEE_API_URL and pinning; check website-index-document.`,
      );
    } catch (e) {
      this.logger.warn(
        `[${logTag}] Bee /bzz verify failed for ${manifestRef.slice(0, 16)}…: ${rootErr ?? 'unknown'}. /bytes: ${(e as Error).message}`,
      );
    }
  }

  private pickIndexDocument(
    relPaths: string[],
    requested?: string,
  ): string | null {
    if (requested) {
      if (!relPaths.includes(requested)) {
        throw new BadRequestException(
          `indexDocument "${requested}" is not among the file relPaths`,
        );
      }
      return requested;
    }
    return resolveWebsiteIndexDocument(relPaths);
  }

  private hasRootIndexInRelPaths(relPaths: string[]): boolean {
    return relPaths.some((p) => {
      const base = p.split('/').pop() ?? p;
      const lower = base.toLowerCase();
      return lower === 'index.html' || lower === 'index.htm';
    });
  }

  private async injectRootIndexFromIpfsIfMissing(
    relPaths: string[],
    tmpRoot: string,
    rootCid: string,
    logTag: string,
  ): Promise<void> {
    if (
      rootCid.includes('/') ||
      rootCid.includes('\\') ||
      this.hasRootIndexInRelPaths(relPaths)
    ) {
      return;
    }

    const api = this.config.ipfsApiUrl.replace(/\/$/, '');
    const attempts: { rel: string; arg: string }[] = [
      { rel: 'index.html', arg: `${rootCid}/index.html` },
      { rel: 'index.htm', arg: `${rootCid}/index.htm` },
    ];

    for (const { rel, arg } of attempts) {
      try {
        const response = await fetch(
          `${api}/api/v0/cat?arg=${encodeURIComponent(arg)}`,
          {
            method: 'POST',
            signal: AbortSignal.timeout(this.config.upstreamTimeout),
          },
        );
        if (!response.ok) {
          continue;
        }
        const content = Buffer.from(await response.arrayBuffer());
        if (!content.length) {
          continue;
        }
        await writeFile(path.join(tmpRoot, rel), content);
        relPaths.push(rel);
        this.logger.log(
          `[${logTag}] added ${rel} from IPFS (${content.length} bytes)`,
        );
        return;
      } catch (error) {
        this.logger.debug(
          `[${logTag}] IPFS cat ${arg}: ${(error as Error).message}`,
        );
      }
    }
  }

  async finalizeDirectoryManifest(
    rootCid: string,
    files: DirectoryManifestFileDto[],
    options?: { indexDocument?: string },
  ): Promise<{ manifestReference: string; fileCount: number }> {
    if (rootCid.includes('/')) {
      throw new BadRequestException('rootCid must be a single CID, not a path');
    }
    const unixfsKind = await kuboUnixfsTypeForCid(
      this.config.ipfsApiUrl,
      this.config.upstreamTimeout,
      rootCid,
    );
    if (unixfsKind !== 'directory') {
      throw new BadRequestException(
        `rootCid must be a UnixFS directory (Kubo reports type "${unixfsKind}" for /ipfs/${rootCid.slice(0, 12)}…). Manifest finalize is only for directory websites, not a single file chunk.`,
      );
    }
    if (!this.config.beePostageStamp) {
      throw new BadRequestException('BEE_POSTAGE_STAMP is required');
    }
    if (!files.length) {
      throw new BadRequestException('files must include at least one sub-path');
    }

    const seen = new Set<string>();
    const normalized: {
      relPath: string;
      bzzHash: string;
      contentType?: string;
      filename?: string;
    }[] = [];
    for (const f of files) {
      const p = f.relPath.trim().replace(/^\/+/, '').replace(/\/+$/g, '');
      if (!p || p.includes('//')) {
        throw new BadRequestException(
          `Invalid relPath: ${f.relPath} (use a path like lib/x.js)`,
        );
      }
      if (seen.has(p)) {
        throw new BadRequestException(`Duplicate relPath: ${p}`);
      }
      seen.add(p);
      if (!f.bzzHash?.trim()) {
        throw new BadRequestException(`Missing bzzHash for relPath: ${p}`);
      }
      normalized.push({
        relPath: p,
        bzzHash: f.bzzHash.trim(),
        contentType: f.contentType,
        filename: f.filename,
      });
    }
    normalized.sort((a, b) => a.relPath.localeCompare(b.relPath));
    const bee = new Bee(this.config.beeApiUrl.replace(/\/$/, ''));
    const logTag = rootCid.slice(0, 12);
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), 'hive-finalize-dir-'));

    let manifestRef: string;
    let indexDoc: string | null;
    const relPaths = normalized.map((f) => f.relPath);

    try {
      for (const f of normalized) {
        const local =
          this.fileIndexService.getByIpfsCid(`${rootCid}/${f.relPath}`) ??
          this.fileIndexService.getByBzzHash(f.bzzHash);
        if (!local) {
          throw new BadRequestException(
            `No local Hive content found for relPath ${f.relPath}`,
          );
        }

        const stored = await this.driveService.get(local.checksum);
        if (!stored?.content) {
          throw new BadRequestException(
            `Missing local bytes for relPath ${f.relPath}`,
          );
        }

        const absFile = path.join(tmpRoot, f.relPath);
        await mkdir(path.dirname(absFile), { recursive: true });
        await writeFile(absFile, stored.content);
      }

      await this.injectRootIndexFromIpfsIfMissing(
        relPaths,
        tmpRoot,
        rootCid,
        logTag,
      );

      const resolvedIndex = resolveWebsiteIndexDocument(relPaths);
      const requestedIndex =
        options?.indexDocument &&
        resolvedIndex &&
        /^index\.html?$/i.test(resolvedIndex) &&
        options.indexDocument !== resolvedIndex
          ? resolvedIndex
          : options?.indexDocument;
      indexDoc = this.pickIndexDocument(relPaths, requestedIndex);

      this.logger.log(
        `[${logTag}] Collection publish: ${relPaths.length} file(s)${indexDoc ? `, indexDocument=${indexDoc}` : ''}`,
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
      await rm(tmpRoot, { recursive: true, force: true }).catch(() => {});
    }

    await this.verifyManifestOnBee(bee, manifestRef, logTag, indexDoc);

    const rootChecksum =
      this.fileIndexService.getChecksumByIpfsCid(rootCid) ?? undefined;

    const linked = await this.driveService.publishIpfsDirectoryManifest(
      rootCid,
      manifestRef,
      rootChecksum,
    );
    if (linked) {
      await this.fileIndexService.addOrUpdate(linked);
    } else {
      this.logger.log(
        `[${rootCid.slice(0, 12)}] Bridge stored for root (no local root row in index)`,
      );
    }

    this.logger.log(
      `[${rootCid.slice(0, 12)}] Root → manifest ${manifestRef.slice(0, 16)}...`,
    );
    return { manifestReference: manifestRef, fileCount: normalized.length };
  }
}
