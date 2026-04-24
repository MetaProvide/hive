import {
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { mkdir, rm } from 'fs/promises';
import Corestore from 'corestore';
import Hyperdrive from 'hyperdrive';
import { ConfigService } from '../../config/config.service';

@Injectable()
export class IdentityService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(IdentityService.name);

  private store: Corestore;
  private drive: Hyperdrive;
  private readyResolve: () => void;
  private readonly readyPromise = new Promise<void>((resolve) => {
    this.readyResolve = resolve;
  });

  constructor(private readonly config: ConfigService) {}

  whenReady(): Promise<void> {
    return this.readyPromise;
  }

  async onModuleInit() {
    const { nodeId, storagePath } = this.config;

    this.logger.log(`Initializing local storage for node: ${nodeId}`);
    this.logger.log(`Storage path: ${storagePath}`);

    this.store = new Corestore(storagePath);
    await this.store.ready();

    this.drive = new Hyperdrive(this.store);
    await this.drive.ready();
    this.logger.log(
      `Drive key: ${this.drive.key.toString('hex').slice(0, 16)}...`,
    );

    this.readyResolve();
  }

  async onModuleDestroy() {
    this.logger.log('Closing identity service');
    if (this.drive) {
      await this.drive.close();
    }
    if (this.store) {
      await this.store.close();
    }
    this.logger.log('Identity service closed');
  }

  getDrive(): Hyperdrive {
    return this.drive;
  }

  /**
   * Deletes all on-disk Corestore/Hyperdrive data under {@link ConfigService.storagePath},
   * then opens a fresh store and drive. In-flight operations should finish before calling.
   */
  async purgeStorageFolder(): Promise<void> {
    const { storagePath } = this.config;
    this.logger.warn(`Purging local Hyperdrive storage at ${storagePath}`);

    if (this.drive) {
      await this.drive.close();
    }
    if (this.store) {
      await this.store.close();
    }

    await rm(storagePath, { recursive: true, force: true });
    await mkdir(storagePath, { recursive: true });

    this.store = new Corestore(storagePath);
    await this.store.ready();
    this.drive = new Hyperdrive(this.store);
    await this.drive.ready();

    this.logger.log(
      `Storage purged; new drive key ${this.drive.key.toString('hex').slice(0, 16)}…`,
    );
  }
}
