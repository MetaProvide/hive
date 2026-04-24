import { Module } from '@nestjs/common';
import { HiveController } from './controllers/hive.controller';
import { DriveService } from './services/drive.service';
import { FileIndexService } from './services/file-index.service';
import { HiveDirectoryBzzService } from './services/hive-directory-bzz.service';
import { IdentityService } from './services/identity.service';
import { SwarmBridgeService } from './services/swarm-bridge.service';

@Module({
  providers: [
    IdentityService,
    DriveService,
    FileIndexService,
    SwarmBridgeService,
    HiveDirectoryBzzService,
  ],
  controllers: [HiveController],
  exports: [
    IdentityService,
    DriveService,
    FileIndexService,
    SwarmBridgeService,
    HiveDirectoryBzzService,
  ],
})
export class HiveModule {}
