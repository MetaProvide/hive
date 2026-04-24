import { Module } from '@nestjs/common';
import { HiveModule } from '../hive/hive.module';
import { IpfsService } from './ipfs.service';

@Module({
  imports: [HiveModule],
  providers: [IpfsService],
})
export class IpfsModule {}
