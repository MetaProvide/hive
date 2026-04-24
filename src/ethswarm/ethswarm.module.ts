import { Module } from '@nestjs/common';
import { HiveModule } from '../hive/hive.module';
import { EthswarmService } from './ethswarm.service';

@Module({
  imports: [HiveModule],
  providers: [EthswarmService],
})
export class EthswarmModule {}
