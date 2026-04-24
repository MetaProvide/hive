import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ConfigModule } from './config/config.module';
import { EthswarmModule } from './ethswarm/ethswarm.module';
import { HiveModule } from './hive/hive.module';
import { IpfsModule } from './ipfs/ipfs.module';

@Module({
  imports: [ConfigModule, HiveModule, IpfsModule, EthswarmModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
