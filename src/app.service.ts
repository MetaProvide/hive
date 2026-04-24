import { Injectable } from '@nestjs/common';
import { ConfigService } from './config/config.service';

@Injectable()
export class AppService {
  constructor(private readonly config: ConfigService) {}

  getHello(): string {
    return `Hive Node ${this.config.nodeId} running on port ${this.config.port}`;
  }
}
