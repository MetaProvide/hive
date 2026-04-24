import { Injectable } from '@nestjs/common';

function env(key: string, defaultValue?: string): string {
  const value = process.env[key];
  if (value === undefined) {
    if (defaultValue !== undefined) {
      return defaultValue;
    }
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

@Injectable()
export class ConfigService {
  readonly nodeId: string;
  readonly port: number;
  readonly storagePath: string;
  readonly beeApiUrl: string;
  readonly beePostageStamp: string;
  readonly ipfsGatewayUrl: string;
  readonly ipfsApiUrl: string;
  readonly upstreamTimeout: number;
  readonly bodyLimit: number;

  constructor() {
    this.nodeId = env('NODE_ID', 'HIVE');
    this.port = Number(env('PORT', '4774'));
    this.storagePath = env('STORAGE_PATH');
    this.beeApiUrl = env('BEE_API_URL');
    this.beePostageStamp = env('BEE_POSTAGE_STAMP');
    this.ipfsGatewayUrl = env('IPFS_GATEWAY_URL');
    this.ipfsApiUrl = env('IPFS_API_URL');
    this.upstreamTimeout = Number.parseInt(env('UPSTREAM_TIMEOUT'), 10);
    this.bodyLimit = Number.parseInt(env('BODY_LIMIT', '1073741824'), 10);
  }
}
