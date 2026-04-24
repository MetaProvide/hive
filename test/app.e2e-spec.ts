import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import 'dotenv/config';
import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import request from 'supertest';
import { createApp } from '../src/bootstrap';
import { IpfsService } from '../src/ipfs/ipfs.service';

const HIVE_ENV_KEYS = [
  'NODE_ID',
  'PORT',
  'STORAGE_PATH',
  'BODY_LIMIT',
  'BEE_API_URL',
  'BEE_POSTAGE_STAMP',
  'IPFS_GATEWAY_URL',
  'IPFS_API_URL',
  'UPSTREAM_TIMEOUT',
] as const;

const realNodeConfig = {
  beeApiUrl: process.env.E2E_BEE_API_URL ?? process.env.BEE_API_URL,
  beePostageStamp:
    process.env.E2E_BEE_POSTAGE_STAMP ?? process.env.BEE_POSTAGE_STAMP,
  ipfsGatewayUrl:
    process.env.E2E_IPFS_GATEWAY_URL ?? process.env.IPFS_GATEWAY_URL,
  ipfsApiUrl: process.env.E2E_IPFS_API_URL ?? process.env.IPFS_API_URL,
};

const hasRealNodeConfig = Object.values(realNodeConfig).every(Boolean);
const describeRealNodes = hasRealNodeConfig ? describe : describe.skip;

function parseKuboAddResponse(body: string): string {
  let lastHash: string | null = null;

  for (const line of body.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }

    try {
      const parsed = JSON.parse(trimmed) as { Hash?: string };
      if (parsed.Hash) {
        lastHash = parsed.Hash;
      }
    } catch {
      // Ignore non-JSON progress output.
    }
  }

  if (!lastHash) {
    throw new Error(`Unable to parse CID from Kubo response: ${body}`);
  }

  return lastHash;
}

async function waitFor(
  predicate: () => Promise<boolean>,
  timeoutMs = 60_000,
  intervalMs = 500,
): Promise<void> {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error('Timed out waiting for condition');
}

async function waitForNoPendingIpfsBridges(
  app: NestFastifyApplication,
): Promise<void> {
  await waitFor(
    async () => {
      const ipfsService = app.get(IpfsService) as unknown as {
        pendingBridges?: Map<string, Promise<void>>;
      };
      return (ipfsService.pendingBridges?.size ?? 0) === 0;
    },
    10_000,
    100,
  );
}

describeRealNodes('Hive bridge with real upstream nodes (e2e)', () => {
  let app: NestFastifyApplication;
  let storagePath: string;
  let previousEnv: Partial<
    Record<(typeof HIVE_ENV_KEYS)[number], string | undefined>
  >;

  const deadUpstreamUrl = 'http://127.0.0.1:1';

  async function startHive(
    overrides: Partial<Record<(typeof HIVE_ENV_KEYS)[number], string>> = {},
  ): Promise<NestFastifyApplication> {
    process.env.NODE_ID = 'e2e-real-nodes';
    process.env.PORT = '0';
    process.env.STORAGE_PATH = storagePath;
    process.env.BODY_LIMIT = '10485760';
    process.env.BEE_API_URL = realNodeConfig.beeApiUrl!;
    process.env.BEE_POSTAGE_STAMP = realNodeConfig.beePostageStamp!;
    process.env.IPFS_GATEWAY_URL = realNodeConfig.ipfsGatewayUrl!;
    process.env.IPFS_API_URL = realNodeConfig.ipfsApiUrl!;
    process.env.UPSTREAM_TIMEOUT = '5000';

    for (const [key, value] of Object.entries(overrides)) {
      process.env[key as (typeof HIVE_ENV_KEYS)[number]] = value;
    }

    const hiveApp = await createApp();
    await hiveApp.init();
    await hiveApp.getHttpAdapter().getInstance().ready();
    return hiveApp;
  }

  beforeEach(async () => {
    previousEnv = Object.fromEntries(
      HIVE_ENV_KEYS.map((key) => [key, process.env[key]]),
    ) as Partial<Record<(typeof HIVE_ENV_KEYS)[number], string | undefined>>;
    storagePath = await mkdtemp(join(tmpdir(), 'hive-real-e2e-'));
    app = await startHive();
  });

  afterEach(async () => {
    if (app) {
      await app.close();
    }

    if (storagePath) {
      await rm(storagePath, { recursive: true, force: true });
    }

    for (const key of HIVE_ENV_KEYS) {
      const value = previousEnv[key];
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  });

  it('uploads to IPFS, serves by real CID, and still serves after IPFS is unavailable', async () => {
    const content = `real-ipfs-e2e-${Date.now()}`;

    const addResponse = await request(app.getHttpServer())
      .post('/api/v0/add?pin=true')
      .attach('file', Buffer.from(content), 'real-ipfs.txt')
      .expect(200);

    const cid = parseKuboAddResponse(addResponse.text);

    await waitFor(async () => {
      const response = await request(app.getHttpServer())
        .get(`/ipfs/${cid}`)
        .expect(200);

      return (
        response.headers['x-content-origin'] === 'swarm' &&
        response.text === content
      );
    });

    await waitForNoPendingIpfsBridges(app);

    await app.close();
    app = await startHive({
      IPFS_GATEWAY_URL: deadUpstreamUrl,
      IPFS_API_URL: deadUpstreamUrl,
    });

    const response = await request(app.getHttpServer())
      .get(`/ipfs/${cid}`)
      .expect(200);

    expect(response.headers['x-content-origin']).toBe('swarm');
    expect(response.text).toBe(content);
  }, 120_000);

  it('uploads to Swarm, serves from Bee first, and then serves from Hive cache after Bee is unavailable', async () => {
    const content = `real-swarm-e2e-${Date.now()}`;

    const uploadResponse = await request(app.getHttpServer())
      .post('/bzz?name=real-swarm.txt')
      .set('content-type', 'text/plain')
      .set('swarm-postage-batch-id', realNodeConfig.beePostageStamp!)
      .send(content);

    expect([200, 201]).toContain(uploadResponse.status);

    const reference =
      typeof uploadResponse.body?.reference === 'string'
        ? uploadResponse.body.reference
        : (JSON.parse(uploadResponse.text) as { reference: string }).reference;

    await waitFor(async () => {
      const listResponse = await request(app.getHttpServer())
        .get('/hive/list')
        .expect(200);

      return listResponse.body.items.some(
        (item: { bzzHash?: string }) => item.bzzHash === reference,
      );
    });

    const swarmFirstResponse = await request(app.getHttpServer())
      .get(`/bzz/${reference}`)
      .expect(200);

    expect(swarmFirstResponse.headers['x-content-origin']).toBe('swarm');
    expect(swarmFirstResponse.text).toBe(content);

    await app.close();
    app = await startHive({
      BEE_API_URL: deadUpstreamUrl,
    });

    const response = await request(app.getHttpServer())
      .get(`/bzz/${reference}`)
      .expect(200);

    expect(response.headers['x-content-origin']).toBe('hive');
    expect(response.text).toBe(content);
  }, 120_000);
});
