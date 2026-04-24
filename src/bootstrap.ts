import replyFrom from '@fastify/reply-from';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  type NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import scalar from '@scalar/fastify-api-reference';
import type { FastifyReply } from 'fastify';
import { AppModule } from './app.module';
import { ConfigService } from './config/config.service';

type FastifyReplyWithFrom = FastifyReply & {
  from: (source: string) => FastifyReply;
};

export async function createApp(
  config = new ConfigService(),
): Promise<NestFastifyApplication> {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({
      bodyLimit: config.bodyLimit,
      connectionTimeout: 0,
      requestTimeout: 0,
    }),
  );

  app.enableCors({
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    // Browsers only expose CORS “simple” response headers to fetch(); custom headers
    // like x-hive-cache must be listed or JS sees them as missing (e.g. Fetch panel).
    exposedHeaders: ['x-hive-cache'],
  });
  app.enableShutdownHooks();

  const fastify = app.getHttpAdapter().getInstance();
  await fastify.register(replyFrom as any, {
    contentTypesToEncode: [],
    http: {
      requestOptions: {
        timeout: 30 * 60 * 1000,
      },
    },
  });

  fastify.addHook('preParsing', (request, _reply, payload, done) => {
    if (
      request.method === 'DELETE' &&
      request.headers['content-type']?.startsWith('application/json')
    ) {
      delete request.headers['content-type'];
    }
    done(null, payload);
  });

  fastify.addContentTypeParser(
    /^(?!application\/json|text\/plain).*/,
    { parseAs: 'buffer' },
    (_req, body, done) => {
      done(null, body);
    },
  );

  const swaggerConfig = new DocumentBuilder()
    .setTitle('Hive API')
    .setDescription('Local IPFS-to-Swarm bridge and cache API')
    .setVersion('1.0')
    .addTag('hive', 'Hive bridge operations')
    .addTag('ipfs', 'IPFS bridge proxy')
    .addTag('ethswarm', 'Swarm cache proxy')
    .build();

  const document = SwaggerModule.createDocument(app, swaggerConfig);
  fastify.get('/hive/docs/openapi.json', (_request, reply) => {
    reply.send(document);
  });

  await fastify.register(scalar as any, {
    routePrefix: '/hive/docs/api',
    configuration: {
      url: '/hive/docs/openapi.json',
      theme: 'purple',
    },
  });

  const proxyLogger = new Logger('UpstreamProxy');

  await fastify.register(async (instance) => {
    instance.removeAllContentTypeParsers();
    instance.addContentTypeParser(
      '*',
      { parseAs: 'buffer' },
      (_req, body, done) => done(null, body),
    );

    instance.route({
      method: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD'],
      url: '/*',
      handler: (req, reply) => {
        const url = req.url;
        if (url === '/' || url.startsWith('/hive/')) {
          return reply
            .status(404)
            .send({ statusCode: 404, message: 'Not Found' });
        }

        const target = url.startsWith('/api/')
          ? config.ipfsApiUrl
          : config.beeApiUrl;
        const proxyTarget = `${target}${url}`;
        proxyLogger.log(`Proxying ${req.method} ${url} -> ${proxyTarget}`);
        return (reply as FastifyReplyWithFrom).from(proxyTarget);
      },
    });
  });

  return app;
}

export async function bootstrap(): Promise<void> {
  const logger = new Logger('Bootstrap');
  const config = new ConfigService();
  const app = await createApp(config);

  await app.listen(config.port, '0.0.0.0');
  logger.log(`Hive Node ${config.nodeId} listening on port ${config.port}`);
  logger.log(`API docs: http://localhost:${config.port}/hive/docs/api`);
}
