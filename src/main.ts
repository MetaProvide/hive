import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import 'dotenv/config';
import { Logger } from '@nestjs/common';
import { bootstrap } from './bootstrap';

async function main() {
  const logger = new Logger('Bootstrap');

  if (!existsSync(resolve(process.cwd(), '.env'))) {
    logger.warn(
      '.env file not found — copy .env.example to .env and configure it',
    );
  }

  await bootstrap();
}

void main();
