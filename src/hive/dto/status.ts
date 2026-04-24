import { ApiProperty } from '@nestjs/swagger';

export class PurgeStorageResponseDto {
  @ApiProperty({ description: 'Whether the storage directory was wiped and reopened' })
  purged!: boolean;

  @ApiProperty({
    description: 'Absolute or configured path that was deleted and recreated',
    example: './storage/node-default',
  })
  storagePath!: string;
}

export class NodeStatusDto {
  @ApiProperty({ description: 'Node identifier', example: 'A' })
  nodeId: string;

  @ApiProperty({ description: 'Number of stored content items', example: 42 })
  contentCount: number;

  @ApiProperty({
    description: 'Number of IPFS items that have a bridged Swarm hash',
    example: 18,
  })
  bridgedCount: number;
}

export class DriveEntryDto {
  @ApiProperty({ description: 'Entry path', example: '/content/ab/abc123...' })
  key: string;

  @ApiProperty({ description: 'Size in bytes', example: 1024 })
  size: number;

  @ApiProperty({ description: 'Whether entry is a directory' })
  isDirectory: boolean;
}

export class DriveListResponseDto {
  @ApiProperty({ description: 'Listed path', example: '/content' })
  path: string;

  @ApiProperty({ type: [DriveEntryDto], description: 'Directory entries' })
  entries: DriveEntryDto[];

  @ApiProperty({ description: 'Total count', example: 5 })
  count: number;
}

export class FeedEntryDto {
  @ApiProperty({
    description: 'SHA256 checksum of content',
    example: '7f83b1657ff1fc53...',
  })
  checksum: string;

  @ApiProperty({ description: 'MIME content type', example: 'text/plain' })
  contentType: string;

  @ApiProperty({ description: 'Content size in bytes', example: 1024 })
  size: number;

  @ApiProperty({
    description: 'Storage timestamp (ms since epoch)',
    example: 1704067200000,
  })
  timestamp: number;
}

export class FeedResponseDto {
  @ApiProperty({ type: [FeedEntryDto], description: 'Feed entries' })
  items: FeedEntryDto[];

  @ApiProperty({ description: 'Number of items returned', example: 10 })
  count: number;

  @ApiProperty({
    description: 'Feed scope',
    example: 'local',
    enum: ['local'],
  })
  scope: string;
}
