import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export type ContentKind = 'file' | 'directory';

export class ContentMetadataDto {
  @ApiProperty({
    description: 'SHA256 checksum of content',
    example: '7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069',
  })
  checksum: string;

  @ApiProperty({ description: 'Content size in bytes', example: 1024 })
  size: number;

  @ApiProperty({ description: 'MIME content type', example: 'text/plain' })
  contentType: string;

  @ApiPropertyOptional({
    description: 'Original filename',
    example: 'document.txt',
  })
  filename?: string;

  @ApiProperty({
    description: 'Storage timestamp (ms since epoch)',
    example: 1704067200000,
  })
  timestamp: number;

  @ApiPropertyOptional({
    description:
      'File last-modified timestamp from the source filesystem (ms since epoch)',
    example: 1704067200000,
  })
  lastModified?: number;

  @ApiPropertyOptional({
    description:
      'Original source path from the uploading client (e.g. relative directory path)',
    example: 'photos/vacation',
  })
  sourcePath?: string;

  @ApiPropertyOptional({
    description: 'IPFS CID if content came from IPFS',
    example: 'QmXxx...',
  })
  ipfsCid?: string;

  @ApiPropertyOptional({
    description:
      'Swarm / Bee content reference: use with HTTP GET {bee}/bzz/{bzzHash}. For IPFS sub-path mirrors this is the per-chunk reference returned from Bee upload, not a slash path.',
    example: 'abc123...',
  })
  bzzHash?: string;

  @ApiPropertyOptional({
    description:
      'Set only after building an IPFS directory website manifest (upload-dir-to-bzz / finalize directory). Swarm mantaray root (GET {bee}/bzz/{manifestBzzHash}/). Omitted for normal files and for ordinary per-chunk +bzz bridges.',
    example: 'abc123...',
  })
  manifestBzzHash?: string;

  @ApiPropertyOptional({
    description:
      'When set, the drive also stores a ref at /refs/bzz/{bzzRefKey} (e.g. root manifest hash plus path for a Swarm sub-asset).',
  })
  bzzRefKey?: string;

  @ApiPropertyOptional({
    description:
      'directory = synthetic row for an IPFS UnixFS folder (root CID + optional mantaray manifest); omit or file for normal blobs',
    enum: ['file', 'directory'],
  })
  contentKind?: ContentKind;
}

export class ContentResponseDto {
  @ApiProperty({ description: 'Raw content as Buffer' })
  content: Buffer;

  @ApiProperty({ type: ContentMetadataDto })
  metadata: ContentMetadataDto;
}

export class ContentListResponseDto {
  @ApiProperty({
    type: [ContentMetadataDto],
    description: 'List of content metadata',
  })
  items: ContentMetadataDto[];

  @ApiProperty({ description: 'Total count', example: 10 })
  count: number;
}

export class StoreContentRequestDto {
  @ApiProperty({
    description: 'Base64 encoded content',
    example: 'SGVsbG8gV29ybGQh',
  })
  content: string;

  @ApiPropertyOptional({
    description: 'MIME content type',
    example: 'text/plain',
  })
  contentType?: string;

  @ApiPropertyOptional({
    description: 'Original filename',
    example: 'hello.txt',
  })
  filename?: string;

  @ApiPropertyOptional({
    description:
      'File last-modified timestamp from the source filesystem (ms since epoch)',
    example: 1704067200000,
  })
  lastModified?: number;

  @ApiPropertyOptional({
    description: 'Original source path from the uploading client',
    example: 'photos/vacation',
  })
  sourcePath?: string;

  @ApiPropertyOptional({
    description: 'IPFS CID reference',
    example: 'QmXxx...',
  })
  ipfsCid?: string;

  @ApiPropertyOptional({
    description: 'Swarm hash reference',
    example: 'abc123...',
  })
  bzzHash?: string;
}

// Internal DTO (not for API)
export class StoreContentDto {
  content: Buffer;
  contentType?: string;
  filename?: string;
  lastModified?: number;
  sourcePath?: string;
  ipfsCid?: string;
  bzzHash?: string;
  bzzRefKey?: string;
  contentKind?: ContentKind;
}
