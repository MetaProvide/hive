import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class DirectoryManifestFileDto {
  @ApiProperty({
    example: 'lib/angular.min.js',
    description: 'Path under the IPFS root (no leading slash)',
  })
  relPath!: string;

  @ApiProperty({
    description: 'Bee chunk reference for this file (from prior +bzz / bridge)',
  })
  bzzHash!: string;

  @ApiPropertyOptional({ example: 'application/javascript' })
  contentType?: string;

  @ApiPropertyOptional({ example: 'angular.min.js' })
  filename?: string;
}

export class FinalizeDirectoryManifestRequestDto {
  @ApiProperty({
    description:
      'Root IPFS directory CID (first path segment). If you have a row for it in the index, its bzzHash is updated; the bridge mapping is always written.',
    example: 'QmXxx...',
  })
  rootCid!: string;

  @ApiProperty({
    type: [DirectoryManifestFileDto],
    description:
      'All files in this directory from the UI (one row per sub-path, with bzz from bridging). Drives the mantaray forks.',
  })
  files!: DirectoryManifestFileDto[];

  @ApiPropertyOptional({
    description:
      'Served for GET /bzz/{manifest}/ . Defaults to index.html or index.htm when those relPaths exist in files',
    example: 'index.html',
  })
  indexDocument?: string;
}

export class FinalizeDirectoryManifestResponseDto {
  @ApiProperty()
  rootCid!: string;

  @ApiProperty({
    description:
      'Mantaray root reference in Swarm (use with GET {bee}/bzz/{reference}/path)',
  })
  manifestReference!: string;

  @ApiProperty({ description: 'Number of file paths in the manifest' })
  fileCount!: number;
}
