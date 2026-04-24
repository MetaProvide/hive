# Hive

Hive is a local IPFS-to-Swarm bridge and cache.

It sits in front of an IPFS node and a Bee node, mirrors IPFS content into Swarm, and keeps a Hyperdrive-backed local cache so content can still be served when one upstream goes away.

## Behavior

IPFS uploads:
- `POST /api/v0/add` proxies to Kubo
- Hive extracts returned CIDs
- the uploaded content is cached locally
- the same content is uploaded to Bee
- Hive stores a `CID -> bzz hash` bridge mapping

IPFS reads:
- `GET /ipfs/<cid>` is Swarm-first when a bridge mapping exists
- if Bee is unavailable, Hive falls back to local cache
- if neither mapping nor local cache exists, Hive falls back to the IPFS gateway
- successful gateway reads are cached and bridged to Swarm

Swarm uploads:
- `POST /bzz` proxies to Bee with `swarm-pin: true`
- Hive caches the uploaded bytes locally
- Swarm uploads are not bridged back into IPFS

Swarm reads:
- `GET /bzz/<hash>` serves from Bee first
- successful Bee reads are cached locally in Hive
- if Bee is unavailable, Hive falls back to the local cached copy

Response headers:
- `x-content-origin: hive` means the response came from the local Hyperdrive cache
- `x-content-origin: swarm` means the response came from Bee/Swarm, including IPFS bridge hits
- `x-content-origin: ipfs` means the response came from an upstream IPFS gateway or Kubo RPC

## Architecture

Core pieces:
- `IdentityService`: owns the local Corestore and Hyperdrive
- `DriveService`: stores content, metadata, and protocol references
- `FileIndexService`: keeps fast in-memory bridge lookups for active runtime use
- `SwarmBridgeService`: uploads mirrored IPFS content to Bee and resolves `CID -> bzz`
- `IpfsService`: handles `/ipfs/*`, `/ipns/*`, and `/api/v0/*`
- `EthswarmService`: handles `/bzz/*`, `/bytes/*`, and `/chunks/*`

Local storage layout:
- `/content/<checksum>` raw bytes
- `/meta/<checksum>.json` metadata
- `/refs/ipfs/<cid>` CID to checksum
- `/refs/bzz/<hash>` bzz hash to checksum
- `/refs/bridge/ipfs/<cid>` CID to bridged bzz hash

## Endpoints

Hive endpoints:
- `GET /hive/status`
- `POST /hive/storage/purge`
- `GET /hive/list`
- `GET /hive/ls`
- `GET /hive/ls/:path`
- `GET /hive/content/:checksum`
- `GET /hive/meta/:checksum`
- `GET /hive/feed/local`
- `POST /hive/content`
- `POST /hive/drive`
- `DELETE /hive/content/:checksum`

Proxy endpoints:
- `GET /ipfs/*`
- `ALL /ipns/*`
- `ALL /api/v0/*`
- `GET /bzz/*`
- `POST /bzz`
- `POST /bzz/*`
- `GET /bytes/*`
- `POST /bytes`
- `GET /chunks/*`
- `POST /chunks`

## Environment

Required variables:
- `STORAGE_PATH`
- `BEE_API_URL`
- `BEE_POSTAGE_STAMP`
- `IPFS_GATEWAY_URL`
- `IPFS_API_URL`
- `UPSTREAM_TIMEOUT`

Optional variables:
- `NODE_ID` default `HIVE`
- `PORT` default `4774`
- `BODY_LIMIT` default `1073741824`

See `.env.example` for a full runnable template.

## Development

Install:

```bash
pnpm install
```

Run locally:

```bash
pnpm start:dev
```

Open API docs:

```text
http://localhost:4774/hive/docs/api
```

## Testing

Unit tests:

```bash
pnpm test
```

E2E tests:

```bash
pnpm test:e2e
```

The e2e suite uses real upstream nodes.

Expected environment:
- `BEE_API_URL`, `BEE_POSTAGE_STAMP`, `IPFS_GATEWAY_URL`, and `IPFS_API_URL` point to running Bee and Kubo nodes
- optional `E2E_BEE_API_URL`, `E2E_BEE_POSTAGE_STAMP`, `E2E_IPFS_GATEWAY_URL`, and `E2E_IPFS_API_URL` can override the normal runtime values for tests only

What it verifies:
- IPFS upload through Hive creates a real CID, Hive serves that CID, and after Hive is restarted with IPFS pointed at an unreachable URL the same CID is still served via the real Swarm bridge
- Swarm upload through Hive creates a real Bee reference, Hive serves that reference, and after Hive is restarted with Bee pointed at an unreachable URL the same reference is still served from Hive's local cache

Lint and format checks:

```bash
pnpm check
```

Build:

```bash
pnpm build
```
