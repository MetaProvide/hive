import {
  BadRequestException,
  ServiceUnavailableException,
} from '@nestjs/common';

/** Kubo `files/stat` JSON `Type` for `/ipfs/<cid>` (UnixFS). */
export function parseKuboUnixfsTypeFromFilesStat(
  body: unknown,
): 'file' | 'directory' | null {
  if (typeof body !== 'object' || body === null) {
    return null;
  }
  const raw = (body as { Type?: unknown }).Type;
  const t = typeof raw === 'string' ? raw.toLowerCase() : '';
  if (t === 'directory') {
    return 'directory';
  }
  if (t === 'file') {
    return 'file';
  }
  return null;
}

function describeFetchError(err: unknown): string {
  if (!(err instanceof Error)) {
    return String(err);
  }
  const parts = [err.message];
  const c = err.cause;
  if (c instanceof Error) {
    parts.push(c.message);
  } else if (typeof c === 'object' && c !== null && 'code' in c) {
    parts.push(String((c as { code: unknown }).code));
  }
  return parts.filter(Boolean).join(' — ');
}

export async function kuboUnixfsTypeForCid(
  ipfsApiUrl: string,
  upstreamTimeoutMs: number,
  cid: string,
): Promise<'file' | 'directory'> {
  const api = ipfsApiUrl.replace(/\/$/, '');
  const arg = `/ipfs/${cid}`;
  const url = `${api}/api/v0/files/stat?arg=${encodeURIComponent(arg)}`;
  let res: Response;
  try {
    res = await fetch(url, {
      method: 'POST',
      signal: AbortSignal.timeout(upstreamTimeoutMs),
    });
  } catch (err) {
    const msg = describeFetchError(err);
    if (
      err instanceof Error &&
      (err.name === 'TimeoutError' || err.name === 'AbortError')
    ) {
      throw new ServiceUnavailableException(
        `IPFS API request timed out after ${upstreamTimeoutMs}ms (files/stat for /ipfs/${cid.slice(0, 12)}…). Check IPFS_API_URL and Kubo.`,
      );
    }
    throw new ServiceUnavailableException(
      `Cannot reach IPFS API at ${api} (${msg}). Is Kubo running on the configured RPC port? Set IPFS_API_URL if it differs.`,
    );
  }
  const text = await res.text();
  if (!res.ok) {
    throw new BadRequestException(
      `IPFS files/stat failed for /ipfs/${cid.slice(0, 12)}…: ${res.status} ${text.slice(0, 240)}`,
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text) as unknown;
  } catch {
    throw new BadRequestException(
      `IPFS files/stat returned non-JSON for /ipfs/${cid.slice(0, 12)}…`,
    );
  }
  const kind = parseKuboUnixfsTypeFromFilesStat(parsed);
  if (kind) {
    return kind;
  }
  const t =
    typeof parsed === 'object' &&
    parsed !== null &&
    'Type' in parsed &&
    typeof (parsed as { Type: unknown }).Type === 'string'
      ? (parsed as { Type: string }).Type
      : 'unknown';
  throw new BadRequestException(
    `IPFS /ipfs/${cid.slice(0, 12)}… has UnixFS Type "${t}"; expected a directory root`,
  );
}
