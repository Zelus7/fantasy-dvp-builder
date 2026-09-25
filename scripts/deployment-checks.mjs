import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const MAX_BYTES = 64 * 1024;

async function readBounded(stream) {
  if (!stream) throw new Error('Empty response');
  const chunks = [];
  let length = 0;
  for await (const chunk of stream) {
    const bytes = Buffer.from(chunk);
    length += bytes.length;
    if (length > MAX_BYTES) throw new Error('Response exceeds deployment-check size limit');
    chunks.push(bytes);
  }
  return Buffer.concat(chunks).toString('utf8');
}

export function verifySecretNames(metadata, required) {
  if (!Array.isArray(required) || !required.length ||
      required.some(name => typeof name !== 'string' || !/^[A-Z][A-Z0-9_]*$/.test(name)) ||
      new Set(required).size !== required.length) {
    throw new Error('Invalid required-secret configuration');
  }
  let secrets;
  try { secrets = JSON.parse(metadata); } catch {
    throw new Error('Invalid Worker secret-name response');
  }
  if (!Array.isArray(secrets) || secrets.some(secret =>
    !secret || typeof secret.name !== 'string' || typeof secret.type !== 'string')) {
    throw new Error('Invalid Worker secret-name response');
  }
  const names = new Set(secrets.filter(secret => secret.type === 'secret_text').map(secret => secret.name));
  const missing = required.filter(name => !names.has(name));
  if (missing.length) {
    throw new Error(`Missing existing Worker secrets: ${missing.join(', ')}. Stop before migrations; use the separate bootstrap/recovery procedure, not automatic secret rotation.`);
  }
  return required.length;
}

export async function verifyProduction(baseUrl, fetchImpl = fetch) {
  let url;
  try { url = new URL(baseUrl); } catch { throw new Error('Invalid production URL'); }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || url.pathname !== '/') {
    throw new Error('Production URL must be an HTTPS origin without credentials, path, query, or fragment');
  }
  const checks = [
    { path: '/api/health', status: 200, valid: body => body?.ok === true },
    { path: '/api/readiness', status: 200, valid: body => body?.ready === true },
    { path: '/api/leagues', status: 401, valid: body => body?.error?.code === 'AUTH_REQUIRED' },
  ];
  for (const check of checks) {
    try {
      const response = await fetchImpl(new URL(check.path, url), {
        redirect: 'error',
        credentials: 'omit',
        headers: { Accept: 'application/json', 'Cache-Control': 'no-cache' },
        signal: AbortSignal.timeout(10_000),
      });
      if (response.status !== check.status || !response.headers.get('content-type')?.includes('application/json')) {
        await response.body?.cancel();
        throw new Error('Unexpected HTTP response');
      }
      const body = JSON.parse(await readBounded(response.body));
      if (!check.valid(body)) throw new Error('Unexpected response body');
    } catch {
      // Never echo an upstream body, URL credentials, or raw networking error.
      throw new Error(`Production check failed: ${check.path} (expected HTTP ${check.status} and matching JSON). Inspect the deployment before retrying; no automatic rollback was attempted.`);
    }
  }
  return checks.length;
}

async function main() {
  const mode = process.argv[2];
  if (mode === 'secrets') {
    const config = JSON.parse(readFileSync(new URL('../wrangler.jsonc', import.meta.url), 'utf8'));
    const count = verifySecretNames(await readBounded(process.stdin), config.secrets?.required);
    console.log(`Verified ${count} existing Worker secret names; no values were read or changed.`);
  } else if (mode === 'smoke') {
    const count = await verifyProduction(process.argv[3]);
    console.log(`Passed ${count} production checks: health, database readiness, and private API authentication boundary.`);
  } else {
    throw new Error('Usage: deployment-checks.mjs secrets (metadata on stdin) | smoke <https-origin>');
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
