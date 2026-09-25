import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { verifySecretNames, verifyProduction } from '../scripts/deployment-checks.mjs';

const config = JSON.parse(readFileSync(new URL('../wrangler.jsonc', import.meta.url), 'utf8'));
const required = config.secrets.required;
const metadata = names => JSON.stringify(names.map(name => ({ name, type: 'secret_text' })));

test('deployment verifies existing secret names without requiring any secret values', () => {
  assert.equal(verifySecretNames(metadata(required), required), required.length);
  assert.equal(verifySecretNames(metadata([...required, 'UNRELATED_SECRET']), required), required.length);
});

test('missing Worker secrets stop deployment with targeted recovery guidance', () => {
  assert.throws(() => verifySecretNames(metadata(required.slice(1)), required),
    /Missing existing Worker secrets: APP_ACCESS_CODE.*Stop before migrations/);
  assert.throws(() => verifySecretNames('[]', required), /Missing existing Worker secrets/);
});

test('invalid metadata never leaks an upstream response into an error', () => {
  for (const value of ['do-not-print-this-body', '{"unexpected":"do-not-print-this-body"}', '[null]', '[{}]']) {
    assert.throws(() => verifySecretNames(value, required), { message: 'Invalid Worker secret-name response' });
  }
});

test('plaintext bindings do not satisfy the encrypted-secret preflight', () => {
  const rows = required.map(name => ({ name, type: 'plain_text' }));
  assert.throws(() => verifySecretNames(JSON.stringify(rows), required), /Missing existing Worker secrets/);
});

test('a missing or malformed required-secret contract fails closed', () => {
  for (const names of [undefined, [], ['bad\nname'], ['APP_ACCESS_CODE', 'APP_ACCESS_CODE'], [42]]) {
    assert.throws(() => verifySecretNames('[]', names), /Invalid required-secret configuration/);
  }
});

const passing = path => path === '/api/health' ? Response.json({ ok: true }) :
  path === '/api/readiness' ? Response.json({ ready: true }) :
  Response.json({ error: { code: 'AUTH_REQUIRED' } }, { status: 401 });

test('production smoke checks are read-only, bounded, unauthenticated, and reject redirects', async () => {
  const paths = [];
  const result = await verifyProduction('https://app.example', async (url, options) => {
    paths.push(url.pathname);
    assert.equal(options.redirect, 'error');
    assert.equal(options.credentials, 'omit');
    assert.equal(options.method, undefined);
    assert.equal(options.headers.Authorization, undefined);
    assert.equal(options.headers['Cache-Control'], 'no-cache');
    assert.ok(options.signal instanceof AbortSignal);
    return passing(url.pathname);
  });
  assert.equal(result, 3);
  assert.deepEqual(paths, ['/api/health', '/api/readiness', '/api/leagues']);
});

for (const [label, path, response] of [
  ['health false', '/api/health', () => Response.json({ ok: false })],
  ['readiness unavailable', '/api/readiness', () => Response.json({ ready: false }, { status: 503 })],
  ['private API exposed', '/api/leagues', () => Response.json({ leagues: [] })],
  ['wrong auth error', '/api/leagues', () => Response.json({ error: { code: 'ESPN_AUTH_EXPIRED' } }, { status: 401 })],
  ['HTML fallback', '/api/health', () => new Response('<html>shell</html>', { headers: { 'content-type': 'text/html' } })],
  ['invalid JSON', '/api/health', () => new Response('do-not-print-this-body', { headers: { 'content-type': 'application/json' } })],
  ['oversized response', '/api/health', () => Response.json({ ok: true, data: 'x'.repeat(65_536) })],
]) {
  test(`smoke fails closed for ${label}`, async () => {
    await assert.rejects(verifyProduction('https://app.example', async url =>
      url.pathname === path ? response() : passing(url.pathname)), error => {
      assert.match(error.message, /Production check failed/);
      assert.ok(error.message.includes(path));
      assert.ok(!error.message.includes('do-not-print-this-body'));
      return true;
    });
  });
}

test('network failures are redacted rather than dumping errors or credentials', async () => {
  await assert.rejects(verifyProduction('https://app.example', async () => {
    throw new Error('do-not-print-this-body');
  }), error => error.message.includes('/api/health') && !error.message.includes('do-not-print'));
});

test('unsafe smoke URLs are rejected before making requests', async () => {
  for (const url of [undefined, 'bad', 'http://app.example', 'https://name:secret@app.example',
    'https://app.example/path', 'https://app.example?secret=value', 'https://app.example/#secret']) {
    await assert.rejects(verifyProduction(url, () => assert.fail('Must not fetch')), /URL/);
  }
});

test('secret preflight CLI succeeds with names alone and returns nonzero on incomplete output', () => {
  const script = new URL('../scripts/deployment-checks.mjs', import.meta.url);
  for (const [input, status] of [[metadata(required), 0], ['[]', 1], ['do-not-print-this-body', 1]]) {
    const result = spawnSync(process.execPath, [script.pathname, 'secrets'], { input, encoding: 'utf8' });
    assert.equal(result.status, status);
    assert.doesNotMatch(result.stderr, /do-not-print-this-body/);
  }
});

test('release workflow preserves runtime secrets and preflights before migrations', () => {
  const workflow = readFileSync(new URL('../.github/workflows/deploy.yml', import.meta.url), 'utf8');
  for (const name of required) assert.ok(!workflow.includes(`secrets.${name}`));
  assert.doesNotMatch(workflow, /--secrets-file|secret (put|bulk|delete)|workflow_run:|\n  push:/);
  assert.match(workflow, /workflow_dispatch:/);
  assert.match(workflow, /environment: production/);
  assert.match(workflow, /persist-credentials: false/);
  const preflight = workflow.indexOf('node scripts/deployment-checks.mjs secrets');
  const migration = workflow.indexOf('wrangler d1 migrations apply');
  const deploy = workflow.indexOf('run: npx wrangler deploy');
  const smoke = workflow.indexOf('node scripts/deployment-checks.mjs smoke');
  assert.ok(preflight > 0 && preflight < migration && migration < deploy && deploy < smoke);
  assert.ok(!workflow.split('    steps:')[0].includes('CLOUDFLARE_API_TOKEN'));
  assert.match(workflow, /set -euo pipefail\n\s+npx wrangler secret list --format json \| node/);
});
