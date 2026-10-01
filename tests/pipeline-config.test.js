import test from 'node:test';
import assert from 'node:assert/strict';
import { buildPipelineConfig } from '../src/pipeline-config.js';
import { HttpError } from '../src/http.js';
import worker from '../src/index.js';

const league = { leagueId: '1', seasonYear: 2026, sport: 'football' };
const settings = { currentWeek: 4, scoringType: 'PPR', scoringItems: [{ statId: 53, points: 1 }] };
const dependencies = () => ({ listLeagues: async () => [league], fetchLeagueBundle: async () => ({ league: settings }) });

test('configuration preserves league scoring and correlates success without roster or secrets', async t => {
  const log = t.mock.method(console, 'log', () => {});
  const deps = dependencies();
  deps.listLeagues = async () => [league, { sport: 'baseball' }];
  const result = await buildPipelineConfig({}, deps);
  assert.deepEqual(result.leagues, [{ leagueId: '1', seasonYear: 2026, ...settings }]);
  assert.match(result.requestId, /^[a-f0-9-]{36}$/);
  assert.equal(log.mock.calls[0].arguments[0].requestId, result.requestId);
});

test('database failure has safe category, exact stage and a correlated request ID', async t => {
  const log = t.mock.method(console, 'error', () => {});
  const deps = dependencies();
  deps.listLeagues = async () => { throw new Error('D1_ERROR SQL with sensitive-fixture-token'); };
  let failure;
  await assert.rejects(buildPipelineConfig({}, deps), error => {
    failure = error;
    return error.status === 500 && error.details.category === 'storage' && error.details.stage === 'list-leagues';
  });
  assert.equal(log.mock.calls[0].arguments[0].requestId, failure.details.requestId);
  assert.equal(JSON.stringify(log.mock.calls).includes('sensitive-fixture-token'), false);
  assert.equal(JSON.stringify(failure).includes('sensitive-fixture-token'), false);
});

test('upstream authentication failures retain their actionable code but not raw exception content', async t => {
  t.mock.method(console, 'error', () => {});
  const deps = dependencies();
  deps.fetchLeagueBundle = async () => { throw new HttpError(502, 'ESPN_AUTH_EXPIRED', 'sensitive-fixture-cookie', { upstreamStatus: 401, secret: 'sensitive-fixture-cookie' }); };
  await assert.rejects(buildPipelineConfig({}, deps), error => {
    assert.equal(error.code, 'ESPN_AUTH_EXPIRED');
    assert.equal(error.details.upstreamStatus, 401);
    assert.equal(error.details.stage, 'league-bundle');
    assert.equal(JSON.stringify(error).includes('sensitive-fixture-cookie'), false);
    return true;
  });
});

test('credential decryption is distinguished from transient storage failures', async t => {
  t.mock.method(console, 'error', () => {});
  const deps = dependencies();
  deps.fetchLeagueBundle = async () => { throw new DOMException('sensitive-fixture-envelope', 'OperationError'); };
  await assert.rejects(buildPipelineConfig({}, deps), error => error.details.category === 'credentials');
});

test('stale fallback settings cannot silently build forecasts for the wrong week', async t => {
  t.mock.method(console, 'error', () => {});
  const deps = dependencies();
  deps.fetchLeagueBundle = async () => ({ league: settings, cache: { stale: true } });
  await assert.rejects(buildPipelineConfig({}, deps), error => error.code === 'PIPELINE_CONFIG_STALE' && error.status === 502);
  deps.fetchLeagueBundle = async () => ({ league: settings, cache: { stale: true, reason: 'ESPN_AUTH_EXPIRED' } });
  await assert.rejects(buildPipelineConfig({}, deps), error => error.code === 'ESPN_AUTH_EXPIRED');
});

test('malformed league settings fail closed before dataset generation', async t => {
  t.mock.method(console, 'error', () => {});
  for (const invalid of [{ ...settings, currentWeek: 0 }, { ...settings, currentWeek: 19 }, { ...settings, scoringItems: null }]) {
    const deps = dependencies();
    deps.fetchLeagueBundle = async () => ({ league: invalid });
    await assert.rejects(buildPipelineConfig({}, deps), error => error.status === 422 && error.details.stage === 'validate-config');
  }
});

test('real internal route retains authentication and returns safe correlated diagnostics', async t => {
  t.mock.method(console, 'error', () => {});
  const env = { DATA_INGEST_TOKEN: 'synthetic-ingest-fixture', DB: { prepare() { throw new Error('D1_ERROR: sensitive-fixture-SQL'); } } };
  const make = headers => worker.fetch(new Request('https://fixture.example/api/internal/pipeline/config', { headers }), env);
  assert.equal((await make({})).status, 401);
  const result = await make({ Authorization: `Bearer ${env.DATA_INGEST_TOKEN}` });
  assert.equal(result.status, 500);
  const body = await result.json();
  assert.equal(body.error.code, 'PIPELINE_CONFIG_FAILED');
  assert.equal(body.error.details.stage, 'list-leagues');
  assert.match(body.error.details.requestId, /^[a-f0-9-]{36}$/);
  assert.equal(JSON.stringify(body).includes('sensitive-fixture-SQL'), false);
});
