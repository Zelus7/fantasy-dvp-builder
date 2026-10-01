import { listLeagues } from './db.js';
import { fetchLeagueBundle } from './espn.js';
import { HttpError } from './http.js';

// Deliberately emit categories, not raw exceptions, SQL, cookies or upstream bodies.
function failureCategory(error) {
  const message = `${error?.message || ''} ${error?.cause?.message || ''}`;
  if (/D1_|SQLITE_|database|sql error/i.test(message)) return 'storage';
  if (error?.name === 'OperationError' || /decrypt|encryption key/i.test(message)) return 'credentials';
  if (error instanceof HttpError && /ESPN_|UPSTREAM_/.test(error.code)) return 'espn';
  return 'application';
}

export async function buildPipelineConfig(env, dependencies = { listLeagues, fetchLeagueBundle }) {
  const requestId = crypto.randomUUID();
  const started = Date.now();
  let stage = 'list-leagues';
  try {
    const leagues = (await dependencies.listLeagues(env)).filter(league => league.sport === 'football');
    const output = [];
    for (const league of leagues) {
      stage = 'league-bundle';
      const bundle = await dependencies.fetchLeagueBundle(env, league);
      // A cached fallback must not silently select last week's forecast period.
      if (bundle.cache?.stale) {
        const code = bundle.cache.reason === 'ESPN_AUTH_EXPIRED' ? 'ESPN_AUTH_EXPIRED' : 'PIPELINE_CONFIG_STALE';
        throw new HttpError(502, code, 'Current ESPN league settings could not be refreshed');
      }
      stage = 'validate-config';
      const { currentWeek, scoringType, scoringItems } = bundle.league || {};
      if (!Number.isInteger(Number(currentWeek)) || Number(currentWeek) < 1 || Number(currentWeek) > 18 || !Array.isArray(scoringItems)) {
        throw new HttpError(422, 'PIPELINE_CONFIG_INVALID', 'League week or scoring settings are invalid');
      }
      output.push({ leagueId: league.leagueId, seasonYear: league.seasonYear, currentWeek, scoringType, scoringItems });
    }
    console.log({ event: 'pipeline_config', requestId, status: 200, leagueCount: output.length, elapsedMs: Date.now() - started });
    return { leagues: output, generatedAt: new Date().toISOString(), requestId };
  } catch (error) {
    const category = failureCategory(error);
    const status = error instanceof HttpError ? error.status : 500;
    const code = error instanceof HttpError ? error.code : 'PIPELINE_CONFIG_FAILED';
    const details = { requestId, stage, category };
    if (Number.isInteger(error?.details?.upstreamStatus)) details.upstreamStatus = error.details.upstreamStatus;
    console.error({ event: 'pipeline_config', ...details, status, code, elapsedMs: Date.now() - started });
    throw new HttpError(status, code, 'Pipeline configuration could not be loaded; see the correlated Worker log', details);
  }
}
