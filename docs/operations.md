# Operations

## Normal weekly cycle

- Every six hours: GitHub rebuilds statistics, DvP, player features and schedules,
  including newly published games and stat corrections. All datasets are
  validated before the first upload; incomplete feeds fail visibly.
- Tuesday and Sunday mornings: the Worker's separate cron refreshes ESPN cache.
- User refresh: bypass short ESPN/weather caches for the selected league.

The refresh workflow is registered on `main` and checks out
`release/private-web-v1` for pipeline code. Its schedule is 00:33, 06:33,
12:33 and 18:33 UTC (GitHub may delay scheduled jobs). Manual recovery uses
**Actions → Refresh NFL intelligence data → Run workflow → main**.
Leave season/week blank for the connected league's current settings. The source
coverage week is reported separately from the requested fantasy week; a fresh
download never implies that an unpublished NFL week is already available.

## Health checks

- `/api/health`: Worker process response.
- `/api/readiness`: D1 query.
- Settings → Data Health: ESPN connection, active DvP snapshot, player-feature count, schedule count, and unsupported scoring.

## Common failures

### ESPN expired

Symptom: `ESPN_AUTH_EXPIRED` or Settings shows `expired`.

Recovery: sign into ESPN in the paired desktop browser and click **Sync ESPN now**. Do not edit GitHub secrets.

Only `AUTH_REQUIRED` should send the website back to app login. An ESPN
authentication failure is an upstream error (`ESPN_AUTH_EXPIRED`, HTTP 502), not
an invalid app session or invalid connector pairing. A public scoreboard failure
must not mark private ESPN credentials expired. The dashboard keeps the roster
visible and labels missing schedule data explicitly.

When Fan account discovery is unavailable, the Worker can read the configured
league directly with the supplied ESPN cookies. `DEFAULT_TEAM_ID` is an explicit
roster selection within that league; it does not bypass ESPN access checks.

### DvP missing or stale

Run **Refresh NFL intelligence data** manually. Inspect the uploaded workflow artifact. The active D1 snapshot remains unchanged on failed validation.

This requires the workflow to be registered on the repository default branch and
`APP_BASE_URL` / `DATA_INGEST_TOKEN` configured in GitHub. Their presence must be
verified; a deployed Worker alone does not enable dataset refreshes. Both were
configured and a real publishing run passed on September 15, 2026. Neither is
your private app login code. Keep the ingestion token synchronized with the
Worker secret of the same name, and never put it in source files or logs.

Inspect the workflow's **Build and publish datasets** step and 14-day artifact
after a failure. GitHub's normal workflow-failure notification preferences apply;
the app also exposes source ages and withholds advice when critical inputs age
out. A successful manual dispatch proves the publishing path, not that GitHub
will execute every future timer without delay.

For a recovery build without an ingestion credential, a locally supplied config
in the pipeline-config API format can generate output without publishing:

```bash
.venv/bin/python pipeline/build_datasets.py --config-file .artifacts/pipeline-config.json --no-upload
```

Validate those outputs through the normal dataset validators before publishing.
Do not commit private config, generated datasets, or credential material.

### No current-season stats

Expected before or early in the regular season. Run with `through_week=1`; the pipeline uses prior-season baselines with explicit 100% prior weight until current data arrives.

### Worker deployment fails

Confirm Cloudflare account ID, API token scope, and D1 database ID. Do not delete the last working deployment. Fix on the feature branch, rerun CI, then redeploy.

## Backup

Before schema changes or major releases:

```bash
npx wrangler d1 export fantasy-command-center --remote --output backup.sql
```

Retain the export outside the repository. ESPN cookies inside the export remain encrypted but should still be treated as sensitive.

## Rollback

1. Redeploy the last successful Git tag or commit.
2. Avoid destructive D1 migrations.
3. If a new dataset is bad, mark its snapshot `failed` and reactivate the previous snapshot, or rerun the prior successful artifact.
4. Keep the `workers.dev` deployment available while testing a custom domain.

## Release checklist

- CI green.
- Secret scan green.
- D1 backup taken.
- Live ESPN sync succeeds.
- Initial data pipeline succeeds.
- Dashboard, legal lineup, current-week matchups, waivers, trades, and settings tested on desktop and Android PWA.
- No credentials visible in logs or browser storage.
