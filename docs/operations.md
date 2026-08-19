# Operations

## Normal weekly cycle

- Tuesday: rebuild prior-week statistics, DvP, player features, and schedules.
- Sunday morning: rebuild the latest available inputs and refresh ESPN league cache.
- User refresh: bypass short ESPN/weather caches for the selected league.

Scheduled GitHub workflows only execute from the default branch. Until private v1 is merged, run refresh manually from the feature/release branch.

## Health checks

- `/api/health`: Worker process response.
- `/api/readiness`: D1 query.
- Settings → Data Health: ESPN connection, active DvP snapshot, player-feature count, schedule count, and unsupported scoring.

## Common failures

### ESPN expired

Symptom: `ESPN_AUTH_EXPIRED` or Settings shows `expired`.

Recovery: sign into ESPN in the paired desktop browser and click **Sync ESPN now**. Do not edit GitHub secrets.

### DvP missing or stale

Run **Refresh NFL intelligence data** manually. Inspect the uploaded workflow artifact. The active D1 snapshot remains unchanged on failed validation.

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
