# CI and deployment failure analysis — August 19, 2026

## Summary

The notification burst around 7:25–7:28 p.m. ET came from CI running against intermediate commits while the repository was still being assembled file by file. The workflow was configured to run on every `feat/**` push. At those commit points, referenced tests and support files had not yet been committed.

Observed intermediate failures included:

- `scripts/secret_scan.py` not yet present when `npm run ci` invoked it.
- `pipeline/tests` not yet importable when Python test discovery ran.

These were transient construction-state failures, not regressions in the completed application. The completed feature branch later passed the full application and pipeline CI jobs.

The production deployment failure was separate and real:

- Cloudflare account and D1 GitHub secrets were empty.
- The migration command passed the database name where this deployment should consistently use the configured D1 binding (`DB`).

## Remediation

- CI now runs on pull requests plus pushes to `release/private-web-v1` and `main`, not every feature-branch commit.
- CI concurrency cancels superseded runs for the same pull request or ref.
- Official GitHub actions were moved to Node 24-compatible major versions.
- Deployment is manual until account-side production secrets are configured.
- Deployment validates all required secrets before installing dependencies or contacting Cloudflare.
- D1 migrations use the `DB` binding.
- Runtime Worker secrets are uploaded alongside the code via Wrangler's secrets-file deployment support.
- Required runtime secret names are declared in `wrangler.jsonc`.
- Scheduled data refreshes skip cleanly rather than fail when deployment secrets are not configured.
