# Legacy prototype

The root `build_dvp_all.py` and its historical commits document the original Sleeper-per-player / Cloudflare KV prototype. They are retained for audit history only.

Production private v1 uses:

- `pipeline/build_datasets.py` for bulk nflverse data.
- Protected staged uploads into D1.
- `src/` for ESPN reads and analysis.
- `.github/workflows/refresh-data.yml` for refreshes.

Do not restore the old scheduled `--through 11` workflow or add ESPN cookies to GitHub secrets.
