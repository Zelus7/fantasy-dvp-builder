# NFL intelligence pipeline

GitHub Actions performs the NFL-wide calculations that do not belong in a Cloudflare request. It downloads nflverse data through `nflreadpy`, applies each connected ESPN league's scoring settings, and uploads three staged datasets:

- League-specific QB/RB/WR/TE DvP: season, last four, and last six games.
- Player performance and opportunity features.
- The full NFL schedule for current-week and rest-of-season joins.

The Worker validates every payload and activates it only after the staged D1 row count matches. A failed or empty refresh cannot replace the previous active dataset.

```bash
python -m pip install -r pipeline/requirements.txt
APP_BASE_URL=https://your-app.workers.dev DATA_INGEST_TOKEN=... \
  python pipeline/build_datasets.py
```

The pipeline receives scoring settings through a protected endpoint. It never receives ESPN cookies.
