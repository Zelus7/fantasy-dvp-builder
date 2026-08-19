# Fantasy Command Center

A private, mobile-first ESPN fantasy football decision app. It runs independently of ChatGPT and combines your actual league settings and roster with league-scoring-aware Defense vs. Position, player production, opportunity, injuries, weather, future schedule, and configurable floor/median/ceiling preferences.

## What it does

- Discovers every ESPN fantasy football league connected to your account.
- Selects league `1269378` by default, while supporting additional leagues and seasons.
- Shows which players on your real roster have the best and toughest current-week matchups based on how many fantasy points the opponent allows to that position under your league's scoring.
- Builds a legal recommended lineup using ESPN slot eligibility.
- Compares floor, median, and ceiling instead of collapsing every decision into one unexplained number.
- Ranks only players actually available in your league for waivers.
- Evaluates standard-redraft trades against roster depth and rest-of-season/playoff schedules.
- Installs as a Progressive Web App on Android, iPhone, and desktop.

DvP is a headline input, not the whole answer. A backup receiver with two targets per game does not become a must-start just because the opposing secondary has been charitable.

## Architecture

```text
Browser / installed PWA
        │
        ▼
Cloudflare Worker + static assets
        │
        ├── ESPN private fantasy reads
        ├── deterministic analysis engine
        ├── Open-Meteo weather
        └── D1 encrypted sessions, settings, cache, and datasets
                 ▲
                 │ staged + validated uploads
        GitHub Actions + nflreadpy / nflverse

Private Chrome/Edge extension ──► secure ESPN cookie sync
```

The app never stores your ESPN password. The private extension reads only `SWID` and `espn_s2`, syncs them over HTTPS, and automatically resyncs when the browser observes cookie rotation.

## Local development

Requirements:

- Node.js 22+
- Python 3.12+
- A Cloudflare account with Workers and D1

```bash
npm install
npx wrangler d1 create fantasy-command-center
# Replace the placeholder database_id in wrangler.jsonc
npm run db:migrate:local
cp .env.example .dev.vars
npm run dev
```

Run all checks:

```bash
npm run ci
python -m pip install -r pipeline/requirements.txt
python -m unittest discover -s pipeline/tests -v
```

## Private ESPN setup

1. Deploy the app and sign in with the private app access code.
2. Open **Settings → Generate pairing code**.
3. In Chrome or Edge, open `chrome://extensions`, enable Developer mode, and load the `extension/` directory as an unpacked extension.
4. Log into ESPN normally in the same browser.
5. Enter the app URL and pairing code in the extension, then click **Pair browser**.
6. The extension verifies the ESPN session, discovers all leagues, and syncs future cookie changes automatically where browser APIs permit.

No ESPN password, DevTools cookie copying, or GitHub ESPN secrets are used.

## Required Cloudflare secrets

Set directly with Wrangler or through the Cloudflare dashboard:

```bash
wrangler secret put APP_ACCESS_CODE
wrangler secret put SESSION_SECRET
wrangler secret put CREDENTIAL_ENCRYPTION_KEY
wrangler secret put DATA_INGEST_TOKEN
```

Generate a 32-byte encryption key:

```bash
python -c "import base64,secrets; print(base64.b64encode(secrets.token_bytes(32)).decode())"
```

Never paste secrets into an issue, pull request, chat, or committed file.

## GitHub Actions secrets

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_D1_DATABASE_ID`
- `APP_BASE_URL`
- `DATA_INGEST_TOKEN`

The Cloudflare token should be narrowly scoped to this Worker and D1 database.

## Data refresh

`.github/workflows/refresh-data.yml` runs after each NFL week and again Sunday morning. It fetches nflverse data in bulk, applies every connected league's scoring profile, and uploads:

- Season, last-four, and last-six DvP.
- Player performance and opportunity features.
- Full NFL schedules for current-week, rest-of-season, and playoff analysis.

Every upload is staged and validated. Empty or partial data cannot replace the last-known-good snapshot.

## Decision weights

Default custom profile:

- Floor: 50%
- Median: 10%
- Ceiling: 40%

Adaptive mode shifts toward floor when projected ahead and toward ceiling when projected behind. The three values are normalized to 100% when saved.

## Read-only boundary

Private v1 does not set ESPN lineups, submit waiver claims, or accept trades. It provides recommendations only.

## Documentation

- `docs/architecture.md`
- `docs/data-methodology.md`
- `docs/cloudflare-setup.md`
- `docs/security.md`
- `docs/operations.md`
- `docs/acceptance.md`

The old `build_dvp_all.py` remains for history, but its Worker/KV workflow is disabled and is not part of the production path.
