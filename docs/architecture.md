# Architecture

## Runtime

Fantasy Command Center is a single Cloudflare Worker deployment that serves both the PWA assets and `/api/*` routes. D1 stores the private user's settings, encrypted ESPN session, connected leagues, revocable extension devices, short-lived caches, analysis history, and versioned NFL datasets.

The runtime is deliberately lightweight. It does not download or aggregate the NFL during a user request. GitHub Actions performs heavy nflverse processing and sends compact JSON payloads to protected ingestion endpoints.

## Data flow

```text
ESPN login in Chrome/Edge
        ↓
Private extension reads SWID + espn_s2
        ↓ HTTPS + revocable device token
Worker encrypts cookies with AES-GCM
        ↓
ESPN private league API
        ↓
Roster, scoring, matchup, standings, free agents

nflverse → GitHub Action → league scoring → staged D1 snapshot
                                      ↓
PWA ← deterministic analysis ← DvP + player features + schedule + weather
```

## Primary modules

- `src/index.js`: HTTP routing and application orchestration.
- `src/espn.js`: league discovery, league response normalization, free agents, and NFL scoreboard fallback.
- `src/analysis.js`: outcomes, matchup adjustments, lineup optimizer, waivers, trades, and schedule outlooks.
- `src/db.js`: encrypted persistence, cache, and atomic dataset activation.
- `src/security.js`: HMAC sessions, AES-GCM envelopes, pairing/device tokens.
- `src/weather.js`: kickoff-window Open-Meteo forecasts.
- `pipeline/build_datasets.py`: nflverse scoring and feature construction.

## Failure model

- ESPN failures return the last cached league response when one exists.
- Dataset uploads are staged, counted, and activated only after validation.
- A failed refresh leaves the previous active DvP/player/schedule snapshot untouched.
- Missing weather removes only the weather adjustment.
- Unsupported scoring rules are disclosed in data health and reduce confidence.
- The app never invents missing statistics.

## Seasonal rollover

League discovery stores league and season together. The NFL pipeline reads season and current week from the connected ESPN league, while explicit workflow inputs permit controlled backfills. Prior-season weighting tapers from 100% with no current games to 0% after six current defensive/player games.

## Future AI integration

A future narrator can consume the deterministic result envelope and current cited reporting. It must not replace scoring, lineup legality, DvP calculation, availability, or numeric outcomes. The core app remains usable without an AI provider.
