# Changelog

## 0.1.0-private — unreleased

- Replaced the brittle Cloudflare KV/Sleeper prototype with a full Cloudflare Worker + D1 PWA.
- Added private ESPN browser-session sync without storing an ESPN password.
- Added league discovery, default league selection, roster/scoring/matchup/free-agent reads, and stale-session recovery.
- Added league-scoring-aware season/last-four/last-six DvP from nflverse.
- Added early-season prior-year blending and source confidence.
- Added player production/opportunity features, schedule, weather, legal lineup optimization, comparisons, waivers, trades, ROS, and playoff schedule analysis.
- Added mobile PWA UI, CI, deployment, data refreshes, secret scanning, and operating documentation.
- Disabled the legacy hard-coded Week 11 Worker/KV workflow.
