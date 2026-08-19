# Private v1 acceptance

The release is complete only when every applicable check passes.

## Connection and privacy

- [ ] App is reachable at a stable HTTPS URL.
- [ ] Private app access works and the session cookie is HttpOnly/Secure.
- [ ] Extension pairs with a single-use code.
- [ ] User logs into ESPN normally; no ESPN password enters the app.
- [ ] All ESPN football leagues are discovered.
- [ ] League `1269378` is initially selected as default.
- [ ] A paired browser can be revoked.
- [ ] ESPN cookies are absent from GitHub, browser local storage, responses, and logs.

## Data

- [ ] The ESPN scoring profile is visible to the pipeline.
- [ ] DvP includes QB/RB/WR/TE for season, last four, and last six.
- [ ] Rank 1 means most points allowed and is labeled unambiguously.
- [ ] DvP is per defense game and shows sample/confidence/prior weight.
- [ ] Player features map ESPN IDs to nflverse players.
- [ ] Full schedule supports current week, ROS, byes, and fantasy playoffs.
- [ ] Unsupported scoring rules are disclosed.
- [ ] A failed/empty upload cannot replace active data.

## Product workflows

- [ ] Dashboard loads current fantasy matchup and data warnings.
- [ ] Best Matchups ranks the user's actual roster against this week's opponents.
- [ ] A favorable matchup with weak usage produces a role caution.
- [ ] Lineup optimizer fills legal ESPN slots with unique players.
- [ ] Players in started games remain locked.
- [ ] Compare reports close decisions honestly.
- [ ] Waivers include only ESPN FREEAGENT/WAIVERS players.
- [ ] Trade analyzer can select players owned by other league teams.
- [ ] Trade output accounts for depth and ROS/playoff schedules.
- [ ] Floor/median/ceiling defaults are 50%/10%/40% and normalize to 100%.
- [ ] Adaptive mode shifts toward floor as favorite and ceiling as underdog.

## Reliability

- [ ] ESPN outage uses last-known-good league cache where available.
- [ ] Weather failure removes only weather adjustment.
- [ ] DvP refresh failure preserves previous active snapshot.
- [ ] `/api/health` and `/api/readiness` work.
- [ ] CI, Node tests, Python tests, migration test, and secret scan pass.
- [ ] D1 export and Worker rollback are documented and tested.

## Devices

- [ ] Desktop Chrome/Edge app flow passes.
- [ ] PWA installs and launches standalone on Android.
- [ ] Refresh and settings remain usable at narrow phone width.
