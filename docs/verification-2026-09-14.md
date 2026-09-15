# Connector and app recovery verification

Verified against the existing production Worker in desktop Chrome on September
14, 2026 (US Eastern), using the installed and previously paired connector.
No password or ESPN session cookies were displayed or committed.

## Verified live

- Clicked **Sync ESPN now**: completed at 8:57:44 PM, one league, no popup error.
- Clicked the connector's **Open app**: dashboard stayed authenticated.
- Clicked **Refresh** and performed full browser reloads: no login redirect loop.
- Real selected team: ICE The Browns, league 1269378, team 9, season 2026.
- Dashboard and lineup use the real synced 15-player roster and correct positions.
- Matchup rankings use imported NFL schedule and league-scoring DvP data.
- Compare and Waivers load; the league-wide trade pool loads.
- Trade Analyze returns a computed result after batching feature lookups.
- Settings shows ESPN connected, one connected browser, 663 player profiles,
  272 scheduled games, and week-1 DvP (384 rows across all windows).

## Regression checks

- 41 Node tests, including full sync-to-dashboard flow with mocked ESPN transport,
  public-scoreboard failure, separate app versus ESPN authentication errors,
  player-position mapping, and D1-safe league-wide feature queries.
- 11 Python pipeline/schema tests.
- Worker dry-run, JavaScript syntax checks, secret scan, and diff whitespace check.
- Public NFL datasets built successfully and passed the app's dataset validators
  before import; existing snapshot activation rules were preserved.

## Limits of this verification

- A physical Android device / installed Android PWA was not controlled or tested.
- No ESPN lineup, waiver, or trade transactions were submitted.
- Custom scoring support is incomplete in the free weekly statistics feed;
  the dashboard intentionally retains that warning.
- Initial datasets were loaded manually. The repository's recurring refresh
  workflow was not registered on its default branch, and its required GitHub
  secrets were absent. Automatic dataset updates are not claimed operational.
