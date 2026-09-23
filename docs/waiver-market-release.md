# League-aware waiver review and saved claim plans

## Scope

- Opponent roster fit uses the current week's best legal lineup before/after adding a candidate. It respects game locks, reserve slots and unavailable designations. The gain is an upper bound before selecting a rival's drop, not a prediction of a bid. Questionable players remain playable.
- All teams retain their known FAAB balances and tiebreak ranks; missing values stay unknown.
- Recent winning bids are deduplicated, position-matched, dated and limited to the last 60 days. Sparse examples remain visible. No hidden pending offers, fabricated losing offers, or win probabilities are inferred.
- The old conservative formula is labeled a **value-policy range**, not a hard cap or predicted winning price. Users choose their own exact bid and aggregate spending limit.
- Discovery loads separate QB/RB/WR/TE/K/DST pools (up to 429 unique candidates), reducing the ownership-ranked all-position pool's coverage bias. This is still not the entire waiver universe. Failed/stale position feeds withhold actionable advice.

## Saved worksheet, not transaction automation

`/api/claim-plan` is session-authenticated. Writes reject cross-origin requests, force fresh ESPN reads, validate targets/drops/budget/game locks, and use optimistic D1 versions to avoid lost edits. The worksheet is scoped by league, season and selected team.

Shared-drop claims are mutually exclusive. Their maximum possible spend is the largest bid in that group, plus the maximum from each independent group. No-drop claims require enough currently open active-roster slots. The ordering displayed in the worksheet does not change ESPN's order.

Saving, removing or reordering worksheet rows never submits, edits, cancels or reorders an ESPN transaction. An optional pending checkbox is explicitly user-reported. Existing ESPN offers are not automatically imported. There is no transaction-write endpoint and the connector permissions remain unchanged.

This is the first safe stage of the assisted-transaction workflow. Remaining work before enabling direct submissions: a verified ESPN transaction adapter, exact-action approval bound to fresh inputs, replay/idempotency protection, reliable pending-offer reconciliation, and independently verified receipts. No background spending authority is implied.

## Verification / acceptance

- Unit and authenticated-route tests: budget bounds, duplicate adds, protected/missing drops, game locks, freshness, historical-week guard, exact conditional spend, team isolation, optimistic conflicts, changed availability, and no external transaction writes.
- Calculation revisions include the league-market module and preserve existing immutable archives.
- Browser fixture at 390 px: add recommendation, reject an insufficient limit, save a valid plan, inspect exact target/drop/bid and page width.
- Production checks should verify actual opponent budgets, pool coverage, source readiness, fresh login/refresh, and the new worksheet. Any unavailable bid-history feed stays visibly unavailable.

Live review identified and added regression coverage for explicit UTC cache timestamps, a fresh stored-schedule fallback when the direct scoreboard is blocked, and position-correct bench fallbacks (unused ESPN superflex eligibility must not suggest a quarterback as a receiver replacement).

The GitHub deployment workflow currently lacks four required deployment secrets and stops before changing production. The established local Cloudflare login can deploy while preserving the existing Worker secrets; no credential values were copied into GitHub or rotated for this release.

User checklist: open Waivers; expand a candidate's evidence and league competition; add it to the worksheet; review exact bid/drop/limit; save and reload; confirm this does not alter ESPN Pending Moves. Only confirm an actual ESPN transaction after checking its final summary.
