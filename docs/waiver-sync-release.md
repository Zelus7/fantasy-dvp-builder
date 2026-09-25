# Read-only ESPN claim sync

This release automatically reads pending claims on Dashboard/Waivers load and
processed offers for the current and previous two scoring periods. Results use
the existing encrypted ESPN connection; no new permissions or connector install
are required. No ESPN transaction writes are enabled.

## Evidence and limits

- `mPendingTransactions` is separate from `mTransactions2`. Only the selected
  team's explicitly pending waiver records are retained. An absent list is
  unavailable, never an invented empty list. Failed refreshes preserve prior
  pending records with their original timestamp and a stale label.
- Processed reads include `scoringPeriodId` and the `WAIVER`/`WAIVER_ERROR` type
  filter. **Do not add `transactions.limit`: live ESPN returned HTTP 400 with
  that filter.** Local code bounds response bytes and records instead.
- `processDate`, not `executionDate`, timestamps completed offers. Exact league,
  season, week, IDs, whole-dollar bid, item shape and terminal status are checked.
  Ambiguous `PENDING`, canceled, malformed and unknown records are not receipts.
  Partial coverage is displayed even when identifiable results are confirmed.
- Requests are fixed-origin HTTPS GETs with bounded time/body size. A redirect
  may retain credentials only on the same ESPN origin and scoped league path.
  No raw upstream payloads or session cookies are logged or stored in the audit.
- Migration 0006 appends normalized terminal evidence. Repeated reads are
  idempotent. Changed terminal records are retained as conflicts and excluded
  from totals and market context. Existing pasted reports remain independent
  and unchanged; they are not silently promoted to direct API evidence.
- A saved-plan match requires one exact target/drop/bid/week match processed
  after the plan was saved. A shared-drop fallback is not a new roster hole
  when the primary claim won. Other follow-ups require a fresh roster check.
- Displayed spend is the observed total for this scoring period, not inferred
  from remaining budget. Archived history is bounded to the latest 1,500 IDs for
  analysis; the database retains older observations. The own-team UI shows 40.
- Position-matched winning prices and separately labeled failed offers inform
  bid context. Failed claims are not all price losses. Neither prices nor rival
  roster needs establish a calibrated success rate or minimum winning price.

## Live verification, September 25, 2026

Against ESPN's September 23 Offers Report for the configured league/team:
Denzel Boston $31 won, Devaughn Vele $16 won, Romeo Doubs $7 skipped and
Panthers D/ST $1 unavailable matched the app's direct API records. Observed
winning spend was $47; the separately refreshed remaining FAAB was $203.
The pending endpoint explicitly returned no pending claims. A nonempty pending
state is covered by fixtures, not a newly submitted live test claim.

The processed history also returned five unclassified week-three records;
they were excluded, not guessed. All four saved-plan outcomes were classified.
The public health, D1 readiness and unauthenticated private-boundary checks
passed. Deployment used existing local Wrangler OAuth and preserved all four
Worker secrets. GitHub deployment remains separately gated on its Cloudflare
token setup; a local deployment is not proof of a successful GitHub deployment.

Primary implementation references checked before authoring the adapter:
[request parameters](https://github.com/cwendt94/espn-api/blob/master/espn_api/requests/espn_requests.py)
and [offer fields](https://github.com/cwendt94/espn-api/blob/master/espn_api/base_offer.py).
They are an independently maintained client, not an official ESPN API contract;
live comparison and fail-closed validation remain required.

## Still gated

Submission, edits, cancellation, reordering and lineup changes remain on ESPN.
Any future app execution requires exact-action approval, replay protection,
acceptance checks and independently verified receipts. Do not submit a test
claim merely to exercise this read-only adapter.
