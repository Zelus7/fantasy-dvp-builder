# Waiver outcome review

The next assisted-transaction stage adds a post-processing audit, without enabling ESPN transaction writes or changing connector permissions.

- Waivers shows current roster observations and fresh FAAB separately from claim outcomes. Ownership alone never proves a winning claim, paid bid, pending status or reason for failure.
- Users can copy one processing date's full ESPN Offers Report table, preview it, then save it. The parser requires exactly one row per saved target for the selected team, exact bids, and matching successful conditional drops. Duplicate rows, partial reports, impossible dates, conflicting drops and mismatched claims are rejected.
- Reports are explicitly **user-supplied evidence**, not independently verified API receipts. Other listed bids are retrospective context, not calibrated odds or a guaranteed counterfactual winning price. Reported spend is not silently equated with a budget delta.
- Fresh roster/report discrepancies are surfaced for review. Stale or unavailable reads do not manufacture outcomes.
- Migration 0005 snapshots the existing worksheet and archives subsequent versions atomically with SQLite triggers. Reports are immutable per plan version; identical retries are idempotent. Team/league/season isolation and optimistic worksheet versions remain intact.
- “Start new worksheet” retains old plans and attached reports. It does not cancel ESPN claims. The audit view shows the latest 12 revisions; older revisions remain stored.
- Defense/kicker streaming recommendations only drop the same position, or use an open slot. A skill-position sacrifice requires a separate deliberate decision. Free agents no longer receive misleading FAAB auction ranges; ESPN's Add screen remains authoritative for acquisition cost.

## Verification

Unit and authenticated-route coverage includes parsing copied tab/newline tables, exact matching, dates, duplicates, partial reports, shared-drop fallback, unknown ownership, stale observations, discrepancies, team isolation, migration/backfill, immutable reports, retries, cross-origin rejection and no outbound ESPN writes. Mobile browser checks cover save, report preview, report persistence, history and overflow. The synthetic browser may fail to download ESPN logo images; these do not block the local workflow.

## Still gated

Read-only pending/processed ingestion is now covered by [the claim sync release](waiver-sync-release.md), including live verification and remaining coverage limits. Supervised direct submission is still gated. Never infer a receipt from an empty feed or a roster change. Exact-action consent, replay prevention, acceptance checks and independently verified receipts are required before enabling transaction execution.
