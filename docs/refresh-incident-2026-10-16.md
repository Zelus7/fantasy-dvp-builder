# Refresh failure investigation — October 16, 2026

Investigation only; no archive mutation, credential change, notification change,
storage-budget increase, or pipeline behavior change was made.

## Confirmed evidence

- GitHub notification emails identify `Refresh NFL intelligence data`, running
  from the scheduler on `main` (`6d6fe93`). That is intentional: the workflow
  checks out the app's release branch for pipeline code.
- Runs [38833115760](https://github.com/Zelus7/fantasy-dvp-builder/actions/runs/38833115760)
  and [38864042123](https://github.com/Zelus7/fantasy-dvp-builder/actions/runs/38864042123)
  both retrieved configuration successfully and published schedule, DvP and
  player features. The later run published 272 games, 384 DvP rows and 742
  player-feature rows. Source-week coverage remains separate from freshness.
- Both failed afterward with `Published data, but live decision readiness or
  snapshot verification failed`. This is not an authentication/configuration
  failure and not a complete failure to publish NFL data.
- A read-only D1 aggregate found 611 decision snapshots, 127.67 MiB of stored
  inputs, all already `fcc:gzip:v1`. Latest saved snapshot:
  `2026-10-13T23:48:21.229Z`. Database allocated size was 358,584,320 bytes.
- `DECISION_STORAGE_BUDGET` in `src/decision-store.js` is 128 MiB. The remaining
  headroom is too small for the attempted new snapshots. This is an application
  safety limit, not evidence that Cloudflare's entire database is full.
- A signed-in live refresh on October 16 displayed ESPN/schedule ready and:
  “The decision archive reached its storage safety limit.” Current advice can
  still run; new visits cannot contribute saved evidence for model evaluation.

## Why the email is unhelpfully vague

`src/index.js` returns only the `snapshotSaved` boolean from the verification
endpoint, omitting the available archive warning. `pipeline/build_datasets.py`
raises a combined error before printing the verification result. Thus the useful
distinction between publishing, advice readiness, and archival is lost in CI.
Do not make this green merely by ignoring `snapshotSaved`: historical evaluation
really is impaired.

## Proposed repair, requiring follow-up authorization

1. Back up and integrity-check D1. Preserve immutable receipts and every
   decision's reconstruction hash. Do not delete old history or raise the cap
   merely to silence notifications.
2. Rehearse a storage redesign offline: content-address shared immutable inputs
   across snapshots, with exact reconstruction, rather than storing the same
   full player/schedule context in each compressed record. Measure savings and
   replay equivalence before choosing or applying a migration. If savings are
   insufficient, present a separately approved archival-storage option.
3. Return safe structured verification diagnostics: publishing status, advice
   readiness/reasons, archive status and a machine-readable archive reason.
   Print the bounded diagnostics before raising; keep genuine failures visible.
4. Deploy a backward-compatible reader before any storage transformation, verify
   every original hash, then prove a new snapshot can be written and replayed.
5. Run the real scheduled publishing path once and verify its final status.
   UI refresh and `/api/readiness` alone do not prove archival has recovered.

Private diagnostic files are in ignored `.artifacts/`. No ESPN cookies or
Cloudflare secret values were read or committed.
