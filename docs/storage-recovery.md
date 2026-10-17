# Lossless decision archive recovery

Decision snapshots retain the exact model inputs needed for historical replay.
They previously repeated megabytes of plain JSON per visit. New writes use the
versioned `fcc:gzip:v1:` representation when smaller. SHA-256 hashes still cover
the original JSON, not the storage representation. Readers accept both formats,
verify hashes, and bound decompression to 8 MiB. No model calculations change.

New archive writes have a 128 MiB aggregate input-storage budget, checked atomically
with insertion. At the limit, current recommendations remain available with an
explicit archive warning; existing records are never evicted. This limits the
decision archive, not total D1 growth. Historical datasets and operational tables
still need capacity monitoring. No paid upgrade or historical deletion is automatic.

## Approved recovery sequence

1. Obtain approval to back up, deploy the compatible reader, and losslessly compact
   production storage. Preserve Worker secrets, token expiry, and ESPN state.
2. Export D1 remotely into a private ignored `.artifacts/` directory. Record its
   SHA-256; restore into a new local SQLite file and run `PRAGMA integrity_check`.
   Exports and Wrangler logs are private: they can contain credentials or signed URLs.
3. Run `node scripts/compact-decision-snapshots.mjs prepare backup.sqlite output-dir`.
   It verifies every original input hash and compression roundtrip, and produces
   owner-readable SQL files plus a manifest. It never connects to production.
4. Rehearse every SQL file on a copy of the restored database, twice to prove
   idempotency. Run the script's `verify before.sqlite after.sqlite` command.
   It checks every original snapshot, metadata, and immutable claim/ESPN observation.
5. Run CI and deploy the new reader through the reviewed GitHub production workflow.
   **Do not compact before that deployment succeeds.** An older Worker cannot read
   compressed inputs. Do not roll back to an incompatible reader afterward.
6. Apply the first (smallest) manifest SQL file remotely with Wrangler, verify one
   converted row and its input hash, then apply the remaining files in bounded batches.
   Every statement is below D1's 100 KB SQL limit. Larger values assemble in the
   dedicated `fcc_snapshot_compaction_v1` staging table, then replace the historical
   input in one statement. No partially compressed input is ever exposed to readers.
   Conditional updates preserve concurrently changed rows; interruption is resumable.
   The script only deletes its own temporary staging values, never history.
7. Export and restore a fresh post-repair backup. Run `verify` against the original.
   Check snapshot count, hashes, immutable receipts, database integrity and capacity.
   Physical allocated bytes may differ from logical bytes after SQLite page reuse.
8. Rerun the dataset-refresh workflow. Verify successful writes and a fresh signed-in
   roster, waivers and historical replay. A `SELECT 1` readiness probe alone does not
   establish that a full database is writable or that ESPN is current.

Keep the original backup. Never restore it wholesale over newer production activity
without a separate recovery decision. For interrupted staging, reapply that row's
SQL file; it resets only that temporary payload and preserves historical metadata.

## No-cost shared snapshots (October 17, 2026)

R2 is not enabled on this account. Do not activate billing or upgrade a plan for
this repair. `fcc:delta:v1:` stores changed JSON values relative to one existing
full snapshot in the same league and season. Full anchors stay immutable and
independently readable. There are no delta chains. Both the base hash and exact
reconstructed input hash are checked; the original decision ID, timestamp,
method, results and receipts do not change. New writes choose a same-day,
same-model full anchor, only using a delta when it saves at least 10%. The total
128 MiB archive limit still applies, counting anchors and deltas alike.

Use `scripts/deduplicate-decision-snapshots.mjs prepare before.sqlite output-dir`
on a verified backup. Today's full rows are never transformed because a concurrent
live writer may reference them after the export. Rehearse every manifest SQL file twice on a copy, then run
`verify before.sqlite after.sqlite`. Deploy the compatible reader BEFORE applying
any production SQL. Pilot one row, verify its original hash, then apply the
remaining manifest in bounded batches. The conditional updates are restartable;
resume the SAME manifest, never re-prepare an already converted archive. Export
and verify production afterward, including every original record and receipt.
The older gzip-only compactor must not be used on a delta archive. Never roll
back to a reader that cannot resolve deltas.

The October 17 offline rehearsal preserved all 611 snapshots and 93 immutable
history rows, reducing stored inputs from 133,871,960 to 61,663,132 bytes. This is
logical space available for reuse, not a promise that allocated database bytes
immediately shrink. Other tables can still grow; capacity monitoring and the
hard archive stop remain necessary. This is not unlimited storage.

Pipeline verification now distinguishes advice readiness from archive failure,
and prints bounded safe diagnostics before failing. Do not suppress a genuine
archive failure merely to silence notification emails.
