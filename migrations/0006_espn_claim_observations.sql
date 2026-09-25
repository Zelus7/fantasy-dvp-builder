-- Append-only ESPN evidence. Changed terminal records are retained as conflicts,
-- not overwritten or silently counted as extra wins.
CREATE TABLE IF NOT EXISTS espn_claim_observations (
  league_id TEXT NOT NULL,
  season INTEGER NOT NULL,
  transaction_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  outcome_json TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (league_id,season,transaction_id,fingerprint)
);
CREATE INDEX IF NOT EXISTS espn_claim_observations_date ON espn_claim_observations(league_id,season,processed_at DESC);
CREATE TRIGGER IF NOT EXISTS espn_claim_observations_no_update BEFORE UPDATE ON espn_claim_observations BEGIN SELECT RAISE(ABORT,'ESPN observations are immutable'); END;
CREATE TRIGGER IF NOT EXISTS espn_claim_observations_no_delete BEFORE DELETE ON espn_claim_observations BEGIN SELECT RAISE(ABORT,'ESPN observations are immutable'); END;
