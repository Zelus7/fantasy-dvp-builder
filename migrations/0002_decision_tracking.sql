ALTER TABLE player_features ADD COLUMN opportunity_json TEXT;
CREATE TABLE IF NOT EXISTS decision_snapshots (
  id TEXT PRIMARY KEY,
  league_id TEXT NOT NULL,
  season INTEGER NOT NULL,
  week INTEGER NOT NULL,
  mode TEXT NOT NULL,
  method TEXT NOT NULL,
  created_at TEXT NOT NULL,
  inputs_json TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  result_json TEXT,
  outcome_json TEXT,
  UNIQUE (league_id, season, week, mode, input_hash)
);
CREATE INDEX IF NOT EXISTS decisions_league ON decision_snapshots(league_id,season,created_at DESC);
