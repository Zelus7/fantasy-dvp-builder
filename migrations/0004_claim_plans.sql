CREATE TABLE IF NOT EXISTS claim_plans (
  league_id TEXT NOT NULL,
  season INTEGER NOT NULL,
  team_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  plan_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (league_id, season, team_id)
);
