CREATE TABLE IF NOT EXISTS claim_plan_versions (
  league_id TEXT NOT NULL, season INTEGER NOT NULL, team_id TEXT NOT NULL,
  version INTEGER NOT NULL, plan_json TEXT NOT NULL, updated_at TEXT NOT NULL,
  PRIMARY KEY (league_id, season, team_id, version)
);
INSERT OR IGNORE INTO claim_plan_versions SELECT * FROM claim_plans;
CREATE TRIGGER IF NOT EXISTS archive_claim_insert AFTER INSERT ON claim_plans BEGIN
  INSERT INTO claim_plan_versions VALUES (NEW.league_id,NEW.season,NEW.team_id,NEW.version,NEW.plan_json,NEW.updated_at);
END;
CREATE TRIGGER IF NOT EXISTS archive_claim_update AFTER UPDATE ON claim_plans BEGIN
  INSERT INTO claim_plan_versions VALUES (NEW.league_id,NEW.season,NEW.team_id,NEW.version,NEW.plan_json,NEW.updated_at);
END;
CREATE TABLE IF NOT EXISTS claim_receipts (
  league_id TEXT NOT NULL, season INTEGER NOT NULL, team_id TEXT NOT NULL,
  version INTEGER NOT NULL, receipt_json TEXT NOT NULL, recorded_at TEXT NOT NULL,
  PRIMARY KEY (league_id, season, team_id, version)
);
