PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS espn_credentials (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  encrypted_credentials TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'connected',
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_verified_at TEXT,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS leagues (
  league_id TEXT NOT NULL,
  season_year INTEGER NOT NULL,
  sport TEXT NOT NULL DEFAULT 'football',
  league_name TEXT NOT NULL,
  team_id TEXT NOT NULL,
  team_name TEXT NOT NULL,
  is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1)),
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (league_id, season_year)
);
CREATE INDEX IF NOT EXISTS idx_leagues_default ON leagues(is_default, season_year DESC);

CREATE TABLE IF NOT EXISTS pairing_codes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code_hash TEXT NOT NULL UNIQUE,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  consumed_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pairing_codes_expiry ON pairing_codes(expires_at, consumed_at);

CREATE TABLE IF NOT EXISTS devices (
  id TEXT PRIMARY KEY,
  token_hash TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TEXT,
  revoked_at TEXT
);

CREATE TABLE IF NOT EXISTS api_cache (
  cache_key TEXT PRIMARY KEY,
  payload_json TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_api_cache_expiry ON api_cache(expires_at);

CREATE TABLE IF NOT EXISTS data_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset_type TEXT NOT NULL,
  dataset_id TEXT NOT NULL UNIQUE,
  scope_key TEXT NOT NULL,
  season INTEGER NOT NULL,
  through_week INTEGER NOT NULL,
  generated_at TEXT NOT NULL,
  source TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'staging' CHECK (status IN ('staging', 'active', 'superseded', 'failed'))
);
CREATE INDEX IF NOT EXISTS idx_data_snapshots_scope_status
  ON data_snapshots(dataset_type, scope_key, status, generated_at DESC);

CREATE TABLE IF NOT EXISTS dvp_stats (
  dataset_id TEXT NOT NULL,
  league_id TEXT NOT NULL,
  position TEXT NOT NULL,
  defense_team TEXT NOT NULL,
  window TEXT NOT NULL,
  season INTEGER NOT NULL,
  through_week INTEGER NOT NULL,
  games INTEGER NOT NULL,
  current_games INTEGER NOT NULL DEFAULT 0,
  prior_games INTEGER NOT NULL DEFAULT 0,
  prior_season INTEGER,
  prior_weight REAL NOT NULL DEFAULT 0,
  points_allowed REAL NOT NULL,
  points_allowed_per_game REAL NOT NULL,
  rank INTEGER NOT NULL,
  percentile REAL NOT NULL,
  league_average_delta REAL NOT NULL,
  trend TEXT NOT NULL,
  grade TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  generated_at TEXT NOT NULL,
  PRIMARY KEY (dataset_id, position, defense_team, window),
  FOREIGN KEY (dataset_id) REFERENCES data_snapshots(dataset_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_dvp_lookup
  ON dvp_stats(league_id, position, defense_team, window, dataset_id);

CREATE TABLE IF NOT EXISTS player_features (
  dataset_id TEXT NOT NULL,
  league_id TEXT NOT NULL,
  espn_id TEXT NOT NULL,
  gsis_id TEXT,
  player_name TEXT NOT NULL,
  position TEXT NOT NULL,
  team TEXT,
  season INTEGER NOT NULL,
  games INTEGER NOT NULL,
  current_games INTEGER NOT NULL DEFAULT 0,
  prior_games INTEGER NOT NULL DEFAULT 0,
  prior_season INTEGER,
  prior_weight REAL NOT NULL DEFAULT 0,
  season_ppg REAL NOT NULL,
  last3_ppg REAL NOT NULL,
  last5_ppg REAL NOT NULL,
  standard_deviation REAL NOT NULL,
  targets_per_game REAL NOT NULL,
  carries_per_game REAL NOT NULL,
  touches_per_game REAL NOT NULL,
  target_share REAL,
  generated_at TEXT NOT NULL,
  PRIMARY KEY (dataset_id, espn_id),
  FOREIGN KEY (dataset_id) REFERENCES data_snapshots(dataset_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_player_features_position
  ON player_features(league_id, position, season_ppg DESC, dataset_id);
CREATE INDEX IF NOT EXISTS idx_player_features_espn
  ON player_features(league_id, espn_id, dataset_id);
CREATE INDEX IF NOT EXISTS idx_player_features_gsis ON player_features(gsis_id);

CREATE TABLE IF NOT EXISTS nfl_schedule (
  dataset_id TEXT NOT NULL,
  season INTEGER NOT NULL,
  week INTEGER NOT NULL CHECK (week BETWEEN 1 AND 22),
  event_id TEXT NOT NULL,
  kickoff TEXT,
  home_team TEXT NOT NULL,
  away_team TEXT NOT NULL,
  venue TEXT,
  indoor INTEGER NOT NULL DEFAULT 0 CHECK (indoor IN (0, 1)),
  status TEXT,
  generated_at TEXT NOT NULL,
  PRIMARY KEY (dataset_id, event_id),
  FOREIGN KEY (dataset_id) REFERENCES data_snapshots(dataset_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_nfl_schedule_lookup
  ON nfl_schedule(season, week, dataset_id);

CREATE TABLE IF NOT EXISTS rate_limits (
  rate_key TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  request_count INTEGER NOT NULL,
  PRIMARY KEY (rate_key, window_start)
);

CREATE TABLE IF NOT EXISTS analysis_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  analysis_type TEXT NOT NULL,
  league_id TEXT NOT NULL,
  season_year INTEGER NOT NULL,
  week INTEGER,
  input_json TEXT,
  output_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
