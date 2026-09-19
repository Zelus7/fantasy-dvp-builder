-- Additive: old snapshots and feature datasets remain readable.
ALTER TABLE player_features ADD COLUMN forecast_json TEXT;
