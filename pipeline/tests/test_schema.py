import sqlite3
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]

class SchemaTests(unittest.TestCase):
 def test_migration_applies_to_sqlite(self):
  connection=sqlite3.connect(':memory:')
  connection.executescript((ROOT/'migrations/0001_initial.sql').read_text())
  tables={row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
  for table in {'app_settings','espn_credentials','leagues','devices','dvp_stats','player_features','nfl_schedule','analysis_history'}:
   self.assertIn(table,tables)

 def test_active_snapshot_lookup_index_exists(self):
  sql=(ROOT/'migrations/0001_initial.sql').read_text()
  self.assertIn('idx_data_snapshots_scope_status',sql)
  self.assertIn("status IN ('staging', 'active', 'superseded', 'failed')",sql)

if __name__=='__main__': unittest.main()
