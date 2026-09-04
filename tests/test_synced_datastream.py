import contextlib
import importlib
import io
import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timezone


class SyncedDatastreamTests(unittest.TestCase):
    def setUp(self):
        os.environ['P1_ENABLE_BACKGROUND_THREADS'] = 'false'
        os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

        capture = io.StringIO()
        with contextlib.redirect_stdout(capture), contextlib.redirect_stderr(capture):
            self.app_module = importlib.import_module('app')

        self.temp_solar_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_solar_db.close()
        self.temp_avg_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_avg_db.close()

        solar_conn = sqlite3.connect(self.temp_solar_db.name)
        solar_conn.execute(
            '''
            CREATE TABLE solar_raw_data (
                timestamp REAL PRIMARY KEY,
                power_w REAL,
                unit_id INTEGER,
                source TEXT,
                created_at REAL
            )
            '''
        )
        solar_conn.execute(
            '''
            CREATE TABLE solar_realtime (
                timestamp TEXT PRIMARY KEY,
                power_w REAL
            )
            '''
        )
        solar_conn.execute(
            '''
            CREATE TABLE five_minute_averages (
                bucket_start TEXT PRIMARY KEY,
                avg_power_w REAL,
                max_power_w REAL,
                sample_count INTEGER,
                energy_kwh REAL,
                updated_at REAL
            )
            '''
        )
        solar_conn.executemany(
            'INSERT INTO solar_realtime (timestamp, power_w) VALUES (?, ?)',
            [
                ('2026-04-18 08:01:05', 800.0),
                ('2026-04-18 08:04:20', 1000.0),
            ],
        )
        solar_conn.commit()
        solar_conn.close()

        avg_conn = sqlite3.connect(self.temp_avg_db.name)
        avg_conn.execute(
            '''
            CREATE TABLE energy_data_5min (
                timestamp REAL PRIMARY KEY,
                power_avg REAL,
                power_min REAL,
                power_max REAL
            )
            '''
        )
        avg_conn.executemany(
            'INSERT INTO energy_data_5min VALUES (?, ?, ?, ?)',
            [
                (datetime(2026, 4, 18, 10, 0, 0).timestamp(), 250.0, 200.0, 300.0),
                (datetime(2026, 4, 18, 10, 5, 0).timestamp(), -100.0, -150.0, -50.0),
            ],
        )
        avg_conn.commit()
        avg_conn.close()

        self.original_get_solar_history_connection = self.app_module.get_solar_history_connection
        self.original_db_file_avg = self.app_module.DB_FILE_AVG
        self.original_runtime_state = dict(self.app_module._solar_runtime_state)
        self.original_runtime_series = list(self.app_module._solar_runtime_series)
        self.original_latest_cache = dict(self.app_module._solar_latest_cache)

        def fake_solar_connection(write=False):
            conn = sqlite3.connect(self.temp_solar_db.name)
            conn.row_factory = sqlite3.Row
            return conn

        self.app_module.get_solar_history_connection = fake_solar_connection
        self.app_module.DB_FILE_AVG = self.temp_avg_db.name
        self.client = self.app_module.app.test_client()

    def tearDown(self):
        self.app_module.get_solar_history_connection = self.original_get_solar_history_connection
        self.app_module.DB_FILE_AVG = self.original_db_file_avg
        self.app_module._solar_runtime_state = self.original_runtime_state
        self.app_module._solar_runtime_series = self.original_runtime_series
        self.app_module._solar_latest_cache = self.original_latest_cache
        for path in (self.temp_solar_db.name, self.temp_avg_db.name):
            if os.path.exists(path):
                os.unlink(path)

    def test_parse_solar_sample_timestamp_treats_naive_strings_as_utc(self):
        parsed_ts = self.app_module._parse_solar_sample_timestamp('2026-04-19 10:25:33')
        expected_ts = datetime(2026, 4, 19, 10, 25, 33, tzinfo=timezone.utc).timestamp()

        self.assertEqual(expected_ts, parsed_ts)

    def test_solar_realtime_fallback_is_bucket_aligned(self):
        series = self.app_module.get_solar_5min_series(datetime(2026, 4, 18))

        self.assertEqual(1, len(series))
        self.assertEqual(int(datetime(2026, 4, 18, 10, 0, 0).timestamp() * 1000), int(series[0]['timestamp']))
        self.assertAlmostEqual(900.0, float(series[0]['avg_power_w']), places=2)

    def test_rebuild_solar_rollups_populates_five_minute_table(self):
        rebuilt = self.app_module.rebuild_solar_rollups_from_history(
            start_ts=datetime(2026, 4, 18, 10, 0, 0).timestamp(),
            end_ts=datetime(2026, 4, 18, 10, 5, 0).timestamp(),
        )

        self.assertEqual(1, rebuilt)

        conn = sqlite3.connect(self.temp_solar_db.name)
        row = conn.execute(
            'SELECT bucket_start, avg_power_w, sample_count FROM five_minute_averages ORDER BY bucket_start LIMIT 1'
        ).fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual('2026-04-18 08:00:00', row[0])
        self.assertAlmostEqual(900.0, float(row[1]), places=2)
        self.assertEqual(2, int(row[2]))

    def test_persist_solar_sample_handles_legacy_realtime_required_columns(self):
        conn = sqlite3.connect(self.temp_solar_db.name)
        cur = conn.cursor()
        cur.execute('DROP TABLE solar_realtime')
        cur.execute(
            '''
            CREATE TABLE solar_realtime (
                timestamp TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                status TEXT NOT NULL,
                power_w REAL NOT NULL,
                pv_v REAL NOT NULL,
                pv_a REAL NOT NULL,
                grid_v REAL NOT NULL,
                grid_a REAL NOT NULL,
                temp_c REAL NOT NULL,
                today_kwh REAL NOT NULL,
                created_at TEXT NOT NULL,
                unit_id INTEGER,
                source TEXT
            )
            '''
        )
        conn.commit()
        conn.close()

        persisted = self.app_module._persist_solar_sample_to_history({
            'timestamp': '2026-04-18 08:02:00',
            'current_power_w': 920.0,
            'unit_id': 1,
            'source': 'live-modbus',
            'status': 'Running',
        })

        self.assertTrue(persisted)

        conn = sqlite3.connect(self.temp_solar_db.name)
        row = conn.execute(
            'SELECT model, status, power_w FROM solar_realtime ORDER BY timestamp LIMIT 1'
        ).fetchone()
        conn.close()

        self.assertEqual('sun2000', row[0])
        self.assertEqual('Running', row[1])
        self.assertAlmostEqual(920.0, float(row[2]), places=2)

    def test_persist_solar_sample_writes_raw_solar_history(self):
        persisted = self.app_module._persist_solar_sample_to_history({
            'timestamp': '2026-04-18 08:02:00',
            'current_power_w': 920.0,
            'unit_id': 1,
            'source': 'live-modbus',
            'status': 'Running',
        })

        self.assertTrue(persisted)

        conn = sqlite3.connect(self.temp_solar_db.name)
        row = conn.execute(
            'SELECT power_w FROM solar_raw_data WHERE ABS(timestamp - ?) < 1',
            (datetime(2026, 4, 18, 8, 2, 0, tzinfo=timezone.utc).timestamp(),)
        ).fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertAlmostEqual(920.0, float(row[0]), places=2)

    def test_ensure_solar_history_schema_migrates_legacy_five_minute_table(self):
        conn = sqlite3.connect(':memory:')
        cur = conn.cursor()
        cur.execute(
            '''
            CREATE TABLE five_minute_averages (
                bucket_start TEXT PRIMARY KEY,
                avg_power_w REAL,
                max_power_w REAL
            )
            '''
        )

        self.app_module.ensure_solar_history_schema(cur)
        cur.execute("PRAGMA table_info(five_minute_averages)")
        columns = {row[1] for row in cur.fetchall()}
        conn.close()

        self.assertIn('sample_count', columns)
        self.assertIn('energy_kwh', columns)
        self.assertIn('updated_at', columns)

    def test_rebuild_solar_rollups_handles_legacy_required_columns(self):
        conn = sqlite3.connect(self.temp_solar_db.name)
        cur = conn.cursor()
        cur.execute('DROP TABLE five_minute_averages')
        cur.execute(
            '''
            CREATE TABLE five_minute_averages (
                bucket_start TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                status TEXT NOT NULL,
                avg_pv_v REAL NOT NULL,
                avg_pv_a REAL NOT NULL,
                avg_power_w REAL NOT NULL,
                max_power_w REAL NOT NULL,
                avg_today_kwh REAL NOT NULL,
                max_today_kwh REAL NOT NULL,
                avg_grid_v REAL NOT NULL,
                avg_grid_a REAL NOT NULL,
                avg_temp_c REAL NOT NULL,
                state INTEGER NOT NULL,
                alarm INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                energy_kwh REAL,
                updated_at REAL
            )
            '''
        )
        cur.execute(
            '''
            CREATE TABLE daily_totals (
                date TEXT PRIMARY KEY,
                total_energy_kwh REAL NOT NULL,
                avg_power_w REAL NOT NULL,
                peak_power_w REAL NOT NULL,
                buckets INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            '''
        )
        cur.execute(
            '''
            CREATE TABLE monthly_totals (
                month TEXT PRIMARY KEY,
                total_energy_kwh REAL NOT NULL,
                avg_power_w REAL NOT NULL,
                peak_power_w REAL NOT NULL,
                days INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            '''
        )
        cur.execute(
            '''
            CREATE TABLE yearly_totals (
                year TEXT PRIMARY KEY,
                total_energy_kwh REAL NOT NULL,
                avg_power_w REAL NOT NULL,
                peak_power_w REAL NOT NULL,
                months INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            '''
        )
        conn.commit()
        conn.close()

        rebuilt = self.app_module.rebuild_solar_rollups_from_history(
            start_ts=datetime(2026, 4, 18, 10, 0, 0).timestamp(),
            end_ts=datetime(2026, 4, 18, 10, 5, 0).timestamp(),
        )

        self.assertEqual(1, rebuilt)

    def test_minute_data_returns_synced_combined_series(self):
        response = self.client.get('/minute_data?date=2026-04-18')
        payload = response.get_json()

        self.assertEqual(200, response.status_code)
        self.assertIn('synced_5min', payload)
        synced = [item for item in payload['synced_5min'] if item.get('grid_power_w') is not None or item.get('solar_power_w') is not None]

        self.assertGreaterEqual(len(synced), 2)
        first = synced[0]
        self.assertEqual(int(datetime(2026, 4, 18, 10, 0, 0).timestamp() * 1000), int(first['timestamp']))
        self.assertAlmostEqual(250.0, float(first['grid_power_w']), places=2)
        self.assertAlmostEqual(900.0, float(first['solar_power_w']), places=2)
        self.assertAlmostEqual(1150.0, float(first['home_load_w']), places=2)

    def test_today_minute_data_includes_fresh_runtime_solar_bucket(self):
        current_bucket_start = int(time.time() // 300) * 300
        now_ts = max(current_bucket_start, time.time() - 1)
        now = datetime.fromtimestamp(now_ts)
        self.app_module._solar_runtime_state = {
            'timestamp': now_ts,
            'power_w': 777.0,
        }
        self.app_module._solar_runtime_series = [{
            'timestamp': now_ts,
            'power': 777.0,
        }]
        self.app_module._solar_latest_cache = {
            'expires': time.time() + 60,
            'data': {'timestamp': now_ts, 'power_w': 777.0},
        }

        response = self.client.get(f"/minute_data?date={now.strftime('%Y-%m-%d')}")
        payload = response.get_json()

        self.assertEqual(200, response.status_code)
        synced = [item for item in payload['synced_5min'] if item.get('solar_power_w') is not None]
        self.assertTrue(any(abs(float(item['solar_power_w']) - 777.0) < 0.1 for item in synced))


if __name__ == '__main__':
    unittest.main()
