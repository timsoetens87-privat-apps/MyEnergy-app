import contextlib
import importlib
import io
import os
import sqlite3
import tempfile
import time
import unittest


class LiveSolarWindowTests(unittest.TestCase):
    def setUp(self):
        os.environ['P1_ENABLE_BACKGROUND_THREADS'] = 'false'
        os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

        capture = io.StringIO()
        with contextlib.redirect_stdout(capture), contextlib.redirect_stderr(capture):
            self.app_module = importlib.import_module('app')

        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()

        conn = sqlite3.connect(self.temp_db.name)
        conn.execute(
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
        conn.execute(
            '''
            CREATE TABLE solar_realtime (
                timestamp TEXT,
                power_w REAL
            )
            '''
        )

        now = time.time()
        raw_rows = [
            (now - 120, 610.0, 1, 'test', now - 120),
            (now - 60, 655.0, 1, 'test', now - 60),
        ]
        conn.executemany('INSERT INTO solar_raw_data (timestamp, power_w, unit_id, source, created_at) VALUES (?, ?, ?, ?, ?)', raw_rows)
        sample_rows = [
            (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now - 120)), 610.0),
            (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now - 60)), 655.0),
        ]
        conn.executemany('INSERT INTO solar_realtime (timestamp, power_w) VALUES (?, ?)', sample_rows)
        conn.commit()
        conn.close()

        self.original_get_connection = self.app_module.get_solar_history_connection
        self.original_runtime_series = list(self.app_module._solar_runtime_series)
        self.original_cache = dict(self.app_module._solar_latest_cache)
        self.original_runtime_state = dict(self.app_module._solar_runtime_state)

        def fake_connection():
            conn = sqlite3.connect(self.temp_db.name)
            conn.row_factory = sqlite3.Row
            return conn

        self.app_module.get_solar_history_connection = fake_connection
        self.app_module._solar_runtime_series = []
        self.app_module._solar_runtime_state = {'timestamp': None, 'power_w': None}
        self.app_module._solar_latest_cache = {'expires': 0.0, 'data': {'timestamp': None, 'power_w': None}}

    def tearDown(self):
        self.app_module.get_solar_history_connection = self.original_get_connection
        self.app_module._solar_runtime_series = self.original_runtime_series
        self.app_module._solar_runtime_state = self.original_runtime_state
        self.app_module._solar_latest_cache = self.original_cache
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)

    def test_live_window_keeps_recent_points_within_requested_window(self):
        points = self.app_module.get_live_solar_points(time.time() - 300)

        self.assertGreaterEqual(len(points), 2)
        self.assertEqual([610.0, 655.0], [round(point['power'], 1) for point in points[-2:]])

    def test_live_window_merges_runtime_and_db_solar_points(self):
        now = time.time()
        self.app_module._solar_runtime_series = [
            {'timestamp': now - 5, 'power': 700.0},
        ]

        points = self.app_module.get_live_solar_points(now - 300)
        powers = [round(point['power'], 1) for point in points if point.get('power') is not None]

        self.assertIn(610.0, powers)
        self.assertIn(655.0, powers)
        self.assertIn(700.0, powers)
        self.assertLessEqual(len(points), 4)

    def test_live_window_reads_raw_solar_history(self):
        conn = sqlite3.connect(self.temp_db.name)
        conn.execute('DELETE FROM solar_realtime')
        conn.commit()
        conn.close()

        points = self.app_module.get_live_solar_points(time.time() - 300)
        powers = [round(point['power'], 1) for point in points if point.get('power') is not None]

        self.assertIn(610.0, powers)
        self.assertIn(655.0, powers)


class LivePowerWindowTests(unittest.TestCase):
    def setUp(self):
        os.environ['P1_ENABLE_BACKGROUND_THREADS'] = 'false'
        os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

        capture = io.StringIO()
        with contextlib.redirect_stdout(capture), contextlib.redirect_stderr(capture):
            self.app_module = importlib.import_module('app')

        self.temp_raw_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_raw_db.close()
        self.temp_battery_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_battery_db.close()
        self.temp_live_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_live_db.close()
        os.unlink(self.temp_live_db.name)

        conn = sqlite3.connect(self.temp_raw_db.name)
        conn.execute(
            '''
            CREATE TABLE energy_data (
                timestamp REAL PRIMARY KEY,
                power REAL,
                import_kwh REAL,
                export_kwh REAL
            )
            '''
        )

        now = time.time()
        self.db_rows = [
            (now - 240, 420.0, None, None),
            (now - 120, 510.0, None, None),
            (now - 12, 615.0, None, None),
        ]
        conn.executemany('INSERT INTO energy_data (timestamp, power, import_kwh, export_kwh) VALUES (?, ?, ?, ?)', self.db_rows)
        conn.commit()
        conn.close()

        battery_conn = sqlite3.connect(self.temp_battery_db.name)
        battery_conn.execute(
            '''
            CREATE TABLE battery_raw_data (
                timestamp REAL PRIMARY KEY,
                consumption_w REAL,
                power_w REAL,
                soc_pct REAL,
                capacity_kwh REAL,
                source TEXT,
                created_at REAL
            )
            '''
        )
        battery_conn.execute(
            '''
            CREATE TABLE battery_realtime (
                timestamp TEXT PRIMARY KEY,
                consumption_w REAL,
                power_w REAL,
                soc_pct REAL,
                capacity_kwh REAL,
                status TEXT,
                source TEXT,
                created_at REAL
            )
            '''
        )
        battery_conn.executemany(
            'INSERT INTO battery_raw_data (timestamp, consumption_w, power_w, soc_pct, capacity_kwh, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [
                (now - 20, 180.0, 180.0, 52.0, 5.1, 'test', now - 20),
                (now - 10, 220.0, 220.0, 52.0, 5.1, 'test', now - 10),
            ],
        )
        battery_conn.commit()
        battery_conn.close()

        self.original_db_file_raw = self.app_module.DB_FILE_RAW
        self.original_db_file_backup = self.app_module.DB_FILE_BACKUP
        self.original_db_file_live_temp = getattr(self.app_module, 'DB_FILE_LIVE_TEMP', None)
        self.original_runtime_series = list(self.app_module._p1_runtime_series)
        self.original_runtime_state = dict(self.app_module._p1_runtime_state)
        self.original_get_live_solar_points = self.app_module.get_live_solar_points
        self.original_open_battery_history_connection = self.app_module.open_battery_history_connection
        self.original_get_marstek_status = self.app_module._get_marstek_status
        self.original_marstek_runtime_state = dict(self.app_module._marstek_runtime_state)
        self.original_marstek_runtime_series = list(self.app_module._marstek_runtime_series)

        self.app_module.DB_FILE_RAW = self.temp_raw_db.name
        self.app_module.DB_FILE_BACKUP = self.temp_raw_db.name
        self.app_module.DB_FILE_LIVE_TEMP = self.temp_live_db.name
        self.app_module._p1_runtime_series = [{
            'timestamp': now - 2,
            'power': 700.0,
        }]
        self.app_module._p1_runtime_state = {
            'timestamp': now - 2,
            'live_power': 700.0,
            'raw_reading': None,
        }
        self.app_module.get_live_solar_points = lambda *args, **kwargs: []
        self.app_module.open_battery_history_connection = lambda kind='raw', write=False: sqlite3.connect(self.temp_battery_db.name)
        self.app_module._get_marstek_status = lambda force_refresh=False: {
            'status': 'error',
            'battery_consumption_w': None,
            'power_w': None,
        }
        self.app_module._marstek_runtime_state = {'timestamp': None, 'power_w': None, 'consumption_w': None}
        self.app_module._marstek_runtime_series = []
        self.client = self.app_module.app.test_client()

    def tearDown(self):
        self.app_module.DB_FILE_RAW = self.original_db_file_raw
        self.app_module.DB_FILE_BACKUP = self.original_db_file_backup
        if self.original_db_file_live_temp is not None:
            self.app_module.DB_FILE_LIVE_TEMP = self.original_db_file_live_temp
        self.app_module._p1_runtime_series = self.original_runtime_series
        self.app_module._p1_runtime_state = self.original_runtime_state
        self.app_module.get_live_solar_points = self.original_get_live_solar_points
        self.app_module.open_battery_history_connection = self.original_open_battery_history_connection
        self.app_module._get_marstek_status = self.original_get_marstek_status
        self.app_module._marstek_runtime_state = self.original_marstek_runtime_state
        self.app_module._marstek_runtime_series = self.original_marstek_runtime_series
        for path in (self.temp_raw_db.name, self.temp_battery_db.name, self.temp_live_db.name):
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass

    def test_live_power_window_returns_recent_raw_points_and_runtime_tail(self):
        response = self.client.get('/live_power_window?seconds=300')
        payload = response.get_json()

        self.assertEqual(200, response.status_code)
        self.assertIn('points', payload)
        self.assertNotIn('synced_points', payload)
        self.assertGreater(len(payload['points']), 50)
        powers = [round(point['power'], 1) for point in payload['points'] if point.get('power') is not None]
        self.assertIn(420.0, powers)
        self.assertIn(510.0, powers)
        self.assertIn(615.0, powers)
        self.assertIn(700.0, powers)

        first_ts = float(payload['points'][0]['timestamp'])
        last_ts = float(payload['points'][-1]['timestamp'])
        self.assertLessEqual(last_ts - first_ts, 305)
        self.assertGreaterEqual(last_ts - first_ts, 250)

    def test_live_power_window_creates_dedicated_temp_db(self):
        response = self.client.get('/live_power_window?seconds=300')

        self.assertEqual(200, response.status_code)
        self.assertTrue(os.path.exists(self.temp_live_db.name))

        conn = sqlite3.connect(self.temp_live_db.name)
        try:
            row = conn.execute('SELECT COUNT(*) FROM live_power_samples').fetchone()
        finally:
            conn.close()

        self.assertIsNotNone(row)
        self.assertGreater(row[0], 0)

    def test_live_power_window_returns_solar_trailing_history(self):
        now = time.time()
        self.app_module.get_live_solar_points = lambda *args, **kwargs: [
            {'timestamp': now - 18, 'power': 300.0},
            {'timestamp': now - 8, 'power': 320.0},
            {'timestamp': now - 2, 'power': 340.0},
        ]

        response = self.client.get('/live_power_window?seconds=300')
        payload = response.get_json()

        self.assertEqual(200, response.status_code)
        self.assertIn('solar_points', payload)
        solar_powers = [round(point['power'], 1) for point in payload['solar_points'] if point.get('power') is not None]
        self.assertIn(300.0, solar_powers)
        self.assertIn(320.0, solar_powers)
        self.assertIn(340.0, solar_powers)
        self.assertGreater(len(payload['solar_points']), 3)

    def test_live_power_window_returns_battery_history_without_runtime_cache(self):
        response = self.client.get('/live_power_window?seconds=300')
        payload = response.get_json()

        self.assertEqual(200, response.status_code)
        self.assertIn('battery_points', payload)
        battery_powers = [round(point['power'], 1) for point in payload['battery_points'] if point.get('power') is not None]
        self.assertIn(180.0, battery_powers)
        self.assertIn(220.0, battery_powers)
        self.assertGreater(len(payload['battery_points']), 3)


if __name__ == '__main__':
    unittest.main()
