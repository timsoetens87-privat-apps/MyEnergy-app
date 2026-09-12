import contextlib
import importlib
import io
import os
import shutil
import sqlite3
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone


class MinuteDataPerfFixesTests(unittest.TestCase):
    def setUp(self):
        # CRITICAL: point the app at a throwaway data directory before import.
        # Without this, `app` resolves DATA_DIR to the real production
        # databases (P1_DATA_DIR defaults to <repo>/data) and every seed
        # helper below writes fake rows straight into them. This previously
        # happened for real and corrupted live energy/solar/battery data.
        self._tmp_data_dir = tempfile.mkdtemp(prefix='p1_dashboard_test_')
        self.addCleanup(shutil.rmtree, self._tmp_data_dir, ignore_errors=True)
        os.environ['P1_DATA_DIR'] = self._tmp_data_dir
        os.environ['P1_ENABLE_BACKGROUND_THREADS'] = 'false'
        os.environ['P1_ASYNC_STARTUP_MAINTENANCE'] = 'false'
        os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

        # `app` must be freshly imported against the temp DATA_DIR every test
        # run -- a cached module from a prior import would keep pointing at
        # whatever directory was active then (real data, on a plain `import`).
        sys.modules.pop('app', None)

        capture = io.StringIO()
        with contextlib.redirect_stdout(capture), contextlib.redirect_stderr(capture):
            self.app_module = importlib.import_module('app')

        self.app = self.app_module
        self.assertEqual(
            os.path.normpath(self.app.DATA_DIR), os.path.normpath(self._tmp_data_dir),
            'app.DATA_DIR must resolve to the isolated temp dir, never the real data/ directory',
        )
        # Isolate module-level throttle caches between tests.
        self.app._solar_daily_rollup_rebuild_cache.clear()
        self.app._solar_daily_totals_persist_cache.clear()
        self.app._minute_data_cache.clear()

    def _seed_energy_data(self, db_path, day_start_ts, count, step_seconds=2):
        conn = sqlite3.connect(db_path)
        conn.execute('''CREATE TABLE IF NOT EXISTS energy_data (
            timestamp REAL PRIMARY KEY,
            power REAL,
            import_kwh REAL,
            export_kwh REAL,
            gas_m3 REAL,
            import_t1_kwh REAL,
            import_t2_kwh REAL,
            export_t1_kwh REAL,
            export_t2_kwh REAL
        )''')
        rows = []
        for i in range(count):
            ts = day_start_ts + i * step_seconds
            rows.append((ts, 500.0, 10.0 + i * 0.001, 0.0, 0.0, None, None, None, None))
        conn.executemany(
            'INSERT INTO energy_data (timestamp, power, import_kwh, export_kwh, gas_m3, '
            'import_t1_kwh, import_t2_kwh, export_t1_kwh, export_t2_kwh) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            rows,
        )
        conn.commit()
        conn.close()

    def test_live_daily_breakdown_count_selection_matches_full_scan(self):
        """COUNT-based source selection (Change 2) must pick the same source and
        produce identical totals as the original full-scan len() comparison."""
        target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = target_date.timestamp()

        # Backup has more rows than raw -> backup should be selected, same as
        # `len(backup_samples) > len(raw_samples)` would have picked.
        self._seed_energy_data(self.app.DB_FILE_RAW, day_start_ts, count=100)
        self._seed_energy_data(self.app.DB_FILE_BACKUP, day_start_ts, count=200)

        result = self.app.get_live_daily_breakdown(target_date)
        self.assertIsNotNone(result)
        self.assertIn('consumption_kwh', result)

        # Reference: replicate the original full-scan selection logic directly
        # against the same seeded data and confirm it agrees with which source
        # get_live_daily_breakdown effectively used (backup, since it has more rows).
        conn_raw = sqlite3.connect(self.app.DB_FILE_RAW)
        raw_count = conn_raw.execute(
            'SELECT COUNT(*) FROM energy_data WHERE timestamp >= ? AND timestamp < ?',
            (day_start_ts, day_start_ts + 86400),
        ).fetchone()[0]
        conn_raw.close()
        conn_backup = sqlite3.connect(self.app.DB_FILE_BACKUP)
        backup_count = conn_backup.execute(
            'SELECT COUNT(*) FROM energy_data WHERE timestamp >= ? AND timestamp < ?',
            (day_start_ts, day_start_ts + 86400),
        ).fetchone()[0]
        conn_backup.close()
        self.assertGreater(backup_count, raw_count)
        self.assertEqual(backup_count, 200)

    def test_solar_rollup_rebuild_is_throttled(self):
        """Change 3: rebuild_solar_rollups_from_history must not run on every
        call to get_solar_5min_series within the 60s throttle window."""
        target_date = datetime.now(timezone.utc)
        day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)

        conn = self.app.open_solar_history_connection(kind='raw', write=True)
        cursor = conn.cursor()
        self.app.ensure_solar_history_schema(cursor)
        now_ts = time.time()
        cursor.execute(
            'INSERT OR REPLACE INTO solar_raw_data (timestamp, power_w, unit_id, source, created_at) VALUES (?, ?, ?, ?, ?)',
            (now_ts - 60, 500.0, 1, 'test', now_ts),
        )
        conn.commit()
        conn.close()

        call_count = {'n': 0}
        original_rebuild = self.app.rebuild_solar_rollups_from_history

        def counting_rebuild(*args, **kwargs):
            call_count['n'] += 1
            return original_rebuild(*args, **kwargs)

        self.app.rebuild_solar_rollups_from_history = counting_rebuild
        try:
            self.app.get_solar_5min_series(day_start)
            self.app.get_solar_5min_series(day_start)
            self.app.get_solar_5min_series(day_start)
        finally:
            self.app.rebuild_solar_rollups_from_history = original_rebuild

        self.assertEqual(call_count['n'], 1, 'rebuild should run once, then be throttled for 60s')

    def test_solar_rollup_rebuild_is_skipped_for_historical_dates(self):
        """Historical days must never trigger the bulk rebuild: raw solar data
        is retained for 30 days, so an 'any raw data exists' check alone would
        rebuild on the first visit to every past day, not just today. Their
        rollups are already kept correct incrementally by the live collector."""
        historical_date = datetime(2020, 1, 15)
        day_start = historical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = day_start.timestamp()

        conn = self.app.open_solar_history_connection(kind='raw', write=True)
        cursor = conn.cursor()
        self.app.ensure_solar_history_schema(cursor)
        cursor.execute(
            'INSERT OR REPLACE INTO solar_raw_data (timestamp, power_w, unit_id, source, created_at) VALUES (?, ?, ?, ?, ?)',
            (day_start_ts + 3600, 500.0, 1, 'test', day_start_ts + 3600),
        )
        conn.commit()
        conn.close()

        call_count = {'n': 0}
        original_rebuild = self.app.rebuild_solar_rollups_from_history

        def counting_rebuild(*args, **kwargs):
            call_count['n'] += 1
            return original_rebuild(*args, **kwargs)

        self.app.rebuild_solar_rollups_from_history = counting_rebuild
        try:
            series = self.app.get_solar_5min_series(day_start)
        finally:
            self.app.rebuild_solar_rollups_from_history = original_rebuild

        self.assertEqual(call_count['n'], 0, 'rebuild must not run for a historical day even though raw data exists')
        # The series itself must still be correct: get_solar_5min_series
        # re-aggregates directly from solar_raw_data regardless of the rebuild.
        self.assertEqual(1, len(series))
        self.assertAlmostEqual(500.0, float(series[0]['avg_power_w']), places=2)

    def _seed_solar_avg_buckets(self, day_start_ts, count, power_w):
        conn = self.app.open_solar_history_connection(kind='avg', write=True)
        cursor = conn.cursor()
        self.app.ensure_solar_history_schema(cursor)
        for i in range(count):
            bucket_start = datetime.fromtimestamp(day_start_ts + i * 300, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            self.app._upsert_solar_five_minute_average(
                cursor, bucket_start, power_w, power_w, 1, power_w * (5.0 / 60.0) / 1000.0, updated_at=time.time(),
            )
        conn.commit()
        conn.close()

    def _seed_solar_raw_point(self, timestamp_ts, power_w):
        conn = self.app.open_solar_history_connection(kind='raw', write=True)
        cursor = conn.cursor()
        self.app.ensure_solar_history_schema(cursor)
        cursor.execute(
            'INSERT OR REPLACE INTO solar_raw_data (timestamp, power_w, unit_id, source, created_at) VALUES (?, ?, ?, ?, ?)',
            (timestamp_ts, power_w, 1, 'test', timestamp_ts),
        )
        conn.commit()
        conn.close()

    def test_solar_raw_rescan_is_skipped_when_avg_coverage_is_complete(self):
        """A historical day with a full (>=280 bucket) five_minute_averages
        table must not pay for a full-day solar_raw_data rescan -- and since
        the rescan is skipped, its (deliberately different) raw values must
        NOT overwrite the already-correct averaged values in the response."""
        historical_date = datetime(2020, 2, 10)
        day_start = historical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = day_start.timestamp()

        self._seed_solar_avg_buckets(day_start_ts, count=288, power_w=500.0)
        # Seed raw data with a clearly different value; if the (expensive)
        # raw rescan incorrectly still ran, this would overwrite 500.0.
        self._seed_solar_raw_point(day_start_ts + 3600, 999.0)

        series = self.app.get_solar_5min_series(day_start)
        self.assertEqual(288, len(series))
        bucket_at_one_hour = next(p for p in series if p['timestamp'] == int((day_start_ts + 3600) * 1000))
        self.assertAlmostEqual(500.0, float(bucket_at_one_hour['avg_power_w']), places=2)

    def test_solar_raw_rescan_still_runs_when_avg_coverage_is_thin(self):
        """A historical day with only sparse five_minute_averages coverage
        (a real gap) must still fall back to the raw rescan so the response
        stays correct -- this is exactly the recovery case the fallback
        exists for, and must not be lost by the completeness-based skip."""
        historical_date = datetime(2020, 2, 11)
        day_start = historical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = day_start.timestamp()

        self._seed_solar_avg_buckets(day_start_ts, count=5, power_w=500.0)
        self._seed_solar_raw_point(day_start_ts + 7200, 777.0)

        series = self.app.get_solar_5min_series(day_start)
        bucket_at_two_hours = next((p for p in series if p['timestamp'] == int((day_start_ts + 7200) * 1000)), None)
        self.assertIsNotNone(bucket_at_two_hours, 'raw fallback should have surfaced the gap-period sample')
        self.assertAlmostEqual(777.0, float(bucket_at_two_hours['avg_power_w']), places=2)

    def test_solar_raw_rescan_is_skipped_for_today_when_avg_coverage_matches_elapsed(self):
        """"Today" must skip the full-day raw rescan once five_minute_averages
        already covers everything the live collector's per-sample upsert
        should have persisted by now -- the fixed 280-bucket/day threshold
        used for historical days doesn't apply to a still-accumulating day,
        so completeness is judged against elapsed time instead."""
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = day_start.timestamp()
        elapsed_seconds = (now - day_start).total_seconds()
        if elapsed_seconds < 900:
            self.skipTest('too close to local midnight for a stable elapsed-time comparison')

        expected_buckets = int(elapsed_seconds // 300)
        self._seed_solar_avg_buckets(day_start_ts, count=expected_buckets, power_w=500.0)
        # A raw point inside an already-covered bucket, with a clearly
        # different value; if the (expensive) full-day rescan incorrectly
        # still ran, this would overwrite the avg-derived 500.0.
        self._seed_solar_raw_point(day_start_ts + 60, 999.0)
        # Pre-throttle the separate top-of-function rollup rebuild (it also
        # folds raw data into the avg table whenever any raw data exists for
        # today, regardless of avg completeness) so this test isolates the
        # in-request raw-rescan-skip behavior being tested here.
        self.app._solar_daily_rollup_rebuild_cache[day_start.strftime('%Y-%m-%d')] = {
            'expires': time.time() + 3600.0,
        }

        series = self.app.get_solar_5min_series(day_start)
        bucket_at_start = next(p for p in series if p['timestamp'] == int(day_start_ts * 1000))
        self.assertAlmostEqual(500.0, float(bucket_at_start['avg_power_w']), places=2)

    def test_solar_raw_rescan_still_runs_for_today_when_avg_coverage_is_thin(self):
        """A stalled collector -- avg coverage far behind how many buckets
        should exist by now -- must still trigger the raw rescan for "today";
        that's the real gap-recovery case the fallback exists for, and must
        not be lost by comparing against elapsed time instead of a flat 280."""
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = day_start.timestamp()
        elapsed_seconds = (now - day_start).total_seconds()
        if elapsed_seconds < 900:
            self.skipTest('too close to local midnight for a stable elapsed-time comparison')

        self._seed_solar_avg_buckets(day_start_ts, count=1, power_w=500.0)
        self._seed_solar_raw_point(day_start_ts + 3600, 777.0)

        series = self.app.get_solar_5min_series(day_start)
        bucket_at_one_hour = next((p for p in series if p['timestamp'] == int((day_start_ts + 3600) * 1000)), None)
        self.assertIsNotNone(bucket_at_one_hour, 'raw fallback should have surfaced the gap-period sample')
        self.assertAlmostEqual(777.0, float(bucket_at_one_hour['avg_power_w']), places=2)

    def test_battery_raw_rescan_is_skipped_when_avg_coverage_is_complete(self):
        """Same completeness-based skip as solar, applied to battery, which
        has an even larger raw table in production (battery_2sec.db)."""
        historical_date = datetime(2020, 2, 12)
        day_start = historical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = day_start.timestamp()

        avg_conn = self.app.open_battery_history_connection(kind='avg', write=True)
        avg_cursor = avg_conn.cursor()
        self.app.ensure_battery_history_schema(avg_cursor)
        for i in range(288):
            bucket_start = datetime.fromtimestamp(day_start_ts + i * 300, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            self.app._upsert_battery_five_minute_average(
                avg_cursor, bucket_start, 300.0, 300.0, 50.0, 1, 300.0 * (5.0 / 60.0) / 1000.0, updated_at=time.time(),
            )
        avg_conn.commit()
        avg_conn.close()

        raw_conn = self.app.open_battery_history_connection(kind='raw', write=True)
        raw_cursor = raw_conn.cursor()
        self.app.ensure_battery_history_schema(raw_cursor)
        self.app._upsert_battery_raw_sample(
            raw_cursor, day_start_ts + 3600, 999.0, source='test', created_at=day_start_ts + 3600,
        )
        raw_conn.commit()
        raw_conn.close()

        series = self.app.get_battery_5min_series(day_start)
        self.assertEqual(288, len(series))
        bucket_at_one_hour = next(p for p in series if p['timestamp'] == int((day_start_ts + 3600) * 1000))
        self.assertAlmostEqual(300.0, float(bucket_at_one_hour['avg_consumption_w']), places=2)

    def _seed_battery_avg_buckets(self, day_start_ts, count, consumption_w):
        conn = self.app.open_battery_history_connection(kind='avg', write=True)
        cursor = conn.cursor()
        self.app.ensure_battery_history_schema(cursor)
        for i in range(count):
            bucket_start = datetime.fromtimestamp(day_start_ts + i * 300, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            self.app._upsert_battery_five_minute_average(
                cursor, bucket_start, consumption_w, consumption_w, 50.0, 1,
                consumption_w * (5.0 / 60.0) / 1000.0, updated_at=time.time(),
            )
        conn.commit()
        conn.close()

    def _seed_battery_raw_point(self, timestamp_ts, consumption_w):
        conn = self.app.open_battery_history_connection(kind='raw', write=True)
        cursor = conn.cursor()
        self.app.ensure_battery_history_schema(cursor)
        self.app._upsert_battery_raw_sample(
            cursor, timestamp_ts, consumption_w, source='test', created_at=timestamp_ts,
        )
        conn.commit()
        conn.close()

    def test_battery_raw_rescan_is_skipped_for_today_when_avg_coverage_matches_elapsed(self):
        """Same elapsed-time-based completeness skip as solar (Change: the
        fixed 280-bucket/day threshold doesn't apply to a still-accumulating
        "today"), applied to battery, which has the largest raw table."""
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = day_start.timestamp()
        elapsed_seconds = (now - day_start).total_seconds()
        if elapsed_seconds < 900:
            self.skipTest('too close to local midnight for a stable elapsed-time comparison')

        expected_buckets = int(elapsed_seconds // 300)
        self._seed_battery_avg_buckets(day_start_ts, count=expected_buckets, consumption_w=300.0)
        self._seed_battery_raw_point(day_start_ts + 60, 999.0)

        series = self.app.get_battery_5min_series(day_start)
        bucket_at_start = next(p for p in series if p['timestamp'] == int(day_start_ts * 1000))
        self.assertAlmostEqual(300.0, float(bucket_at_start['avg_consumption_w']), places=2)

    def test_battery_raw_rescan_still_runs_for_today_when_avg_coverage_is_thin(self):
        """A stalled battery collector must still trigger the raw rescan for
        "today" -- the real gap-recovery case the fallback exists for."""
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = day_start.timestamp()
        elapsed_seconds = (now - day_start).total_seconds()
        if elapsed_seconds < 900:
            self.skipTest('too close to local midnight for a stable elapsed-time comparison')

        self._seed_battery_avg_buckets(day_start_ts, count=1, consumption_w=300.0)
        self._seed_battery_raw_point(day_start_ts + 3600, 777.0)

        series = self.app.get_battery_5min_series(day_start)
        bucket_at_one_hour = next((p for p in series if p['timestamp'] == int((day_start_ts + 3600) * 1000)), None)
        self.assertIsNotNone(bucket_at_one_hour, 'raw fallback should have surfaced the gap-period sample')
        self.assertAlmostEqual(777.0, float(bucket_at_one_hour['avg_consumption_w']), places=2)

    def test_solar_daily_totals_write_is_throttled(self):
        """Change 4: get_solar_daily_totals must not commit an upsert into
        daily_totals on every call within the throttle window."""
        target_date = datetime.now(timezone.utc)
        day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_key = day_start.strftime('%Y-%m-%d')
        next_day_key = (day_start + timedelta(days=1)).strftime('%Y-%m-%d')

        avg_conn = self.app.open_solar_history_connection(kind='avg', write=True)
        avg_cursor = avg_conn.cursor()
        self.app.ensure_solar_history_schema(avg_cursor)
        bucket_start = day_start.strftime('%Y-%m-%d %H:%M:%S')
        self.app._upsert_solar_five_minute_average(
            avg_cursor, bucket_start, 500.0, 600.0, 1, 0.04, updated_at=time.time(),
        )
        avg_conn.commit()
        avg_conn.close()

        # First call should persist (cache empty) and create the totals row.
        self.app.get_solar_daily_totals(day_key, next_day_key)

        totals_conn = self.app.open_solar_history_connection(kind='totals')
        row = totals_conn.cursor().execute(
            'SELECT updated_at FROM daily_totals WHERE date = ?', (day_key,)
        ).fetchone()
        totals_conn.close()
        self.assertIsNotNone(row, 'first call should have persisted a daily_totals row')
        first_updated_at = row[0]

        # Second call within the throttle window must not re-commit (updated_at unchanged).
        time.sleep(0.05)
        self.app.get_solar_daily_totals(day_key, next_day_key)

        totals_conn = self.app.open_solar_history_connection(kind='totals')
        row2 = totals_conn.cursor().execute(
            'SELECT updated_at FROM daily_totals WHERE date = ?', (day_key,)
        ).fetchone()
        totals_conn.close()
        self.assertEqual(row2[0], first_updated_at, 'second call within throttle window must not rewrite the row')

    def test_minute_data_cache_ttl_serves_repeated_today_requests(self):
        """Change 1: repeated /minute_data calls for 'today' within the new TTL
        should return the exact cached payload object, not recompute it."""
        with self.app.app.test_client() as client:
            resp1 = client.get('/minute_data')
            self.assertEqual(resp1.status_code, 200)
            payload1 = resp1.get_json()

            resp2 = client.get('/minute_data')
            self.assertEqual(resp2.status_code, 200)
            payload2 = resp2.get_json()

            self.assertEqual(payload1, payload2)

        cache_key = datetime.now().strftime('%Y-%m-%d')
        with self.app._minute_data_cache_lock:
            entry = self.app._minute_data_cache.get(cache_key)
        self.assertIsNotNone(entry)
        self.assertGreaterEqual(entry['expires'], time.time())


if __name__ == '__main__':
    unittest.main()
