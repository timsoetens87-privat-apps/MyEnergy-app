import unittest

import db_backend


class DbBackendTranslationTests(unittest.TestCase):
    def test_insert_or_replace_becomes_postgres_upsert(self):
        sql = "INSERT OR REPLACE INTO daily_totals (date, total_energy_kwh, updated_at) VALUES (?, ?, ?)"
        translated = db_backend.translate_query(sql)
        self.assertIn('ON CONFLICT (date) DO UPDATE SET', translated)
        self.assertIn('%s, %s, %s', translated)

    def test_insert_or_ignore_becomes_do_nothing(self):
        sql = "INSERT OR IGNORE INTO energy_data (timestamp, power) VALUES (?, ?)"
        translated = db_backend.translate_query(sql)
        self.assertIn('ON CONFLICT DO NOTHING', translated)
        self.assertNotIn('OR IGNORE', translated)

    def test_strftime_epoch_is_translated(self):
        sql = "SELECT CAST(strftime('%s', timestamp, 'utc') AS INTEGER) AS point_ts FROM solar_realtime"
        translated = db_backend.translate_query(sql)
        self.assertIn('EXTRACT(EPOCH FROM CAST(timestamp AS timestamp))', translated)


if __name__ == '__main__':
    unittest.main()
