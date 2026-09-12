"""Merge one-time solar recovery copies into the active solar databases."""
import argparse
import os
import shutil
import sqlite3
import time

TABLES = {
    'solar_2sec.db': ('solar_raw_data', 'solar_realtime'),
    'solar_5min_avg.db': ('five_minute_averages',),
    'solar_totals.db': ('daily_totals', 'monthly_totals', 'yearly_totals'),
}


def columns(connection, table_name):
    return [row[1] for row in connection.execute(f'PRAGMA table_info({table_name})').fetchall()]


def merge_table(source, target, table_name):
    source_columns = columns(source, table_name)
    target_columns = columns(target, table_name)
    shared = [column for column in source_columns if column in target_columns]
    if not shared:
        return 0

    column_sql = ', '.join(shared)
    placeholders = ', '.join('?' for _ in shared)
    sql = f'INSERT OR IGNORE INTO {table_name} ({column_sql}) VALUES ({placeholders})'
    rows = source.execute(f'SELECT {column_sql} FROM {table_name}').fetchall()
    target.executemany(sql, rows)
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'))
    parser.add_argument('--recovery-dir', required=True)
    args = parser.parse_args()

    solar_dir = os.path.join(args.data_dir, 'solar')
    stamp = time.strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(solar_dir, 'pre_merge_' + stamp)
    os.makedirs(backup_dir, exist_ok=True)

    for database_name, table_names in TABLES.items():
        active_path = os.path.join(solar_dir, database_name)
        recovery_path = os.path.join(args.recovery_dir, database_name)
        if not os.path.exists(active_path) or not os.path.exists(recovery_path):
            print(f'SKIP {database_name}: active or recovery file missing')
            continue

        shutil.copy2(active_path, os.path.join(backup_dir, database_name))
        target = sqlite3.connect(active_path, timeout=30)
        source = sqlite3.connect(f'file:{os.path.abspath(recovery_path)}?mode=ro', uri=True, timeout=30)
        try:
            counts = {table: merge_table(source, target, table) for table in table_names}
            target.commit()
            print(f'{database_name}: merged {counts}')
        finally:
            source.close()
            target.close()

    print(f'Active solar DB backups saved in: {backup_dir}')


if __name__ == '__main__':
    main()
