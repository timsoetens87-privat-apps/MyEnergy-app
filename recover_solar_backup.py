"""One-time salvage tool for quarantined solar SQLite backups.

Reads corrupt-but-partially-readable source databases and writes clean recovery
copies without touching the active application databases. Run with the same
Python environment used by the NAS app.
"""
import argparse
import glob
import os
import shutil
import sqlite3
import time

TABLES = {
    'solar_2sec.db': ('solar_raw_data', 'solar_realtime'),
    'solar_5min_avg.db': ('five_minute_averages',),
    'solar_totals.db': ('daily_totals', 'monthly_totals', 'yearly_totals'),
}


def table_columns(connection, table_name):
    rows = connection.execute(f'PRAGMA table_info({table_name})').fetchall()
    return [row[1] for row in rows]


def recover_database(source_path, target_path, table_names, batch_size=5000):
    source = sqlite3.connect(source_path, timeout=10)
    target = sqlite3.connect(target_path, timeout=30)
    copied = {}
    try:
        for table_name in table_names:
            source_columns = table_columns(source, table_name)
            target_columns = table_columns(target, table_name)
            columns = [column for column in source_columns if column in target_columns]
            if not columns:
                copied[table_name] = 0
                continue

            column_sql = ', '.join(columns)
            placeholders = ', '.join('?' for _ in columns)
            insert_sql = f'INSERT OR IGNORE INTO {table_name} ({column_sql}) VALUES ({placeholders})'
            count = 0
            skipped = 0
            max_rowid = source.execute(f'SELECT MAX(rowid) FROM {table_name}').fetchone()[0] or 0
            rowid = 1
            current_batch = batch_size
            while rowid <= max_rowid:
                upper_rowid = rowid + current_batch
                try:
                    rows = source.execute(
                        f'SELECT {column_sql} FROM {table_name} WHERE rowid >= ? AND rowid < ?',
                        (rowid, upper_rowid),
                    ).fetchall()
                except sqlite3.DatabaseError:
                    if current_batch > 1:
                        current_batch = max(1, current_batch // 2)
                        continue
                    skipped += 1
                    rowid += 1
                    continue

                if rows:
                    target.executemany(insert_sql, rows)
                    target.commit()
                    count += len(rows)
                rowid = upper_rowid
                current_batch = min(batch_size, current_batch * 2)
            copied[table_name] = {'copied': count, 'skipped': skipped}
    finally:
        source.close()
        target.close()
    return copied


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'))
    parser.add_argument('--source-suffix', default='')
    parser.add_argument('--output-dir', default='')
    args = parser.parse_args()

    solar_dir = os.path.join(args.data_dir, 'solar')
    output_dir = args.output_dir or os.path.join(solar_dir, 'recovered_solar_' + time.strftime('%Y%m%d_%H%M%S'))
    os.makedirs(output_dir, exist_ok=True)

    results = {}
    for database_name, table_names in TABLES.items():
        if args.source_suffix:
            source_path = os.path.join(solar_dir, database_name + '.corrupt_backup_' + args.source_suffix)
        else:
            candidates = [
                path for path in glob.glob(os.path.join(solar_dir, database_name + '.corrupt_backup_*'))
                if not path.endswith(('-wal', '-shm'))
            ]
            source_path = max(candidates, key=os.path.getmtime) if candidates else ''
        if not os.path.exists(source_path):
            print(f'SKIP missing source: {source_path}')
            continue
        target_path = os.path.join(output_dir, database_name)
        shutil.copy2(os.path.join(solar_dir, database_name), target_path)
        try:
            results[database_name] = recover_database(source_path, target_path, table_names)
            print(f'{database_name}: {results[database_name]} -> {target_path}')
        except sqlite3.DatabaseError as exc:
            results[database_name] = {'error': str(exc)}
            print(f'{database_name}: partial recovery failed: {exc}')

    print(f'Recovery copies written to: {output_dir}')


if __name__ == '__main__':
    main()
