import os
import sqlite3


if not str(os.getenv('DATABASE_URL', '') or '').strip().lower().startswith('postgres'):
    raise SystemExit('Set DATABASE_URL to your PostgreSQL connection string before running this migration.')

os.environ.setdefault('P1_ENABLE_BACKGROUND_THREADS', 'false')

import app


TABLES_BY_SOURCE = {
    app.DB_FILE_RAW: ['energy_data'],
    app.DB_FILE_AVG: ['energy_data_5min'],
    app.DB_FILE_DAILY: ['daily_consumption', 'solar_manual_data', 'monthly_manual_totals'],
    app.DB_FILE_BACKUP: ['energy_data', 'energy_data_5min'],
    app.DB_FILE_OVERVIEW: ['monthly_overview', 'yearly_overview', 'overview_build_info'],
}


def ensure_target_schema():
    app.init_db()
    app.init_backup_db()


def table_exists_sqlite(conn, table_name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def copy_table(sqlite_path, table_name):
    if not os.path.exists(sqlite_path):
        print(f'SKIP {sqlite_path} (missing)')
        return 0

    src = sqlite3.connect(sqlite_path)
    try:
        if not table_exists_sqlite(src, table_name):
            print(f'SKIP {table_name} from {sqlite_path} (table missing)')
            return 0

        columns = [row[1] for row in src.execute(f'PRAGMA table_info({table_name})').fetchall()]
        if not columns:
            print(f'SKIP {table_name} from {sqlite_path} (no columns)')
            return 0

        rows = src.execute(f'SELECT * FROM {table_name}').fetchall()
        if not rows:
            print(f'OK {table_name} from {sqlite_path} (0 rows)')
            return 0

        placeholders = ', '.join('?' for _ in columns)
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        dest = app.get_db_connection(sqlite_path, write=True)
        try:
            cur = dest.cursor()
            cur.executemany(insert_sql, rows)
            dest.commit()
        finally:
            dest.close()

        print(f'OK {table_name} from {sqlite_path} ({len(rows)} rows)')
        return len(rows)
    finally:
        src.close()


def main():
    ensure_target_schema()
    total_rows = 0
    for source_path, tables in TABLES_BY_SOURCE.items():
        for table_name in tables:
            total_rows += copy_table(source_path, table_name)

    print(f'\nMigration complete. Total copied rows: {total_rows}')
    print(f'Backend now configured for: {app.current_db_backend()}')


if __name__ == '__main__':
    main()
