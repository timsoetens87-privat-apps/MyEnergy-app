import csv
import datetime
import glob
import os
import shutil
import sqlite3
import tempfile
import time


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
FLUVIUS_DIR = os.path.join(BASE_DIR, 'Fluvius-history')
TARGET_DB = os.path.join(DATA_DIR, 'data_overview.db')


def parse_decimal(value):
    if value in (None, ''):
        return None
    text = str(value).strip().replace(' ', '')
    if ',' in text:
        text = text.replace('.', '').replace(',', '.')
    try:
        return float(text)
    except Exception:
        return None


def ensure_schema(cursor):
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS monthly_overview (
               month TEXT PRIMARY KEY,
               year INTEGER NOT NULL,
               month_num INTEGER NOT NULL,
               first_date TEXT,
               last_date TEXT,
               covered_days INTEGER NOT NULL DEFAULT 0,
               consumption_kwh REAL,
               injection_kwh REAL,
               consumption_offpeak_kwh REAL,
               consumption_peak_kwh REAL,
               injection_offpeak_kwh REAL,
               injection_peak_kwh REAL,
               gas_m3 REAL,
               row_count INTEGER NOT NULL DEFAULT 0,
               updated_at REAL NOT NULL
           )'''
    )
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS yearly_overview (
               year INTEGER PRIMARY KEY,
               first_month TEXT,
               last_month TEXT,
               covered_days INTEGER NOT NULL DEFAULT 0,
               consumption_kwh REAL,
               injection_kwh REAL,
               consumption_offpeak_kwh REAL,
               consumption_peak_kwh REAL,
               injection_offpeak_kwh REAL,
               injection_peak_kwh REAL,
               gas_m3 REAL,
               row_count INTEGER NOT NULL DEFAULT 0,
               updated_at REAL NOT NULL
           )'''
    )
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS overview_build_info (
               id INTEGER PRIMARY KEY CHECK (id = 1),
               built_at REAL NOT NULL,
               source_dir TEXT NOT NULL,
               file_count INTEGER NOT NULL,
               notes TEXT
           )'''
    )


def get_month_bucket(buckets, month_key):
    bucket = buckets.get(month_key)
    if bucket is None:
        year, month_num = month_key.split('-')
        bucket = {
            'year': int(year),
            'month_num': int(month_num),
            'dates': set(),
            'consumption_kwh': 0.0,
            'injection_kwh': 0.0,
            'consumption_offpeak_kwh': 0.0,
            'consumption_peak_kwh': 0.0,
            'injection_offpeak_kwh': 0.0,
            'injection_peak_kwh': 0.0,
            'gas_m3': 0.0,
            'row_count': 0,
        }
        buckets[month_key] = bucket
    return bucket


def process_electricity_csv(path, monthly_buckets):
    with open(path, 'r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle, delimiter=';')
        for row in reader:
            from_date = (row.get('Van (datum)') or '').strip()
            register = (row.get('Register') or '').strip().lower()
            unit = (row.get('Eenheid') or '').strip().lower()
            volume = parse_decimal(row.get('Volume'))
            if not from_date or volume is None or unit != 'kwh':
                continue

            try:
                date_value = datetime.datetime.strptime(from_date, '%d-%m-%Y').date()
            except Exception:
                continue

            month_key = date_value.strftime('%Y-%m')
            bucket = get_month_bucket(monthly_buckets, month_key)
            bucket['dates'].add(date_value.isoformat())
            bucket['row_count'] += 1

            if 'afname' in register:
                bucket['consumption_kwh'] += volume
                if 'nacht' in register:
                    bucket['consumption_offpeak_kwh'] += volume
                elif 'dag' in register:
                    bucket['consumption_peak_kwh'] += volume
            elif 'injectie' in register:
                bucket['injection_kwh'] += volume
                if 'nacht' in register:
                    bucket['injection_offpeak_kwh'] += volume
                elif 'dag' in register:
                    bucket['injection_peak_kwh'] += volume


def process_gas_csv(path, monthly_buckets):
    with open(path, 'r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle, delimiter=';')
        for row in reader:
            from_date = (row.get('Van (datum)') or '').strip()
            unit = (row.get('Eenheid') or '').strip().lower()
            volume = parse_decimal(row.get('Volume'))
            if not from_date or volume is None or unit not in ('m3', 'm³'):
                continue

            try:
                date_value = datetime.datetime.strptime(from_date, '%d-%m-%Y').date()
            except Exception:
                continue

            month_key = date_value.strftime('%Y-%m')
            bucket = get_month_bucket(monthly_buckets, month_key)
            bucket['gas_m3'] += volume


def write_overview_db(target_db, monthly_buckets, file_count):
    temp_dir = tempfile.mkdtemp(prefix='fluvius_overview_')
    temp_db = os.path.join(temp_dir, 'data_overview.db')

    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    ensure_schema(cursor)

    updated_at = time.time()
    monthly_rows = []
    yearly_buckets = {}
    for month_key in sorted(monthly_buckets.keys()):
        bucket = monthly_buckets[month_key]
        sorted_dates = sorted(bucket['dates'])
        first_date = sorted_dates[0] if sorted_dates else None
        last_date = sorted_dates[-1] if sorted_dates else None
        covered_days = len(sorted_dates)
        monthly_rows.append(
            (
                month_key,
                bucket['year'],
                bucket['month_num'],
                first_date,
                last_date,
                covered_days,
                round(bucket['consumption_kwh'], 6),
                round(bucket['injection_kwh'], 6),
                round(bucket['consumption_offpeak_kwh'], 6),
                round(bucket['consumption_peak_kwh'], 6),
                round(bucket['injection_offpeak_kwh'], 6),
                round(bucket['injection_peak_kwh'], 6),
                round(bucket['gas_m3'], 6),
                int(bucket['row_count']),
                updated_at,
            )
        )

        year_bucket = yearly_buckets.setdefault(
            bucket['year'],
            {
                'months': [],
                'covered_days': 0,
                'consumption_kwh': 0.0,
                'injection_kwh': 0.0,
                'consumption_offpeak_kwh': 0.0,
                'consumption_peak_kwh': 0.0,
                'injection_offpeak_kwh': 0.0,
                'injection_peak_kwh': 0.0,
                'gas_m3': 0.0,
                'row_count': 0,
            },
        )
        year_bucket['months'].append(month_key)
        year_bucket['covered_days'] += covered_days
        year_bucket['consumption_kwh'] += bucket['consumption_kwh']
        year_bucket['injection_kwh'] += bucket['injection_kwh']
        year_bucket['consumption_offpeak_kwh'] += bucket['consumption_offpeak_kwh']
        year_bucket['consumption_peak_kwh'] += bucket['consumption_peak_kwh']
        year_bucket['injection_offpeak_kwh'] += bucket['injection_offpeak_kwh']
        year_bucket['injection_peak_kwh'] += bucket['injection_peak_kwh']
        year_bucket['gas_m3'] += bucket['gas_m3']
        year_bucket['row_count'] += bucket['row_count']

    cursor.executemany(
        '''INSERT OR REPLACE INTO monthly_overview (
               month, year, month_num, first_date, last_date, covered_days,
               consumption_kwh, injection_kwh,
               consumption_offpeak_kwh, consumption_peak_kwh,
               injection_offpeak_kwh, injection_peak_kwh,
               gas_m3, row_count, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        monthly_rows,
    )

    yearly_rows = []
    for year in sorted(yearly_buckets.keys()):
        bucket = yearly_buckets[year]
        months = sorted(bucket['months'])
        yearly_rows.append(
            (
                year,
                months[0],
                months[-1],
                bucket['covered_days'],
                round(bucket['consumption_kwh'], 6),
                round(bucket['injection_kwh'], 6),
                round(bucket['consumption_offpeak_kwh'], 6),
                round(bucket['consumption_peak_kwh'], 6),
                round(bucket['injection_offpeak_kwh'], 6),
                round(bucket['injection_peak_kwh'], 6),
                round(bucket['gas_m3'], 6),
                bucket['row_count'],
                updated_at,
            )
        )

    cursor.executemany(
        '''INSERT OR REPLACE INTO yearly_overview (
               year, first_month, last_month, covered_days,
               consumption_kwh, injection_kwh,
               consumption_offpeak_kwh, consumption_peak_kwh,
               injection_offpeak_kwh, injection_peak_kwh,
               gas_m3, row_count, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        yearly_rows,
    )

    cursor.execute('DELETE FROM overview_build_info WHERE id = 1')
    cursor.execute(
        '''INSERT INTO overview_build_info (id, built_at, source_dir, file_count, notes)
           VALUES (1, ?, ?, ?, ?)''',
        (updated_at, FLUVIUS_DIR, file_count, 'Built from Fluvius-history CSV files only'),
    )
    conn.commit()
    conn.close()

    os.makedirs(os.path.dirname(target_db), exist_ok=True)
    shutil.copy2(temp_db, target_db)
    shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    csv_files = sorted(glob.glob(os.path.join(FLUVIUS_DIR, '*.csv')))
    if not csv_files:
        raise SystemExit('No CSV files found in Fluvius-history')

    monthly_buckets = {}
    for path in csv_files:
        name = os.path.basename(path).lower()
        print(f'Processing {os.path.basename(path)}')
        if 'gas' in name:
            process_gas_csv(path, monthly_buckets)
        else:
            process_electricity_csv(path, monthly_buckets)

    write_overview_db(TARGET_DB, monthly_buckets, len(csv_files))

    conn = sqlite3.connect(TARGET_DB)
    cursor = conn.cursor()
    print('Monthly overview rows:', cursor.execute('SELECT COUNT(*) FROM monthly_overview').fetchone()[0])
    print('Yearly overview rows:', cursor.execute('SELECT COUNT(*) FROM yearly_overview').fetchone()[0])
    for row in cursor.execute(
        '''SELECT month, consumption_kwh, injection_kwh,
                  consumption_offpeak_kwh, consumption_peak_kwh,
                  injection_offpeak_kwh, injection_peak_kwh, gas_m3
           FROM monthly_overview
           ORDER BY month DESC
           LIMIT 6'''
    ).fetchall():
        print(row)
    conn.close()


if __name__ == '__main__':
    main()