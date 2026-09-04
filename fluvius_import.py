"""
Standalone Fluvius historical CSV importer.
Does NOT import app.py – runs entirely on its own so no background threads start.
"""
import csv
import datetime
import glob
import os
import sqlite3
import sys
import time

DATA_DIR    = r'\\soetens-nas\web\my_P1_dashboard\data'
FLUVIUS_DIR = r'\\soetens-nas\web\my_P1_dashboard\Fluvius-history'
# Allow env-var overrides so the import can run against a local copy of the DB
# (avoids SQLite-over-SMB corruption on large batch writes)
DB_RAW    = os.getenv('IMPORT_DB_RAW',    os.path.join(DATA_DIR, 'data_raw.db'))
DB_AVG    = os.getenv('IMPORT_DB_AVG',    os.path.join(DATA_DIR, 'data_avg.db'))
DB_DAILY  = os.getenv('IMPORT_DB_DAILY',  os.path.join(DATA_DIR, 'data_daily.db'))
DB_BACKUP = os.getenv('IMPORT_DB_BACKUP', os.path.join(DATA_DIR, 'data_backup.db'))


# ── helpers ────────────────────────────────────────────────────────────────────

def open_db(path, write=False):
    conn = sqlite3.connect(path, timeout=60)
    c = conn.cursor()
    c.execute('PRAGMA journal_mode=DELETE')
    if write:
        # NORMAL is safe for local disks; FULL caused very slow commits via fsync
        c.execute('PRAGMA synchronous=NORMAL')
    c.execute('PRAGMA busy_timeout=30000')
    c.execute('PRAGMA cache_size=-65536')   # 64 MB page cache
    c.execute('PRAGMA temp_store=MEMORY')
    return conn


def parse_decimal(val):
    if val in (None, ''):
        return None
    s = str(val).strip().replace(' ', '')
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return None


# ── Fluvius CSV → normalised interval rows ─────────────────────────────────────

def parse_fluvius_file(path):
    """Return list of normalised dicts keyed by start-timestamp."""
    buckets = {}
    with open(path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            from_date = row.get('Van (datum)', '').strip()
            from_time = row.get('Van (tijdstip)', '').strip()
            to_date   = row.get('Tot (datum)', '').strip()
            to_time   = row.get('Tot (tijdstip)', '').strip()
            register  = row.get('Register', '').strip().lower()
            unit      = row.get('Eenheid', '').strip().lower()
            volume    = parse_decimal(row.get('Volume'))

            if not from_date or not from_time or volume is None:
                continue

            try:
                start_dt = datetime.datetime.strptime(f'{from_date} {from_time}', '%d-%m-%Y %H:%M:%S')
                end_dt   = datetime.datetime.strptime(f'{to_date} {to_time}',   '%d-%m-%Y %H:%M:%S')
            except Exception:
                continue

            interval_s = int((end_dt - start_dt).total_seconds())
            if interval_s <= 0:
                continue

            ts = start_dt.timestamp()
            b = buckets.setdefault(ts, dict(interval_s=interval_s,
                                             import_kwh=0.0, export_kwh=0.0,
                                             gas_m3=0.0,
                                             import_t1=0.0, import_t2=0.0,
                                             export_t1=0.0, export_t2=0.0))
            b['interval_s'] = max(b['interval_s'], interval_s)

            if unit in ('m3', 'm³'):
                b['gas_m3'] += volume
                continue
            if unit != 'kwh':
                continue

            if 'afname' in register:
                b['import_kwh'] += volume
                if 'nacht' in register:
                    b['import_t1'] += volume
                elif 'dag' in register:
                    b['import_t2'] += volume
            elif 'injectie' in register:
                b['export_kwh'] += volume
                if 'nacht' in register:
                    b['export_t1'] += volume
                elif 'dag' in register:
                    b['export_t2'] += volume

    # Convert interval volumes → cumulative counters
    cum_import = cum_export = cum_gas = 0.0
    cum_t1 = cum_t2 = cum_et1 = cum_et2 = 0.0
    rows = []
    for ts in sorted(buckets):
        b = buckets[ts]
        cum_import += b['import_kwh']
        cum_export += b['export_kwh']
        cum_gas    += b['gas_m3']
        cum_t1     += b['import_t1']
        cum_t2     += b['import_t2']
        cum_et1    += b['export_t1']
        cum_et2    += b['export_t2']
        h = b['interval_s'] / 3600.0
        power_w = ((b['import_kwh'] - b['export_kwh']) / h * 1000.0) if h > 0 else 0.0
        rows.append((float(ts), power_w, cum_import, cum_export, cum_gas,
                     cum_t1, cum_t2, cum_et1, cum_et2))
    return rows


# ── cumulative-offset correction ────────────────────────────────────────────────
# Fluvius CSVs each start from 0.  If rows for a file already exist in the DB
# we need to add an offset so the cumulative series is continuous.

def get_max_cumulative_before(conn, ts_start):
    """Return (import_kwh, export_kwh, gas_m3, t1, t2, et1, et2) just before ts_start."""
    c = conn.cursor()
    c.execute(
        '''SELECT import_kwh, export_kwh, gas_m3,
                  import_t1_kwh, import_t2_kwh, export_t1_kwh, export_t2_kwh
           FROM energy_data
           WHERE timestamp < ?
           ORDER BY timestamp DESC LIMIT 1''',
        (ts_start,)
    )
    row = c.fetchone()
    if row:
        return tuple(v if v is not None else 0.0 for v in row)
    return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)


# ── insert rows in batches ──────────────────────────────────────────────────────

BATCH = 100  # small batches reduce re-work on reconnect

def _reconnect(db_path):
    """Force a fresh connection after a dropped/IO-error connection."""
    conn = open_db(db_path, write=True)
    return conn


def insert_rows(conn, rows, db_path):
    """INSERT OR IGNORE rows into energy_data; reconnects on I/O error."""
    inserted = 0
    i = 0
    while i < len(rows):
        chunk = rows[i:i+BATCH]
        for attempt in range(5):
            try:
                c = conn.cursor()
                c.executemany(
                    '''INSERT OR IGNORE INTO energy_data
                       (timestamp, power, import_kwh, export_kwh, gas_m3,
                        import_t1_kwh, import_t2_kwh, export_t1_kwh, export_t2_kwh)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    chunk
                )
                conn.commit()
                inserted += c.rowcount if c.rowcount > 0 else 0
                i += BATCH
                break
            except sqlite3.OperationalError as e:
                print(f'    I/O error at row {i} (attempt {attempt+1}): {e} – reconnecting …')
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(2 * (attempt + 1))
                conn = _reconnect(db_path)
        else:
            print(f'    Giving up on batch starting at row {i} after 5 attempts')
            i += BATCH  # skip and continue
    return conn, inserted


def update_gas_rows(conn, gas_rows, db_path):
    """UPDATE energy_data gas_m3 for rows that already exist (electricity inserts).
    gas_rows: list of (timestamp, cumulative_gas_m3).
    For timestamps missing from DB, finds the nearest row within 30 minutes."""
    updated = 0
    i = 0
    while i < len(gas_rows):
        chunk = gas_rows[i:i+BATCH]
        for attempt in range(5):
            try:
                c = conn.cursor()
                for ts, gas_cum in chunk:
                    # Try exact match first, then nearest within 30 min
                    c.execute(
                        'UPDATE energy_data SET gas_m3=? WHERE timestamp=?',
                        (gas_cum, ts)
                    )
                    if c.rowcount == 0:
                        c.execute(
                            '''UPDATE energy_data SET gas_m3=?
                               WHERE ABS(timestamp - ?) = (
                                   SELECT MIN(ABS(timestamp - ?))
                                   FROM energy_data
                                   WHERE ABS(timestamp - ?) <= 1800
                               )''',
                            (gas_cum, ts, ts, ts)
                        )
                        if c.rowcount > 0:
                            updated += c.rowcount
                    else:
                        updated += 1
                conn.commit()
                i += BATCH
                break
            except sqlite3.OperationalError as e:
                print(f'    Gas update I/O error at row {i} (attempt {attempt+1}): {e} – reconnecting …')
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(2 * (attempt + 1))
                conn = _reconnect(db_path)
        else:
            print(f'    Gas update giving up on batch at row {i}')
            i += BATCH
    return conn, updated


# ── 5-minute rebuild ────────────────────────────────────────────────────────────

def rebuild_5min(conn_raw, conn_avg, start_ts, end_ts):
    c_raw = conn_raw.cursor()
    c_raw.execute(
        '''SELECT CAST(timestamp / 300 AS INTEGER) * 300,
                  AVG(power), MIN(power), MAX(power)
           FROM energy_data
           WHERE timestamp >= ? AND timestamp < ?
           GROUP BY 1 ORDER BY 1''',
        (start_ts, end_ts)
    )
    rows = c_raw.fetchall()
    if not rows:
        return 0
    c_avg = conn_avg.cursor()
    c_avg.execute('DELETE FROM energy_data_5min WHERE timestamp >= ? AND timestamp < ?',
                  (start_ts, end_ts))
    c_avg.executemany('INSERT OR REPLACE INTO energy_data_5min VALUES (?, ?, ?, ?)', rows)
    conn_avg.commit()
    return len(rows)


# ── daily rebuild ───────────────────────────────────────────────────────────────

def rebuild_daily(conn_raw, conn_daily, start_ts, end_ts):
    """Minimal daily rebuild: net consumption per calendar day."""
    c = conn_raw.cursor()
    # Get date boundaries
    c.execute(
        '''SELECT DATE(timestamp, "unixepoch", "localtime")  AS day,
                  MIN(timestamp), MAX(timestamp),
                  MIN(import_kwh), MAX(import_kwh),
                  MIN(export_kwh), MAX(export_kwh),
                  MIN(import_t1_kwh), MAX(import_t1_kwh),
                  MIN(import_t2_kwh), MAX(import_t2_kwh),
                  MIN(export_t1_kwh), MAX(export_t1_kwh),
                  MIN(export_t2_kwh), MAX(export_t2_kwh),
                  MAX(power), COUNT(*)
           FROM energy_data
           WHERE timestamp >= ? AND timestamp < ? AND import_kwh IS NOT NULL
           GROUP BY day ORDER BY day''',
        (start_ts, end_ts)
    )
    rows = c.fetchall()
    cd = conn_daily.cursor()
    inserted = 0
    for row in rows:
        (day, day_start, day_end, min_imp, max_imp,
         min_exp, max_exp, min_t1, max_t1, min_t2, max_t2,
         min_et1, max_et1, min_et2, max_et2, peak_w, n) = row
        if day is None or max_imp is None:
            continue
        cons   = round(max_imp - min_imp, 4) if max_imp and min_imp else 0.0
        inj    = round(max_exp - min_exp, 4) if max_exp and min_exp else 0.0
        t1     = round((max_t1 or 0.0) - (min_t1 or 0.0), 4)
        t2     = round((max_t2 or 0.0) - (min_t2 or 0.0), 4)
        et1    = round((max_et1 or 0.0) - (min_et1 or 0.0), 4)
        et2    = round((max_et2 or 0.0) - (min_et2 or 0.0), 4)
        cd.execute(
            '''INSERT OR REPLACE INTO daily_consumption
               (date, day_start_ts, day_end_ts,
                consumption_kwh, injection_kwh, net_consumption_kwh,
                consumption_offpeak_kwh, consumption_peak_kwh,
                injection_offpeak_kwh, injection_peak_kwh,
                peak_consumption_w, sample_count, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (day, day_start, day_end, cons, inj, round(cons - inj, 4),
             t1, t2, et1, et2, peak_w, n, time.time())
        )
        inserted += 1
    conn_daily.commit()
    return inserted


# ── main ────────────────────────────────────────────────────────────────────────

def main():
    csv_files = sorted(
        f for f in glob.glob(os.path.join(FLUVIUS_DIR, '*.csv'))
    )
    if not csv_files:
        print('No CSV files found in', FLUVIUS_DIR)
        sys.exit(1)

    print(f'Found {len(csv_files)} CSV file(s):\n' +
          '\n'.join('  ' + os.path.basename(f) for f in csv_files))

    conn_raw   = open_db(DB_RAW,   write=True)
    conn_avg   = open_db(DB_AVG,   write=True)
    conn_daily = open_db(DB_DAILY, write=True)

    global_min_ts = None
    global_max_ts = None

    for path in csv_files:
        fname = os.path.basename(path)
        is_gas = 'gas' in fname.lower()
        print(f'\n── {fname}  ({"gas – UPDATE mode" if is_gas else "electricity"})')
        t0 = time.time()
        rows = parse_fluvius_file(path)
        if not rows:
            print('  No rows parsed – skipping')
            continue

        ts_start = rows[0][0]
        ts_end   = rows[-1][0]
        print(f'  Parsed {len(rows)} intervals  '
              f'[{datetime.datetime.fromtimestamp(ts_start).date()} '
              f'→ {datetime.datetime.fromtimestamp(ts_end).date()}]')

        if is_gas:
            # Gas CSV: cumulative gas counter → UPDATE existing electricity rows
            offset = get_max_cumulative_before(conn_raw, ts_start)
            off_gas = offset[2]  # gas offset only
            gas_update_rows = [
                (float(ts), round(gas + off_gas, 6))   # (timestamp, gas_m3)
                for (ts, _pw, _imp, _exp, gas, *_) in rows
            ]
            conn_raw, n_updated = update_gas_rows(conn_raw, gas_update_rows, DB_RAW)
            elapsed = time.time() - t0
            print(f'  Updated gas_m3 on {n_updated} rows  ({elapsed:.1f}s)')
        else:
            # Electricity CSV: cumulative import/export → INSERT OR IGNORE
            offset = get_max_cumulative_before(conn_raw, ts_start)
            off_imp, off_exp, off_gas, off_t1, off_t2, off_et1, off_et2 = offset

            shifted = []
            for (ts, pw, imp, exp, gas, t1, t2, et1, et2) in rows:
                shifted.append((
                    ts, pw,
                    round(imp + off_imp, 6),
                    round(exp + off_exp, 6),
                    round(gas + off_gas, 6),
                    round(t1  + off_t1,  6),
                    round(t2  + off_t2,  6),
                    round(et1 + off_et1, 6),
                    round(et2 + off_et2, 6),
                ))

            # Progress bar
            CHUNK = 5000
            total_inserted = 0
            for start_i in range(0, len(shifted), CHUNK):
                chunk_rows = shifted[start_i:start_i+CHUNK]
                conn_raw, ins = insert_rows(conn_raw, chunk_rows, DB_RAW)
                total_inserted += ins
                print(f'  … {start_i + len(chunk_rows)}/{len(shifted)} rows processed, '
                      f'{total_inserted} inserted')
            elapsed = time.time() - t0
            print(f'  Total: {total_inserted} new rows  ({elapsed:.1f}s)')

        if global_min_ts is None or ts_start < global_min_ts:
            global_min_ts = ts_start
        if global_max_ts is None or ts_end > global_max_ts:
            global_max_ts = ts_end

    # Rebuild derived tables for the whole imported range
    if global_min_ts is not None:
        start = int(global_min_ts // 300) * 300
        end   = int(global_max_ts // 300 + 1) * 300

        print('\n── Rebuilding 5-minute aggregates …')
        n5 = rebuild_5min(conn_raw, conn_avg, start, end)
        print(f'   {n5} 5-minute rows written')

        print('── Rebuilding daily consumption …')
        nd = rebuild_daily(conn_raw, conn_daily, start, end)
        print(f'   {nd} daily rows written')

    conn_raw.close()
    conn_avg.close()
    conn_daily.close()

    # Final counts
    print('\n── Final DB counts ──')
    for label, db, table, col in [
        ('RAW',   DB_RAW,   'energy_data',     'timestamp'),
        ('5MIN',  DB_AVG,   'energy_data_5min','timestamp'),
        ('DAILY', DB_DAILY, 'daily_consumption','date'),
    ]:
        conn = sqlite3.connect(db, timeout=30)
        c = conn.cursor()
        c.execute(f'SELECT COUNT(*), MIN({col}), MAX({col}) FROM {table}')
        cnt, mn, mx = c.fetchone()
        if col == 'timestamp' and mn:
            mn = datetime.datetime.fromtimestamp(mn).strftime('%Y-%m-%d')
            mx = datetime.datetime.fromtimestamp(mx).strftime('%Y-%m-%d')
        print(f'  {label}: {cnt} rows  [{mn} → {mx}]')
        # Per-year breakdown
        if col == 'timestamp':
            c.execute(
                f'SELECT CAST(strftime("%Y", datetime({col},"unixepoch")) AS TEXT),'
                f' COUNT(*) FROM {table} GROUP BY 1 ORDER BY 1'
            )
        else:
            c.execute(
                f'SELECT substr({col},1,4), COUNT(*) FROM {table} GROUP BY 1 ORDER BY 1'
            )
        for row in c.fetchall():
            print(f'    {row[0]}: {row[1]}')
        conn.close()

    print('\nDone.')


if __name__ == '__main__':
    main()
