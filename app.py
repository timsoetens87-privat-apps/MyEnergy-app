import requests
from flask import Flask, jsonify, make_response, render_template, request
from datetime import datetime, timedelta, timezone
from collections import deque
import time
import json
import sys
import csv
import sqlite3
import os
import shutil
import smtplib
import socket
from email.mime.text import MIMEText
import threading
import tempfile
from werkzeug.middleware.proxy_fix import ProxyFix

from db_backend import connect_database, current_db_backend, is_postgres_enabled, masked_database_url

# Sun2000 solar inverter integration (live power only)
try:
    from pymodbus.client import ModbusTcpClient
    SUN2000_AVAILABLE = True
except ImportError as e:
    try:
        from pymodbus.client.tcp import ModbusTcpClient
        SUN2000_AVAILABLE = True
    except ImportError:
        print(f"Warning: Sun2000 module not available: {e}")
        SUN2000_AVAILABLE = False

try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)
P1_HTTP = requests.Session()
MARSTEK_HTTP = requests.Session()

P1_URL = os.getenv('P1_URL', 'http://192.168.0.11/api/v1/data').strip()
P1_CONNECT_TIMEOUT_SECONDS = max(1.0, float(os.getenv('P1_CONNECT_TIMEOUT_SECONDS', os.getenv('P1_TIMEOUT_SECONDS', '2.5'))))
P1_READ_TIMEOUT_SECONDS = max(1.0, float(os.getenv('P1_READ_TIMEOUT_SECONDS', os.getenv('P1_TIMEOUT_SECONDS', '3'))))
P1_HTTP_TIMEOUT = (P1_CONNECT_TIMEOUT_SECONDS, P1_READ_TIMEOUT_SECONDS)
MARSTEK_IP = os.getenv('MARSTEK_IP', '192.168.0.27').strip()
MARSTEK_UDP_PORT = int(os.getenv('MARSTEK_UDP_PORT', '30000'))
MARSTEK_UDP_TIMEOUT_SECONDS = max(1.0, float(os.getenv('MARSTEK_UDP_TIMEOUT_SECONDS', '2.5')))
MARSTEK_STATUS_URL = os.getenv('MARSTEK_STATUS_URL', '').strip()
MARSTEK_API_TOKEN = os.getenv('MARSTEK_API_TOKEN', '').strip()
MARSTEK_TIMEOUT_SECONDS = max(2.0, float(os.getenv('MARSTEK_TIMEOUT_SECONDS', '3')))
MARSTEK_FETCH_LOCK_TIMEOUT_SECONDS = max(
    1.0,
    float(os.getenv('MARSTEK_FETCH_LOCK_TIMEOUT_SECONDS', str(max(MARSTEK_TIMEOUT_SECONDS, MARSTEK_UDP_TIMEOUT_SECONDS + 0.5))))
)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Default storage is the dedicated data folder. Optional override via P1_DATA_DIR.
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_DIR = os.path.normpath(os.getenv('P1_DATA_DIR', DEFAULT_DATA_DIR))
P1_DB_DIR = os.path.join(DATA_DIR, 'p1')
SOLAR_DB_DIR = os.path.join(DATA_DIR, 'solar')
BATTERY_DB_DIR = os.path.join(DATA_DIR, 'battery')
GAS_DB_DIR = os.path.join(DATA_DIR, 'gas')
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(P1_DB_DIR, exist_ok=True)
os.makedirs(SOLAR_DB_DIR, exist_ok=True)
os.makedirs(BATTERY_DB_DIR, exist_ok=True)
os.makedirs(GAS_DB_DIR, exist_ok=True)

DB_FILE_RAW = os.path.join(P1_DB_DIR, 'p1_2sec.db')  # Raw 2-second interval data (30-day retention)
DB_FILE_AVG = os.path.join(P1_DB_DIR, 'p1_5min_avg.db')  # 5-minute averaged data (permanent storage)
DB_FILE_DAILY = os.path.join(P1_DB_DIR, 'p1_totals.db')  # Daily/monthly/yearly P1 totals
DB_FILE_BACKUP = os.path.join(P1_DB_DIR, 'p1_backup.db')  # Full backup copy (synced every minute)
DB_FILE_OVERVIEW = os.path.join(P1_DB_DIR, 'p1_overview.db')  # CSV-derived monthly/yearly overview database
DB_FILE_GAS_TOTALS = os.path.join(GAS_DB_DIR, 'gas_totals.db')  # Daily/monthly gas totals in kWh
LEGACY_DB_FILE = os.path.join(DATA_DIR, 'data.db')   # Legacy combined database used by older versions
LEGACY_JSON_FILE = os.path.join(DATA_DIR, 'data.json')
LEGACY_P1_DB_FILES = {
    DB_FILE_RAW: [os.path.join(DATA_DIR, 'data_raw.db'), os.path.join(BASE_DIR, 'data_raw.db')],
    DB_FILE_AVG: [os.path.join(DATA_DIR, 'data_avg.db'), os.path.join(BASE_DIR, 'data_avg.db')],
    DB_FILE_DAILY: [os.path.join(DATA_DIR, 'data_daily.db'), os.path.join(BASE_DIR, 'data_daily.db')],
    DB_FILE_BACKUP: [os.path.join(DATA_DIR, 'data_backup.db'), os.path.join(BASE_DIR, 'data_backup.db')],
    DB_FILE_OVERVIEW: [os.path.join(DATA_DIR, 'data_overview.db'), os.path.join(BASE_DIR, 'data_overview.db')],
}
SOLAR_DB_RAW = os.path.join(SOLAR_DB_DIR, 'solar_2sec.db')
SOLAR_DB_AVG = os.path.join(SOLAR_DB_DIR, 'solar_5min_avg.db')
SOLAR_DB_TOTALS = os.path.join(SOLAR_DB_DIR, 'solar_totals.db')
BATTERY_DB_RAW = os.path.join(BATTERY_DB_DIR, 'battery_2sec.db')
BATTERY_DB_AVG = os.path.join(BATTERY_DB_DIR, 'battery_5min_avg.db')
BATTERY_DB_TOTALS = os.path.join(BATTERY_DB_DIR, 'battery_totals.db')
LEGACY_SOLAR_DB_CANDIDATES = (
    os.path.join(DATA_DIR, 'solar_history.db'),
    os.path.join(BASE_DIR, 'solar_history.db'),
)
SOLAR_HISTORY_DB_CANDIDATES = LEGACY_SOLAR_DB_CANDIDATES
SOLAR_DB_FILES = {
    SOLAR_DB_RAW: list(LEGACY_SOLAR_DB_CANDIDATES),
    SOLAR_DB_AVG: list(LEGACY_SOLAR_DB_CANDIDATES),
    SOLAR_DB_TOTALS: list(LEGACY_SOLAR_DB_CANDIDATES),
}
DATA_RETENTION_DAYS = 30  # Keep raw data for 30 days, then delete
IS_NETWORK_DATA_DIR = str(DATA_DIR).startswith('\\\\')
SAVE_INTERVAL = max(2, int(float(os.getenv('P1_SAVE_INTERVAL_SECONDS', '10'))))
BACKUP_INTERVAL_SECONDS = max(300 if IS_NETWORK_DATA_DIR else 120, int(float(os.getenv('P1_BACKUP_INTERVAL_SECONDS', '900'))))
DB_BUSY_TIMEOUT_MS = 3000
DB_WRITE_RETRIES = 5
DB_CONNECT_RETRIES = 5
DB_CONNECT_RETRY_BASE_SECONDS = 0.2
MAX_COUNTER_STEP_KWH = float(os.getenv('P1_MAX_COUNTER_STEP_KWH', '0.2'))
GAS_KWH_PER_M3 = max(0.1, float(os.getenv('GAS_KWH_PER_M3', '11.2')))
GAS_TOTALS_REFRESH_SECONDS = max(30.0, float(os.getenv('GAS_TOTALS_REFRESH_SECONDS', '300')))
GAS_MAX_COUNTER_STEP_M3 = max(0.05, float(os.getenv('GAS_MAX_COUNTER_STEP_M3', '2.0')))
GAS_MAX_RATE_M3_PER_HOUR = max(0.1, float(os.getenv('GAS_MAX_RATE_M3_PER_HOUR', '1.2')))
GAS_MAX_DAILY_USAGE_M3 = max(0.1, float(os.getenv('GAS_MAX_DAILY_USAGE_M3', '25.0')))
WEEKLY_CURRENT_MONTH_CACHE_TTL_SECONDS = max(15.0, float(os.getenv('WEEKLY_CURRENT_MONTH_CACHE_TTL_SECONDS', '60')))
RAW_WRITE_QUEUE_FILE = os.path.join(P1_DB_DIR, 'p1_raw_write_queue.jsonl')
SETTINGS_FILE = os.path.join(DATA_DIR, 'settings.json')
FLUVIUS_HISTORY_DIR = os.path.join(BASE_DIR, 'Fluvius-history')
LIVE_TEMP_DB_DIR = os.path.join(tempfile.gettempdir(), 'myenergy_live')
os.makedirs(LIVE_TEMP_DB_DIR, exist_ok=True)
DB_FILE_LIVE_TEMP = os.path.join(LIVE_TEMP_DB_DIR, 'live_power_temp.db')
RAW_DB_FALLBACK_DIR = os.path.join(tempfile.gettempdir(), 'myenergy_runtime')
os.makedirs(RAW_DB_FALLBACK_DIR, exist_ok=True)
DB_FILE_RAW_FALLBACK = os.path.join(RAW_DB_FALLBACK_DIR, 'p1_2sec_fallback.db')
RAW_WRITE_QUEUE_FILE_FALLBACK = os.path.join(RAW_DB_FALLBACK_DIR, 'p1_raw_write_queue.jsonl')
SOLAR_DB_FALLBACK_DIR = os.path.join(tempfile.gettempdir(), 'myenergy_solar_runtime')
os.makedirs(SOLAR_DB_FALLBACK_DIR, exist_ok=True)
SOLAR_DB_RAW_FALLBACK = os.path.join(SOLAR_DB_FALLBACK_DIR, 'solar_2sec_fallback.db')
SOLAR_DB_AVG_FALLBACK = os.path.join(SOLAR_DB_FALLBACK_DIR, 'solar_5min_avg_fallback.db')
SOLAR_DB_TOTALS_FALLBACK = os.path.join(SOLAR_DB_FALLBACK_DIR, 'solar_totals_fallback.db')

BOOTSTRAP_COPY_FILES = (
    'data_raw.db',
    'data_raw.db-shm',
    'data_raw.db-wal',
    'data_avg.db',
    'data_avg.db-shm',
    'data_avg.db-wal',
    'data_daily.db',
    'data_daily.db-shm',
    'data_daily.db-wal',
    'data_backup.db',
    'data_backup.db-shm',
    'data_backup.db-wal',
    'data_overview.db',
    'data_overview.db-shm',
    'data_overview.db-wal',
    'data.db',
    'data.db-shm',
    'data.db-wal',
    'p1_data_complete.db',
    'p1_data_complete.db-shm',
    'p1_data_complete.db-wal',
    'settings.json',
    'data.json',
    'raw_write_queue.jsonl',
)

_db_warning_once = set()
PEAK_THRESHOLD_W = 2500.0
SOLAR_LIVE_CACHE_TTL_SECONDS = 2.0
SOLAR_WINDOW_MAX_POINTS = 90
AVG_PROFILE_WINDOW_DAYS = 365
SOLAR_SAMPLE_STALE_AFTER_SECONDS = 60.0
SOLAR_LIVE_REFRESH_SECONDS = 30.0
SOLAR_POINT_MAX_AGE_SECONDS = 300.0
SERIES_CACHE_TTL_SECONDS = max(15.0, float(os.getenv('SERIES_CACHE_TTL_SECONDS', '30')))
SOLAR_PERSIST_INTERVAL_SECONDS = max(2.0, float(os.getenv('SOLAR_PERSIST_INTERVAL_SECONDS', '10')))
MARSTEK_CACHE_TTL_SECONDS = max(2.0, float(os.getenv('MARSTEK_CACHE_TTL_SECONDS', '2')))
BATTERY_PERSIST_INTERVAL_SECONDS = max(2.0, float(os.getenv('MARSTEK_PERSIST_INTERVAL_SECONDS', '10')))
SOLAR_ROLLUP_INTERVAL_SECONDS = max(60.0, float(os.getenv('SOLAR_ROLLUP_INTERVAL_SECONDS', '60')))
BATTERY_ROLLUP_INTERVAL_SECONDS = max(60.0, float(os.getenv('BATTERY_ROLLUP_INTERVAL_SECONDS', '60')))
PERSIST_REALTIME_SAMPLES = os.getenv('PERSIST_REALTIME_SAMPLES', 'false').lower() in ('1', 'true', 'yes', 'on')
MARSTEK_STABILITY_WINDOW = max(1, int(float(os.getenv('MARSTEK_STABILITY_WINDOW', '3'))))
LIVE_DENSIFY_POWER_HOLD_SECONDS = max(10.0, float(os.getenv('LIVE_DENSIFY_POWER_HOLD_SECONDS', '20')))
LIVE_DENSIFY_SOLAR_HOLD_SECONDS = max(10.0, float(os.getenv('LIVE_DENSIFY_SOLAR_HOLD_SECONDS', '20')))
LIVE_DENSIFY_BATTERY_HOLD_SECONDS = max(10.0, float(os.getenv('LIVE_DENSIFY_BATTERY_HOLD_SECONDS', '20')))
P1_LIVE_POLL_INTERVAL_SECONDS = max(
    2.0,
    float(os.getenv('P1_LIVE_POLL_INTERVAL_SECONDS', '2'))
)
LIVE_SAMPLE_STALE_AFTER_SECONDS = max(10.0, P1_LIVE_POLL_INTERVAL_SECONDS * 4.0)
BACKGROUND_LOOP_SLEEP_SECONDS = 0.5
P1_RUNTIME_WINDOW_SECONDS = 600.0
_solar_latest_cache = {
    'expires': 0.0,
    'data': {'timestamp': None, 'power_w': None},
}
_solar_fetch_lock = threading.Lock()
_last_solar_fetch_ts = 0.0
_last_solar_persist_ts = 0.0
_last_solar_rollup_ts = 0.0
_solar_runtime_state = {'timestamp': None, 'power_w': None}
_solar_runtime_series = []
_solar_history_write_lock = threading.RLock()
_solar_schema_checked_paths = set()
_solar_table_schema_cache = {}
_solar_series_cache = {}
_battery_series_cache = {}
_series_cache_lock = threading.Lock()
_marstek_status_cache = {'expires': 0.0, 'data': None}
_marstek_fetch_lock = threading.Lock()
_last_marstek_persist_ts = 0.0
_last_battery_rollup_ts = 0.0
_marstek_runtime_state = {'timestamp': None, 'power_w': None, 'consumption_w': None, 'soc_pct': None}
_marstek_runtime_series = []
_marstek_consumption_window = deque(maxlen=MARSTEK_STABILITY_WINDOW)
_marstek_power_window = deque(maxlen=MARSTEK_STABILITY_WINDOW)
_marstek_soc_window = deque(maxlen=MARSTEK_STABILITY_WINDOW)
_battery_schema_checked_paths = set()
_battery_table_schema_cache = {}
SUN2000_IP = (os.getenv('SUN2000_IP', '192.168.0.6')).strip()
SUN2000_PORT = int(os.getenv('SUN2000_PORT', os.getenv('SUN2000_MODBUS_PORT', '502')))
SUN2000_DEVICE_ID = (os.getenv('SUN2000_DEVICE_ID', 'sun2000') or 'sun2000').strip()
SUN2000_UNIT_ID_CANDIDATES = [
    int(item.strip())
    for item in os.getenv('SUN2000_UNIT_IDS', '1').split(',')
    if str(item).strip()
]
SUN2000_READ_TIMEOUT_SECONDS = float(os.getenv('SUN2000_READ_TIMEOUT_SECONDS', '10'))
SUN2000_STATUS_MAP = {
    0: 'Offline',
    1: 'Standby',
    2: 'Starting',
    3: 'Running',
    4: 'Grid sync',
    5: 'Shutdown',
    6: 'Standby',
    7: 'Fault',
}
_p1_runtime_lock = threading.Lock()
_p1_runtime_state = {'timestamp': None, 'live_power': None, 'raw_reading': None}
_p1_runtime_series = []


def _sun2000_read_holding_with_compat(client, address, count, unit_id):
    """Read holding registers across pymodbus 2.x/3.x argument variants."""
    read_func = client.read_holding_registers
    try:
        return read_func(address=address, count=count, unit=unit_id)
    except TypeError:
        try:
            return read_func(address=address, count=count, slave=unit_id)
        except TypeError:
            try:
                return read_func(address=address, count=count, device_id=unit_id)
            except TypeError:
                try:
                    return read_func(address, count, unit=unit_id)
                except TypeError:
                    try:
                        return read_func(address, count, slave=unit_id)
                    except TypeError:
                        return read_func(address, count, device_id=unit_id)


def _sun2000_decode_i32(registers):
    raw = (int(registers[0]) << 16) | int(registers[1])
    if raw & 0x80000000:
        raw -= 0x100000000
    return raw


def _sun2000_read_active_power_w(client):
    """Read signed active power from Sun2000 register 32080."""
    last_error = None
    for unit_id in SUN2000_UNIT_ID_CANDIDATES:
        try:
            result = _sun2000_read_holding_with_compat(client, address=32080, count=2, unit_id=unit_id)
            if not result.isError() and len(getattr(result, 'registers', []) or []) >= 2:
                return float(_sun2000_decode_i32(result.registers)), int(unit_id), None
            last_error = f'unit={unit_id} response={result}'
        except Exception as exc:
            last_error = f'unit={unit_id} error={exc}'
    return None, None, last_error


def _is_fresh_sample_timestamp(timestamp_value, max_age_seconds):
    """Return whether a numeric or formatted timestamp is recent enough."""
    if timestamp_value in (None, ''):
        return False

    try:
        sample_ts = _parse_solar_sample_timestamp(timestamp_value)
        return (time.time() - sample_ts) <= float(max_age_seconds)
    except Exception:
        return False

def _parse_solar_sample_timestamp(timestamp_value):
    """Parse solar sample timestamps from epoch or formatted strings as UTC."""
    if timestamp_value in (None, ''):
        return time.time()

    try:
        if isinstance(timestamp_value, (int, float)):
            return float(timestamp_value)

        text = str(timestamp_value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
        except ValueError:
            parsed = datetime.strptime(text, '%Y-%m-%d %H:%M:%S')

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except Exception:
        return time.time()


def _cache_solar_runtime_point(timestamp_value, power_w):
    """Keep a short in-memory solar history for the live tab without hammering SQLite."""
    global _solar_latest_cache, _solar_runtime_state, _solar_runtime_series

    if timestamp_value in (None, ''):
        timestamp_ts = time.time()
    else:
        try:
            timestamp_ts = _parse_solar_sample_timestamp(timestamp_value)
        except Exception:
            timestamp_ts = time.time()

    point = {
        'timestamp': float(timestamp_ts),
        'power_w': float(power_w) if power_w is not None else None,
    }
    _solar_runtime_state = dict(point)
    _solar_latest_cache = {
        'expires': time.time() + SOLAR_LIVE_CACHE_TTL_SECONDS,
        'data': dict(point),
    }

    _solar_runtime_series.append({
        'timestamp': point['timestamp'],
        'power': point['power_w'],
    })
    cutoff = time.time() - 600
    _solar_runtime_series = [entry for entry in _solar_runtime_series if float(entry.get('timestamp', 0) or 0) >= cutoff]
    return point


def _get_sqlite_cursor_cache_key(cursor):
    """Return a stable schema-cache key for a SQLite cursor."""
    try:
        cursor.execute('PRAGMA database_list')
        db_rows = cursor.fetchall()
        if db_rows:
            return os.path.abspath(str(db_rows[0][2] or 'main'))
    except Exception:
        pass
    return f'cursor:{id(getattr(cursor, "connection", cursor))}'


def ensure_solar_history_schema(cursor):
    """Ensure solar history tables exist for raw, realtime, and rollup reporting."""
    db_key = _get_sqlite_cursor_cache_key(cursor)
    if db_key in _solar_schema_checked_paths:
        return

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS solar_raw_data (
               timestamp REAL PRIMARY KEY,
               power_w REAL,
               unit_id INTEGER,
               source TEXT,
               created_at REAL
           )'''
    )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_solar_raw_ts ON solar_raw_data(timestamp)')
    cursor.execute('PRAGMA table_info(solar_raw_data)')
    raw_columns = {row[1] for row in cursor.fetchall()}
    if 'unit_id' not in raw_columns:
        cursor.execute('ALTER TABLE solar_raw_data ADD COLUMN unit_id INTEGER')
    if 'source' not in raw_columns:
        cursor.execute('ALTER TABLE solar_raw_data ADD COLUMN source TEXT')
    if 'created_at' not in raw_columns:
        cursor.execute('ALTER TABLE solar_raw_data ADD COLUMN created_at REAL')

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS solar_realtime (
               timestamp TEXT PRIMARY KEY,
               power_w REAL,
               unit_id INTEGER,
               source TEXT,
               created_at REAL
           )'''
    )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_solar_realtime_ts ON solar_realtime(timestamp)')
    cursor.execute('PRAGMA table_info(solar_realtime)')
    solar_columns = {row[1] for row in cursor.fetchall()}
    if 'unit_id' not in solar_columns:
        cursor.execute('ALTER TABLE solar_realtime ADD COLUMN unit_id INTEGER')
    if 'source' not in solar_columns:
        cursor.execute('ALTER TABLE solar_realtime ADD COLUMN source TEXT')
    if 'created_at' not in solar_columns:
        cursor.execute('ALTER TABLE solar_realtime ADD COLUMN created_at REAL')

    cursor.execute(
        '''INSERT OR IGNORE INTO solar_raw_data (timestamp, power_w, unit_id, source, created_at)
           SELECT CAST(strftime('%s', timestamp) AS REAL),
                  power_w,
                  unit_id,
                  source,
                  COALESCE(NULLIF(CAST(created_at AS REAL), 0), CAST(strftime('%s', timestamp) AS REAL))
           FROM solar_realtime
           WHERE timestamp IS NOT NULL
             AND power_w IS NOT NULL'''
    )

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS five_minute_averages (
               bucket_start TEXT PRIMARY KEY,
               avg_power_w REAL,
               max_power_w REAL,
               sample_count INTEGER,
               energy_kwh REAL,
               updated_at REAL
           )'''
    )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_solar_5min_bucket_start ON five_minute_averages(bucket_start)')
    cursor.execute('PRAGMA table_info(five_minute_averages)')
    avg_columns = {row[1] for row in cursor.fetchall()}
    if 'sample_count' not in avg_columns:
        cursor.execute('ALTER TABLE five_minute_averages ADD COLUMN sample_count INTEGER')
    if 'energy_kwh' not in avg_columns:
        cursor.execute('ALTER TABLE five_minute_averages ADD COLUMN energy_kwh REAL')
    if 'updated_at' not in avg_columns:
        cursor.execute('ALTER TABLE five_minute_averages ADD COLUMN updated_at REAL')

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS daily_totals (
               date TEXT PRIMARY KEY,
               total_energy_kwh REAL,
               updated_at REAL
           )'''
    )

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS monthly_totals (
               month TEXT PRIMARY KEY,
               total_energy_kwh REAL,
               updated_at REAL
           )'''
    )

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS yearly_totals (
               year INTEGER PRIMARY KEY,
               total_energy_kwh REAL,
               updated_at REAL
           )'''
    )

    _solar_schema_checked_paths.add(db_key)
    _solar_table_schema_cache.pop((db_key, 'solar_raw_data'), None)
    _solar_table_schema_cache.pop((db_key, 'solar_realtime'), None)


def _upsert_solar_raw_sample(cursor, timestamp_ts, power_w, unit_id=None, source=None, created_at=None):
    """Insert one raw solar sample for the live 2-second history workflow."""
    db_key = _get_sqlite_cursor_cache_key(cursor)
    cache_key = (db_key, 'solar_raw_data')
    table_columns = _solar_table_schema_cache.get(cache_key)
    if table_columns is None:
        cursor.execute('PRAGMA table_info(solar_raw_data)')
        table_columns = [row[1] for row in cursor.fetchall()]
        _solar_table_schema_cache[cache_key] = table_columns
    created_at = time.time() if created_at is None else created_at

    values_by_column = {
        'timestamp': float(timestamp_ts),
        'power_w': float(power_w or 0.0),
        'unit_id': int(unit_id) if isinstance(unit_id, (int, float)) else None,
        'source': str(source or 'live-modbus'),
        'created_at': float(created_at),
    }

    insert_columns = [column for column in table_columns if column in values_by_column]
    placeholders = ', '.join('?' for _ in insert_columns)
    column_sql = ', '.join(insert_columns)
    params = tuple(values_by_column[column] for column in insert_columns)
    cursor.execute(
        f'INSERT OR REPLACE INTO solar_raw_data ({column_sql}) VALUES ({placeholders})',
        params,
    )


def _upsert_solar_realtime_sample(cursor, timestamp_text, power_w, unit_id=None, source=None, status=None, created_at=None):
    """Insert one solar realtime sample across both new and legacy realtime table layouts."""
    db_key = _get_sqlite_cursor_cache_key(cursor)
    cache_key = (db_key, 'solar_realtime')
    cached_schema = _solar_table_schema_cache.get(cache_key)
    if cached_schema is None:
        cursor.execute('PRAGMA table_info(solar_realtime)')
        column_rows = cursor.fetchall()
        table_columns = [row[1] for row in column_rows]
        column_types = {row[1]: str(row[2] or '').upper() for row in column_rows}
        _solar_table_schema_cache[cache_key] = (table_columns, column_types)
    else:
        table_columns, column_types = cached_schema

    created_at = time.time() if created_at is None else created_at
    created_at_value = created_at if 'REAL' in column_types.get('created_at', '') else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    values_by_column = {
        'timestamp': timestamp_text,
        'model': str(SUN2000_DEVICE_ID or 'sun2000'),
        'status': str(status or 'Running'),
        'power_w': float(power_w or 0.0),
        'pv_v': 0.0,
        'pv_a': 0.0,
        'grid_v': 0.0,
        'grid_a': 0.0,
        'temp_c': 0.0,
        'today_kwh': 0.0,
        'created_at': created_at_value,
        'unit_id': int(unit_id) if isinstance(unit_id, (int, float)) else None,
        'source': str(source or 'live-modbus'),
    }

    insert_columns = [column for column in table_columns if column in values_by_column]
    placeholders = ', '.join('?' for _ in insert_columns)
    column_sql = ', '.join(insert_columns)
    params = tuple(values_by_column[column] for column in insert_columns)
    cursor.execute(
        f'INSERT OR REPLACE INTO solar_realtime ({column_sql}) VALUES ({placeholders})',
        params,
    )


def _persist_solar_sample_to_history(sample, _allow_recovery=True):
    """Persist one robust solar sample and update rollup totals."""
    global _last_solar_rollup_ts

    if not sample:
        return False

    power_w = sample.get('current_power_w')
    if power_w is None:
        return False

    sample_ts = _parse_solar_sample_timestamp(sample.get('timestamp'))
    sample_dt_utc = datetime.fromtimestamp(float(sample_ts), timezone.utc)
    timestamp_text = sample_dt_utc.strftime('%Y-%m-%d %H:%M:%S')

    bucket_epoch = int(float(sample_ts) // 300) * 300
    bucket_start_dt = datetime.fromtimestamp(bucket_epoch, timezone.utc)
    bucket_start = bucket_start_dt.strftime('%Y-%m-%d %H:%M:%S')

    day_start = sample_dt_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    day_key = day_start.strftime('%Y-%m-%d')
    month_key = sample_dt_utc.strftime('%Y-%m')
    year_key = int(sample_dt_utc.strftime('%Y'))

    acquired = _solar_history_write_lock.acquire(timeout=30.0)
    if not acquired:
        print('Solar history persist skipped: write lock timeout')
        return False

    raw_conn = None
    avg_conn = None
    totals_conn = None
    try:
        raw_conn = open_solar_history_connection(kind='raw', write=True)
        if raw_conn is None:
            return False

        raw_cursor = raw_conn.cursor()
        ensure_solar_history_schema(raw_cursor)

        unit_id = sample.get('unit_id')
        source = sample.get('source', 'live-modbus')
        created_at = time.time()
        _upsert_solar_raw_sample(
            raw_cursor,
            sample_ts,
            power_w,
            unit_id=unit_id,
            source=source,
            created_at=created_at,
        )
        if PERSIST_REALTIME_SAMPLES:
            _upsert_solar_realtime_sample(
                raw_cursor,
                timestamp_text,
                power_w,
                unit_id=unit_id,
                source=source,
                status=sample.get('status', 'Running'),
                created_at=created_at,
            )
        raw_conn.commit()

        rollup_now = time.time()
        if rollup_now - _last_solar_rollup_ts < SOLAR_ROLLUP_INTERVAL_SECONDS:
            return True

        avg_conn = open_solar_history_connection(kind='avg', write=True)
        totals_conn = open_solar_history_connection(kind='totals', write=True)
        avg_conn = _reuse_if_same_sqlite_file(raw_conn, avg_conn)
        totals_conn = _reuse_if_same_sqlite_file(avg_conn, totals_conn)
        totals_conn = _reuse_if_same_sqlite_file(raw_conn, totals_conn)
        if avg_conn is None or totals_conn is None:
            return True

        avg_cursor = avg_conn.cursor()
        totals_cursor = totals_conn.cursor()
        ensure_solar_history_schema(avg_cursor)
        ensure_solar_history_schema(totals_cursor)

        bucket_end_epoch = bucket_epoch + 300
        raw_cursor.execute(
            '''SELECT AVG(power_w), MAX(power_w), COUNT(*)
               FROM solar_raw_data
               WHERE timestamp >= ? AND timestamp < ?''',
            (float(bucket_epoch), float(bucket_end_epoch)),
        )
        avg_power_w, max_power_w, sample_count = raw_cursor.fetchone()
        avg_power_w = float(avg_power_w) if avg_power_w is not None else None
        max_power_w = float(max_power_w) if max_power_w is not None else None
        sample_count = int(sample_count or 0)
        energy_kwh = ((avg_power_w or 0.0) * (5.0 / 60.0) / 1000.0) if sample_count > 0 else 0.0

        _upsert_solar_five_minute_average(
            avg_cursor,
            bucket_start,
            avg_power_w,
            max_power_w,
            sample_count,
            energy_kwh,
            updated_at=time.time(),
        )
        avg_conn.commit()

        avg_cursor.execute(
            '''SELECT SUM(energy_kwh), AVG(avg_power_w), MAX(max_power_w), COUNT(*)
               FROM five_minute_averages
               WHERE bucket_start >= ? AND bucket_start < ?''',
            (day_start.strftime('%Y-%m-%d %H:%M:%S'), day_end.strftime('%Y-%m-%d %H:%M:%S')),
        )
        day_total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
        _upsert_solar_total_row(
            totals_cursor,
            'daily_totals',
            'date',
            day_key,
            day_total_row[0],
            day_total_row[1],
            day_total_row[2],
            day_total_row[3],
            updated_at=time.time(),
        )

        month_start = day_start.replace(day=1)
        next_month_start = (month_start + timedelta(days=32)).replace(day=1)
        avg_cursor.execute(
            '''SELECT SUM(energy_kwh), AVG(avg_power_w), MAX(max_power_w), COUNT(DISTINCT substr(bucket_start, 1, 10))
               FROM five_minute_averages
               WHERE bucket_start >= ? AND bucket_start < ?''',
            (month_start.strftime('%Y-%m-%d %H:%M:%S'), next_month_start.strftime('%Y-%m-%d %H:%M:%S')),
        )
        month_total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
        _upsert_solar_total_row(
            totals_cursor,
            'monthly_totals',
            'month',
            month_key,
            month_total_row[0],
            month_total_row[1],
            month_total_row[2],
            month_total_row[3],
            updated_at=time.time(),
        )

        year_start = day_start.replace(month=1, day=1)
        next_year_start = year_start.replace(year=year_start.year + 1)
        avg_cursor.execute(
            '''SELECT SUM(energy_kwh), AVG(avg_power_w), MAX(max_power_w), COUNT(DISTINCT substr(bucket_start, 1, 7))
               FROM five_minute_averages
               WHERE bucket_start >= ? AND bucket_start < ?''',
            (year_start.strftime('%Y-%m-%d %H:%M:%S'), next_year_start.strftime('%Y-%m-%d %H:%M:%S')),
        )
        year_total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
        _upsert_solar_total_row(
            totals_cursor,
            'yearly_totals',
            'year',
            year_key,
            year_total_row[0],
            year_total_row[1],
            year_total_row[2],
            year_total_row[3],
            updated_at=time.time(),
        )

        totals_conn.commit()
        _last_solar_rollup_ts = rollup_now
        return True
    except Exception as e:
        if _allow_recovery and _is_sqlite_storage_error(e):
            if reset_solar_db_files(reason=f'solar_persist_repair: {e}'):
                return _persist_solar_sample_to_history(sample, _allow_recovery=False)
            if activate_local_solar_db_fallback(reason=f'solar_persist_failure: {e}'):
                return _persist_solar_sample_to_history(sample, _allow_recovery=False)
        print(f'Solar history persist failed: {e}')
        return False
    finally:
        for conn in {raw_conn, avg_conn, totals_conn}:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        _solar_history_write_lock.release()


def _upsert_solar_five_minute_average(cursor, bucket_start, avg_power_w, max_power_w, sample_count, energy_kwh, updated_at=None):
    """Insert one solar 5-minute aggregate row across both new and legacy table layouts."""
    cursor.execute('PRAGMA table_info(five_minute_averages)')
    table_columns = [row[1] for row in cursor.fetchall()]
    updated_at = time.time() if updated_at is None else updated_at

    values_by_column = {
        'bucket_start': bucket_start,
        'model': SUN2000_DEVICE_ID or 'sun2000',
        'status': 'Running',
        'avg_pv_v': 0.0,
        'avg_pv_a': 0.0,
        'avg_power_w': float(avg_power_w or 0.0),
        'max_power_w': float(max_power_w or 0.0),
        'avg_today_kwh': 0.0,
        'max_today_kwh': 0.0,
        'avg_grid_v': 0.0,
        'avg_grid_a': 0.0,
        'avg_temp_c': 0.0,
        'state': 3,
        'alarm': 0,
        'sample_count': int(sample_count or 0),
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'energy_kwh': float(energy_kwh or 0.0),
        'updated_at': float(updated_at),
    }

    insert_columns = [column for column in table_columns if column in values_by_column]
    placeholders = ', '.join('?' for _ in insert_columns)
    column_sql = ', '.join(insert_columns)
    params = tuple(values_by_column[column] for column in insert_columns)
    cursor.execute(
        f'INSERT OR REPLACE INTO five_minute_averages ({column_sql}) VALUES ({placeholders})',
        params,
    )


def _upsert_solar_total_row(cursor, table_name, key_column, key_value, total_energy_kwh, avg_power_w, peak_power_w, period_count, updated_at=None):
    """Insert one solar total row across both new and legacy totals-table layouts."""
    cursor.execute(f'PRAGMA table_info({table_name})')
    column_rows = cursor.fetchall()
    table_columns = [row[1] for row in column_rows]
    column_types = {row[1]: str(row[2] or '').upper() for row in column_rows}

    updated_at = time.time() if updated_at is None else updated_at
    updated_at_value = updated_at if 'REAL' in column_types.get('updated_at', '') else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    key_value_out = int(key_value) if 'INT' in column_types.get(key_column, '') and str(key_value).isdigit() else str(key_value)

    values_by_column = {
        key_column: key_value_out,
        'total_energy_kwh': float(total_energy_kwh or 0.0),
        'avg_power_w': float(avg_power_w or 0.0),
        'peak_power_w': float(peak_power_w or 0.0),
        'buckets': int(period_count or 0),
        'days': int(period_count or 0),
        'months': int(period_count or 0),
        'updated_at': updated_at_value,
    }

    insert_columns = [column for column in table_columns if column in values_by_column]
    placeholders = ', '.join('?' for _ in insert_columns)
    column_sql = ', '.join(insert_columns)
    params = tuple(values_by_column[column] for column in insert_columns)
    cursor.execute(
        f'INSERT OR REPLACE INTO {table_name} ({column_sql}) VALUES ({placeholders})',
        params,
    )


def rebuild_solar_rollups_from_history(start_ts=None, end_ts=None):
    """Rebuild solar 5-minute and calendar rollups from stored raw solar samples."""
    acquired = _solar_history_write_lock.acquire(timeout=30.0)
    if not acquired:
        print('Solar rollup rebuild skipped: write lock timeout')
        return 0

    raw_conn = None
    avg_conn = None
    totals_conn = None
    try:
        raw_conn = open_solar_history_connection(kind='raw', write=True)
        avg_conn = open_solar_history_connection(kind='avg', write=True)
        totals_conn = open_solar_history_connection(kind='totals', write=True)
        avg_conn = _reuse_if_same_sqlite_file(raw_conn, avg_conn)
        totals_conn = _reuse_if_same_sqlite_file(avg_conn, totals_conn)
        totals_conn = _reuse_if_same_sqlite_file(raw_conn, totals_conn)
        if raw_conn is None or avg_conn is None or totals_conn is None:
            return 0

        raw_cursor = raw_conn.cursor()
        avg_cursor = avg_conn.cursor()
        totals_cursor = totals_conn.cursor()
        ensure_solar_history_schema(raw_cursor)
        ensure_solar_history_schema(avg_cursor)
        ensure_solar_history_schema(totals_cursor)

        if start_ts is None or end_ts is None:
            raw_cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM solar_raw_data')
            min_row = raw_cursor.fetchone()
            if not min_row or min_row[0] in (None, '') or min_row[1] in (None, ''):
                raw_cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM solar_realtime')
                min_row = raw_cursor.fetchone()
            if not min_row or min_row[0] in (None, '') or min_row[1] in (None, ''):
                avg_conn.commit()
                totals_conn.commit()
                return 0
            start_ts = _parse_solar_sample_timestamp(min_row[0])
            end_ts = _parse_solar_sample_timestamp(min_row[1]) + 300

        start_ts = float(start_ts)
        end_ts = max(float(end_ts), start_ts + 1)
        bucket_start_ts = int(start_ts // 300) * 300
        bucket_end_ts = ((int(end_ts - 1) // 300) + 1) * 300

        start_dt = datetime.fromtimestamp(bucket_start_ts, timezone.utc)
        end_dt = datetime.fromtimestamp(bucket_end_ts, timezone.utc)
        start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        # Do NOT bulk-delete existing averages here.  If raw data is missing for
        # part of the window (e.g. after a DB restore) we want to keep whatever
        # averaged rows already exist so the chart does not go blank.
        # _upsert_solar_five_minute_average will overwrite individual buckets
        # only when fresh raw data is actually available.
        raw_cursor.execute(
            '''SELECT CAST(timestamp / 300 AS INTEGER) * 300 AS bucket_ts,
                      AVG(power_w) AS avg_power_w,
                      MAX(power_w) AS max_power_w,
                      COUNT(*) AS sample_count
               FROM solar_raw_data
               WHERE timestamp >= ? AND timestamp < ?
                 AND power_w IS NOT NULL
               GROUP BY CAST(timestamp / 300 AS INTEGER) * 300
               ORDER BY bucket_ts''',
            (float(bucket_start_ts), float(bucket_end_ts)),
        )
        rows = raw_cursor.fetchall()
        if not rows:
            raw_cursor.execute(
                '''SELECT CAST(CAST(strftime('%s', timestamp) AS INTEGER) / 300 AS INTEGER) * 300 AS bucket_ts,
                          AVG(power_w) AS avg_power_w,
                          MAX(power_w) AS max_power_w,
                          COUNT(*) AS sample_count
                   FROM solar_realtime
                   WHERE timestamp >= ? AND timestamp < ?
                     AND power_w IS NOT NULL
                   GROUP BY CAST(CAST(strftime('%s', timestamp) AS INTEGER) / 300 AS INTEGER) * 300
                   ORDER BY bucket_ts''',
                (start_str, end_str),
            )
            rows = raw_cursor.fetchall()

        touched_days = set()
        touched_months = set()
        touched_years = set()
        rebuilt_count = 0

        for row in rows:
            point_ts = _normalize_bucket_timestamp_ms(row['bucket_ts'], bucket_seconds=300)
            if point_ts is None:
                continue

            bucket_dt = datetime.fromtimestamp(point_ts / 1000.0, timezone.utc)
            bucket_start = bucket_dt.strftime('%Y-%m-%d %H:%M:%S')
            avg_power_w = float(row['avg_power_w']) if row['avg_power_w'] is not None else None
            max_power_w = float(row['max_power_w']) if row['max_power_w'] is not None else None
            sample_count = int(row['sample_count'] or 0)
            energy_kwh = ((avg_power_w or 0.0) * (5.0 / 60.0) / 1000.0) if sample_count > 0 else 0.0

            _upsert_solar_five_minute_average(
                avg_cursor,
                bucket_start,
                avg_power_w,
                max_power_w,
                sample_count,
                energy_kwh,
                updated_at=time.time(),
            )
            rebuilt_count += 1
            touched_days.add(bucket_dt.strftime('%Y-%m-%d'))
            touched_months.add(bucket_dt.strftime('%Y-%m'))
            touched_years.add(bucket_dt.year)

        avg_conn.commit()

        for day_key in sorted(touched_days):
            day_dt = datetime.strptime(day_key, '%Y-%m-%d')
            next_day = day_dt + timedelta(days=1)
            avg_cursor.execute(
                '''SELECT SUM(energy_kwh), AVG(avg_power_w), MAX(max_power_w), COUNT(*)
                   FROM five_minute_averages
                   WHERE bucket_start >= ? AND bucket_start < ?''',
                (day_dt.strftime('%Y-%m-%d %H:%M:%S'), next_day.strftime('%Y-%m-%d %H:%M:%S')),
            )
            total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
            _upsert_solar_total_row(
                totals_cursor,
                'daily_totals',
                'date',
                day_key,
                total_row[0],
                total_row[1],
                total_row[2],
                total_row[3],
                updated_at=time.time(),
            )

        for month_key in sorted(touched_months):
            month_dt = datetime.strptime(f'{month_key}-01', '%Y-%m-%d')
            next_month = (month_dt.replace(day=28) + timedelta(days=4)).replace(day=1)
            avg_cursor.execute(
                '''SELECT SUM(energy_kwh), AVG(avg_power_w), MAX(max_power_w), COUNT(DISTINCT substr(bucket_start, 1, 10))
                   FROM five_minute_averages
                   WHERE bucket_start >= ? AND bucket_start < ?''',
                (month_dt.strftime('%Y-%m-%d %H:%M:%S'), next_month.strftime('%Y-%m-%d %H:%M:%S')),
            )
            total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
            _upsert_solar_total_row(
                totals_cursor,
                'monthly_totals',
                'month',
                month_key,
                total_row[0],
                total_row[1],
                total_row[2],
                total_row[3],
                updated_at=time.time(),
            )

        for year_key in sorted(touched_years):
            avg_cursor.execute(
                '''SELECT SUM(energy_kwh), AVG(avg_power_w), MAX(max_power_w), COUNT(DISTINCT substr(bucket_start, 1, 7))
                   FROM five_minute_averages
                   WHERE bucket_start >= ? AND bucket_start < ?''',
                (f'{year_key}-01-01 00:00:00', f'{year_key + 1}-01-01 00:00:00'),
            )
            total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
            _upsert_solar_total_row(
                totals_cursor,
                'yearly_totals',
                'year',
                year_key,
                total_row[0],
                total_row[1],
                total_row[2],
                total_row[3],
                updated_at=time.time(),
            )

        totals_conn.commit()
        return rebuilt_count
    except Exception as e:
        print(f'Solar rollup rebuild failed: {e}')
        return 0
    finally:
        for conn in {raw_conn, avg_conn, totals_conn}:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        _solar_history_write_lock.release()


def _clear_solar_runtime_cache():
    """Drop any stale in-memory solar points so the live chart does not freeze on old values."""
    global _solar_latest_cache, _solar_runtime_state, _solar_runtime_series
    _solar_latest_cache = {
        'expires': 0.0,
        'data': {'timestamp': None, 'power_w': None},
    }
    _solar_runtime_state = {'timestamp': None, 'power_w': None}
    _solar_runtime_series = []


def _normalize_live_window_timestamp(timestamp_value, step_seconds=2.0):
    """Normalize a live-sample timestamp onto a stable 2-second grid."""
    if timestamp_value in (None, ''):
        return None
    try:
        timestamp_ts = float(timestamp_value)
    except (TypeError, ValueError):
        return None
    step_seconds = max(1.0, float(step_seconds or 2.0))
    return round(timestamp_ts / step_seconds) * step_seconds


def _cache_p1_runtime_point(timestamp_value, live_power, raw_reading=None):
    """Keep recent P1 samples in memory so the live tab stays smooth even on NAS-backed storage."""
    global _p1_runtime_state, _p1_runtime_series

    timestamp_ts = _normalize_live_window_timestamp(timestamp_value, step_seconds=2.0)
    if timestamp_ts is None:
        timestamp_ts = _normalize_live_window_timestamp(time.time(), step_seconds=2.0) or time.time()

    point = {
        'timestamp': float(timestamp_ts),
        'live_power': float(live_power) if live_power is not None else None,
        'raw_reading': dict(raw_reading) if isinstance(raw_reading, dict) else raw_reading,
    }

    with _p1_runtime_lock:
        _p1_runtime_state = dict(point)
        replaced = False
        for entry in _p1_runtime_series:
            if abs(float(entry.get('timestamp', 0) or 0) - point['timestamp']) < 0.001:
                entry['power'] = point['live_power']
                replaced = True
                break
        if not replaced:
            _p1_runtime_series.append({
                'timestamp': point['timestamp'],
                'power': point['live_power'],
            })
        cutoff = time.time() - P1_RUNTIME_WINDOW_SECONDS
        _p1_runtime_series = sorted(
            [entry for entry in _p1_runtime_series if float(entry.get('timestamp', 0) or 0) >= cutoff],
            key=lambda item: float(item.get('timestamp', 0) or 0),
        )

    _append_live_power_temp_point(point['timestamp'], live_power=point['live_power'])
    return point


def _get_p1_runtime_points(start_timestamp):
    """Return recent in-memory P1 points for the requested live chart window."""
    with _p1_runtime_lock:
        return [
            {'timestamp': float(item['timestamp']), 'power': item.get('power')}
            for item in _p1_runtime_series
            if float(item.get('timestamp', 0) or 0) >= float(start_timestamp)
        ]


def _init_live_power_temp_db():
    """Create the dedicated temporary DB used by the live tab."""
    conn = None
    try:
        conn = get_db_connection(DB_FILE_LIVE_TEMP, write=True)
        cursor = conn.cursor()
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS live_power_samples (
                   timestamp REAL PRIMARY KEY,
                   power REAL,
                   updated_at REAL
               )'''
        )
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS live_solar_samples (
                   timestamp REAL PRIMARY KEY,
                   power REAL,
                   updated_at REAL
               )'''
        )
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS live_meta (
                   key TEXT PRIMARY KEY,
                   value REAL
               )'''
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"Live temp DB init failed: {e}")
        return False
    finally:
        if conn:
            conn.close()


def _write_live_power_temp_window(power_points, solar_points, window_start_ts, window_end_ts):
    """Refresh the dedicated live-tab temp DB incrementally without rebuilding tables each poll."""
    if not _init_live_power_temp_db():
        return False

    conn = None
    try:
        conn = get_db_connection(DB_FILE_LIVE_TEMP, write=True)
        cursor = conn.cursor()
        now_ts = time.time()
        lower_bound = float(window_start_ts) - 5.0
        upper_bound = float(window_end_ts) + 5.0

        cursor.execute('DELETE FROM live_power_samples WHERE timestamp < ? OR timestamp > ?', (lower_bound, upper_bound))
        cursor.execute('DELETE FROM live_solar_samples WHERE timestamp < ? OR timestamp > ?', (lower_bound, upper_bound))

        if power_points:
            cursor.executemany(
                'INSERT OR REPLACE INTO live_power_samples (timestamp, power, updated_at) VALUES (?, ?, ?)',
                [
                    (
                        float(point['timestamp']),
                        float(point['power']) if point.get('power') is not None else None,
                        now_ts,
                    )
                    for point in power_points
                    if point.get('timestamp') is not None
                ],
            )

        if solar_points:
            cursor.executemany(
                'INSERT OR REPLACE INTO live_solar_samples (timestamp, power, updated_at) VALUES (?, ?, ?)',
                [
                    (
                        float(point['timestamp']),
                        float(point['power']) if point.get('power') is not None else None,
                        now_ts,
                    )
                    for point in solar_points
                    if point.get('timestamp') is not None
                ],
            )

        cursor.executemany(
            'INSERT OR REPLACE INTO live_meta (key, value) VALUES (?, ?)',
            [
                ('window_start_ts', float(window_start_ts)),
                ('window_end_ts', float(window_end_ts)),
                ('updated_at', now_ts),
            ],
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"Live temp DB refresh failed: {e}")
        return False
    finally:
        if conn:
            conn.close()


def _append_live_power_temp_point(timestamp_value, live_power=None, solar_power=None):
    """Append the newest tail sample into the live-tab temp DB when it already exists."""
    if not os.path.exists(DB_FILE_LIVE_TEMP):
        return

    point_ts = _normalize_live_window_timestamp(timestamp_value, step_seconds=2.0)
    if point_ts is None:
        return

    conn = None
    try:
        conn = get_db_connection(DB_FILE_LIVE_TEMP, write=True)
        cursor = conn.cursor()
        now_ts = time.time()
        cutoff = now_ts - P1_RUNTIME_WINDOW_SECONDS

        if live_power is not None:
            cursor.execute(
                'INSERT OR REPLACE INTO live_power_samples (timestamp, power, updated_at) VALUES (?, ?, ?)',
                (float(point_ts), float(live_power), now_ts),
            )
        if solar_power is not None:
            cursor.execute(
                'INSERT OR REPLACE INTO live_solar_samples (timestamp, power, updated_at) VALUES (?, ?, ?)',
                (float(point_ts), float(solar_power), now_ts),
            )

        cursor.execute('DELETE FROM live_power_samples WHERE timestamp < ?', (cutoff,))
        cursor.execute('DELETE FROM live_solar_samples WHERE timestamp < ?', (cutoff,))
        conn.commit()
    except Exception as e:
        print(f"Live temp DB append failed: {e}")
    finally:
        if conn:
            conn.close()


HISTORICAL_LIFETIME_TOTALS_MWH = {
    2023: {
        'solar_yield_mwh': 4.61,
        'injection_mwh': 3.088,
        'consumption_mwh': 4.715,
    },
    2024: {
        'solar_yield_mwh': 4.36,
        'injection_mwh': 2.736,
        'consumption_mwh': 4.891,
    },
    2025: {
        'solar_yield_mwh': 5.09,
        'injection_mwh': 3.29,
        'consumption_mwh': 5.281,
    },
}
MANUAL_MONTHLY_TOTALS_KWH = {
    '2026-01': {
        'consumption_kwh': 549.0,
        'injection_kwh': 43.9,
        'solar_yield_kwh': 107.99,
    },
    '2026-02': {
        'consumption_kwh': 484.0,
        'injection_kwh': 82.1,
        'solar_yield_kwh': 168.60,
    },
    '2026-03': {
        'consumption_kwh': 432.0,
        'injection_kwh': 318.0,
        'solar_yield_kwh': 487.61,
    },
    '2026-04': {
        'consumption_kwh': 333.0,
    },
}

# Elia public API configuration for Belgian imbalance prices
ELIA_API_BASE_URL = 'https://opendata.elia.be/api/explore/v2.1/catalog/datasets'
ELIA_IMBALANCE_PRICES_NEAR_REALTIME_DATASET = 'ods162'
ELIA_IMBALANCE_PRICES_HISTORICAL_DATASET = 'ods134'

# Email notification configuration
DEFAULT_EMAIL_FROM = os.getenv('EMAIL_FROM', '')
DEFAULT_EMAIL_TO = os.getenv('EMAIL_TO', '')
DEFAULT_INJECTION_THRESHOLD = 500
DEFAULT_NOTIFICATIONS_ENABLED = True

EMAIL_FROM = DEFAULT_EMAIL_FROM
EMAIL_TO = DEFAULT_EMAIL_TO  # For SMS via email gateway
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', '')
SMTP_PASS = os.getenv('SMTP_PASS', '')
DEFAULT_SMTP_USER = SMTP_USER
DEFAULT_SMTP_PASS = SMTP_PASS

# Server binding configuration
SERVER_HOST = os.getenv('SERVER_HOST', '0.0.0.0')  # Use specific IP if needed
SERVER_PORT = int(os.getenv('SERVER_PORT', '8000'))
DEBUG_MODE = os.getenv('FLASK_DEBUG', 'false').lower() in ('1', 'true', 'yes', 'on')
ENABLE_BACKGROUND_THREADS = os.getenv('P1_ENABLE_BACKGROUND_THREADS', 'true').lower() in ('1', 'true', 'yes', 'on')
DATA_DIR_IS_NETWORK_PATH = os.path.abspath(DATA_DIR).startswith('\\\\')
DEFAULT_SQLITE_JOURNAL_MODE = 'DELETE' if DATA_DIR_IS_NETWORK_PATH else 'WAL'
SQLITE_JOURNAL_MODE = os.getenv('SQLITE_JOURNAL_MODE', DEFAULT_SQLITE_JOURNAL_MODE).strip().upper()
SQLITE_WRITE_SYNCHRONOUS = 'NORMAL' if DATA_DIR_IS_NETWORK_PATH else 'FULL'
ALLOW_RAW_DB_AUTO_RESTORE = os.getenv('P1_ALLOW_RAW_DB_AUTO_RESTORE', 'true').lower() in ('1', 'true', 'yes', 'on')
ASYNC_STARTUP_MAINTENANCE = os.getenv('P1_ASYNC_STARTUP_MAINTENANCE', 'true').lower() in ('1', 'true', 'yes', 'on')


def is_network_sqlite_path(path_value):
    """Return whether the SQLite file lives on a UNC or network-backed path."""
    try:
        normalized = os.path.abspath(str(path_value or ''))
    except Exception:
        normalized = str(path_value or '')
    return normalized.startswith('\\') or normalized.startswith('//')


def normalize_url_prefix(raw_prefix):
    """Normalize an optional reverse-proxy URL prefix like '/p1-dashboard'."""
    prefix = str(raw_prefix or '').strip()
    if not prefix or prefix == '/':
        return ''
    return '/' + prefix.strip('/')


APPLICATION_ROOT = normalize_url_prefix(os.getenv('APPLICATION_ROOT', ''))
app.config['APPLICATION_ROOT'] = APPLICATION_ROOT

# Optional admin token for protecting sensitive endpoints
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')
DEFAULT_ADMIN_TOKEN = ADMIN_TOKEN
runtime_admin_token = DEFAULT_ADMIN_TOKEN

# Notification state
excess_start_time = None
notification_sent = False
last_notification_date = None
injection_threshold = DEFAULT_INJECTION_THRESHOLD
notifications_enabled = DEFAULT_NOTIFICATIONS_ENABLED


def check_db_integrity(db_file):
    """Return (is_ok, detail) for the active database backend."""
    if is_postgres_enabled():
        return True, 'postgresql'

    if not os.path.exists(db_file):
        return True, 'missing'

    conn = None
    try:
        conn = sqlite3.connect(db_file, timeout=DB_BUSY_TIMEOUT_MS / 1000)
        cursor = conn.cursor()
        cursor.execute('PRAGMA integrity_check')
        row = cursor.fetchone()
        detail = row[0] if row and row[0] is not None else 'unknown'
        return detail == 'ok', str(detail)
    except Exception as e:
        return False, str(e)
    finally:
        if conn:
            conn.close()


def backup_file_with_timestamp(file_path, suffix):
    """Create a timestamped copy of a file and return the backup path."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f'{file_path}.{suffix}_{timestamp}'
    shutil.copy2(file_path, backup_path)
    return backup_path


def restore_raw_db_from_backup(reason=''):
    """Restore the raw database from backup when corruption is detected."""
    if is_postgres_enabled():
        print('Raw DB restore is not used in PostgreSQL mode')
        return False

    if not os.path.exists(DB_FILE_BACKUP):
        print('Raw DB restore skipped: backup DB does not exist yet')
        return False

    backup_ok, backup_detail = check_db_integrity(DB_FILE_BACKUP)
    if not backup_ok:
        print(f'Raw DB restore skipped: backup DB integrity failed ({backup_detail})')
        return False

    if os.path.exists(DB_FILE_RAW):
        try:
            raw_backup = backup_file_with_timestamp(DB_FILE_RAW, 'corrupt_backup')
            print(f'Backed up current raw DB before restore: {raw_backup}')
        except Exception as e:
            print(f'Raw DB restore warning: failed to backup corrupted DB ({e})')

    try:
        shutil.copy2(DB_FILE_BACKUP, DB_FILE_RAW)
        for suffix in ('-wal', '-shm'):
            sidecar = f'{DB_FILE_RAW}{suffix}'
            if os.path.exists(sidecar):
                os.remove(sidecar)
        print(f'✓ Restored raw DB from backup ({reason or "integrity failure"})')
        return True
    except Exception as e:
        print(f'Raw DB restore failed: {e}')
        return False


def activate_local_raw_db_fallback(reason=''):
    """Switch raw data writes to a local temp SQLite DB when UNC storage is unavailable."""
    global DB_FILE_RAW, RAW_WRITE_QUEUE_FILE

    if is_postgres_enabled():
        print('Raw DB fallback is not used in PostgreSQL mode')
        return False

    try:
        fallback_dir = os.path.dirname(DB_FILE_RAW_FALLBACK)
        if fallback_dir:
            os.makedirs(fallback_dir, exist_ok=True)

        if DB_FILE_RAW != DB_FILE_RAW_FALLBACK:
            copied = False
            for source_path in (DB_FILE_RAW, DB_FILE_BACKUP):
                if not source_path or not os.path.exists(source_path):
                    continue
                source_ok, source_detail = check_db_integrity(source_path)
                if not source_ok:
                    print(f'Raw DB fallback skipped invalid source {source_path}: {source_detail}')
                    continue
                try:
                    shutil.copy2(source_path, DB_FILE_RAW_FALLBACK)
                    copied = True
                    break
                except Exception:
                    continue

            if not copied and os.path.exists(DB_FILE_RAW_FALLBACK):
                fallback_ok, fallback_detail = check_db_integrity(DB_FILE_RAW_FALLBACK)
                if not fallback_ok:
                    print(f'Removing invalid raw DB fallback: {fallback_detail}')
                    for suffix in ('', '-wal', '-shm'):
                        try:
                            os.remove(f'{DB_FILE_RAW_FALLBACK}{suffix}')
                        except FileNotFoundError:
                            pass
                        except OSError:
                            pass

            if not copied and not os.path.exists(DB_FILE_RAW_FALLBACK):
                conn = sqlite3.connect(DB_FILE_RAW_FALLBACK, timeout=DB_BUSY_TIMEOUT_MS / 1000)
                conn.close()

        DB_FILE_RAW = DB_FILE_RAW_FALLBACK
        RAW_WRITE_QUEUE_FILE = RAW_WRITE_QUEUE_FILE_FALLBACK
        print(f'⚠ Switched raw DB to local fallback ({reason or "network SQLite unavailable"}): {DB_FILE_RAW}')
        return True
    except Exception as e:
        print(f'Local raw DB fallback failed: {e}')
        return False


def reset_raw_db_file(reason=''):
    """Quarantine current raw DB and create a fresh, empty SQLite file.

    This is a last-resort recovery path when integrity restore fails or the raw
    file cannot be opened due persistent disk I/O errors.
    """
    if is_postgres_enabled():
        print('Raw DB reset is not used in PostgreSQL mode')
        return False

    if is_network_sqlite_path(DB_FILE_RAW):
        return activate_local_raw_db_fallback(reason=reason or 'network raw DB reset requested')

    try:
        if os.path.exists(DB_FILE_RAW):
            try:
                quarantine_path = backup_file_with_timestamp(DB_FILE_RAW, 'unhealthy')
                print(f'Raw DB quarantined: {quarantine_path}')
            except Exception as e:
                print(f'Raw DB quarantine warning: {e}')

        for suffix in ('', '-wal', '-shm'):
            target = f'{DB_FILE_RAW}{suffix}'
            if os.path.exists(target):
                try:
                    os.remove(target)
                except Exception:
                    pass

        conn = sqlite3.connect(DB_FILE_RAW, timeout=DB_BUSY_TIMEOUT_MS / 1000)
        try:
            cur = conn.cursor()
            try:
                cur.execute('PRAGMA journal_mode=DELETE')
            except Exception:
                pass
            conn.commit()
        finally:
            conn.close()

        print(f'Created fresh raw DB fallback ({reason or "unknown reason"})')
        return True
    except Exception as e:
        print(f'Raw DB reset failed: {e}')
        return False


def _is_sqlite_storage_error(error):
    """Return True when an error indicates a corrupt or unavailable SQLite file."""
    message = str(error or '').lower()
    return (
        'database disk image is malformed' in message
        or 'file is not a database' in message
        or 'malformed' in message
        or 'disk i/o error' in message
        or 'unable to open database file' in message
    )


def activate_local_solar_db_fallback(reason=''):
    """Switch solar history writes to local temp SQLite DBs when NAS files are unhealthy."""
    global SOLAR_DB_RAW, SOLAR_DB_AVG, SOLAR_DB_TOTALS

    if is_postgres_enabled():
        print('Solar DB fallback is not used in PostgreSQL mode')
        return False

    fallback_pairs = (
        (SOLAR_DB_RAW, SOLAR_DB_RAW_FALLBACK),
        (SOLAR_DB_AVG, SOLAR_DB_AVG_FALLBACK),
        (SOLAR_DB_TOTALS, SOLAR_DB_TOTALS_FALLBACK),
    )

    try:
        os.makedirs(SOLAR_DB_FALLBACK_DIR, exist_ok=True)
        for source_path, fallback_path in fallback_pairs:
            if source_path == fallback_path:
                continue

            if not os.path.exists(fallback_path):
                copied = False
                if source_path and os.path.exists(source_path):
                    try:
                        shutil.copy2(source_path, fallback_path)
                        copied = True
                    except Exception:
                        copied = False
                if not copied:
                    conn = sqlite3.connect(fallback_path, timeout=DB_BUSY_TIMEOUT_MS / 1000)
                    conn.close()

        SOLAR_DB_RAW = SOLAR_DB_RAW_FALLBACK
        SOLAR_DB_AVG = SOLAR_DB_AVG_FALLBACK
        SOLAR_DB_TOTALS = SOLAR_DB_TOTALS_FALLBACK
        _solar_schema_checked_paths.clear()
        _solar_table_schema_cache.clear()
        print(f'⚠ Switched solar DBs to local fallback ({reason or "solar storage unavailable"}): {SOLAR_DB_FALLBACK_DIR}')
        return True
    except Exception as e:
        print(f'Local solar DB fallback failed: {e}')
        return False


def reset_solar_db_files(reason=''):
    """Quarantine and recreate solar DB files when corruption is detected."""
    if is_postgres_enabled():
        print('Solar DB reset is not used in PostgreSQL mode')
        return False

    try:
        for db_path in (SOLAR_DB_RAW, SOLAR_DB_AVG, SOLAR_DB_TOTALS):
            if not db_path:
                continue

            db_dir = os.path.dirname(os.path.abspath(db_path))
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

            if os.path.exists(db_path):
                try:
                    quarantine_path = backup_file_with_timestamp(db_path, 'corrupt_backup')
                    print(f'Solar DB quarantined: {quarantine_path}')
                except Exception as e:
                    print(f'Solar DB quarantine warning ({db_path}): {e}')

            for suffix in ('', '-wal', '-shm'):
                target = f'{db_path}{suffix}'
                if os.path.exists(target):
                    try:
                        os.remove(target)
                    except Exception:
                        pass

            conn_local = sqlite3.connect(db_path, timeout=DB_BUSY_TIMEOUT_MS / 1000)
            try:
                cursor_local = conn_local.cursor()
                try:
                    cursor_local.execute('PRAGMA journal_mode=DELETE')
                except Exception:
                    pass
                conn_local.commit()
            finally:
                conn_local.close()

        for kind in ('raw', 'avg', 'totals'):
            conn_local = None
            try:
                conn_local = open_solar_history_connection(kind=kind, write=True)
                if conn_local is not None:
                    ensure_solar_history_schema(conn_local.cursor())
                    conn_local.commit()
            finally:
                if conn_local:
                    conn_local.close()

        _solar_schema_checked_paths.clear()
        _solar_table_schema_cache.clear()
        print(f'Created fresh solar DB files ({reason or "unknown reason"})')
        return True
    except Exception as e:
        print(f'Solar DB reset failed: {e}')
        return False


def ensure_raw_db_health():
    """Validate raw DB integrity and attempt restore from backup if needed."""
    raw_ok, raw_detail = check_db_integrity(DB_FILE_RAW)
    if raw_ok:
        return

    print(f'Raw DB integrity check failed: {raw_detail}')
    if not ALLOW_RAW_DB_AUTO_RESTORE:
        print('Raw DB auto-restore is disabled (P1_ALLOW_RAW_DB_AUTO_RESTORE=false)')
        return

    restored = restore_raw_db_from_backup(reason=raw_detail)
    if not restored:
        reset_raw_db_file(reason=f'restore_failed: {raw_detail}')
        return

    restored_ok, restored_detail = check_db_integrity(DB_FILE_RAW)
    if restored_ok:
        print('✓ Raw DB integrity restored from backup')
    else:
        print(f'Raw DB still unhealthy after restore: {restored_detail}')
        reset_raw_db_file(reason=f'unhealthy_after_restore: {restored_detail}')


def get_db_connection(db_file, write=False):
    """Create a database connection for the active backend."""
    if is_postgres_enabled():
        return connect_database(db_file, timeout_seconds=DB_BUSY_TIMEOUT_MS / 1000, write=write)

    last_error = None
    db_file_is_network = is_network_sqlite_path(db_file)
    requested_mode = 'DELETE' if db_file_is_network else (SQLITE_JOURNAL_MODE if SQLITE_JOURNAL_MODE else DEFAULT_SQLITE_JOURNAL_MODE)
    if requested_mode not in ('WAL', 'DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'OFF'):
        requested_mode = 'DELETE' if db_file_is_network else DEFAULT_SQLITE_JOURNAL_MODE

    for attempt in range(DB_CONNECT_RETRIES):
        conn = None
        try:
            db_dir = os.path.dirname(os.path.abspath(db_file))
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
            conn = sqlite3.connect(db_file, timeout=DB_BUSY_TIMEOUT_MS / 1000)
            cursor = conn.cursor()

            # Setting journal mode can require an exclusive lock, especially on
            # NAS shares. Only writers need to perform this one-time tuning;
            # readers should not contend with an active solar write transaction.
            if write:
                try:
                    cursor.execute(f'PRAGMA journal_mode={requested_mode}')
                except sqlite3.OperationalError:
                    if requested_mode != 'DELETE':
                        try:
                            cursor.execute('PRAGMA journal_mode=DELETE')
                        except sqlite3.OperationalError as mode_error:
                            warning_key = 'journal_mode_unset'
                            if warning_key not in _db_warning_once:
                                print(f'DB warning: unable to set journal mode ({mode_error})')
                                _db_warning_once.add(warning_key)
                    else:
                        warning_key = 'journal_mode_unset'
                        if warning_key not in _db_warning_once:
                            print('DB warning: unable to set journal mode=DELETE')
                            _db_warning_once.add(warning_key)

            try:
                cursor.execute(f'PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}')
            except sqlite3.OperationalError:
                pass

            try:
                cursor.execute(f"PRAGMA synchronous={SQLITE_WRITE_SYNCHRONOUS if write else 'NORMAL'}")
            except sqlite3.OperationalError:
                pass

            try:
                cursor.execute('PRAGMA temp_store=MEMORY')
            except sqlite3.OperationalError:
                pass

            try:
                cursor.execute('PRAGMA foreign_keys=ON')
            except sqlite3.OperationalError:
                pass

            return conn
        except sqlite3.OperationalError as e:
            last_error = e
            if conn:
                conn.close()
            err_text = str(e).lower()
            retryable = (
                'database is locked' in err_text
                or 'database is busy' in err_text
                or 'disk i/o error' in err_text
                or 'unable to open database file' in err_text
            )
            if retryable and attempt < DB_CONNECT_RETRIES - 1:
                time.sleep(DB_CONNECT_RETRY_BASE_SECONDS * (attempt + 1))
                continue
            break
        except Exception as e:
            last_error = e
            if conn:
                conn.close()
            break

    raise last_error


def ensure_solar_manual_table(cursor):
    """Ensure manual solar-yield storage exists before reading or writing it."""
    cursor.execute('''CREATE TABLE IF NOT EXISTS solar_manual_data (
        date TEXT PRIMARY KEY,
        solar_yield_kwh REAL,
        updated_at REAL
    )''')


def ensure_manual_monthly_totals_table(cursor):
    """Ensure manual monthly totals storage exists for year/lifetime overrides."""
    cursor.execute('''CREATE TABLE IF NOT EXISTS monthly_manual_totals (
        month TEXT PRIMARY KEY,
        consumption_kwh REAL,
        injection_kwh REAL,
        consumption_offpeak_kwh REAL,
        consumption_peak_kwh REAL,
        injection_offpeak_kwh REAL,
        injection_peak_kwh REAL,
        solar_yield_kwh REAL,
        updated_at REAL
    )''')

    cursor.execute("PRAGMA table_info(monthly_manual_totals)")
    existing_columns = {row[1] for row in cursor.fetchall()}
    if 'consumption_offpeak_kwh' not in existing_columns:
        cursor.execute('ALTER TABLE monthly_manual_totals ADD COLUMN consumption_offpeak_kwh REAL')
    if 'consumption_peak_kwh' not in existing_columns:
        cursor.execute('ALTER TABLE monthly_manual_totals ADD COLUMN consumption_peak_kwh REAL')
    if 'injection_offpeak_kwh' not in existing_columns:
        cursor.execute('ALTER TABLE monthly_manual_totals ADD COLUMN injection_offpeak_kwh REAL')
    if 'injection_peak_kwh' not in existing_columns:
        cursor.execute('ALTER TABLE monthly_manual_totals ADD COLUMN injection_peak_kwh REAL')


def seed_manual_monthly_totals(cursor):
    """Seed the configured manual monthly totals if they are not present yet."""
    ensure_manual_monthly_totals_table(cursor)
    cursor.executemany(
        '''INSERT OR REPLACE INTO monthly_manual_totals
           (month, consumption_kwh, injection_kwh,
            consumption_offpeak_kwh, consumption_peak_kwh,
            injection_offpeak_kwh, injection_peak_kwh,
            solar_yield_kwh, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        [
            (
                month_key,
                values['consumption_kwh'],
                values.get('injection_kwh'),
                values.get('consumption_offpeak_kwh'),
                values.get('consumption_peak_kwh'),
                values.get('injection_offpeak_kwh'),
                values.get('injection_peak_kwh'),
                values.get('solar_yield_kwh'),
                time.time(),
            )
            for month_key, values in MANUAL_MONTHLY_TOTALS_KWH.items()
        ],
    )


def _is_sqlite_lock_error(error):
    """Return True when an OperationalError is caused by a transient SQLite lock."""
    if not isinstance(error, sqlite3.OperationalError):
        return False
    message = str(error).lower()
    return 'database is locked' in message or 'database is busy' in message or 'locked' in message


def prepare_manual_tables_best_effort(cursor, seed_monthly=False):
    """Best-effort schema prep for read endpoints: never fail on transient DB locks."""
    try:
        ensure_solar_manual_table(cursor)
    except Exception as e:
        if _is_sqlite_lock_error(e):
            print(f"Manual solar table prepare skipped (db lock): {e}")
        else:
            raise

    if not seed_monthly:
        return

    try:
        seed_manual_monthly_totals(cursor)
    except Exception as e:
        if _is_sqlite_lock_error(e):
            print(f"Manual monthly seed skipped (db lock): {e}")
        else:
            raise


def load_csv_monthly_overview(start_month=None, end_month=None):
    """Load monthly overview rows derived from Fluvius CSVs, keyed by YYYY-MM."""
    if not is_postgres_enabled() and not os.path.exists(DB_FILE_OVERVIEW):
        return {}

    conn = None
    try:
        conn = get_db_connection(DB_FILE_OVERVIEW)
        cursor = conn.cursor()
        query = '''SELECT month,
                          covered_days,
                          consumption_kwh,
                          injection_kwh,
                          consumption_offpeak_kwh,
                          consumption_peak_kwh,
                          injection_offpeak_kwh,
                          injection_peak_kwh,
                          gas_m3
                   FROM monthly_overview'''
        params = []
        conditions = []
        if start_month is not None:
            conditions.append('month >= ?')
            params.append(start_month)
        if end_month is not None:
            conditions.append('month < ?')
            params.append(end_month)
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        query += ' ORDER BY month'
        cursor.execute(query, tuple(params))
        return {
            row[0]: {
                'covered_days': int(row[1] or 0),
                'consumption': float(row[2]) if row[2] is not None else None,
                'injection': float(row[3]) if row[3] is not None else None,
                'consumption_offpeak': float(row[4]) if row[4] is not None else None,
                'consumption_peak': float(row[5]) if row[5] is not None else None,
                'injection_offpeak': float(row[6]) if row[6] is not None else None,
                'injection_peak': float(row[7]) if row[7] is not None else None,
                'gas_m3': float(row[8]) if row[8] is not None else None,
                'source': 'csv-overview',
            }
            for row in cursor.fetchall()
            if row and row[0]
        }
    except Exception as e:
        warning_key = 'csv_overview_unavailable'
        if warning_key not in _db_warning_once:
            print(f'Overview DB warning: unable to load CSV overview ({e})')
            _db_warning_once.add(warning_key)
        return {}
    finally:
        if conn:
            conn.close()


def get_table_max_timestamp(cursor, table_name):
    """Return the latest timestamp from a known table used for append-only sync."""
    if table_name not in ('energy_data', 'energy_data_5min'):
        raise ValueError(f'Unsupported table name: {table_name}')
    cursor.execute(f'SELECT MAX(timestamp) FROM {table_name}')
    result = cursor.fetchone()
    return result[0] if result and result[0] is not None else 0


def queue_raw_entry(entry):
    """Persist failed raw writes to a queue so they can be replayed later."""
    try:
        with open(RAW_WRITE_QUEUE_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + '\n')
    except Exception as e:
        print(f"Failed to queue raw entry: {e}")


def insert_raw_entry(entry):
    """Insert one raw sample with retries for transient SQLite lock/busy errors."""
    for attempt in range(DB_WRITE_RETRIES):
        conn = None
        try:
            conn = get_db_connection(DB_FILE_RAW, write=True)
            c = conn.cursor()
            c.execute(
                '''INSERT OR IGNORE INTO energy_data
                   (timestamp, power, import_kwh, export_kwh, gas_m3,
                    import_t1_kwh, import_t2_kwh, export_t1_kwh, export_t2_kwh)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    entry['timestamp'],
                    entry['power'],
                    entry['import'],
                    entry['export'],
                    entry['gas'],
                    entry.get('import_t1'),
                    entry.get('import_t2'),
                    entry.get('export_t1'),
                    entry.get('export_t2'),
                ),
            )
            conn.commit()
            return True
        except sqlite3.Error as e:
            err_text = str(e).lower()
            if conn:
                conn.close()
                conn = None

            if ('database is locked' in err_text or 'database is busy' in err_text or 'locked' in err_text) and attempt < DB_WRITE_RETRIES - 1:
                time.sleep(0.2 * (attempt + 1))
                continue

            if 'database is locked' in err_text or 'database is busy' in err_text or 'locked' in err_text:
                queue_raw_entry(entry)
                return True

            is_malformed_db = (
                'database disk image is malformed' in err_text
                or 'malformed' in err_text
                or 'file is not a database' in err_text
            )
            if is_malformed_db:
                recovered = False
                if ALLOW_RAW_DB_AUTO_RESTORE:
                    recovered = restore_raw_db_from_backup(reason=f'raw_insert_malformed: {e}')
                if not recovered:
                    recovered = reset_raw_db_file(reason=f'raw_insert_malformed: {e}')
                if recovered and attempt < DB_WRITE_RETRIES - 1:
                    continue

            if ('disk i/o error' in err_text or 'unable to open database file' in err_text) and is_network_sqlite_path(DB_FILE_RAW):
                if activate_local_raw_db_fallback(reason=f'raw_insert_failure: {e}'):
                    continue
            print(f"Raw insert failed: {e}")
            return False
        except Exception as e:
            print(f"Raw insert failed: {e}")
            return False
        finally:
            if conn:
                conn.close()
    return False


def flush_raw_write_queue(max_items=1000):
    """Replay queued raw samples into the database and keep only failed leftovers."""
    if not os.path.exists(RAW_WRITE_QUEUE_FILE):
        return 0

    try:
        with open(RAW_WRITE_QUEUE_FILE, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Failed to read raw write queue: {e}")
        return 0

    if not lines:
        return 0

    flushed = 0
    remaining = []

    for idx, line in enumerate(lines):
        if idx >= max_items:
            remaining.extend(lines[idx:])
            break
        try:
            entry = json.loads(line)
        except Exception:
            # Keep malformed lines so nothing is silently discarded.
            remaining.append(line)
            continue

        if insert_raw_entry(entry):
            flushed += 1
        else:
            remaining.append(line)

    try:
        if remaining:
            with open(RAW_WRITE_QUEUE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(remaining) + '\n')
        else:
            os.remove(RAW_WRITE_QUEUE_FILE)
    except Exception as e:
        print(f"Failed to update raw write queue: {e}")

    return flushed


def require_admin_token():
    """Validate admin token for sensitive operations if ADMIN_TOKEN is configured."""
    if not runtime_admin_token:
        return None

    request_token = request.headers.get('X-Admin-Token', '')
    if request_token != runtime_admin_token:
        return jsonify({"error": "unauthorized", "message": "Invalid admin token"}), 401
    return None


def bootstrap_data_dir():
    """Copy legacy runtime files from the project root into the active data directory once."""
    if DATA_DIR == BASE_DIR:
        return

    copied_files = []
    for file_name in BOOTSTRAP_COPY_FILES:
        source_path = os.path.join(BASE_DIR, file_name)
        target_path = os.path.join(DATA_DIR, file_name)

        if not os.path.exists(source_path) or os.path.exists(target_path):
            continue

        try:
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            shutil.copy2(source_path, target_path)
            copied_files.append(file_name)
        except Exception as e:
            print(f"Bootstrap copy failed for {file_name}: {e}")

    if copied_files:
        print(f"✓ Bootstrapped data dir with {len(copied_files)} file(s): {', '.join(copied_files)}")


def migrate_split_storage_layout():
    """Populate the new P1/solar folder layout from any legacy flat DB files."""
    migrated_files = []
    migration_map = {}
    migration_map.update(LEGACY_P1_DB_FILES)
    migration_map.update(SOLAR_DB_FILES)

    for target_path, candidate_paths in migration_map.items():
        if os.path.exists(target_path):
            continue

        for source_path in candidate_paths:
            if not source_path or not os.path.exists(source_path):
                continue
            if os.path.abspath(source_path) == os.path.abspath(target_path):
                continue

            try:
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.copy2(source_path, target_path)
                migrated_files.append(f'{os.path.basename(source_path)} -> {target_path}')
                break
            except Exception as e:
                print(f"DB layout migration failed for {target_path}: {e}")
                break

    if migrated_files:
        print(f"✓ Migrated storage layout into split folders ({len(migrated_files)} file(s))")


def log_storage_paths():
    """Print effective runtime paths so storage location is always explicit."""
    print(f"✓ Runtime cwd: {os.getcwd()}")
    print(f"✓ Script base dir: {BASE_DIR}")
    print(f"✓ P1 URL: {P1_URL}")
    print(f"✓ DB backend: {current_db_backend()}")
    if is_postgres_enabled():
        print(f"✓ DATABASE_URL: {masked_database_url()}")
    print(f"✓ Application root: {APPLICATION_ROOT or '/'}")
    print(f"✓ Data dir: {DATA_DIR}")
    print(f"✓ P1 DB dir: {P1_DB_DIR}")
    print(f"✓ Solar DB dir: {SOLAR_DB_DIR}")
    print(f"✓ Battery DB dir: {BATTERY_DB_DIR}")
    print(f"✓ P1 RAW DB: {DB_FILE_RAW}")
    print(f"✓ P1 AVG DB: {DB_FILE_AVG}")
    print(f"✓ P1 TOTALS DB: {DB_FILE_DAILY}")
    print(f"✓ P1 BACKUP DB: {DB_FILE_BACKUP}")
    print(f"✓ Solar RAW DB: {SOLAR_DB_RAW}")
    print(f"✓ Solar AVG DB: {SOLAR_DB_AVG}")
    print(f"✓ Solar TOTALS DB: {SOLAR_DB_TOTALS}")
    print(f"✓ Battery RAW DB: {BATTERY_DB_RAW}")
    print(f"✓ Battery AVG DB: {BATTERY_DB_AVG}")
    print(f"✓ Battery TOTALS DB: {BATTERY_DB_TOTALS}")
    print(f"✓ Queue file: {RAW_WRITE_QUEUE_FILE}")
    print(f"✓ Settings file: {SETTINGS_FILE}")


@app.context_processor
def inject_app_base_path():
    return {'app_base_path': APPLICATION_ROOT}


def get_runtime_settings():
    """Return the current mutable notification settings."""
    return {
        'email': EMAIL_TO,
        'threshold': injection_threshold,
        'sendNotification': notifications_enabled,
        'adminToken': runtime_admin_token,
        'smtpUser': SMTP_USER,
        'smtpPass': SMTP_PASS,
        'smtpPassConfigured': bool(SMTP_PASS),
    }


def load_settings():
    """Load persisted notification settings, falling back to environment defaults."""
    global EMAIL_TO, injection_threshold, notifications_enabled, runtime_admin_token, SMTP_USER, SMTP_PASS

    EMAIL_TO = DEFAULT_EMAIL_TO
    injection_threshold = DEFAULT_INJECTION_THRESHOLD
    notifications_enabled = DEFAULT_NOTIFICATIONS_ENABLED
    runtime_admin_token = DEFAULT_ADMIN_TOKEN
    SMTP_USER = DEFAULT_SMTP_USER
    SMTP_PASS = DEFAULT_SMTP_PASS

    if not os.path.exists(SETTINGS_FILE):
        return get_runtime_settings()

    try:
        with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
            saved_settings = json.load(f)
    except Exception as e:
        print(f"Failed to load settings file: {e}")
        return get_runtime_settings()

    saved_email = str(saved_settings.get('email', '')).strip()
    if saved_email:
        EMAIL_TO = saved_email

    try:
        saved_threshold = int(saved_settings.get('threshold', DEFAULT_INJECTION_THRESHOLD))
        if 0 <= saved_threshold <= 100000:
            injection_threshold = saved_threshold
    except (TypeError, ValueError):
        pass

    saved_notifications = saved_settings.get('sendNotification', DEFAULT_NOTIFICATIONS_ENABLED)
    if isinstance(saved_notifications, bool):
        notifications_enabled = saved_notifications

    # Environment token takes precedence if provided; otherwise allow persisted token.
    if not DEFAULT_ADMIN_TOKEN:
        saved_admin_token = str(saved_settings.get('adminToken', '')).strip()
        runtime_admin_token = saved_admin_token

    # Environment SMTP credentials take precedence if provided.F
    if not DEFAULT_SMTP_USER:
        saved_smtp_user = str(saved_settings.get('smtpUser', '')).strip()
        if saved_smtp_user:
            SMTP_USER = saved_smtp_user

    if not DEFAULT_SMTP_PASS:
        saved_smtp_pass = str(saved_settings.get('smtpPass', '')).strip()
        if saved_smtp_pass:
            SMTP_PASS = saved_smtp_pass

    # Backward-compatible fallback: if SMTP_PASS is not configured via env,
    # use the persisted admin token as app password.
    if not DEFAULT_SMTP_PASS and not SMTP_PASS and runtime_admin_token:
        SMTP_PASS = runtime_admin_token

    return get_runtime_settings()


def save_settings():
    """Persist notification settings so they survive process restarts."""
    runtime_settings = get_runtime_settings()
    settings_payload = {
        'email': runtime_settings['email'],
        'threshold': runtime_settings['threshold'],
        'sendNotification': runtime_settings['sendNotification'],
        'adminToken': runtime_settings['adminToken'],
        'smtpUser': SMTP_USER,
        'smtpPass': SMTP_PASS,
    }

    with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
        json.dump(settings_payload, f, indent=2)

    return runtime_settings


def _read_sun2000_live_power(host=SUN2000_IP, port=SUN2000_PORT):
    """Read current live power from Sun2000 Modbus registers."""

    # Match solar-check.py behavior first (works reliably on this setup),
    # then fall back to extended kwargs when supported.
    try:
        client = ModbusTcpClient(host, port=int(port), timeout=SUN2000_READ_TIMEOUT_SECONDS)
    except TypeError:
        client = ModbusTcpClient(
            host,
            port=int(port),
            timeout=SUN2000_READ_TIMEOUT_SECONDS,
            retries=0,
            reconnect_delay=0,
            reconnect_delay_max=0,
        )
    try:
        if not client.connect():
            raise ConnectionError('Unable to connect to inverter')

        active_power_w, unit_id, read_error = _sun2000_read_active_power_w(client)
        if active_power_w is None:
            raise ConnectionError(f'Modbus register read failed ({read_error})')

        return {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'device_id': SUN2000_DEVICE_ID,
            'unit_id': int(unit_id),
            'current_power_w': float(active_power_w),
            'status': 'Running',
            'source': f'live-modbus-unit-{unit_id}',
        }
    finally:
        try:
            client.close()
        except Exception:
            pass


def fetch_and_store_current_power(force=False, persist=None):
    """Read live solar power and cache it for fast live-tab responses."""
    global _last_solar_fetch_ts, _last_solar_persist_ts

    if not SUN2000_AVAILABLE:
        return None

    now_ts = time.time()

    acquired = _solar_fetch_lock.acquire(timeout=1)
    if not acquired:
        if _is_fresh_sample_timestamp(_solar_runtime_state.get('timestamp'), SOLAR_SAMPLE_STALE_AFTER_SECONDS):
            return {
                'timestamp': datetime.fromtimestamp(_solar_runtime_state['timestamp'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'device_id': SUN2000_DEVICE_ID,
                'current_power_w': _solar_runtime_state.get('power_w'),
                'status': 'Cached',
                'source': 'cache',
            }
        return None

    try:
        if not force and (now_ts - _last_solar_fetch_ts) < SOLAR_LIVE_CACHE_TTL_SECONDS:
            if _is_fresh_sample_timestamp(_solar_runtime_state.get('timestamp'), SOLAR_SAMPLE_STALE_AFTER_SECONDS):
                return {
                    'timestamp': datetime.fromtimestamp(_solar_runtime_state['timestamp'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    'device_id': SUN2000_DEVICE_ID,
                    'current_power_w': _solar_runtime_state.get('power_w'),
                    'status': 'Cached',
                    'source': 'cache',
                }

        data = _read_sun2000_live_power()

        _last_solar_fetch_ts = time.time()
        _cache_solar_runtime_point(data.get('timestamp'), data.get('current_power_w'))
        if persist is None:
            persist = (_last_solar_fetch_ts - _last_solar_persist_ts) >= SOLAR_PERSIST_INTERVAL_SECONDS
        if persist and _persist_solar_sample_to_history(data):
            _last_solar_persist_ts = time.time()
        return data
    except Exception as e:
        if _is_fresh_sample_timestamp(_solar_runtime_state.get('timestamp'), SOLAR_SAMPLE_STALE_AFTER_SECONDS):
            return {
                'timestamp': datetime.fromtimestamp(_solar_runtime_state['timestamp'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'device_id': SUN2000_DEVICE_ID,
                'current_power_w': _solar_runtime_state.get('power_w'),
                'status': 'Cached',
                'source': 'cache',
            }
        print(f'Sun2000 live read failed: {e}')
        return None
    finally:
        _solar_fetch_lock.release()


def get_solar_history_db_path(kind='raw'):
    """Return the preferred solar database path for the requested storage tier."""
    kind_key = str(kind or 'raw').lower()
    if kind_key == 'avg':
        return SOLAR_DB_AVG
    if kind_key == 'totals':
        return SOLAR_DB_TOTALS
    return SOLAR_DB_RAW


def get_solar_history_connection(write=False, kind='raw'):
    """Open the requested solar database file if it exists."""
    solar_db_path = get_solar_history_db_path(kind=kind)
    if not is_postgres_enabled() and not os.path.exists(solar_db_path) and not write:
        return None

    conn = get_db_connection(solar_db_path, write=write)
    try:
        if write:
            ensure_solar_history_schema(conn.cursor())
            conn.commit()
        conn.row_factory = sqlite3.Row
    except Exception:
        pass
    return conn


def open_solar_history_connection(kind='raw', write=False):
    """Compatibility wrapper for tests that monkeypatch the legacy signature."""
    try:
        return get_solar_history_connection(write=write, kind=kind)
    except TypeError:
        try:
            return get_solar_history_connection(write=write)
        except TypeError:
            return get_solar_history_connection()


def get_battery_history_db_path(kind='raw'):
    """Return the preferred battery database path for the requested storage tier."""
    kind_key = str(kind or 'raw').lower()
    if kind_key == 'avg':
        return BATTERY_DB_AVG
    if kind_key == 'totals':
        return BATTERY_DB_TOTALS
    return BATTERY_DB_RAW


def get_battery_history_connection(write=False, kind='raw'):
    """Open the requested battery database file and ensure schema when writing."""
    battery_db_path = get_battery_history_db_path(kind=kind)
    if not is_postgres_enabled() and not os.path.exists(battery_db_path) and not write:
        return None

    conn = get_db_connection(battery_db_path, write=write)
    try:
        if write:
            ensure_battery_history_schema(conn.cursor())
            conn.commit()
        conn.row_factory = sqlite3.Row
    except Exception:
        pass
    return conn


def open_battery_history_connection(kind='raw', write=False):
    """Compatibility wrapper for battery history connections."""
    try:
        return get_battery_history_connection(write=write, kind=kind)
    except TypeError:
        try:
            return get_battery_history_connection(write=write)
        except TypeError:
            return get_battery_history_connection()


def init_battery_history_dbs():
    """Create battery raw/average/total databases and their schema up front."""
    initialized_paths = []
    for kind in ('raw', 'avg', 'totals'):
        conn = None
        try:
            conn = open_battery_history_connection(kind=kind, write=True)
            if conn is None:
                continue
            ensure_battery_history_schema(conn.cursor())
            conn.commit()
            initialized_paths.append(get_battery_history_db_path(kind=kind))
        finally:
            if conn:
                conn.close()

    if initialized_paths:
        print(f"✓ Initialized battery raw database ({BATTERY_DB_RAW})")
        print(f"✓ Initialized battery averages database ({BATTERY_DB_AVG})")
        print(f"✓ Initialized battery totals database ({BATTERY_DB_TOTALS})")


def ensure_battery_history_schema(cursor):
    """Ensure battery history tables exist for raw, realtime, and rollup reporting."""
    db_key = _get_sqlite_cursor_cache_key(cursor)
    if db_key in _battery_schema_checked_paths:
        return

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS battery_raw_data (
               timestamp REAL PRIMARY KEY,
               consumption_w REAL,
               power_w REAL,
               soc_pct REAL,
               capacity_kwh REAL,
               source TEXT,
               created_at REAL
           )'''
    )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_battery_raw_ts ON battery_raw_data(timestamp)')

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS battery_realtime (
               timestamp TEXT PRIMARY KEY,
               consumption_w REAL,
               power_w REAL,
               soc_pct REAL,
               capacity_kwh REAL,
               status TEXT,
               source TEXT,
               created_at REAL
           )'''
    )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_battery_realtime_ts ON battery_realtime(timestamp)')

    cursor.execute(
        '''INSERT OR IGNORE INTO battery_raw_data
               (timestamp, consumption_w, power_w, soc_pct, capacity_kwh, source, created_at)
           SELECT CAST(strftime('%s', timestamp) AS REAL),
                  consumption_w,
                  power_w,
                  soc_pct,
                  capacity_kwh,
                  source,
                  COALESCE(NULLIF(CAST(created_at AS REAL), 0), CAST(strftime('%s', timestamp) AS REAL))
           FROM battery_realtime
           WHERE timestamp IS NOT NULL'''
    )

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS five_minute_averages (
               bucket_start TEXT PRIMARY KEY,
               avg_consumption_w REAL,
               max_consumption_w REAL,
               avg_soc_pct REAL,
               sample_count INTEGER,
               energy_kwh REAL,
               updated_at REAL
           )'''
    )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_battery_5min_bucket_start ON five_minute_averages(bucket_start)')

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS daily_totals (
               date TEXT PRIMARY KEY,
               total_energy_kwh REAL,
               avg_consumption_w REAL,
               peak_consumption_w REAL,
               buckets INTEGER,
               updated_at REAL
           )'''
    )

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS monthly_totals (
               month TEXT PRIMARY KEY,
               total_energy_kwh REAL,
               avg_consumption_w REAL,
               peak_consumption_w REAL,
               days INTEGER,
               updated_at REAL
           )'''
    )

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS yearly_totals (
               year INTEGER PRIMARY KEY,
               total_energy_kwh REAL,
               avg_consumption_w REAL,
               peak_consumption_w REAL,
               months INTEGER,
               updated_at REAL
           )'''
    )

    _battery_schema_checked_paths.add(db_key)
    _battery_table_schema_cache.pop((db_key, 'battery_raw_data'), None)
    _battery_table_schema_cache.pop((db_key, 'battery_realtime'), None)


def _upsert_battery_raw_sample(cursor, timestamp_ts, consumption_w, power_w=None, soc_pct=None, capacity_kwh=None, source=None, created_at=None):
    """Insert one raw battery sample for history and rollups."""
    db_key = _get_sqlite_cursor_cache_key(cursor)
    cache_key = (db_key, 'battery_raw_data')
    table_columns = _battery_table_schema_cache.get(cache_key)
    if table_columns is None:
        cursor.execute('PRAGMA table_info(battery_raw_data)')
        table_columns = [row[1] for row in cursor.fetchall()]
        _battery_table_schema_cache[cache_key] = table_columns

    created_at = time.time() if created_at is None else created_at
    values_by_column = {
        'timestamp': float(timestamp_ts),
        'consumption_w': float(consumption_w) if isinstance(consumption_w, (int, float)) else None,
        'power_w': float(power_w) if isinstance(power_w, (int, float)) else None,
        'soc_pct': float(soc_pct) if isinstance(soc_pct, (int, float)) else None,
        'capacity_kwh': float(capacity_kwh) if isinstance(capacity_kwh, (int, float)) else None,
        'source': str(source or 'marstek'),
        'created_at': float(created_at),
    }

    insert_columns = [column for column in table_columns if column in values_by_column]
    placeholders = ', '.join('?' for _ in insert_columns)
    column_sql = ', '.join(insert_columns)
    params = tuple(values_by_column[column] for column in insert_columns)
    cursor.execute(
        f'INSERT OR REPLACE INTO battery_raw_data ({column_sql}) VALUES ({placeholders})',
        params,
    )


def _upsert_battery_realtime_sample(cursor, timestamp_text, consumption_w, power_w=None, soc_pct=None, capacity_kwh=None, status=None, source=None, created_at=None):
    """Insert one battery realtime sample with compatibility across schema variants."""
    db_key = _get_sqlite_cursor_cache_key(cursor)
    cache_key = (db_key, 'battery_realtime')
    cached_schema = _battery_table_schema_cache.get(cache_key)
    if cached_schema is None:
        cursor.execute('PRAGMA table_info(battery_realtime)')
        column_rows = cursor.fetchall()
        table_columns = [row[1] for row in column_rows]
        column_types = {row[1]: str(row[2] or '').upper() for row in column_rows}
        _battery_table_schema_cache[cache_key] = (table_columns, column_types)
    else:
        table_columns, column_types = cached_schema

    created_at = time.time() if created_at is None else created_at
    created_at_value = created_at if 'REAL' in column_types.get('created_at', '') else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    values_by_column = {
        'timestamp': timestamp_text,
        'consumption_w': float(consumption_w) if isinstance(consumption_w, (int, float)) else None,
        'power_w': float(power_w) if isinstance(power_w, (int, float)) else None,
        'soc_pct': float(soc_pct) if isinstance(soc_pct, (int, float)) else None,
        'capacity_kwh': float(capacity_kwh) if isinstance(capacity_kwh, (int, float)) else None,
        'status': str(status or 'ok'),
        'source': str(source or 'marstek'),
        'created_at': created_at_value,
    }

    insert_columns = [column for column in table_columns if column in values_by_column]
    placeholders = ', '.join('?' for _ in insert_columns)
    column_sql = ', '.join(insert_columns)
    params = tuple(values_by_column[column] for column in insert_columns)
    cursor.execute(
        f'INSERT OR REPLACE INTO battery_realtime ({column_sql}) VALUES ({placeholders})',
        params,
    )


def _upsert_battery_five_minute_average(cursor, bucket_start, avg_consumption_w, max_consumption_w, avg_soc_pct, sample_count, energy_kwh, updated_at=None):
    """Insert one battery 5-minute aggregate row."""
    cursor.execute('PRAGMA table_info(five_minute_averages)')
    table_columns = [row[1] for row in cursor.fetchall()]
    updated_at = time.time() if updated_at is None else updated_at

    values_by_column = {
        'bucket_start': bucket_start,
        'avg_consumption_w': float(avg_consumption_w or 0.0),
        'max_consumption_w': float(max_consumption_w or 0.0),
        'avg_soc_pct': float(avg_soc_pct) if isinstance(avg_soc_pct, (int, float)) else None,
        'sample_count': int(sample_count or 0),
        'energy_kwh': float(energy_kwh or 0.0),
        'updated_at': float(updated_at),
    }

    insert_columns = [column for column in table_columns if column in values_by_column]
    placeholders = ', '.join('?' for _ in insert_columns)
    column_sql = ', '.join(insert_columns)
    params = tuple(values_by_column[column] for column in insert_columns)
    cursor.execute(
        f'INSERT OR REPLACE INTO five_minute_averages ({column_sql}) VALUES ({placeholders})',
        params,
    )


def _upsert_battery_total_row(cursor, table_name, key_column, key_value, total_energy_kwh, avg_consumption_w, peak_consumption_w, period_count, updated_at=None):
    """Insert one battery total row across daily/monthly/yearly tables."""
    cursor.execute(f'PRAGMA table_info({table_name})')
    column_rows = cursor.fetchall()
    table_columns = [row[1] for row in column_rows]
    column_types = {row[1]: str(row[2] or '').upper() for row in column_rows}

    updated_at = time.time() if updated_at is None else updated_at
    updated_at_value = updated_at if 'REAL' in column_types.get('updated_at', '') else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    key_value_out = int(key_value) if 'INT' in column_types.get(key_column, '') and str(key_value).isdigit() else str(key_value)

    values_by_column = {
        key_column: key_value_out,
        'total_energy_kwh': float(total_energy_kwh or 0.0),
        'avg_consumption_w': float(avg_consumption_w or 0.0),
        'peak_consumption_w': float(peak_consumption_w or 0.0),
        'buckets': int(period_count or 0),
        'days': int(period_count or 0),
        'months': int(period_count or 0),
        'updated_at': updated_at_value,
    }

    insert_columns = [column for column in table_columns if column in values_by_column]
    placeholders = ', '.join('?' for _ in insert_columns)
    column_sql = ', '.join(insert_columns)
    params = tuple(values_by_column[column] for column in insert_columns)
    cursor.execute(
        f'INSERT OR REPLACE INTO {table_name} ({column_sql}) VALUES ({placeholders})',
        params,
    )


def _persist_marstek_sample_to_history(sample):
    """Persist one stable Marstek sample and update battery rollups."""
    global _last_battery_rollup_ts

    if not sample or sample.get('status') != 'ok':
        return False

    consumption_w = sample.get('battery_consumption_w')
    if not isinstance(consumption_w, (int, float)):
        power_w = sample.get('power_w')
        if isinstance(power_w, (int, float)):
            consumption_w = max(0.0, float(power_w))
    if not isinstance(consumption_w, (int, float)):
        return False

    sample_ts = _parse_solar_sample_timestamp(sample.get('timestamp'))
    sample_dt_utc = datetime.fromtimestamp(float(sample_ts), timezone.utc)
    timestamp_text = sample_dt_utc.strftime('%Y-%m-%d %H:%M:%S')

    bucket_epoch = int(float(sample_ts) // 300) * 300
    bucket_start = datetime.fromtimestamp(bucket_epoch, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    day_start = sample_dt_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    day_key = day_start.strftime('%Y-%m-%d')
    month_key = sample_dt_utc.strftime('%Y-%m')
    year_key = int(sample_dt_utc.strftime('%Y'))

    raw_conn = None
    avg_conn = None
    totals_conn = None
    try:
        raw_conn = open_battery_history_connection(kind='raw', write=True)
        if raw_conn is None:
            return False

        raw_cursor = raw_conn.cursor()
        ensure_battery_history_schema(raw_cursor)

        created_at = time.time()
        _upsert_battery_raw_sample(
            raw_cursor,
            sample_ts,
            consumption_w,
            power_w=sample.get('power_w'),
            soc_pct=sample.get('soc_pct'),
            capacity_kwh=sample.get('capacity_kwh'),
            source=sample.get('source', 'marstek'),
            created_at=created_at,
        )
        if PERSIST_REALTIME_SAMPLES:
            _upsert_battery_realtime_sample(
                raw_cursor,
                timestamp_text,
                consumption_w,
                power_w=sample.get('power_w'),
                soc_pct=sample.get('soc_pct'),
                capacity_kwh=sample.get('capacity_kwh'),
                status=sample.get('status', 'ok'),
                source=sample.get('source', 'marstek'),
                created_at=created_at,
            )
        raw_conn.commit()

        rollup_now = time.time()
        if rollup_now - _last_battery_rollup_ts < BATTERY_ROLLUP_INTERVAL_SECONDS:
            return True

        avg_conn = open_battery_history_connection(kind='avg', write=True)
        totals_conn = open_battery_history_connection(kind='totals', write=True)
        avg_conn = _reuse_if_same_sqlite_file(raw_conn, avg_conn)
        totals_conn = _reuse_if_same_sqlite_file(avg_conn, totals_conn)
        totals_conn = _reuse_if_same_sqlite_file(raw_conn, totals_conn)
        if avg_conn is None or totals_conn is None:
            return True

        avg_cursor = avg_conn.cursor()
        totals_cursor = totals_conn.cursor()
        ensure_battery_history_schema(avg_cursor)
        ensure_battery_history_schema(totals_cursor)

        bucket_end_epoch = bucket_epoch + 300
        raw_cursor.execute(
            '''SELECT AVG(consumption_w), MAX(consumption_w), AVG(soc_pct), COUNT(*)
               FROM battery_raw_data
               WHERE timestamp >= ? AND timestamp < ?''',
            (float(bucket_epoch), float(bucket_end_epoch)),
        )
        avg_consumption_w, max_consumption_w, avg_soc_pct, sample_count = raw_cursor.fetchone()
        avg_consumption_w = float(avg_consumption_w) if avg_consumption_w is not None else None
        max_consumption_w = float(max_consumption_w) if max_consumption_w is not None else None
        avg_soc_pct = float(avg_soc_pct) if avg_soc_pct is not None else None
        sample_count = int(sample_count or 0)
        energy_kwh = ((avg_consumption_w or 0.0) * (5.0 / 60.0) / 1000.0) if sample_count > 0 else 0.0

        _upsert_battery_five_minute_average(
            avg_cursor,
            bucket_start,
            avg_consumption_w,
            max_consumption_w,
            avg_soc_pct,
            sample_count,
            energy_kwh,
            updated_at=time.time(),
        )
        avg_conn.commit()

        avg_cursor.execute(
            '''SELECT SUM(energy_kwh), AVG(avg_consumption_w), MAX(max_consumption_w), COUNT(*)
               FROM five_minute_averages
               WHERE bucket_start >= ? AND bucket_start < ?''',
            (day_start.strftime('%Y-%m-%d %H:%M:%S'), day_end.strftime('%Y-%m-%d %H:%M:%S')),
        )
        day_total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
        _upsert_battery_total_row(
            totals_cursor,
            'daily_totals',
            'date',
            day_key,
            day_total_row[0],
            day_total_row[1],
            day_total_row[2],
            day_total_row[3],
            updated_at=time.time(),
        )

        month_start = day_start.replace(day=1)
        next_month_start = (month_start + timedelta(days=32)).replace(day=1)
        avg_cursor.execute(
            '''SELECT SUM(energy_kwh), AVG(avg_consumption_w), MAX(max_consumption_w), COUNT(DISTINCT substr(bucket_start, 1, 10))
               FROM five_minute_averages
               WHERE bucket_start >= ? AND bucket_start < ?''',
            (month_start.strftime('%Y-%m-%d %H:%M:%S'), next_month_start.strftime('%Y-%m-%d %H:%M:%S')),
        )
        month_total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
        _upsert_battery_total_row(
            totals_cursor,
            'monthly_totals',
            'month',
            month_key,
            month_total_row[0],
            month_total_row[1],
            month_total_row[2],
            month_total_row[3],
            updated_at=time.time(),
        )

        year_start = day_start.replace(month=1, day=1)
        next_year_start = year_start.replace(year=year_start.year + 1)
        avg_cursor.execute(
            '''SELECT SUM(energy_kwh), AVG(avg_consumption_w), MAX(max_consumption_w), COUNT(DISTINCT substr(bucket_start, 1, 7))
               FROM five_minute_averages
               WHERE bucket_start >= ? AND bucket_start < ?''',
            (year_start.strftime('%Y-%m-%d %H:%M:%S'), next_year_start.strftime('%Y-%m-%d %H:%M:%S')),
        )
        year_total_row = avg_cursor.fetchone() or [0.0, 0.0, 0.0, 0]
        _upsert_battery_total_row(
            totals_cursor,
            'yearly_totals',
            'year',
            year_key,
            year_total_row[0],
            year_total_row[1],
            year_total_row[2],
            year_total_row[3],
            updated_at=time.time(),
        )

        totals_conn.commit()
        _last_battery_rollup_ts = rollup_now
        return True
    except Exception as e:
        print(f'Battery history persist failed: {e}')
        return False
    finally:
        for conn in {raw_conn, avg_conn, totals_conn}:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


def _get_connection_db_path(conn):
    """Return the underlying SQLite file path for a connection when available."""
    if conn is None:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute('PRAGMA database_list')
        rows = cursor.fetchall() or []
        for row in rows:
            row_path = row[2] if len(row) >= 3 else None
            if row_path:
                return os.path.abspath(str(row_path))
    except Exception:
        return None
    return None


def _reuse_if_same_sqlite_file(primary_conn, secondary_conn):
    """Reuse the first connection when both point to the same SQLite file."""
    if primary_conn is None:
        return secondary_conn
    if secondary_conn is None or secondary_conn is primary_conn:
        return primary_conn

    primary_path = _get_connection_db_path(primary_conn)
    secondary_path = _get_connection_db_path(secondary_conn)
    if primary_path and secondary_path and primary_path == secondary_path:
        try:
            secondary_conn.close()
        except Exception:
            pass
        return primary_conn
    return secondary_conn


def get_solar_5min_series(target_date):
    """Return solar 5-minute average power for the selected day."""
    day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    cache_key = day_start.strftime('%Y-%m-%d')
    now_for_cache = time.time()
    with _series_cache_lock:
        cache_entry = _solar_series_cache.get(cache_key)
        if cache_entry and now_for_cache < cache_entry['expires']:
            return list(cache_entry['data'])

    avg_conn = None
    raw_conn = None

    try:
        # Only rebuild rollups for "today". Raw solar data is retained for 30
        # days (not just today), so a raw-data-exists check alone would
        # trigger a full-day rebuild scan on the *first* visit to every
        # historical day too -- and historical days' rollups are already kept
        # correct incrementally by the live collector's per-sample persist,
        # so re-scanning their raw history on read is pure redundant cost.
        _is_today = target_date.date() == datetime.now().date()
        if _is_today:
            _raw_conn_check = open_solar_history_connection(kind='raw')
            _has_raw_data = False
            if _raw_conn_check is not None:
                try:
                    _c = _raw_conn_check.cursor()
                    _c.execute(
                        'SELECT 1 FROM solar_raw_data WHERE timestamp >= ? AND timestamp < ? LIMIT 1',
                        (day_start.timestamp(), day_end.timestamp()),
                    )
                    _has_raw_data = _c.fetchone() is not None
                except Exception:
                    pass
                finally:
                    _raw_conn_check.close()
            if _has_raw_data:
                # The rebuild is write-heavy (write lock + 3 connections); throttle
                # it to at most once per minute per day instead of running it on
                # every cache-miss request for "today".
                _day_key = day_start.strftime('%Y-%m-%d')
                _now_ts = time.time()
                _rebuild_entry = _solar_daily_rollup_rebuild_cache.get(_day_key)
                if not _rebuild_entry or _now_ts >= _rebuild_entry.get('expires', 0.0):
                    rebuild_solar_rollups_from_history(day_start.timestamp(), day_end.timestamp())
                    _solar_daily_rollup_rebuild_cache[_day_key] = {'expires': _now_ts + 60.0}
        avg_conn = open_solar_history_connection(kind='avg')
        raw_conn = open_solar_history_connection(kind='raw')
        if avg_conn is None and raw_conn is None:
            return []

        avg_cursor = avg_conn.cursor() if avg_conn is not None else None
        raw_cursor = raw_conn.cursor() if raw_conn is not None else None
        day_start_str = datetime.fromtimestamp(day_start.timestamp(), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        day_end_str = datetime.fromtimestamp(day_end.timestamp(), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        series_map = {}
        avg_bucket_count = 0

        if avg_cursor is not None:
            avg_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='solar_hourly_imports'")
            has_hourly_table = avg_cursor.fetchone() is not None
            if has_hourly_table:
                avg_cursor.execute(
                    '''SELECT CAST(strftime('%s', period_start) AS INTEGER) AS period_ts,
                              avg_power_w,
                              energy_kwh
                       FROM solar_hourly_imports
                       WHERE period_start >= ? AND period_start < ?
                       ORDER BY period_start''',
                    (day_start_str, day_end_str),
                )
                hourly_rows = avg_cursor.fetchall()
                for row in hourly_rows:
                    if row['period_ts'] is None:
                        continue
                    for offset in range(12):
                        point_ts = (int(row['period_ts']) + (offset * 300)) * 1000
                        series_map[point_ts] = {
                            'timestamp': point_ts,
                            'avg_power_w': float(row['avg_power_w']) if row['avg_power_w'] is not None else None,
                            'max_power_w': float(row['avg_power_w']) if row['avg_power_w'] is not None else None,
                        }

            avg_cursor.execute(
                '''SELECT CAST(strftime('%s', bucket_start) AS INTEGER) AS bucket_ts,
                          avg_power_w,
                          max_power_w
                   FROM five_minute_averages
                   WHERE bucket_start >= ? AND bucket_start < ?
                   ORDER BY bucket_start''',
                (day_start_str, day_end_str),
            )
            rows = avg_cursor.fetchall()
            avg_bucket_count = len(rows)
            for row in rows:
                point_ts = _normalize_bucket_timestamp_ms(row['bucket_ts'], bucket_seconds=300)
                if point_ts is None:
                    continue
                series_map[point_ts] = {
                    'timestamp': point_ts,
                    'avg_power_w': float(row['avg_power_w']) if row['avg_power_w'] is not None else None,
                    'max_power_w': float(row['max_power_w']) if row['max_power_w'] is not None else None,
                }

        # The raw-table rescan below re-aggregates the whole day directly from
        # solar_raw_data -- expensive, and redundant whenever
        # five_minute_averages is already keeping up: the live collector
        # upserts each bucket's average on every incoming sample (see the
        # persist path above), so the rollup table is normally current to
        # within one sample even for "today", not just for finished
        # historical days. Compare against how many buckets *should* exist by
        # now rather than a fixed 280/day threshold, so a genuinely stalled
        # collector (today or historical) still falls back to this rescan for
        # gap recovery, but a healthy "today" no longer pays for a full-day
        # scan on every cache refresh.
        if _is_today:
            _now_for_completeness = time.time()
            _expected_buckets_today = max(
                0, int((min(_now_for_completeness, day_end.timestamp()) - day_start.timestamp()) // 300)
            )
            # The in-progress bucket is still filling and is refreshed
            # separately below via get_live_solar_points, so don't count it
            # against completeness.
            _avg_is_complete = avg_bucket_count >= max(0, _expected_buckets_today - 1)
        else:
            _avg_is_complete = avg_bucket_count >= 280
        realtime_rows = []
        if raw_cursor is not None and not _avg_is_complete:
            raw_cursor.execute(
                '''SELECT CAST(timestamp / 300 AS INTEGER) * 300 AS bucket_ts,
                          AVG(power_w) AS avg_power_w,
                          MAX(power_w) AS max_power_w
                   FROM solar_raw_data
                   WHERE timestamp >= ? AND timestamp < ?
                   GROUP BY CAST(timestamp / 300 AS INTEGER) * 300
                   ORDER BY bucket_ts''',
                (day_start.timestamp(), day_end.timestamp()),
            )
            realtime_rows = raw_cursor.fetchall()
            if not realtime_rows:
                raw_cursor.execute(
                    '''SELECT CAST(CAST(strftime('%s', timestamp) AS INTEGER) / 300 AS INTEGER) * 300 AS bucket_ts,
                              AVG(power_w) AS avg_power_w,
                              MAX(power_w) AS max_power_w
                       FROM solar_realtime
                       WHERE timestamp >= ? AND timestamp < ?
                       GROUP BY CAST(CAST(strftime('%s', timestamp) AS INTEGER) / 300 AS INTEGER) * 300
                       ORDER BY bucket_ts''',
                    (day_start_str, day_end_str),
                )
                realtime_rows = raw_cursor.fetchall()
        for row in realtime_rows:
            point_ts = _normalize_bucket_timestamp_ms(row['bucket_ts'], bucket_seconds=300)
            if point_ts is None:
                continue
            series_map[point_ts] = {
                'timestamp': point_ts,
                'avg_power_w': float(row['avg_power_w']) if row['avg_power_w'] is not None else None,
                'max_power_w': float(row['max_power_w']) if row['max_power_w'] is not None else None,
            }

        if target_date.date() == datetime.now().date():
            now_ts = time.time()
            live_bucket_start_ts = int(now_ts // 300) * 300
            day_start_ts = day_start.timestamp()
            day_end_ts = day_end.timestamp()
            if day_start_ts <= live_bucket_start_ts < day_end_ts:
                live_points = get_live_solar_points(
                    live_bucket_start_ts,
                    max_point_age_seconds=SOLAR_POINT_MAX_AGE_SECONDS,
                    allow_direct_refresh=True,
                )
                bucket_values = [
                    float(point['power'])
                    for point in live_points
                    if point.get('power') is not None
                    and live_bucket_start_ts <= float(point.get('timestamp', 0) or 0) < min(now_ts, day_end_ts) + 0.001
                ]
                if bucket_values:
                    point_ts = _normalize_bucket_timestamp_ms(live_bucket_start_ts, bucket_seconds=300)
                    if point_ts is not None:
                        series_map[point_ts] = {
                            'timestamp': point_ts,
                            'avg_power_w': sum(bucket_values) / len(bucket_values),
                            'max_power_w': max(bucket_values),
                        }

        result = [series_map[key] for key in sorted(series_map.keys())]
        with _series_cache_lock:
            _solar_series_cache[cache_key] = {
                'expires': time.time() + SERIES_CACHE_TTL_SECONDS,
                'data': result,
            }
        return list(result)

    except Exception as e:
        print(f"Solar 5-min series query failed: {e}")
        return []
    finally:
        for conn in (avg_conn, raw_conn):
            if conn:
                conn.close()


def get_battery_5min_series(target_date):
    """Return battery 5-minute average power for the selected day."""
    day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    cache_key = day_start.strftime('%Y-%m-%d')
    now_for_cache = time.time()
    with _series_cache_lock:
        cache_entry = _battery_series_cache.get(cache_key)
        if cache_entry and now_for_cache < cache_entry['expires']:
            return list(cache_entry['data'])

    avg_conn = None
    raw_conn = None

    try:
        avg_conn = open_battery_history_connection(kind='avg')
        raw_conn = open_battery_history_connection(kind='raw')
        if avg_conn is None and raw_conn is None:
            return []

        avg_cursor = avg_conn.cursor() if avg_conn is not None else None
        raw_cursor = raw_conn.cursor() if raw_conn is not None else None
        day_start_str = datetime.fromtimestamp(day_start.timestamp(), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        day_end_str = datetime.fromtimestamp(day_end.timestamp(), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        series_map = {}
        avg_bucket_count = 0
        _is_today = target_date.date() == datetime.now().date()

        avg_has_soc = False
        raw_has_soc = False
        realtime_has_soc = False

        if avg_cursor is not None:
            try:
                avg_cursor.execute('PRAGMA table_info(five_minute_averages)')
                avg_columns = {str(row[1]) for row in avg_cursor.fetchall()}
                avg_has_soc = 'avg_soc_pct' in avg_columns
            except Exception:
                avg_has_soc = False

        if raw_cursor is not None:
            try:
                raw_cursor.execute('PRAGMA table_info(battery_raw_data)')
                raw_columns = {str(row[1]) for row in raw_cursor.fetchall()}
                raw_has_soc = 'soc_pct' in raw_columns
            except Exception:
                raw_has_soc = False

            try:
                raw_cursor.execute('PRAGMA table_info(battery_realtime)')
                realtime_columns = {str(row[1]) for row in raw_cursor.fetchall()}
                realtime_has_soc = 'soc_pct' in realtime_columns
            except Exception:
                realtime_has_soc = False

        if avg_cursor is not None:
            avg_cursor.execute(
                f'''SELECT CAST(strftime('%s', bucket_start) AS INTEGER) AS bucket_ts,
                          avg_consumption_w,
                          max_consumption_w,
                          {'avg_soc_pct' if avg_has_soc else 'NULL'} AS avg_soc_pct
                   FROM five_minute_averages
                   WHERE bucket_start >= ? AND bucket_start < ?
                   ORDER BY bucket_start''',
                (day_start_str, day_end_str),
            )
            _avg_rows = avg_cursor.fetchall()
            avg_bucket_count = len(_avg_rows)
            for row in _avg_rows:
                point_ts = _normalize_bucket_timestamp_ms(row['bucket_ts'], bucket_seconds=300)
                if point_ts is None:
                    continue
                series_map[point_ts] = {
                    'timestamp': point_ts,
                    'avg_consumption_w': float(row['avg_consumption_w']) if row['avg_consumption_w'] is not None else None,
                    'max_consumption_w': float(row['max_consumption_w']) if row['max_consumption_w'] is not None else None,
                    'avg_soc_pct': float(row['avg_soc_pct']) if row['avg_soc_pct'] is not None else None,
                }

        # Skip the full-day raw rescan whenever five_minute_averages is
        # already keeping up -- battery_2sec.db is large (hundreds of MB), so
        # this scan is expensive, and it's redundant whenever the live
        # collector's per-sample bucket upsert (see the persist path above)
        # is doing its job: that keeps the rollup table current to within one
        # sample even for "today", not just for finished historical days.
        # Compare against how many buckets *should* exist by now rather than
        # a fixed 280/day threshold, so a genuinely stalled collector (today
        # or historical) still falls back to this rescan for gap recovery,
        # but a healthy "today" no longer pays for a full-day scan on every
        # cache refresh.
        if _is_today:
            _now_for_completeness = time.time()
            _expected_buckets_today = max(
                0, int((min(_now_for_completeness, day_end.timestamp()) - day_start.timestamp()) // 300)
            )
            # The in-progress bucket is still filling and is already kept
            # live by the collector's per-sample upsert, so don't count it
            # against completeness.
            _avg_is_complete = avg_bucket_count >= max(0, _expected_buckets_today - 1)
        else:
            _avg_is_complete = avg_bucket_count >= 280
        realtime_rows = []
        if raw_cursor is not None and not _avg_is_complete:
            raw_cursor.execute(
                f'''SELECT CAST(timestamp / 300 AS INTEGER) * 300 AS bucket_ts,
                          AVG(consumption_w) AS avg_consumption_w,
                          MAX(consumption_w) AS max_consumption_w,
                          {'AVG(soc_pct)' if raw_has_soc else 'NULL'} AS avg_soc_pct
                   FROM battery_raw_data
                   WHERE timestamp >= ? AND timestamp < ?
                   GROUP BY CAST(timestamp / 300 AS INTEGER) * 300
                   ORDER BY bucket_ts''',
                (day_start.timestamp(), day_end.timestamp()),
            )
            realtime_rows = raw_cursor.fetchall()
            if not realtime_rows:
                raw_cursor.execute(
                    f'''SELECT CAST(CAST(strftime('%s', timestamp) AS INTEGER) / 300 AS INTEGER) * 300 AS bucket_ts,
                              AVG(consumption_w) AS avg_consumption_w,
                              MAX(consumption_w) AS max_consumption_w,
                              {'AVG(soc_pct)' if realtime_has_soc else 'NULL'} AS avg_soc_pct
                       FROM battery_realtime
                       WHERE timestamp >= ? AND timestamp < ?
                       GROUP BY CAST(CAST(strftime('%s', timestamp) AS INTEGER) / 300 AS INTEGER) * 300
                       ORDER BY bucket_ts''',
                    (day_start_str, day_end_str),
                )
                realtime_rows = raw_cursor.fetchall()

        for row in realtime_rows:
            point_ts = _normalize_bucket_timestamp_ms(row['bucket_ts'], bucket_seconds=300)
            if point_ts is None:
                continue
            series_map[point_ts] = {
                'timestamp': point_ts,
                'avg_consumption_w': float(row['avg_consumption_w']) if row['avg_consumption_w'] is not None else None,
                'max_consumption_w': float(row['max_consumption_w']) if row['max_consumption_w'] is not None else None,
                'avg_soc_pct': float(row['avg_soc_pct']) if row['avg_soc_pct'] is not None else None,
            }

        result = [series_map[key] for key in sorted(series_map.keys())]
        with _series_cache_lock:
            _battery_series_cache[cache_key] = {
                'expires': time.time() + SERIES_CACHE_TTL_SECONDS,
                'data': result,
            }
        return list(result)
    except Exception as e:
        print(f"Battery 5-min series query failed: {e}")
        return []
    finally:
        for conn in {avg_conn, raw_conn}:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


def get_solar_daily_totals(start_date=None, end_date=None):
    """Return a date -> solar yield map, sourced from 5-minute rollups with fallback."""
    result_map = {}
    avg_conn = None
    totals_conn = None

    # This is called on every "today" /minute_data cache miss just to read a
    # day's solar yield; only pay for the daily_totals upsert (write lock +
    # commit) at most once per minute per requested range, not on every read.
    persist_key = (str(start_date), str(end_date))
    now_ts = time.time()
    persist_entry = _solar_daily_totals_persist_cache.get(persist_key)
    should_persist = not persist_entry or now_ts >= persist_entry.get('expires', 0.0)

    try:
        avg_conn = open_solar_history_connection(kind='avg')
        totals_conn = open_solar_history_connection(kind='totals', write=should_persist)
        totals_conn = _reuse_if_same_sqlite_file(avg_conn, totals_conn)

        if avg_conn is not None:
            avg_cursor = avg_conn.cursor()
            ensure_solar_history_schema(avg_cursor)

            query = (
                "SELECT date(datetime(bucket_start, 'localtime')) AS local_date, "
                "SUM(energy_kwh) AS total_energy_kwh, "
                "AVG(avg_power_w) AS avg_power_w, "
                "MAX(max_power_w) AS peak_power_w, "
                "COUNT(*) AS buckets "
                "FROM five_minute_averages"
            )
            clauses = []
            params = []
            if start_date is not None:
                clauses.append("date(datetime(bucket_start, 'localtime')) >= ?")
                params.append(str(start_date))
            if end_date is not None:
                clauses.append("date(datetime(bucket_start, 'localtime')) < ?")
                params.append(str(end_date))
            if clauses:
                query += ' WHERE ' + ' AND '.join(clauses)
            query += ' GROUP BY local_date ORDER BY local_date'

            avg_cursor.execute(query, tuple(params))
            avg_rows = avg_cursor.fetchall()

            for row in avg_rows:
                local_date = row['local_date']
                if not local_date:
                    continue
                result_map[str(local_date)] = (
                    float(row['total_energy_kwh']) if row['total_energy_kwh'] is not None else None
                )

            if totals_conn is not None and avg_rows and should_persist:
                totals_cursor = totals_conn.cursor()
                ensure_solar_history_schema(totals_cursor)
                for row in avg_rows:
                    local_date = row['local_date']
                    if not local_date:
                        continue
                    _upsert_solar_total_row(
                        totals_cursor,
                        'daily_totals',
                        'date',
                        str(local_date),
                        row['total_energy_kwh'],
                        row['avg_power_w'],
                        row['peak_power_w'],
                        row['buckets'],
                        updated_at=time.time(),
                    )
                totals_conn.commit()
                _solar_daily_totals_persist_cache[persist_key] = {'expires': now_ts + 60.0}
    except Exception as e:
        print(f"Solar daily totals aggregation failed: {e}")
    finally:
        for conn_local in {avg_conn, totals_conn}:
            if conn_local:
                try:
                    conn_local.close()
                except Exception:
                    pass

    # Fallback to stored daily_totals for dates not present in 5-minute rollups.
    # During the current day the collector may have raw samples before the
    # throttled rollup has created its first aggregate bucket. Read only
    # today's missing bucket range in that case so the daily chart remains
    # current without rescanning historical raw data.
    today_key = datetime.now().strftime('%Y-%m-%d')
    if (
        start_date is not None
        and end_date is not None
        and str(start_date) <= today_key < str(end_date)
        and today_key not in result_map
    ):
        raw_conn = None
        try:
            today_start = datetime.strptime(today_key, '%Y-%m-%d')
            today_end = today_start + timedelta(days=1)
            raw_conn = open_solar_history_connection(kind='raw')
            if raw_conn is not None:
                raw_cursor = raw_conn.cursor()
                raw_cursor.execute(
                    '''SELECT CAST(timestamp / 300 AS INTEGER) * 300 AS bucket_ts,
                              AVG(power_w) AS avg_power_w
                       FROM solar_raw_data
                       WHERE timestamp >= ? AND timestamp < ?
                         AND power_w IS NOT NULL
                       GROUP BY CAST(timestamp / 300 AS INTEGER) * 300''',
                    (today_start.timestamp(), today_end.timestamp()),
                )
                raw_rows = raw_cursor.fetchall()
                if raw_rows:
                    result_map[today_key] = sum(
                        float(row['avg_power_w'] or 0.0) * (5.0 / 60.0) / 1000.0
                        for row in raw_rows
                    )
        except Exception as e:
            print(f'Solar daily raw fallback query failed: {e}')
        finally:
            if raw_conn:
                raw_conn.close()

    conn = None
    try:
        conn = open_solar_history_connection(kind='totals')
        if conn is None:
            return dict(sorted(result_map.items()))

        query = 'SELECT date, total_energy_kwh FROM daily_totals'
        clauses = []
        params = []
        if start_date is not None:
            clauses.append('date >= ?')
            params.append(str(start_date))
        if end_date is not None:
            clauses.append('date < ?')
            params.append(str(end_date))
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY date'

        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        for row in cursor.fetchall():
            date_key = row['date']
            if date_key is None:
                continue
            date_key = str(date_key)
            if date_key not in result_map or result_map[date_key] is None:
                result_map[date_key] = (
                    float(row['total_energy_kwh']) if row['total_energy_kwh'] is not None else None
                )
    except Exception as e:
        print(f"Solar daily totals fallback query failed: {e}")
    finally:
        if conn:
            conn.close()

    return dict(sorted(result_map.items()))


def get_solar_monthly_totals(start_month=None, end_month=None):
    """Return month -> solar yield map, sourced from 5-minute rollups with fallback."""
    result_map = {}
    avg_conn = None
    totals_conn = None

    try:
        avg_conn = open_solar_history_connection(kind='avg')
        totals_conn = open_solar_history_connection(kind='totals', write=True)
        totals_conn = _reuse_if_same_sqlite_file(avg_conn, totals_conn)

        if avg_conn is not None:
            avg_cursor = avg_conn.cursor()
            ensure_solar_history_schema(avg_cursor)

            query = (
                "SELECT strftime('%Y-%m', datetime(bucket_start, 'localtime')) AS month_key, "
                "SUM(energy_kwh) AS total_energy_kwh, "
                "AVG(avg_power_w) AS avg_power_w, "
                "MAX(max_power_w) AS peak_power_w, "
                "COUNT(DISTINCT date(datetime(bucket_start, 'localtime'))) AS days "
                "FROM five_minute_averages"
            )
            clauses = []
            params = []
            if start_month is not None:
                clauses.append("strftime('%Y-%m', datetime(bucket_start, 'localtime')) >= ?")
                params.append(str(start_month))
            if end_month is not None:
                clauses.append("strftime('%Y-%m', datetime(bucket_start, 'localtime')) < ?")
                params.append(str(end_month))
            if clauses:
                query += ' WHERE ' + ' AND '.join(clauses)
            query += ' GROUP BY month_key ORDER BY month_key'

            avg_cursor.execute(query, tuple(params))
            avg_rows = avg_cursor.fetchall()

            for row in avg_rows:
                month_key = row['month_key']
                if not month_key:
                    continue
                result_map[str(month_key)] = (
                    float(row['total_energy_kwh']) if row['total_energy_kwh'] is not None else None
                )

            if totals_conn is not None and avg_rows:
                totals_cursor = totals_conn.cursor()
                ensure_solar_history_schema(totals_cursor)
                for row in avg_rows:
                    month_key = row['month_key']
                    if not month_key:
                        continue
                    _upsert_solar_total_row(
                        totals_cursor,
                        'monthly_totals',
                        'month',
                        str(month_key),
                        row['total_energy_kwh'],
                        row['avg_power_w'],
                        row['peak_power_w'],
                        row['days'],
                        updated_at=time.time(),
                    )
                totals_conn.commit()
    except Exception as e:
        print(f"Solar monthly totals aggregation failed: {e}")
    finally:
        for conn_local in {avg_conn, totals_conn}:
            if conn_local:
                try:
                    conn_local.close()
                except Exception:
                    pass

    conn = None
    try:
        conn = open_solar_history_connection(kind='totals')
        if conn is None:
            return dict(sorted(result_map.items()))

        query = 'SELECT month, total_energy_kwh FROM monthly_totals'
        clauses = []
        params = []
        if start_month is not None:
            clauses.append('month >= ?')
            params.append(str(start_month))
        if end_month is not None:
            clauses.append('month < ?')
            params.append(str(end_month))
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY month'

        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        for row in cursor.fetchall():
            month_key = row['month']
            if month_key is None:
                continue
            month_key = str(month_key)
            if month_key not in result_map or result_map[month_key] is None:
                result_map[month_key] = (
                    float(row['total_energy_kwh']) if row['total_energy_kwh'] is not None else None
                )
    except Exception as e:
        print(f"Solar monthly totals fallback query failed: {e}")
    finally:
        if conn:
            conn.close()

    conn = None
    try:
        conn = open_solar_history_connection(kind='totals')
        if conn is None:
            return dict(sorted(result_map.items()))

        query = (
            "SELECT substr(date, 1, 7) AS month_key, "
            "SUM(total_energy_kwh) AS total_energy_kwh "
            "FROM daily_totals"
        )
        clauses = []
        params = []
        if start_month is not None:
            clauses.append("date >= ?")
            params.append(f"{start_month}-01")
        if end_month is not None:
            clauses.append("date < ?")
            params.append(f"{end_month}-01")
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' GROUP BY month_key ORDER BY month_key'

        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        for row in cursor.fetchall():
            month_key = row['month_key']
            if month_key is None:
                continue
            month_key = str(month_key)
            if month_key not in result_map or result_map[month_key] is None:
                result_map[month_key] = (
                    float(row['total_energy_kwh']) if row['total_energy_kwh'] is not None else None
                )
    except Exception as e:
        print(f"Solar monthly daily fallback query failed: {e}")
    finally:
        if conn:
            conn.close()

    return dict(sorted(result_map.items()))


def get_battery_daily_discharge_totals(start_date=None, end_date=None):
    """Return date -> battery discharge kWh map (discharging counted as positive kWh)."""
    cache_key = (str(start_date), str(end_date))
    now_ts = time.time()
    with _battery_daily_totals_cache_lock:
        cached = _battery_daily_totals_cache.get(cache_key)
        if cached and now_ts < cached.get('expires', 0.0):
            return dict(cached['discharge'])

    avg_conn = None
    totals_conn = None

    try:
        # Prefer 5-minute averages grouped by local day so monthly tab totals are
        # consistent with the day-series values shown in Daily.
        avg_conn = open_battery_history_connection(kind='avg')
        result = {}

        if avg_conn is not None:
            avg_cursor = avg_conn.cursor()
            query = (
                "SELECT local_date, SUM(discharge_kwh) AS total_discharge_kwh, "
                "       SUM(charge_kwh) AS total_charge_kwh, "
                "       SUM(net_kwh) AS total_net_kwh "
                "FROM ("
                "  SELECT date(datetime(bucket_start, 'localtime')) AS local_date, "
                "         CASE WHEN avg_consumption_w > 0 "
                "              THEN (avg_consumption_w * (5.0 / 60.0) / 1000.0) "
                "              ELSE 0.0 END AS discharge_kwh, "
                "         CASE WHEN avg_consumption_w < 0 "
                "              THEN ((-avg_consumption_w) * (5.0 / 60.0) / 1000.0) "
                "              ELSE 0.0 END AS charge_kwh, "
                "         (avg_consumption_w * (5.0 / 60.0) / 1000.0) AS net_kwh "
                "  FROM five_minute_averages"
                ")"
            )
            clauses = []
            params = []
            if start_date is not None:
                clauses.append('local_date >= ?')
                params.append(str(start_date))
            if end_date is not None:
                clauses.append('local_date < ?')
                params.append(str(end_date))
            if clauses:
                query += ' WHERE ' + ' AND '.join(clauses)
            query += ' GROUP BY local_date ORDER BY local_date'

            avg_cursor.execute(query, tuple(params))
            discharge_result = {}
            charge_result = {}
            net_result = {}
            for row in avg_cursor.fetchall():
                date_value = row['local_date']
                total_discharge_kwh = row['total_discharge_kwh']
                if date_value is None or total_discharge_kwh is None:
                    continue
                date_key = str(date_value)
                discharge_result[date_key] = max(0.0, float(total_discharge_kwh))
                charge_result[date_key] = max(0.0, float(row['total_charge_kwh'] or 0.0))
                net_result[date_key] = float(row['total_net_kwh'] or 0.0)

            if discharge_result or charge_result or net_result:
                with _battery_daily_totals_cache_lock:
                    _battery_daily_totals_cache[cache_key] = {
                        'expires': now_ts + SERIES_CACHE_TTL_SECONDS,
                        'discharge': discharge_result,
                        'charge': charge_result,
                        'net': net_result,
                    }
                return discharge_result

        # Fallback for older installs where avg DB may be missing.
        totals_conn = open_battery_history_connection(kind='totals')
        if totals_conn is None:
            return {}

        query = 'SELECT date, total_energy_kwh FROM daily_totals'
        clauses = []
        params = []

        if start_date is not None:
            clauses.append('date >= ?')
            params.append(str(start_date))
        if end_date is not None:
            clauses.append('date < ?')
            params.append(str(end_date))
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY date'

        totals_cursor = totals_conn.cursor()
        totals_cursor.execute(query, tuple(params))

        fallback_result = {}
        for row in totals_cursor.fetchall():
            date_value = row['date']
            total_energy_kwh = row['total_energy_kwh']
            if date_value is None or total_energy_kwh is None:
                continue
            fallback_result[str(date_value)] = max(0.0, float(total_energy_kwh))
        return fallback_result
    except Exception as e:
        print(f"Battery daily discharge totals query failed: {e}")
        return {}
    finally:
        for conn in (avg_conn, totals_conn):
            if conn:
                conn.close()


def get_battery_daily_net_totals(start_date=None, end_date=None):
    """Return date -> net battery kWh map (discharge positive, charge negative)."""
    cache_key = (str(start_date), str(end_date))
    now_ts = time.time()
    with _battery_daily_totals_cache_lock:
        cached = _battery_daily_totals_cache.get(cache_key)
        if cached and now_ts < cached.get('expires', 0.0):
            return dict(cached['net'])

    avg_conn = None
    totals_conn = None

    try:
        avg_conn = open_battery_history_connection(kind='avg')
        result = {}

        if avg_conn is not None:
            avg_cursor = avg_conn.cursor()
            query = (
                "SELECT local_date, SUM(net_kwh) AS total_net_kwh "
                "FROM ("
                "  SELECT date(datetime(bucket_start, 'localtime')) AS local_date, "
                "         (avg_consumption_w * (5.0 / 60.0) / 1000.0) AS net_kwh "
                "  FROM five_minute_averages"
                ")"
            )
            clauses = []
            params = []
            if start_date is not None:
                clauses.append('local_date >= ?')
                params.append(str(start_date))
            if end_date is not None:
                clauses.append('local_date < ?')
                params.append(str(end_date))
            if clauses:
                query += ' WHERE ' + ' AND '.join(clauses)
            query += ' GROUP BY local_date ORDER BY local_date'

            avg_cursor.execute(query, tuple(params))
            for row in avg_cursor.fetchall():
                date_value = row['local_date']
                total_net_kwh = row['total_net_kwh']
                if date_value is None or total_net_kwh is None:
                    continue
                result[str(date_value)] = float(total_net_kwh)

            if result:
                return result

        totals_conn = open_battery_history_connection(kind='totals')
        if totals_conn is None:
            return {}

        query = 'SELECT date, total_energy_kwh FROM daily_totals'
        clauses = []
        params = []

        if start_date is not None:
            clauses.append('date >= ?')
            params.append(str(start_date))
        if end_date is not None:
            clauses.append('date < ?')
            params.append(str(end_date))
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY date'

        totals_cursor = totals_conn.cursor()
        totals_cursor.execute(query, tuple(params))

        fallback_result = {}
        for row in totals_cursor.fetchall():
            date_value = row['date']
            total_energy_kwh = row['total_energy_kwh']
            if date_value is None or total_energy_kwh is None:
                continue
            fallback_result[str(date_value)] = float(total_energy_kwh)
        return fallback_result
    except Exception as e:
        print(f"Battery daily net totals query failed: {e}")
        return {}
    finally:
        for conn in (avg_conn, totals_conn):
            if conn:
                conn.close()


def get_battery_daily_charge_totals(start_date=None, end_date=None):
    """Return date -> battery charge kWh map (charging counted as positive kWh)."""
    cache_key = (str(start_date), str(end_date))
    now_ts = time.time()
    with _battery_daily_totals_cache_lock:
        cached = _battery_daily_totals_cache.get(cache_key)
        if cached and now_ts < cached.get('expires', 0.0):
            return dict(cached['charge'])

    avg_conn = None
    totals_conn = None

    try:
        # Same source as discharge totals for internal consistency.
        avg_conn = open_battery_history_connection(kind='avg')
        result = {}

        if avg_conn is not None:
            avg_cursor = avg_conn.cursor()
            query = (
                "SELECT local_date, SUM(charge_kwh) AS total_charge_kwh "
                "FROM ("
                "  SELECT date(datetime(bucket_start, 'localtime')) AS local_date, "
                "         CASE WHEN avg_consumption_w < 0 "
                "              THEN ((-avg_consumption_w) * (5.0 / 60.0) / 1000.0) "
                "              ELSE 0.0 END AS charge_kwh "
                "  FROM five_minute_averages"
                ")"
            )
            clauses = []
            params = []
            if start_date is not None:
                clauses.append('local_date >= ?')
                params.append(str(start_date))
            if end_date is not None:
                clauses.append('local_date < ?')
                params.append(str(end_date))
            if clauses:
                query += ' WHERE ' + ' AND '.join(clauses)
            query += ' GROUP BY local_date ORDER BY local_date'

            avg_cursor.execute(query, tuple(params))
            for row in avg_cursor.fetchall():
                date_value = row['local_date']
                total_charge_kwh = row['total_charge_kwh']
                if date_value is None or total_charge_kwh is None:
                    continue
                result[str(date_value)] = max(0.0, float(total_charge_kwh))

            if result:
                return result

        totals_conn = open_battery_history_connection(kind='totals')
        if totals_conn is None:
            return {}

        query = 'SELECT date, total_energy_kwh FROM daily_totals'
        clauses = []
        params = []

        if start_date is not None:
            clauses.append('date >= ?')
            params.append(str(start_date))
        if end_date is not None:
            clauses.append('date < ?')
            params.append(str(end_date))
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY date'

        totals_cursor = totals_conn.cursor()
        totals_cursor.execute(query, tuple(params))

        fallback_result = {}
        for row in totals_cursor.fetchall():
            date_value = row['date']
            total_energy_kwh = row['total_energy_kwh']
            if date_value is None or total_energy_kwh is None:
                continue
            fallback_result[str(date_value)] = max(0.0, -float(total_energy_kwh))
        return fallback_result
    except Exception as e:
        print(f"Battery daily totals query failed: {e}")
        return {}
    finally:
        for conn in (avg_conn, totals_conn):
            if conn:
                conn.close()


def get_battery_monthly_discharge_totals(start_month=None, end_month=None):
    """Return month -> battery discharge kWh map (discharging counted as positive kWh)."""
    conn = None

    try:
        conn = open_battery_history_connection(kind='totals')
        if conn is None:
            return {}

        query = 'SELECT month, total_energy_kwh FROM monthly_totals'
        clauses = []
        params = []

        if start_month is not None:
            clauses.append('month >= ?')
            params.append(start_month)
        if end_month is not None:
            clauses.append('month < ?')
            params.append(end_month)
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY month'

        cursor = conn.cursor()
        cursor.execute(query, tuple(params))

        result = {}
        for row in cursor.fetchall():
            month_value = row['month']
            total_energy_kwh = row['total_energy_kwh']
            if month_value is None or total_energy_kwh is None:
                continue
            discharge_kwh = max(0.0, float(total_energy_kwh))
            result[str(month_value)] = discharge_kwh
        return result
    except Exception as e:
        print(f"Battery monthly discharge totals query failed: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def get_battery_monthly_charge_totals(start_month=None, end_month=None):
    """Return month -> battery charge kWh map (charging counted as positive kWh)."""
    conn = None

    try:
        conn = open_battery_history_connection(kind='totals')
        if conn is None:
            return {}

        query = 'SELECT month, total_energy_kwh FROM monthly_totals'
        clauses = []
        params = []

        if start_month is not None:
            clauses.append('month >= ?')
            params.append(start_month)
        if end_month is not None:
            clauses.append('month < ?')
            params.append(end_month)
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY month'

        cursor = conn.cursor()
        cursor.execute(query, tuple(params))

        result = {}
        for row in cursor.fetchall():
            month_value = row['month']
            total_energy_kwh = row['total_energy_kwh']
            if month_value is None or total_energy_kwh is None:
                continue
            # Charging is stored as negative power/energy in battery history.
            charge_kwh = max(0.0, -float(total_energy_kwh))
            result[str(month_value)] = charge_kwh
        return result
    except Exception as e:
        print(f"Battery monthly totals query failed: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def get_battery_monthly_net_totals(start_month=None, end_month=None):
    """Return month -> net battery kWh map (discharge positive, charge negative)."""
    conn = None

    try:
        conn = open_battery_history_connection(kind='totals')
        if conn is None:
            return {}

        query = 'SELECT month, total_energy_kwh FROM monthly_totals'
        clauses = []
        params = []

        if start_month is not None:
            clauses.append('month >= ?')
            params.append(start_month)
        if end_month is not None:
            clauses.append('month < ?')
            params.append(end_month)
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY month'

        cursor = conn.cursor()
        cursor.execute(query, tuple(params))

        result = {}
        for row in cursor.fetchall():
            month_value = row['month']
            total_energy_kwh = row['total_energy_kwh']
            if month_value is None or total_energy_kwh is None:
                continue
            result[str(month_value)] = float(total_energy_kwh)
        return result
    except Exception as e:
        print(f"Battery monthly net totals query failed: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def get_solar_yearly_totals(start_year=None, end_year=None):
    """Return year -> solar yield map, sourced from 5-minute rollups with fallback."""
    result_map = {}
    avg_conn = None
    totals_conn = None

    try:
        avg_conn = open_solar_history_connection(kind='avg')
        totals_conn = open_solar_history_connection(kind='totals', write=True)
        totals_conn = _reuse_if_same_sqlite_file(avg_conn, totals_conn)

        if avg_conn is not None:
            avg_cursor = avg_conn.cursor()
            ensure_solar_history_schema(avg_cursor)

            query = (
                "SELECT strftime('%Y', datetime(bucket_start, 'localtime')) AS year_key, "
                "SUM(energy_kwh) AS total_energy_kwh, "
                "AVG(avg_power_w) AS avg_power_w, "
                "MAX(max_power_w) AS peak_power_w, "
                "COUNT(DISTINCT strftime('%Y-%m', datetime(bucket_start, 'localtime'))) AS months "
                "FROM five_minute_averages"
            )
            clauses = []
            params = []
            if start_year is not None:
                clauses.append("strftime('%Y', datetime(bucket_start, 'localtime')) >= ?")
                params.append(str(start_year))
            if end_year is not None:
                clauses.append("strftime('%Y', datetime(bucket_start, 'localtime')) < ?")
                params.append(str(end_year))
            if clauses:
                query += ' WHERE ' + ' AND '.join(clauses)
            query += ' GROUP BY year_key ORDER BY year_key'

            avg_cursor.execute(query, tuple(params))
            avg_rows = avg_cursor.fetchall()

            for row in avg_rows:
                year_key = row['year_key']
                if year_key is None:
                    continue
                result_map[str(year_key)] = (
                    float(row['total_energy_kwh']) if row['total_energy_kwh'] is not None else None
                )

            if totals_conn is not None and avg_rows:
                totals_cursor = totals_conn.cursor()
                ensure_solar_history_schema(totals_cursor)
                for row in avg_rows:
                    year_key = row['year_key']
                    if year_key is None:
                        continue
                    _upsert_solar_total_row(
                        totals_cursor,
                        'yearly_totals',
                        'year',
                        int(year_key),
                        row['total_energy_kwh'],
                        row['avg_power_w'],
                        row['peak_power_w'],
                        row['months'],
                        updated_at=time.time(),
                    )
                totals_conn.commit()
    except Exception as e:
        print(f"Solar yearly totals aggregation failed: {e}")
    finally:
        for conn_local in {avg_conn, totals_conn}:
            if conn_local:
                try:
                    conn_local.close()
                except Exception:
                    pass

    conn = None
    try:
        conn = open_solar_history_connection(kind='totals')
        if conn is None:
            return dict(sorted(result_map.items()))

        query = 'SELECT year, total_energy_kwh FROM yearly_totals'
        clauses = []
        params = []
        if start_year is not None:
            clauses.append('year >= ?')
            params.append(str(start_year))
        if end_year is not None:
            clauses.append('year < ?')
            params.append(str(end_year))
        if clauses:
            query += ' WHERE ' + ' AND '.join(clauses)
        query += ' ORDER BY year'

        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        for row in cursor.fetchall():
            year_key = row['year']
            if year_key is None:
                continue
            year_key = str(year_key)
            if year_key not in result_map or result_map[year_key] is None:
                result_map[year_key] = (
                    float(row['total_energy_kwh']) if row['total_energy_kwh'] is not None else None
                )
    except Exception as e:
        print(f"Solar yearly totals fallback query failed: {e}")
    finally:
        if conn:
            conn.close()

    return dict(sorted(result_map.items()))


def _downsample_solar_points(points, max_points=SOLAR_WINDOW_MAX_POINTS):
    """Reduce point count for smoother chart rendering without losing the trend."""
    if len(points) <= max_points:
        return points

    step = max(1, len(points) // max_points)
    sampled = points[::step]
    if sampled[-1] != points[-1]:
        sampled.append(points[-1])
    return sampled


def _normalize_bucket_timestamp_ms(timestamp_value, bucket_seconds=300):
    """Floor a seconds-or-milliseconds timestamp to a shared bucket boundary in milliseconds."""
    if timestamp_value in (None, ''):
        return None

    try:
        timestamp_float = float(timestamp_value)
    except (TypeError, ValueError):
        return None

    timestamp_seconds = timestamp_float / 1000.0 if timestamp_float > 1_000_000_000_000 else timestamp_float
    bucket_seconds = max(1, int(round(float(bucket_seconds or 300))))
    return int(timestamp_seconds // bucket_seconds) * bucket_seconds * 1000


def _get_local_utc_offset_seconds(reference_ts=None):
    """Return the local UTC offset in seconds for the provided timestamp."""
    try:
        if reference_ts is None:
            dt_local = datetime.now().astimezone()
        else:
            dt_local = datetime.fromtimestamp(float(reference_ts)).astimezone()
        return int((dt_local.utcoffset() or timedelta(0)).total_seconds())
    except Exception:
        return 0


def _correct_legacy_solar_series_timestamps(series, target_date=None, reference_now=None):
    """Shift old solar daily points when they still show the historic UTC/local skew."""
    if not series:
        return series

    if target_date is not None and target_date.date() != datetime.now().date():
        return series

    numeric_timestamps = [
        float(item.get('timestamp')) / 1000.0
        for item in series
        if isinstance(item, dict) and item.get('timestamp') is not None
    ]
    if not numeric_timestamps:
        return series

    reference_now = float(reference_now or time.time())
    latest_point_ts = max(numeric_timestamps)
    local_offset_seconds = _get_local_utc_offset_seconds(reference_now)
    if abs(local_offset_seconds) < 1800:
        return series

    lag_seconds = reference_now - latest_point_ts
    tolerance_seconds = max(900, abs(local_offset_seconds) // 2)
    if lag_seconds < 0 or abs(lag_seconds - local_offset_seconds) > tolerance_seconds:
        return series

    corrected = []
    shift_ms = int(local_offset_seconds * 1000)
    for item in series:
        corrected_item = dict(item)
        if corrected_item.get('timestamp') is not None:
            corrected_item['timestamp'] = int(float(corrected_item['timestamp']) + shift_ms)
        corrected.append(corrected_item)
    return corrected


def _build_synced_power_solar_series(grid_series=None, solar_series=None, bucket_seconds=300):
    """Align P1 and solar samples onto the same timeline for cleaner averages and charts."""
    synced_map = {}

    for sample in grid_series or []:
        bucket_ts = _normalize_bucket_timestamp_ms(sample.get('timestamp'), bucket_seconds=bucket_seconds)
        if bucket_ts is None:
            continue

        entry = synced_map.setdefault(bucket_ts, {
            'timestamp': bucket_ts,
            'grid_power_w': None,
            'solar_power_w': None,
            'home_load_w': None,
        })
        value = sample.get('grid_power_w', sample.get('consumption', sample.get('power')))
        try:
            entry['grid_power_w'] = float(value) if value is not None else None
        except (TypeError, ValueError):
            entry['grid_power_w'] = None

    for sample in solar_series or []:
        bucket_ts = _normalize_bucket_timestamp_ms(sample.get('timestamp'), bucket_seconds=bucket_seconds)
        if bucket_ts is None:
            continue

        entry = synced_map.setdefault(bucket_ts, {
            'timestamp': bucket_ts,
            'grid_power_w': None,
            'solar_power_w': None,
            'home_load_w': None,
        })
        value = sample.get('solar_power_w', sample.get('avg_power_w', sample.get('power')))
        try:
            entry['solar_power_w'] = float(value) if value is not None else None
        except (TypeError, ValueError):
            entry['solar_power_w'] = None

    for entry in synced_map.values():
        grid_power = entry.get('grid_power_w')
        solar_power = entry.get('solar_power_w')
        if grid_power is not None and solar_power is not None:
            entry['home_load_w'] = float(grid_power) + float(solar_power)

    return [synced_map[key] for key in sorted(synced_map.keys())]


def get_latest_solar_point(allow_direct_refresh=False):
    """Return the latest available solar power point for live charts."""
    global _solar_latest_cache
    now_ts = time.time()
    if now_ts < _solar_latest_cache['expires']:
        return dict(_solar_latest_cache['data'])

    if _is_fresh_sample_timestamp(_solar_runtime_state.get('timestamp'), SOLAR_SAMPLE_STALE_AFTER_SECONDS):
        _solar_latest_cache = {
            'expires': now_ts + SOLAR_LIVE_CACHE_TTL_SECONDS,
            'data': dict(_solar_runtime_state),
        }
        return dict(_solar_runtime_state)

    raw_conn = None
    avg_conn = None
    stale_result = None

    try:
        raw_conn = open_solar_history_connection(kind='raw')
        if raw_conn is not None:
            cursor = raw_conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='solar_raw_data'")
            has_raw_table = cursor.fetchone() is not None

            if has_raw_table:
                cursor.execute(
                    '''SELECT timestamp AS point_ts,
                              power_w
                       FROM solar_raw_data
                       ORDER BY timestamp DESC
                       LIMIT 1'''
                )
                row = cursor.fetchone()
                if row and row['point_ts'] is not None:
                    result = {
                        'timestamp': float(row['point_ts']),
                        'power_w': float(row['power_w']) if row['power_w'] is not None else None,
                    }
                    stale_result = result
                    if _is_fresh_sample_timestamp(result['timestamp'], SOLAR_LIVE_REFRESH_SECONDS):
                        _solar_latest_cache = {
                            'expires': now_ts + SOLAR_LIVE_CACHE_TTL_SECONDS,
                            'data': result,
                        }
                        return result

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='solar_realtime'")
            has_realtime_table = cursor.fetchone() is not None
            if has_realtime_table:
                cursor.execute(
                    '''SELECT CAST(strftime('%s', timestamp) AS INTEGER) AS point_ts,
                              power_w
                       FROM solar_realtime
                       ORDER BY timestamp DESC
                       LIMIT 1'''
                )
                row = cursor.fetchone()
                if row and row['point_ts'] is not None:
                    result = {
                        'timestamp': float(row['point_ts']),
                        'power_w': float(row['power_w']) if row['power_w'] is not None else None,
                    }
                    stale_result = stale_result or result
                    if _is_fresh_sample_timestamp(result['timestamp'], SOLAR_LIVE_REFRESH_SECONDS):
                        _solar_latest_cache = {
                            'expires': now_ts + SOLAR_LIVE_CACHE_TTL_SECONDS,
                            'data': result,
                        }
                        return result

        avg_conn = open_solar_history_connection(kind='avg')
        if avg_conn is not None:
            cursor = avg_conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='five_minute_averages'")
            has_5min_table = cursor.fetchone() is not None
            if has_5min_table:
                cursor.execute(
                    '''SELECT CAST(strftime('%s', bucket_start) AS INTEGER) AS point_ts,
                              avg_power_w AS power_w
                       FROM five_minute_averages
                       ORDER BY bucket_start DESC
                       LIMIT 1'''
                )
                row = cursor.fetchone()
                if row and row['point_ts'] is not None:
                    result = {
                        'timestamp': float(row['point_ts']),
                        'power_w': float(row['power_w']) if row['power_w'] is not None else None,
                    }
                    stale_result = stale_result or result
                    if _is_fresh_sample_timestamp(result['timestamp'], SOLAR_LIVE_REFRESH_SECONDS):
                        _solar_latest_cache = {
                            'expires': now_ts + SOLAR_LIVE_CACHE_TTL_SECONDS,
                            'data': result,
                        }
                        return result
    except Exception as e:
        print(f"Latest solar point query failed: {e}")
    finally:
        for conn in (raw_conn, avg_conn):
            if conn:
                conn.close()

    if allow_direct_refresh and SUN2000_AVAILABLE:
        try:
            live_data = fetch_and_store_current_power(force=True, persist=None)
            result = {
                'timestamp': now_ts,
                'power_w': float(live_data.get('current_power_w')) if live_data and live_data.get('current_power_w') is not None else None,
            }
            _cache_solar_runtime_point(result['timestamp'], result['power_w'])
            return result
        except Exception as e:
            print(f"Direct live solar refresh failed: {e}")

    if stale_result is not None and _is_fresh_sample_timestamp(stale_result.get('timestamp'), SOLAR_SAMPLE_STALE_AFTER_SECONDS):
        _solar_latest_cache = {
            'expires': now_ts + SOLAR_LIVE_CACHE_TTL_SECONDS,
            'data': stale_result,
        }
        return stale_result

    return {
        'timestamp': None,
        'power_w': None,
    }


def get_live_solar_points(start_timestamp, max_point_age_seconds=SOLAR_POINT_MAX_AGE_SECONDS, allow_direct_refresh=False):
    """Return solar points for the requested live chart window using raw solar history samples."""
    max_point_age_seconds = max(5.0, float(max_point_age_seconds or SOLAR_SAMPLE_STALE_AFTER_SECONDS))

    runtime_points = [
        {'timestamp': float(item['timestamp']), 'power': item.get('power')}
        for item in _solar_runtime_series
        if float(item.get('timestamp', 0) or 0) >= float(start_timestamp)
    ]

    db_points = []
    conn = None

    try:
        conn = open_solar_history_connection(kind='raw')
        if conn is not None:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='solar_raw_data'")
            has_raw_table = cursor.fetchone() is not None
            if has_raw_table:
                cursor.execute(
                    '''SELECT timestamp AS point_ts,
                              power_w
                       FROM solar_raw_data
                       WHERE timestamp >= ?
                       ORDER BY timestamp ASC''',
                    (float(start_timestamp),),
                )
                rows = cursor.fetchall()
                db_points = [
                    {
                        'timestamp': float(row['point_ts']),
                        'power': float(row['power_w']) if row['power_w'] is not None else None,
                    }
                    for row in rows
                    if row['point_ts'] is not None
                ]

            if not db_points:
                start_dt = datetime.fromtimestamp(float(start_timestamp), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(
                    '''SELECT CAST(strftime('%s', timestamp) AS INTEGER) AS point_ts,
                              power_w
                       FROM solar_realtime
                       WHERE timestamp >= ?
                       ORDER BY timestamp ASC''',
                    (start_dt,),
                )
                rows = cursor.fetchall()
                db_points = [
                    {
                        'timestamp': float(row['point_ts']),
                        'power': float(row['power_w']) if row['power_w'] is not None else None,
                    }
                    for row in rows
                    if row['point_ts'] is not None
                ]
    except Exception as e:
        print(f"Live solar window query failed: {e}")
    finally:
        if conn:
            conn.close()

    latest_point = get_latest_solar_point(allow_direct_refresh=allow_direct_refresh)
    latest_points = []
    fallback_max_age_seconds = max(max_point_age_seconds, SOLAR_SAMPLE_STALE_AFTER_SECONDS * 5)
    if latest_point.get('timestamp') is not None and _is_fresh_sample_timestamp(latest_point.get('timestamp'), fallback_max_age_seconds):
        latest_points.append({
            'timestamp': float(latest_point['timestamp']),
            'power': latest_point.get('power_w'),
        })

    merged_points = _merge_live_window_points(
        db_points + runtime_points + latest_points,
        float(start_timestamp),
        time.time(),
        step_seconds=2.0,
    )
    if merged_points:
        return _downsample_solar_points(merged_points, max_points=120)

    return []

# Fallback prices used only if Elia is temporarily unavailable.
FALLBACK_PRICES = {
    0: 45, 1: 40, 2: 35, 3: 32, 4: 30, 5: 35,
    6: 50, 7: 70, 8: 85, 9: 80, 10: 75, 11: 72,
    12: 70, 13: 68, 14: 65, 15: 70, 16: 85, 17: 95,
    18: 100, 19: 105, 20: 100, 21: 85, 22: 70, 23: 55
}

@app.route('/update_settings', methods=['POST'])
def update_settings():
    global EMAIL_TO, injection_threshold, notifications_enabled, runtime_admin_token, SMTP_USER, SMTP_PASS

    auth_error = require_admin_token()
    if auth_error:
        return auth_error

    data = request.get_json(silent=True)
    if not data:
        return jsonify({"message": "Invalid JSON payload"}), 400

    email = str(data.get('email', '')).strip()
    threshold_raw = data.get('threshold', injection_threshold)
    send_notification = data.get('sendNotification', notifications_enabled)
    admin_token = str(data.get('adminToken', runtime_admin_token)).strip()
    smtp_user = str(data.get('smtpUser', SMTP_USER)).strip()
    smtp_pass = str(data.get('smtpPass', SMTP_PASS)).strip()

    if email and '@' not in email:
        return jsonify({"message": "Invalid email address"}), 400

    try:
        threshold = int(threshold_raw)
    except (TypeError, ValueError):
        return jsonify({"message": "Threshold must be a valid number"}), 400

    if threshold < 0 or threshold > 100000:
        return jsonify({"message": "Threshold must be between 0 and 100000"}), 400

    if not isinstance(send_notification, bool):
        return jsonify({"message": "sendNotification must be true or false"}), 400

    if len(admin_token) > 512:
        return jsonify({"message": "Admin token is too long"}), 400

    if len(smtp_user) > 512:
        return jsonify({"message": "SMTP user is too long"}), 400

    if len(smtp_pass) > 1024:
        return jsonify({"message": "SMTP password is too long"}), 400

    # Update global variables or configuration
    if email:
        EMAIL_TO = email
    injection_threshold = threshold
    notifications_enabled = send_notification
    if not DEFAULT_ADMIN_TOKEN:
        runtime_admin_token = admin_token

    if not DEFAULT_SMTP_USER:
        SMTP_USER = smtp_user

    if not DEFAULT_SMTP_PASS:
        SMTP_PASS = smtp_pass

    if not DEFAULT_SMTP_PASS and not SMTP_PASS:
        SMTP_PASS = runtime_admin_token

    try:
        persisted_settings = save_settings()
    except Exception as e:
        return jsonify({"message": f"Failed to persist settings: {e}"}), 500

    return jsonify({
        "message": "Settings updated successfully!",
        "settings": persisted_settings
    })


@app.route('/settings', methods=['GET'])
def get_settings():
    """Return the current persisted notification settings for the UI."""
    auth_error = require_admin_token()
    if auth_error:
        return auth_error

    return jsonify(get_runtime_settings())

def get_elia_imbalance_prices(date_str):
    """Fetch Belgian quarter-hour imbalance prices from Elia's public API."""
    params = {
        'select': 'datetime,imbalanceprice',
        'where': f"datetime >= date'{date_str}' AND datetime < date'{(datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')}'",
        'order_by': 'datetime',
        'limit': 100,
        'timezone': 'Europe/Brussels',
    }

    datasets_to_try = [
        ELIA_IMBALANCE_PRICES_NEAR_REALTIME_DATASET,
        ELIA_IMBALANCE_PRICES_HISTORICAL_DATASET,
    ]

    for dataset_id in datasets_to_try:
        try:
            response = requests.get(
                f'{ELIA_API_BASE_URL}/{dataset_id}/records',
                params=params,
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            rows = payload.get('results', [])
            if not rows:
                continue

            prices = {}
            for row in rows:
                dt_str = row.get('datetime')
                price = row.get('imbalanceprice')
                if not dt_str or price is None:
                    continue

                try:
                    dt = datetime.fromisoformat(dt_str)
                    prices[(dt.hour, dt.minute)] = float(price)
                except (TypeError, ValueError):
                    continue

            if prices:
                return prices, 'elia'
        except Exception as e:
            print(f'Elia API error ({dataset_id}): {e}')

    fallback_prices = {}
    for hour, value in FALLBACK_PRICES.items():
        for minute in (0, 15, 30, 45):
            fallback_prices[(hour, minute)] = value
    return fallback_prices, 'fallback'

def send_notification(message):
    """Send notification via email"""
    if not SMTP_USER or not SMTP_PASS:
        print(f"Notification: {message} (email not configured)")
        return False, "SMTP is not configured. Configure smtpUser and smtpPass in settings (or SMTP_USER/SMTP_PASS env vars)."

    if not EMAIL_TO:
        return False, "Recipient email is not configured. Set notification email in Settings."
    
    try:
        sender = EMAIL_FROM or SMTP_USER
        msg = MIMEText(message)
        msg['Subject'] = 'P1 Dashboard Notification'
        msg['From'] = sender
        msg['To'] = EMAIL_TO
        
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(sender, EMAIL_TO, msg.as_string())
        server.quit()
        print(f"✓ Notification sent: {message}")
        return True, "Notification sent successfully"
    except Exception as e:
        print(f"Failed to send notification: {e}")
        return False, str(e)

def check_excess_power():
    """Check for excess power > injection_threshold for > 15 minutes."""
    global excess_start_time, notification_sent, last_notification_date

    while True:
        try:
            sample_getter = globals().get('_get_latest_live_power_sample')
            latest_sample = sample_getter() if callable(sample_getter) else dict(_p1_runtime_state)
            latest_sample = latest_sample or {}
            current_power = latest_sample.get('live_power', _p1_runtime_state.get('live_power'))

            if current_power is not None:
                current_power = float(current_power)
                current_time = time.time()
                current_date = datetime.now().date()

                if notifications_enabled and current_power < -injection_threshold:
                    if excess_start_time is None:
                        excess_start_time = current_time
                        notification_sent = False
                    elif not notification_sent and (current_time - excess_start_time) > 15 * 60:
                        if last_notification_date != current_date:
                            send_notification(f"Laadt auto - Meer dan 15 minuten overschot van {injection_threshold}+ watt")
                            notification_sent = True
                            last_notification_date = current_date
                else:
                    excess_start_time = None
                    notification_sent = False
            else:
                excess_start_time = None
                notification_sent = False

        except Exception as e:
            print(f"Error checking excess power: {e}")

        time.sleep(60)  # Check every minute

# Initialize databases
def init_db():
    # Initialize raw data database
    try:
        conn_raw = get_db_connection(DB_FILE_RAW, write=True)
    except sqlite3.OperationalError as e:
        err_text = str(e).lower()
        if 'disk i/o error' in err_text or 'unable to open database file' in err_text:
            print(f'Raw DB open failed during init_db: {e}')
            if reset_raw_db_file(reason=f'init_db_open_failure: {e}'):
                conn_raw = get_db_connection(DB_FILE_RAW, write=True)
            else:
                raise
        else:
            raise
    def _initialize_raw_schema(connection):
        c_raw = connection.cursor()
        # Raw data table (2-second intervals, will be cleaned up after 30 days)
        c_raw.execute('''CREATE TABLE IF NOT EXISTS energy_data (
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
        c_raw.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON energy_data(timestamp)')

        c_raw.execute("PRAGMA table_info(energy_data)")
        raw_columns = {row[1] for row in c_raw.fetchall()}
        if 'import_t1_kwh' not in raw_columns:
            c_raw.execute('ALTER TABLE energy_data ADD COLUMN import_t1_kwh REAL')
        if 'import_t2_kwh' not in raw_columns:
            c_raw.execute('ALTER TABLE energy_data ADD COLUMN import_t2_kwh REAL')
        if 'export_t1_kwh' not in raw_columns:
            c_raw.execute('ALTER TABLE energy_data ADD COLUMN export_t1_kwh REAL')
        if 'export_t2_kwh' not in raw_columns:
            c_raw.execute('ALTER TABLE energy_data ADD COLUMN export_t2_kwh REAL')
        connection.commit()

    try:
        _initialize_raw_schema(conn_raw)
    except sqlite3.OperationalError as e:
        conn_raw.close()
        err_text = str(e).lower()
        if 'disk i/o error' in err_text or 'unable to open database file' in err_text:
            print(f'Raw DB schema init failed: {e}')
            if reset_raw_db_file(reason=f'init_db_schema_failure: {e}'):
                conn_raw = get_db_connection(DB_FILE_RAW, write=True)
                _initialize_raw_schema(conn_raw)
            else:
                raise
        else:
            raise
    finally:
        conn_raw.close()
    
    # Initialize averages database
    conn_avg = get_db_connection(DB_FILE_AVG, write=True)
    c_avg = conn_avg.cursor()
    # 5-minute averages table (permanent storage)
    c_avg.execute('''CREATE TABLE IF NOT EXISTS energy_data_5min (
        timestamp REAL PRIMARY KEY,
        power_avg REAL,
        power_min REAL,
        power_max REAL
    )''')
    c_avg.execute('CREATE INDEX IF NOT EXISTS idx_timestamp_5min ON energy_data_5min(timestamp)')
    conn_avg.commit()
    conn_avg.close()

    # Initialize daily consumption database (derived from raw cumulative meters)
    conn_daily = get_db_connection(DB_FILE_DAILY, write=True)
    c_daily = conn_daily.cursor()
    c_daily.execute('''CREATE TABLE IF NOT EXISTS daily_consumption (
        date TEXT PRIMARY KEY,
        day_start_ts REAL,
        day_end_ts REAL,
        consumption_kwh REAL,
        injection_kwh REAL,
        net_consumption_kwh REAL,
        consumption_offpeak_kwh REAL,
        consumption_peak_kwh REAL,
        injection_offpeak_kwh REAL,
        injection_peak_kwh REAL,
        peak_consumption_w REAL,
        peak_consumption_interval TEXT,
        peak_injection_w REAL,
        peak_injection_interval TEXT,
        sample_count INTEGER,
        updated_at REAL
    )''')
    c_daily.execute('CREATE INDEX IF NOT EXISTS idx_daily_day_start ON daily_consumption(day_start_ts)')

    # Backward-compatible schema upgrade for existing installs.
    c_daily.execute("PRAGMA table_info(daily_consumption)")
    existing_columns = {row[1] for row in c_daily.fetchall()}
    if 'peak_consumption_w' not in existing_columns:
        c_daily.execute('ALTER TABLE daily_consumption ADD COLUMN peak_consumption_w REAL')
    if 'peak_consumption_interval' not in existing_columns:
        c_daily.execute('ALTER TABLE daily_consumption ADD COLUMN peak_consumption_interval TEXT')
    if 'peak_injection_w' not in existing_columns:
        c_daily.execute('ALTER TABLE daily_consumption ADD COLUMN peak_injection_w REAL')
    if 'peak_injection_interval' not in existing_columns:
        c_daily.execute('ALTER TABLE daily_consumption ADD COLUMN peak_injection_interval TEXT')
    if 'consumption_offpeak_kwh' not in existing_columns:
        c_daily.execute('ALTER TABLE daily_consumption ADD COLUMN consumption_offpeak_kwh REAL')
    if 'consumption_peak_kwh' not in existing_columns:
        c_daily.execute('ALTER TABLE daily_consumption ADD COLUMN consumption_peak_kwh REAL')
    if 'injection_offpeak_kwh' not in existing_columns:
        c_daily.execute('ALTER TABLE daily_consumption ADD COLUMN injection_offpeak_kwh REAL')
    if 'injection_peak_kwh' not in existing_columns:
        c_daily.execute('ALTER TABLE daily_consumption ADD COLUMN injection_peak_kwh REAL')

    prepare_manual_tables_best_effort(c_daily)

    conn_daily.commit()
    conn_daily.close()

    conn_gas = get_db_connection(DB_FILE_GAS_TOTALS, write=True)
    c_gas = conn_gas.cursor()
    c_gas.execute('''CREATE TABLE IF NOT EXISTS gas_daily_totals (
        date TEXT PRIMARY KEY,
        usage_m3 REAL,
        usage_kwh REAL,
        sample_count INTEGER,
        updated_at REAL
    )''')
    c_gas.execute('CREATE INDEX IF NOT EXISTS idx_gas_daily_date ON gas_daily_totals(date)')
    c_gas.execute('''CREATE TABLE IF NOT EXISTS gas_monthly_totals (
        month TEXT PRIMARY KEY,
        usage_m3 REAL,
        usage_kwh REAL,
        day_count INTEGER,
        updated_at REAL
    )''')
    c_gas.execute('CREATE INDEX IF NOT EXISTS idx_gas_monthly_month ON gas_monthly_totals(month)')
    conn_gas.commit()
    conn_gas.close()
    
    print(f"✓ Initialized raw data database ({DB_FILE_RAW})")
    print(f"✓ Initialized averages database ({DB_FILE_AVG})")
    print(f"✓ Initialized daily database ({DB_FILE_DAILY})")
    print(f"✓ Initialized gas totals database ({DB_FILE_GAS_TOTALS})")


def format_quarter_hour_interval(hour, minute):
    """Format a fixed quarter-hour window label like 18:15-18:30."""
    end_minutes_total = hour * 60 + minute + 15
    end_hour = end_minutes_total // 60
    end_minute = end_minutes_total % 60
    end_label = '24:00' if end_minutes_total >= 24 * 60 else f'{end_hour:02d}:{end_minute:02d}'
    return f'{hour:02d}:{minute:02d}-{end_label}'


def counter_delta(min_value, max_value):
    """Return the monotonic delta for cumulative counters when both values exist."""
    if min_value is None or max_value is None:
        return None
    return max(0.0, float(max_value) - float(min_value))


def safe_counter_step(previous_value, current_value, max_step_kwh=MAX_COUNTER_STEP_KWH):
    """Return a safe monotonic counter step and filter reset/corrupt jumps."""
    if previous_value is None or current_value is None:
        return 0.0

    step = float(current_value) - float(previous_value)
    if step < 0:
        return 0.0
    if max_step_kwh is not None and step > float(max_step_kwh):
        return 0.0
    return step


def is_peak_tariff_timestamp(timestamp_value):
    """Return True for peak tariff (weekdays 07:00-22:00 local time), else off-peak."""
    dt_local = datetime.fromtimestamp(float(timestamp_value))
    if dt_local.weekday() >= 5:
        return False
    minute_of_day = dt_local.hour * 60 + dt_local.minute
    return 7 * 60 <= minute_of_day < 22 * 60


def get_fixed_quarter_hour_peaks(c_raw, day_start, day_end):
    """Return daily peak import/export values and their fixed local 15-minute windows."""
    # Only finalized quarter-hours are eligible for a definitive daily peak.
    # If the requested day includes "now", exclude the currently running quarter-hour.
    effective_day_end = day_end
    now_ts = time.time()
    if day_start <= now_ts < day_end:
        current_quarter_start = int(now_ts // 900) * 900
        effective_day_end = min(day_end, float(current_quarter_start))

    if effective_day_end <= day_start:
        return 0.0, 0.0, None, None

    c_raw.execute(
        '''SELECT CAST(strftime('%H', datetime(timestamp, 'unixepoch', 'localtime')) AS INTEGER) AS bucket_hour,
                  (CAST(strftime('%M', datetime(timestamp, 'unixepoch', 'localtime')) AS INTEGER) / 15) * 15 AS bucket_minute,
                  AVG(CASE WHEN power > 0 THEN power END) AS avg_consumption_w,
                  AVG(CASE WHEN power < 0 THEN ABS(power) END) AS avg_injection_w
           FROM energy_data
           WHERE timestamp >= ? AND timestamp < ?
           GROUP BY strftime('%Y-%m-%d %H', datetime(timestamp, 'unixepoch', 'localtime')),
                    CAST(strftime('%M', datetime(timestamp, 'unixepoch', 'localtime')) AS INTEGER) / 15
           ORDER BY MIN(timestamp)''',
        (day_start, effective_day_end),
    )
    quarter_hour_rows = c_raw.fetchall()

    peak_consumption = 0.0
    peak_injection = 0.0
    peak_consumption_interval = None
    peak_injection_interval = None
    for bucket_hour, bucket_minute, avg_consumption_w, avg_injection_w in quarter_hour_rows:
        interval_label = format_quarter_hour_interval(int(bucket_hour), int(bucket_minute))

        current_consumption = float(avg_consumption_w or 0.0)
        if current_consumption > peak_consumption:
            peak_consumption = current_consumption
            peak_consumption_interval = interval_label

        current_injection = float(avg_injection_w or 0.0)
        if current_injection > peak_injection:
            peak_injection = current_injection
            peak_injection_interval = interval_label

    return peak_consumption, peak_injection, peak_consumption_interval, peak_injection_interval


def rebuild_daily_consumption_from_raw_range(start_ts=None, end_ts=None):
    """Rebuild daily consumption rows from raw cumulative import/export counters.

    Prefer whichever source (raw or backup DB) contains the fuller day coverage,
    so partial raw gaps do not underreport consumption or injection totals.
    """

    def load_distinct_days(db_path):
        conn_local = None
        try:
            conn_local = get_db_connection(db_path)
            c_local = conn_local.cursor()
            c_local.execute(
                '''SELECT DISTINCT strftime('%Y-%m-%d', datetime(timestamp, 'unixepoch', 'localtime')) AS day
                   FROM energy_data
                   ORDER BY day'''
            )
            return [row[0] for row in c_local.fetchall() if row and row[0]]
        except Exception as e:
            print(f'Daily rebuild day scan failed ({db_path}): {e}')
            return []
        finally:
            if conn_local:
                conn_local.close()

    def load_day_samples(db_path, day_start, day_end):
        conn_local = None
        try:
            conn_local = get_db_connection(db_path)
            c_local = conn_local.cursor()
            c_local.execute(
                '''SELECT import_kwh,
                          export_kwh,
                          import_t1_kwh,
                          import_t2_kwh,
                          export_t1_kwh,
                          export_t2_kwh,
                          timestamp,
                          power
                   FROM energy_data
                   WHERE timestamp >= ? AND timestamp < ?
                   ORDER BY timestamp''',
                (day_start, day_end),
            )
            return c_local.fetchall()
        except Exception as e:
            print(f'Daily rebuild source read failed ({db_path}): {e}')
            return []
        finally:
            if conn_local:
                conn_local.close()

    if start_ts is not None and end_ts is not None:
        start_day = datetime.fromtimestamp(start_ts).replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = datetime.fromtimestamp(end_ts).replace(hour=0, minute=0, second=0, microsecond=0)
        if end_day <= start_day:
            end_day = start_day + timedelta(days=1)
        days = []
        current_day = start_day
        while current_day < end_day:
            days.append(current_day.strftime('%Y-%m-%d'))
            current_day += timedelta(days=1)
    else:
        days = sorted(set(load_distinct_days(DB_FILE_RAW)) | set(load_distinct_days(DB_FILE_BACKUP)))

    conn_daily = get_db_connection(DB_FILE_DAILY, write=True)
    c_daily = conn_daily.cursor()

    rebuilt = 0
    for day in days:
        if not day:
            continue

        day_start_dt = datetime.strptime(day, '%Y-%m-%d')
        day_end_dt = day_start_dt + timedelta(days=1)
        day_start = day_start_dt.timestamp()
        day_end = day_end_dt.timestamp()

        raw_samples = load_day_samples(DB_FILE_RAW, day_start, day_end)
        backup_samples = load_day_samples(DB_FILE_BACKUP, day_start, day_end)
        use_backup = len(backup_samples) > len(raw_samples)
        samples = backup_samples if use_backup else raw_samples
        sample_count = len(samples)

        conn_peaks = None
        try:
            conn_peaks = get_db_connection(DB_FILE_BACKUP if use_backup else DB_FILE_RAW)
            c_peaks = conn_peaks.cursor()
            peak_consumption, peak_injection, peak_consumption_interval, peak_injection_interval = get_fixed_quarter_hour_peaks(c_peaks, day_start, day_end)
        except Exception as e:
            print(f'Daily peak rebuild failed for {day}: {e}')
            peak_consumption, peak_injection, peak_consumption_interval, peak_injection_interval = 0.0, 0.0, None, None
        finally:
            if conn_peaks:
                conn_peaks.close()

        consumption = 0.0
        injection = 0.0
        consumption_offpeak = 0.0
        consumption_peak = 0.0
        injection_offpeak = 0.0
        injection_peak = 0.0
        consumption_offpeak_time = 0.0
        consumption_peak_time = 0.0
        injection_offpeak_time = 0.0
        injection_peak_time = 0.0

        if sample_count >= 2:
            # Accumulate total import/export with a time-aware cap:
            # - 2-second P1 rows: 0.2 kWh cap (blocks counter resets)
            # - 15-min Fluvius rows: 1.5 kWh cap (one quarter-hour ≤ ~1 kWh for residential)
            previous_sample = samples[0]
            for current_sample in samples[1:]:
                dt = float(current_sample[6]) - float(previous_sample[6])  # timestamp delta
                cap = MAX_COUNTER_STEP_KWH if dt < 30 else 1.5
                consumption_step = safe_counter_step(previous_sample[0], current_sample[0], cap)
                injection_step = safe_counter_step(previous_sample[1], current_sample[1], cap)
                current_power = float(current_sample[7]) if current_sample[7] is not None else 0.0
                consumption += consumption_step
                if current_power < 0.0:
                    injection += injection_step
                if is_peak_tariff_timestamp(current_sample[6]):
                    consumption_peak_time += consumption_step
                    if current_power < 0.0:
                        injection_peak_time += injection_step
                else:
                    consumption_offpeak_time += consumption_step
                    if current_power < 0.0:
                        injection_offpeak_time += injection_step
                previous_sample = current_sample

            # Accumulate T1/T2 (off-peak/peak) using split counters when available.
            # Only store split values when coverage is high enough for the day.
            T1T2_MAX_STEP = 5.0
            consumption_split_samples = [s for s in samples if s[2] is not None and s[3] is not None]
            injection_split_samples = [s for s in samples if s[4] is not None and s[5] is not None]

            if len(consumption_split_samples) >= 2:
                prev_t = consumption_split_samples[0]
                for curr_t in consumption_split_samples[1:]:
                    consumption_offpeak += safe_counter_step(prev_t[2], curr_t[2], T1T2_MAX_STEP)
                    consumption_peak += safe_counter_step(prev_t[3], curr_t[3], T1T2_MAX_STEP)
                    prev_t = curr_t

            if len(injection_split_samples) >= 2:
                prev_t = injection_split_samples[0]
                for curr_t in injection_split_samples[1:]:
                    injection_offpeak += safe_counter_step(prev_t[4], curr_t[4], T1T2_MAX_STEP)
                    injection_peak += safe_counter_step(prev_t[5], curr_t[5], T1T2_MAX_STEP)
                    prev_t = curr_t

            split_coverage_consumption = (len(consumption_split_samples) / sample_count) if sample_count else 0.0
            split_coverage_injection = (len(injection_split_samples) / sample_count) if sample_count else 0.0
            MIN_SPLIT_COVERAGE = 0.8

            if split_coverage_consumption < MIN_SPLIT_COVERAGE:
                consumption_offpeak = consumption_offpeak_time
                consumption_peak = consumption_peak_time
            if split_coverage_injection < MIN_SPLIT_COVERAGE:
                injection_offpeak = injection_offpeak_time
                injection_peak = injection_peak_time

        net_consumption = consumption - injection

        c_daily.execute(
            '''INSERT OR REPLACE INTO daily_consumption
               (date, day_start_ts, day_end_ts, consumption_kwh, injection_kwh, net_consumption_kwh,
                consumption_offpeak_kwh, consumption_peak_kwh, injection_offpeak_kwh, injection_peak_kwh,
                peak_consumption_w, peak_consumption_interval, peak_injection_w, peak_injection_interval,
                sample_count, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                day,
                day_start,
                day_end,
                consumption,
                injection,
                net_consumption,
                consumption_offpeak,
                consumption_peak,
                injection_offpeak,
                injection_peak,
                peak_consumption,
                peak_consumption_interval,
                peak_injection,
                peak_injection_interval,
                int(sample_count or 0),
                time.time(),
            ),
        )
        rebuilt += 1

    conn_daily.commit()
    conn_daily.close()
    return rebuilt


def get_live_daily_breakdown(target_date):
    """Compute an up-to-the-second daily breakdown directly from raw samples.

    Used for the current (in-progress) day so the dashboard always reflects the
    most recent P1 reading instead of the last 5-minute snapshot.
    Returns None when insufficient raw data is available.
    """
    day_start_dt = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_dt = day_start_dt + timedelta(days=1)
    day_start = day_start_dt.timestamp()
    day_end = day_end_dt.timestamp()

    def count_day_samples(db_path):
        conn_local = None
        try:
            conn_local = get_db_connection(db_path)
            c_local = conn_local.cursor()
            c_local.execute(
                'SELECT COUNT(*) FROM energy_data WHERE timestamp >= ? AND timestamp < ?',
                (day_start, day_end),
            )
            row = c_local.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        except Exception as e:
            print(f'Live daily breakdown count read failed ({db_path}): {e}')
            return 0
        finally:
            if conn_local:
                conn_local.close()

    def load_day_samples(db_path):
        conn_local = None
        try:
            conn_local = get_db_connection(db_path)
            c_local = conn_local.cursor()
            c_local.execute(
                '''SELECT import_kwh, export_kwh,
                          import_t1_kwh, import_t2_kwh,
                          export_t1_kwh, export_t2_kwh,
                          timestamp,
                          power
                   FROM energy_data
                   WHERE timestamp >= ? AND timestamp < ?
                   ORDER BY timestamp''',
                (day_start, day_end),
            )
            return c_local.fetchall()
        except Exception as e:
            print(f'Live daily breakdown source read failed ({db_path}): {e}')
            return []
        finally:
            if conn_local:
                conn_local.close()

    # Pick whichever source has fuller day coverage using a cheap COUNT(*)
    # first, so we only pay for one full-column, full-day fetch instead of
    # transferring both raw and backup tables over the network share just to
    # compare lengths.
    raw_count = count_day_samples(DB_FILE_RAW)
    backup_count = count_day_samples(DB_FILE_BACKUP)
    samples = load_day_samples(DB_FILE_BACKUP if backup_count > raw_count else DB_FILE_RAW)

    if len(samples) < 2:
        return None

    consumption = 0.0
    injection = 0.0
    consumption_offpeak = 0.0
    consumption_peak = 0.0
    injection_offpeak = 0.0
    injection_peak = 0.0
    consumption_offpeak_time = 0.0
    consumption_peak_time = 0.0
    injection_offpeak_time = 0.0
    injection_peak_time = 0.0

    previous = samples[0]
    for current in samples[1:]:
        dt = float(current[6]) - float(previous[6])
        cap = MAX_COUNTER_STEP_KWH if dt < 30 else 1.5
        consumption_step = safe_counter_step(previous[0], current[0], cap)
        injection_step = safe_counter_step(previous[1], current[1], cap)
        current_power = float(current[7]) if current[7] is not None else 0.0
        consumption += consumption_step
        if current_power < 0.0:
            injection += injection_step
        if is_peak_tariff_timestamp(current[6]):
            consumption_peak_time += consumption_step
            if current_power < 0.0:
                injection_peak_time += injection_step
        else:
            consumption_offpeak_time += consumption_step
            if current_power < 0.0:
                injection_offpeak_time += injection_step
        previous = current

    T1T2_MAX_STEP = 5.0
    consumption_split_samples = [s for s in samples if s[2] is not None and s[3] is not None]
    injection_split_samples = [s for s in samples if s[4] is not None and s[5] is not None]

    if len(consumption_split_samples) >= 2:
        prev_t = consumption_split_samples[0]
        for curr_t in consumption_split_samples[1:]:
            consumption_offpeak += safe_counter_step(prev_t[2], curr_t[2], T1T2_MAX_STEP)
            consumption_peak += safe_counter_step(prev_t[3], curr_t[3], T1T2_MAX_STEP)
            prev_t = curr_t

    if len(injection_split_samples) >= 2:
        prev_t = injection_split_samples[0]
        for curr_t in injection_split_samples[1:]:
            injection_offpeak += safe_counter_step(prev_t[4], curr_t[4], T1T2_MAX_STEP)
            injection_peak += safe_counter_step(prev_t[5], curr_t[5], T1T2_MAX_STEP)
            prev_t = curr_t

    consumption_split_sum = consumption_offpeak + consumption_peak
    injection_split_sum = injection_offpeak + injection_peak
    split_coverage_consumption = (len(consumption_split_samples) / len(samples)) if samples else 0.0
    split_coverage_injection = (len(injection_split_samples) / len(samples)) if samples else 0.0
    # Require broad coverage for live split values; sparse split samples (e.g.
    # a handful of legacy/import rows) should not be shown as full-day T1/T2.
    MIN_SPLIT_COVERAGE = 0.8
    use_consumption_split = (
        len(consumption_split_samples) >= 2
        and split_coverage_consumption >= MIN_SPLIT_COVERAGE
        and (consumption_split_sum > 0.0 or consumption <= 0.0)
    )
    use_injection_split = (
        len(injection_split_samples) >= 2
        and split_coverage_injection >= MIN_SPLIT_COVERAGE
        and (injection_split_sum > 0.0 or injection <= 0.0)
    )

    # T1+T2 is only authoritative when split counters are actually usable.
    if use_consumption_split:
        consumption = consumption_split_sum
    else:
        consumption_offpeak = consumption_offpeak_time
        consumption_peak = consumption_peak_time

    if use_injection_split:
        injection = injection_split_sum
    else:
        injection_offpeak = injection_offpeak_time
        injection_peak = injection_peak_time

    return {
        'consumption_kwh': consumption,
        'injection_kwh': injection,
        'consumption_offpeak_kwh': consumption_offpeak,
        'consumption_peak_kwh': consumption_peak,
        'injection_offpeak_kwh': injection_offpeak,
        'injection_peak_kwh': injection_peak,
    }


def init_backup_db():
    """Initialize backup database schema used for periodic full-copy sync."""
    conn_backup = get_db_connection(DB_FILE_BACKUP, write=True)
    c_backup = conn_backup.cursor()

    c_backup.execute('''CREATE TABLE IF NOT EXISTS energy_data (
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
    c_backup.execute('CREATE INDEX IF NOT EXISTS idx_backup_timestamp ON energy_data(timestamp)')

    c_backup.execute("PRAGMA table_info(energy_data)")
    backup_columns = {row[1] for row in c_backup.fetchall()}
    if 'import_t1_kwh' not in backup_columns:
        c_backup.execute('ALTER TABLE energy_data ADD COLUMN import_t1_kwh REAL')
    if 'import_t2_kwh' not in backup_columns:
        c_backup.execute('ALTER TABLE energy_data ADD COLUMN import_t2_kwh REAL')
    if 'export_t1_kwh' not in backup_columns:
        c_backup.execute('ALTER TABLE energy_data ADD COLUMN export_t1_kwh REAL')
    if 'export_t2_kwh' not in backup_columns:
        c_backup.execute('ALTER TABLE energy_data ADD COLUMN export_t2_kwh REAL')

    c_backup.execute('''CREATE TABLE IF NOT EXISTS energy_data_5min (
        timestamp REAL PRIMARY KEY,
        power_avg REAL,
        power_min REAL,
        power_max REAL
    )''')
    c_backup.execute('CREATE INDEX IF NOT EXISTS idx_backup_timestamp_5min ON energy_data_5min(timestamp)')

    conn_backup.commit()
    conn_backup.close()
    print(f"✓ Initialized backup database ({DB_FILE_BACKUP})")


def sync_backup_db():
    """Synchronize only new raw and 5-minute rows into the backup database."""
    conn_backup = None
    conn_raw = None
    conn_avg = None
    try:
        conn_backup = get_db_connection(DB_FILE_BACKUP, write=True)
        c_backup = conn_backup.cursor()
        try:
            backup_raw_max_ts = get_table_max_timestamp(c_backup, 'energy_data')
            backup_avg_max_ts = get_table_max_timestamp(c_backup, 'energy_data_5min')
        except sqlite3.OperationalError as e:
            if 'no such table' not in str(e).lower():
                raise
            if conn_backup:
                conn_backup.close()
                conn_backup = None
            init_backup_db()
            conn_backup = get_db_connection(DB_FILE_BACKUP, write=True)
            c_backup = conn_backup.cursor()
            backup_raw_max_ts = get_table_max_timestamp(c_backup, 'energy_data')
            backup_avg_max_ts = get_table_max_timestamp(c_backup, 'energy_data_5min')

        conn_raw = get_db_connection(DB_FILE_RAW)
        c_raw = conn_raw.cursor()
        c_raw.execute(
            '''SELECT timestamp, power, import_kwh, export_kwh, gas_m3,
                      import_t1_kwh, import_t2_kwh, export_t1_kwh, export_t2_kwh
               FROM energy_data
               WHERE timestamp > ?
               ORDER BY timestamp''',
            (backup_raw_max_ts,)
        )
        raw_rows = c_raw.fetchall()

        conn_avg = get_db_connection(DB_FILE_AVG)
        c_avg = conn_avg.cursor()
        c_avg.execute(
            '''SELECT timestamp, power_avg, power_min, power_max
               FROM energy_data_5min
               WHERE timestamp > ?
               ORDER BY timestamp''',
            (backup_avg_max_ts,)
        )
        avg_rows = c_avg.fetchall()

        if raw_rows:
            c_backup.executemany(
                '''INSERT OR REPLACE INTO energy_data
                   (timestamp, power, import_kwh, export_kwh, gas_m3,
                    import_t1_kwh, import_t2_kwh, export_t1_kwh, export_t2_kwh)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                raw_rows,
            )
        if avg_rows:
            c_backup.executemany(
                'INSERT OR REPLACE INTO energy_data_5min VALUES (?, ?, ?, ?)',
                avg_rows,
            )

        conn_backup.commit()
        return len(raw_rows), len(avg_rows)
    except Exception as e:
        print(f"Backup sync error: {e}")
        return 0, 0
    finally:
        if conn_raw:
            conn_raw.close()
        if conn_avg:
            conn_avg.close()
        if conn_backup:
            conn_backup.close()


def parse_measurement_timestamp(value):
    """Parse timestamps from epoch seconds/ms or common datetime string formats."""
    if value is None:
        return None

    if isinstance(value, (int, float)):
        ts = float(value)
        return ts / 1000.0 if ts > 1e12 else ts

    text = str(value).strip()
    if not text:
        return None

    # Numeric string epoch.
    try:
        ts = float(text)
        return ts / 1000.0 if ts > 1e12 else ts
    except Exception:
        pass

    # ISO and common datetime formats.
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp()
    except Exception:
        pass

    for fmt in (
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%d-%m-%Y %H:%M:%S',
        '%d-%m-%Y %H:%M',
        '%Y/%m/%d %H:%M:%S',
        '%Y/%m/%d %H:%M',
    ):
        try:
            return datetime.strptime(text, fmt).timestamp()
        except Exception:
            continue

    return None


def _value_from_any(record, keys, default=0.0):
    for key in keys:
        if key in record and record[key] not in (None, ''):
            try:
                return float(record[key])
            except Exception:
                return default
    return default


def parse_decimal_value(value):
    """Parse decimal numbers that may use comma as separator (e.g. 0,123)."""
    if value in (None, ''):
        return None

    text = str(value).strip().replace(' ', '')
    if not text:
        return None

    # Handle locale-style decimals and optional thousands separators.
    text = text.replace('.', '').replace(',', '.') if ',' in text else text

    try:
        return float(text)
    except Exception:
        return None


def parse_fluvius_csv_measurements(file_path):
    """Convert Fluvius interval CSV exports into cumulative raw measurement records."""
    interval_buckets = {}

    with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter=';')

        for row in reader:
            from_date = str(row.get('Van (datum)', '')).strip()
            from_time = str(row.get('Van (tijdstip)', '')).strip()
            to_date = str(row.get('Tot (datum)', '')).strip()
            to_time = str(row.get('Tot (tijdstip)', '')).strip()
            register = str(row.get('Register', '')).strip().lower()
            unit = str(row.get('Eenheid', '')).strip().lower()
            volume = parse_decimal_value(row.get('Volume'))

            if not from_date or not from_time or not to_date or not to_time or volume is None:
                continue

            try:
                start_dt = datetime.strptime(f'{from_date} {from_time}', '%d-%m-%Y %H:%M:%S')
                end_dt = datetime.strptime(f'{to_date} {to_time}', '%d-%m-%Y %H:%M:%S')
            except Exception:
                continue

            interval_seconds = int((end_dt - start_dt).total_seconds())
            if interval_seconds <= 0:
                continue

            ts = start_dt.timestamp()
            bucket = interval_buckets.setdefault(
                ts,
                {
                    'interval_seconds': interval_seconds,
                    'import_kwh': 0.0,
                    'export_kwh': 0.0,
                    'gas_m3': 0.0,
                    'import_t1_kwh': 0.0,
                    'import_t2_kwh': 0.0,
                    'export_t1_kwh': 0.0,
                    'export_t2_kwh': 0.0,
                },
            )
            bucket['interval_seconds'] = max(bucket['interval_seconds'], interval_seconds)

            if unit in ('m3', 'm³'):
                bucket['gas_m3'] += volume
                continue

            if unit != 'kwh':
                continue

            if 'afname' in register:
                bucket['import_kwh'] += volume
                if 'nacht' in register:
                    bucket['import_t1_kwh'] += volume
                elif 'dag' in register:
                    bucket['import_t2_kwh'] += volume
            elif 'injectie' in register:
                bucket['export_kwh'] += volume
                if 'nacht' in register:
                    bucket['export_t1_kwh'] += volume
                elif 'dag' in register:
                    bucket['export_t2_kwh'] += volume

    cumulative_import = 0.0
    cumulative_export = 0.0
    cumulative_gas = 0.0
    cumulative_import_t1 = 0.0
    cumulative_import_t2 = 0.0
    cumulative_export_t1 = 0.0
    cumulative_export_t2 = 0.0
    normalized = []

    for ts in sorted(interval_buckets.keys()):
        bucket = interval_buckets[ts]
        cumulative_import += bucket['import_kwh']
        cumulative_export += bucket['export_kwh']
        cumulative_gas += bucket['gas_m3']
        cumulative_import_t1 += bucket['import_t1_kwh']
        cumulative_import_t2 += bucket['import_t2_kwh']
        cumulative_export_t1 += bucket['export_t1_kwh']
        cumulative_export_t2 += bucket['export_t2_kwh']

        interval_hours = bucket['interval_seconds'] / 3600.0
        power_w = 0.0
        if interval_hours > 0:
            power_w = ((bucket['import_kwh'] - bucket['export_kwh']) / interval_hours) * 1000.0

        normalized.append(
            {
                'timestamp': float(ts),
                'power': power_w,
                'import': cumulative_import,
                'export': cumulative_export,
                'gas': cumulative_gas,
                'import_t1': cumulative_import_t1,
                'import_t2': cumulative_import_t2,
                'export_t1': cumulative_export_t1,
                'export_t2': cumulative_export_t2,
            }
        )

    return normalized


def normalize_measurement_record(record):
    """Normalize old measurement formats to the internal raw schema."""
    if not isinstance(record, dict):
        return None

    timestamp = parse_measurement_timestamp(
        record.get('timestamp', record.get('time', record.get('datetime', record.get('date'))))
    )
    if timestamp is None:
        return None

    return {
        'timestamp': float(timestamp),
        'power': _value_from_any(record, ['power', 'active_power_w', 'vermogen', 'consumption_w'], 0.0),
        'import': _value_from_any(record, ['import_kwh', 'total_power_import_kwh', 'import', 'consumption_kwh'], 0.0),
        'export': _value_from_any(record, ['export_kwh', 'total_power_export_kwh', 'export', 'injection_kwh'], 0.0),
        'gas': _value_from_any(record, ['gas_m3', 'total_gas_m3', 'gas'], 0.0),
        'import_t1': _value_from_any(record, ['import_t1_kwh', 'energy_import_t1'], None),
        'import_t2': _value_from_any(record, ['import_t2_kwh', 'energy_import_t2'], None),
        'export_t1': _value_from_any(record, ['export_t1_kwh', 'energy_export_t1'], None),
        'export_t2': _value_from_any(record, ['export_t2_kwh', 'energy_export_t2'], None),
    }


def load_measurement_records_from_file(file_path):
    """Load old measurement records from JSON/JSONL/CSV files."""
    ext = os.path.splitext(file_path)[1].lower()

    if ext in ('.json', '.jsonl'):
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()

        if not content:
            return []

        if ext == '.jsonl':
            return [json.loads(line) for line in content.splitlines() if line.strip()]

        parsed = json.loads(content)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            for key in ('data', 'history', 'measurements', 'records', 'items'):
                if isinstance(parsed.get(key), list):
                    return parsed[key]
        return []

    if ext == '.csv':
        with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
            first_line = f.readline()

        if 'Van (datum);Van (tijdstip);Tot (datum)' in first_line:
            return parse_fluvius_csv_measurements(file_path)

        with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
            return list(csv.DictReader(f))

    # Fallback: try JSON first, then CSV.
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            parsed = json.load(f)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            for key in ('data', 'history', 'measurements', 'records', 'items'):
                if isinstance(parsed.get(key), list):
                    return parsed[key]
    except Exception:
        pass

    with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
        first_line = f.readline()

    if 'Van (datum);Van (tijdstip);Tot (datum)' in first_line:
        return parse_fluvius_csv_measurements(file_path)

    with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
        return list(csv.DictReader(f))


def rebuild_5min_from_raw_range(start_ts, end_ts):
    """Rebuild 5-minute aggregates from raw data for a specific time range."""
    conn_raw = get_db_connection(DB_FILE_RAW)
    c_raw = conn_raw.cursor()
    c_raw.execute(
        '''SELECT CAST(timestamp / 300 AS INTEGER) * 300 AS interval_start,
                  AVG(power) AS power_avg,
                  MIN(power) AS power_min,
                  MAX(power) AS power_max
           FROM energy_data
           WHERE timestamp >= ? AND timestamp < ?
           GROUP BY interval_start
           ORDER BY interval_start''',
        (start_ts, end_ts),
    )
    rows = c_raw.fetchall()
    conn_raw.close()

    conn_avg = get_db_connection(DB_FILE_AVG, write=True)
    c_avg = conn_avg.cursor()
    c_avg.execute('DELETE FROM energy_data_5min WHERE timestamp >= ? AND timestamp < ?', (start_ts, end_ts))
    if rows:
        c_avg.executemany('INSERT INTO energy_data_5min VALUES (?, ?, ?, ?)', rows)
    conn_avg.commit()
    conn_avg.close()
    return len(rows)


def import_old_measurements(file_path):
    """Import old P1 measurements and reconstruct 5-minute data for the imported range."""
    records = load_measurement_records_from_file(file_path)

    parsed_count = 0
    inserted_count = 0
    queued_count = 0
    min_ts = None
    max_ts = None

    for record in records:
        normalized = normalize_measurement_record(record)
        if not normalized:
            continue

        parsed_count += 1
        ts = normalized['timestamp']
        min_ts = ts if min_ts is None else min(min_ts, ts)
        max_ts = ts if max_ts is None else max(max_ts, ts)

        if insert_raw_entry(normalized):
            inserted_count += 1
        else:
            queue_raw_entry(normalized)
            queued_count += 1

    rebuilt_rows = 0
    rebuilt_daily_rows = 0
    if min_ts is not None and max_ts is not None:
        start_ts = (int(min_ts) // 300) * 300
        end_ts = ((int(max_ts) // 300) + 1) * 300
        rebuilt_rows = rebuild_5min_from_raw_range(start_ts, end_ts)
        rebuilt_daily_rows = rebuild_daily_consumption_from_raw_range(start_ts, end_ts)

    backup_raw, backup_avg = sync_backup_db()

    return {
        'source_records': len(records),
        'parsed_records': parsed_count,
        'inserted_raw_records': inserted_count,
        'queued_raw_records': queued_count,
        'rebuilt_5min_records': rebuilt_rows,
        'rebuilt_daily_records': rebuilt_daily_rows,
        'backup_synced_raw': backup_raw,
        'backup_synced_avg': backup_avg,
        'range_start': datetime.fromtimestamp(min_ts).isoformat() if min_ts else None,
        'range_end': datetime.fromtimestamp(max_ts).isoformat() if max_ts else None,
    }


def import_old_measurements_folder(folder_path):
    """Import all supported historical measurement files in a folder tree."""
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f'Folder not found: {folder_path}')

    supported_ext = ('.csv', '.json', '.jsonl')
    files_to_import = []
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            if file_name.lower().endswith(supported_ext):
                files_to_import.append(os.path.join(root, file_name))

    files_to_import.sort()
    if not files_to_import:
        return {
            'imported_files': 0,
            'skipped_files': 0,
            'errors': [],
            'source_records': 0,
            'parsed_records': 0,
            'inserted_raw_records': 0,
            'queued_raw_records': 0,
            'rebuilt_5min_records': 0,
            'rebuilt_daily_records': 0,
            'backup_synced_raw': 0,
            'backup_synced_avg': 0,
        }

    totals = {
        'imported_files': 0,
        'skipped_files': 0,
        'errors': [],
        'source_records': 0,
        'parsed_records': 0,
        'inserted_raw_records': 0,
        'queued_raw_records': 0,
        'rebuilt_5min_records': 0,
        'rebuilt_daily_records': 0,
        'backup_synced_raw': 0,
        'backup_synced_avg': 0,
    }

    for file_path in files_to_import:
        try:
            result = import_old_measurements(file_path)
            totals['imported_files'] += 1
            totals['source_records'] += int(result.get('source_records', 0) or 0)
            totals['parsed_records'] += int(result.get('parsed_records', 0) or 0)
            totals['inserted_raw_records'] += int(result.get('inserted_raw_records', 0) or 0)
            totals['queued_raw_records'] += int(result.get('queued_raw_records', 0) or 0)
            totals['rebuilt_5min_records'] += int(result.get('rebuilt_5min_records', 0) or 0)
            totals['rebuilt_daily_records'] += int(result.get('rebuilt_daily_records', 0) or 0)
            totals['backup_synced_raw'] += int(result.get('backup_synced_raw', 0) or 0)
            totals['backup_synced_avg'] += int(result.get('backup_synced_avg', 0) or 0)
        except Exception as e:
            totals['skipped_files'] += 1
            totals['errors'].append({'file': file_path, 'error': str(e)})

    return totals


def backup_data_background():
    """Background thread that performs full backup sync every minute."""
    print(f"✓ Starting backup sync thread (interval: {BACKUP_INTERVAL_SECONDS}s)")
    raw_synced, avg_synced = sync_backup_db()
    if raw_synced or avg_synced:
        print(f"✓ Initial backup sync complete: {raw_synced} raw rows, {avg_synced} 5-minute rows")

    while True:
        try:
            raw_synced, avg_synced = sync_backup_db()
            if raw_synced or avg_synced:
                print(f"✓ Backup sync complete: {raw_synced} raw rows, {avg_synced} 5-minute rows")
        except Exception as e:
            print(f"Backup thread error: {e}")
        time.sleep(BACKUP_INTERVAL_SECONDS)


@app.route('/import_old_measurements', methods=['POST'])
def import_old_measurements_endpoint():
    """Import old CSV/JSON P1 measurements and reconstruct derived 5-minute data."""
    auth_error = require_admin_token()
    if auth_error:
        return auth_error

    payload = request.get_json(silent=True) or {}
    file_path = str(payload.get('file_path', '')).strip()
    if not file_path:
        return jsonify({
            'status': 'error',
            'message': 'Missing file_path in request body'
        }), 400

    if not os.path.exists(file_path):
        return jsonify({
            'status': 'error',
            'message': f'File not found: {file_path}'
        }), 404

    try:
        result = import_old_measurements(file_path)
        return jsonify({
            'status': 'success',
            'message': 'Old measurements imported and reconstructed',
            **result,
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/import_fluvius_history', methods=['POST'])
def import_fluvius_history_endpoint():
    """Import all historical Fluvius CSV files from a folder."""
    auth_error = require_admin_token()
    if auth_error:
        return auth_error

    payload = request.get_json(silent=True) or {}
    folder_path = str(payload.get('folder_path', FLUVIUS_HISTORY_DIR)).strip()
    if not folder_path:
        folder_path = FLUVIUS_HISTORY_DIR

    if not os.path.isdir(folder_path):
        return jsonify({
            'status': 'error',
            'message': f'Folder not found: {folder_path}'
        }), 404

    try:
        result = import_old_measurements_folder(folder_path)
        return jsonify({
            'status': 'success',
            'message': 'Fluvius history import completed',
            'folder_path': folder_path,
            **result,
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# Calculate and store 5-minute averages
def store_5min_average(interval_timestamp):
    try:
        interval_end = interval_timestamp + 300  # 5 minute window
        conn_raw = get_db_connection(DB_FILE_RAW)
        c_raw = conn_raw.cursor()
        c_raw.execute(
            '''SELECT AVG(power), MIN(power), MAX(power), COUNT(*)
               FROM energy_data
               WHERE timestamp >= ? AND timestamp < ?''',
            (interval_timestamp, interval_end)
        )
        avg_power, min_power, max_power, sample_count = c_raw.fetchone()
        conn_raw.close()
        
        if sample_count:
            conn_avg = get_db_connection(DB_FILE_AVG, write=True)
            c_avg = conn_avg.cursor()
            c_avg.execute('''INSERT OR IGNORE INTO energy_data_5min VALUES (?, ?, ?, ?)''',
                         (interval_timestamp, avg_power, min_power, max_power))
            inserted = c_avg.rowcount
            conn_avg.commit()
            conn_avg.close()

            # Keep daily derived table fresh at the same cadence as new 5-minute intervals.
            if inserted:
                rebuild_daily_consumption_from_raw_range(interval_timestamp, interval_end)
        return True
    except Exception as e:
        print(f"Error storing 5-min average: {e}")
        return False

def cleanup_old_data():
    """Delete raw samples older than the retention window and reclaim free pages."""
    cutoff_timestamp = time.time() - (DATA_RETENTION_DAYS * 86400)
    deleted_raw = 0
    deleted_backup_raw = 0
    deleted_solar_raw = 0

    for db_file, table_name in ((DB_FILE_RAW, 'energy_data'), (DB_FILE_BACKUP, 'energy_data')):
        conn = None
        try:
            conn = get_db_connection(db_file, write=True)
            c = conn.cursor()
            c.execute(f'DELETE FROM {table_name} WHERE timestamp < ?', (cutoff_timestamp,))
            deleted_count = c.rowcount if c.rowcount != -1 else 0
            conn.commit()

            if db_file == DB_FILE_RAW:
                deleted_raw = deleted_count
            else:
                deleted_backup_raw = deleted_count
        except Exception as e:
            print(f"Cleanup error for {db_file}: {e}")
        finally:
            if conn:
                conn.close()

    solar_conn = None
    try:
        solar_conn = open_solar_history_connection(kind='raw', write=True)
        if solar_conn is not None:
            c_solar = solar_conn.cursor()
            c_solar.execute('DELETE FROM solar_raw_data WHERE timestamp < ?', (cutoff_timestamp,))
            deleted_solar_raw = c_solar.rowcount if c_solar.rowcount != -1 else 0
            solar_conn.commit()
    except Exception as e:
        print(f"Solar cleanup error: {e}")
    finally:
        if solar_conn:
            solar_conn.close()

    print(
        f"Cleanup complete: removed {deleted_raw} raw rows, {deleted_backup_raw} backup raw rows, and {deleted_solar_raw} solar raw rows older than {DATA_RETENTION_DAYS} days."
    )
    return deleted_raw

# Auto-recover missing 5-minute averages on startup
def auto_recover_5min_data():
    try:
        conn_raw = get_db_connection(DB_FILE_RAW)
        c_raw = conn_raw.cursor()
        
        conn_avg = get_db_connection(DB_FILE_AVG, write=True)
        c_avg = conn_avg.cursor()

        c_raw.execute(
            '''SELECT DISTINCT CAST(timestamp / 300 AS INTEGER) * 300 AS interval_start
               FROM energy_data
               ORDER BY interval_start DESC
               LIMIT 100'''
        )
        raw_intervals = [int(row[0]) for row in c_raw.fetchall() if row[0] is not None]

        if raw_intervals:
            placeholders = ','.join('?' for _ in raw_intervals)
            c_avg.execute(
                f'''SELECT timestamp
                    FROM energy_data_5min
                    WHERE timestamp IN ({placeholders})''',
                raw_intervals,
            )
            avg_intervals = {int(row[0]) for row in c_avg.fetchall() if row[0] is not None}
        else:
            avg_intervals = set()

        gaps_to_recover = sorted(interval for interval in raw_intervals if interval not in avg_intervals)
        
        if gaps_to_recover:
            print(f"Auto-recovering {len(gaps_to_recover)} missing 5-minute intervals...")
            for interval_start in gaps_to_recover:
                interval_end = interval_start + 300
                c_raw.execute(
                    '''SELECT AVG(power), MIN(power), MAX(power), COUNT(*)
                       FROM energy_data
                       WHERE timestamp >= ? AND timestamp < ?''',
                    (interval_start, interval_end)
                )
                avg_power, min_power, max_power, sample_count = c_raw.fetchone()
                
                if sample_count:
                    c_avg.execute('INSERT OR IGNORE INTO energy_data_5min VALUES (?, ?, ?, ?)',
                             (interval_start, avg_power, min_power, max_power))
            conn_avg.commit()
            print(f"✓ Recovered {len(gaps_to_recover)} 5-minute intervals")
        
        conn_raw.close()
        conn_avg.close()
    except Exception as e:
        print(f"Auto-recovery error: {e}")

# Migrate data from raw database to averages database
def migrate_5min_data():
    """Calculate 5-minute averages from raw data"""
    try:
        conn_avg = get_db_connection(DB_FILE_AVG, write=True)
        c_avg = conn_avg.cursor()
        
        # Check current state
        c_avg.execute('SELECT COUNT(*) FROM energy_data_5min')
        existing_5min = c_avg.fetchone()[0]
        
        if existing_5min > 0:
            print(f"✓ {existing_5min} 5-minute records already exist, skipping migration")
            conn_avg.close()
            return
        
        # Calculate 5-minute averages from raw data
        conn_raw = get_db_connection(DB_FILE_RAW)
        c_raw = conn_raw.cursor()
        
        c_raw.execute('SELECT COUNT(*) FROM energy_data')
        raw_count = c_raw.fetchone()[0]
        
        if raw_count > 0:
            print(f"Calculating 5-minute averages from {raw_count} raw records...")
            c_raw.execute(
                '''SELECT CAST(timestamp / 300 AS INTEGER) * 300 AS interval_start,
                          AVG(power),
                          MIN(power),
                          MAX(power)
                   FROM energy_data
                   GROUP BY interval_start
                   ORDER BY interval_start'''
            )
            interval_rows = c_raw.fetchall()
            inserted = 0
            if interval_rows:
                c_avg.executemany(
                    'INSERT OR IGNORE INTO energy_data_5min VALUES (?, ?, ?, ?)',
                    interval_rows
                )
                inserted = len(interval_rows)
            
            conn_avg.commit()
            print(f"✓ Calculated and stored {inserted} 5-minute averages")
            
            # Verify
            c_avg.execute('SELECT COUNT(*) FROM energy_data_5min')
            final_count = c_avg.fetchone()[0]
            print(f"✓ Total 5-minute records in averages database: {final_count}")
        
        conn_raw.close()
        conn_avg.close()
        
    except Exception as e:
        print(f"Migration error: {e}")


def migrate_daily_consumption_data():
    """Rebuild daily consumption table from all available raw data."""
    try:
        rebuilt_days = rebuild_daily_consumption_from_raw_range()
        print(f"✓ Rebuilt daily consumption rows: {rebuilt_days}")
    except Exception as e:
        print(f"Daily migration error: {e}")

# Migrate old JSON data to RAW database
def migrate_json_to_db():
    if os.path.exists(LEGACY_JSON_FILE):
        try:
            with open(LEGACY_JSON_FILE, 'r') as f:
                data_history = json.load(f)
            for entry in data_history:
                normalized = normalize_measurement_record(entry)
                if normalized:
                    insert_raw_entry(normalized)
            # Optionally remove old file
            os.rename(LEGACY_JSON_FILE, LEGACY_JSON_FILE + '.backup')
        except:
            pass

def migrate_legacy_db():
    """Import historical data from legacy data.db into split raw/avg databases."""
    if not os.path.exists(LEGACY_DB_FILE):
        return

    try:
        conn_legacy = get_db_connection(LEGACY_DB_FILE)
        c_legacy = conn_legacy.cursor()

        conn_raw = get_db_connection(DB_FILE_RAW, write=True)
        c_raw = conn_raw.cursor()

        conn_avg = get_db_connection(DB_FILE_AVG, write=True)
        c_avg = conn_avg.cursor()

        imported_raw = 0
        imported_avg = 0

        # Import raw samples if present in legacy database.
        c_legacy.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='energy_data'")
        if c_legacy.fetchone():
            c_legacy.execute("PRAGMA table_info(energy_data)")
            legacy_raw_columns = {row[1] for row in c_legacy.fetchall()}

            def legacy_column(name):
                return name if name in legacy_raw_columns else f'NULL AS {name}'

            c_legacy.execute(
                f'''SELECT timestamp, power, import_kwh, export_kwh, gas_m3,
                           {legacy_column('import_t1_kwh')}, {legacy_column('import_t2_kwh')},
                           {legacy_column('export_t1_kwh')}, {legacy_column('export_t2_kwh')}
                    FROM energy_data'''
            )
            rows = c_legacy.fetchall()
            if rows:
                c_raw.executemany(
                    '''INSERT OR IGNORE INTO energy_data
                       (timestamp, power, import_kwh, export_kwh, gas_m3,
                        import_t1_kwh, import_t2_kwh, export_t1_kwh, export_t2_kwh)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    rows
                )
                imported_raw = c_raw.rowcount if c_raw.rowcount != -1 else 0

        # Import 5-minute aggregates if present in legacy database.
        c_legacy.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='energy_data_5min'")
        if c_legacy.fetchone():
            c_legacy.execute('SELECT timestamp, power_avg, power_min, power_max FROM energy_data_5min')
            rows = c_legacy.fetchall()
            if rows:
                c_avg.executemany(
                    'INSERT OR IGNORE INTO energy_data_5min VALUES (?, ?, ?, ?)',
                    rows
                )
                imported_avg = c_avg.rowcount if c_avg.rowcount != -1 else 0

        conn_raw.commit()
        conn_avg.commit()

        conn_legacy.close()
        conn_raw.close()
        conn_avg.close()

        if imported_raw > 0 or imported_avg > 0:
            print(f"✓ Imported legacy data: {imported_raw} raw rows, {imported_avg} 5-minute rows")
    except Exception as e:
        print(f"Legacy migration error: {e}")

def collect_data_background():
    """Background thread that polls P1 frequently for the live tab and persists samples at the configured save interval."""
    print(f"✓ Starting background data collection thread (storage interval: {SAVE_INTERVAL}s, live poll: {P1_LIVE_POLL_INTERVAL_SECONDS}s)")
    startup_replayed = flush_raw_write_queue(max_items=5000)
    if startup_replayed > 0:
        print(f"✓ Replayed {startup_replayed} queued raw samples on startup")

    try:
        last_saved = get_last_timestamp()
    except Exception as e:
        print(f"Startup timestamp lookup failed: {e}")
        # Fall back to current time so the loop can continue and recover later.
        last_saved = time.time()

    last_completed_5min_interval = None
    last_cleanup = time.time()
    last_queue_flush = time.time()
    next_live_poll_at = 0.0
    live_error_count = 0
    latest_entry = None

    while True:
        try:
            current_time = time.time()

            # Periodically replay queued samples from previous transient failures.
            if current_time - last_queue_flush >= 30:
                replayed = flush_raw_write_queue(max_items=500)
                if replayed > 0:
                    print(f"✓ Replayed {replayed} queued raw samples")
                last_queue_flush = current_time

            # Poll the meter at a modest cadence so the live tab stays responsive
            # without hammering the P1 endpoint or the NAS-backed app on errors.
            if latest_entry is None or current_time >= next_live_poll_at:
                try:
                    res = P1_HTTP.get(P1_URL, timeout=P1_HTTP_TIMEOUT)
                    res.raise_for_status()
                    d = res.json()

                    latest_entry = {
                        'timestamp': current_time,
                        'power': d.get("active_power_w", 0),
                        'import': d.get("total_power_import_kwh", 0),
                        'export': d.get("total_power_export_kwh", 0),
                        'gas': d.get("total_gas_m3", 0),
                        'import_t1': d.get("energy_import_t1"),
                        'import_t2': d.get("energy_import_t2"),
                        'export_t1': d.get("energy_export_t1"),
                        'export_t2': d.get("energy_export_t2"),
                    }
                    _cache_p1_runtime_point(
                        latest_entry['timestamp'],
                        latest_entry['power'],
                        {
                            'import_kwh': latest_entry['import'],
                            'export_kwh': latest_entry['export'],
                        },
                    )
                    live_error_count = 0
                    next_live_poll_at = current_time + P1_LIVE_POLL_INTERVAL_SECONDS
                except Exception as e:
                    live_error_count += 1
                    retry_delay = min(30.0, max(P1_LIVE_POLL_INTERVAL_SECONDS, P1_LIVE_POLL_INTERVAL_SECONDS * live_error_count))
                    next_live_poll_at = current_time + retry_delay
                    print(f"Error collecting live sample (retry in {retry_delay:.1f}s): {e}")

            # Persist raw data on the live 2-second cadence so the live tab can immediately load a full recent history.
            if latest_entry is not None and (current_time - last_saved) >= SAVE_INTERVAL:
                if not insert_raw_entry(latest_entry):
                    queue_raw_entry(latest_entry)

                # Finalize only completed 5-minute intervals to avoid recalculating the same block every cycle.
                current_interval = int(current_time // 300) * 300
                if last_completed_5min_interval is None:
                    last_completed_5min_interval = current_interval
                elif current_interval > last_completed_5min_interval:
                    store_5min_average(last_completed_5min_interval)
                    last_completed_5min_interval = current_interval

                last_saved = current_time

            # Cleanup old raw data every 24 hours
            if current_time - last_cleanup >= 86400:
                cleanup_old_data()
                last_cleanup = current_time

        except Exception as e:
            print(f"Background collection error: {e}")

        # Sleep briefly to avoid busy-waiting
        time.sleep(BACKGROUND_LOOP_SLEEP_SECONDS)

def collect_solar_background():
    """Background thread that polls the Sun2000 inverter and stores live solar samples."""
    if not SUN2000_AVAILABLE:
        print('Sun2000 solar collection is disabled: module not available.')
        return

    error_count = 0
    poll_interval_seconds = max(2, int(float(os.getenv('SOLAR_POLL_INTERVAL_SECONDS', '2'))))
    print(f'✓ Starting Sun2000 solar collection thread (interval: {poll_interval_seconds}s)')
    while True:
        try:
            fetch_and_store_current_power(force=True, persist=True)
            error_count = 0
            time.sleep(poll_interval_seconds)
        except Exception as e:
            error_count += 1
            retry_delay = min(30, max(poll_interval_seconds, error_count * 2))
            print(f"Sun2000 collection error #{error_count}: {e}")
            time.sleep(retry_delay)


def collect_marstek_background():
    """Background thread that polls Marstek and persists stable battery samples."""
    global _last_marstek_persist_ts

    if not (MARSTEK_IP or MARSTEK_STATUS_URL):
        print('Marstek collection is disabled: set MARSTEK_IP or MARSTEK_STATUS_URL.')
        return

    error_count = 0
    poll_interval_seconds = max(2, int(float(os.getenv('MARSTEK_POLL_INTERVAL_SECONDS', '2'))))
    print(f'✓ Starting Marstek battery collection thread (interval: {poll_interval_seconds}s)')

    while True:
        try:
            marstek_status_getter = globals().get('_get_marstek_status')
            if not callable(marstek_status_getter):
                time.sleep(1)
                continue

            status = marstek_status_getter(force_refresh=True)
            now_ts = time.time()
            should_persist = (now_ts - _last_marstek_persist_ts) >= BATTERY_PERSIST_INTERVAL_SECONDS
            if should_persist and _persist_marstek_sample_to_history(status):
                _last_marstek_persist_ts = now_ts
            error_count = 0
            time.sleep(poll_interval_seconds)
        except Exception as e:
            error_count += 1
            retry_delay = min(30, max(poll_interval_seconds, error_count * 2))
            print(f"Marstek collection error #{error_count}: {e}")
            time.sleep(retry_delay)


def run_startup_maintenance():
    """Run heavy one-off startup tasks after the app is already serving requests."""
    started_at = time.time()
    print('Starting async startup maintenance...')
    if is_network_sqlite_path(DB_FILE_RAW):
        try:
            ensure_raw_db_health()
        except Exception as e:
            print(f'Async startup: raw DB health check failed: {e}')

    try:
        migrate_json_to_db()
    except Exception as e:
        print(f'Async startup: JSON migration failed: {e}')

    try:
        migrate_legacy_db()
    except Exception as e:
        print(f'Async startup: legacy DB migration failed: {e}')

    try:
        migrate_5min_data()
    except Exception as e:
        print(f'Async startup: 5-minute migration failed: {e}')

    try:
        migrate_daily_consumption_data()
    except Exception as e:
        print(f'Async startup: daily migration failed: {e}')

    try:
        auto_recover_5min_data()
    except Exception as e:
        print(f'Async startup: 5-minute recovery failed: {e}')

    try:
        init_battery_history_dbs()
    except Exception as e:
        print(f'Async startup: battery DB init failed: {e}')

    try:
        init_backup_db()
    except Exception as e:
        print(f'Async startup: backup DB init failed: {e}')

    elapsed = max(0.0, time.time() - started_at)
    print(f'✓ Async startup maintenance complete in {elapsed:.1f}s')

# Get last timestamp from RAW database
def get_last_timestamp():
    conn = None
    try:
        conn = get_db_connection(DB_FILE_RAW)
        c = conn.cursor()
        c.execute('SELECT MAX(timestamp) FROM energy_data')
        result = c.fetchone()
    finally:
        if conn:
            conn.close()
    return result[0] if result and result[0] else 0

bootstrap_data_dir()
migrate_split_storage_layout()
log_storage_paths()
load_settings()
init_db()
# DB integrity checks and data rebuild migrations are intentionally skipped on
# startup for maximum launch speed. All migration functions (migrate_json_to_db,
# migrate_legacy_db, migrate_5min_data, migrate_daily_consumption_data,
# auto_recover_5min_data, ensure_raw_db_health) are no-ops on an established
# install and are available as manual admin actions if ever needed.

if ASYNC_STARTUP_MAINTENANCE:
    startup_maintenance_thread = threading.Thread(target=run_startup_maintenance, daemon=True)
    startup_maintenance_thread.start()

# Start background data collection thread (runs continuously, independent of GUI access)
if ENABLE_BACKGROUND_THREADS:
    data_collection_thread = threading.Thread(target=collect_data_background, daemon=True)
    data_collection_thread.start()

    if SUN2000_AVAILABLE:
        solar_collection_thread = threading.Thread(target=collect_solar_background, daemon=True)
        solar_collection_thread.start()

    if MARSTEK_IP or MARSTEK_STATUS_URL:
        marstek_collection_thread = threading.Thread(target=collect_marstek_background, daemon=True)
        marstek_collection_thread.start()

    # Start excess power monitoring thread
    monitoring_thread = threading.Thread(target=check_excess_power, daemon=True)
    monitoring_thread.start()

    # Start backup sync thread (full backup every minute)
    backup_thread = threading.Thread(target=backup_data_background, daemon=True)
    backup_thread.start()
else:
    print('Background threads are disabled (P1_ENABLE_BACKGROUND_THREADS=false).')

@app.route("/")
def index():
    return render_template("index.html")


@app.route('/health')
def health():
    return jsonify({
        'status': 'ok',
        'p1_url': P1_URL,
        'application_root': APPLICATION_ROOT,
        'data_dir': DATA_DIR,
    })

@app.route("/data")
def get_data():
    """Return the latest data from RAW database (collected by background thread)"""
    try:
        conn = None
        try:
            conn = get_db_connection(DB_FILE_RAW)
            c = conn.cursor()
            # Get the most recent raw data point
            c.execute('SELECT power, import_kwh, export_kwh, gas_m3 FROM energy_data ORDER BY timestamp DESC LIMIT 1')
            result = c.fetchone()
        finally:
            if conn:
                conn.close()
        
        if result:
            power, import_kwh, export_kwh, gas_m3 = result
            return jsonify({
                "power": power,
                "import": import_kwh,
                "export": export_kwh,
                "gas": gas_m3,
            })
        else:
            return jsonify({"error": "no_data"}), 404
            
    except Exception as e:
        print(f"Error getting data: {e}")
        return jsonify({"error": "failed"}), 500

@app.route("/cleanup", methods=['POST'])
def trigger_cleanup():
    """Manual cleanup endpoint that enforces the configured raw-data retention window."""
    auth_error = require_admin_token()
    if auth_error:
        return auth_error

    deleted = cleanup_old_data()
    return jsonify({
        "status": "success",
        "deleted_raw_records": deleted,
        "retention_days": DATA_RETENTION_DAYS,
        "message": f"Removed raw records older than {DATA_RETENTION_DAYS} days."
    })

@app.route("/recover_data", methods=['POST'])
def recover_data():
    """Recover and restore data from database"""
    auth_error = require_admin_token()
    if auth_error:
        return auth_error

    try:
        print("\n=== Starting Data Recovery ===")
        migrate_legacy_db()
        migrate_5min_data()
        migrate_daily_consumption_data()
        auto_recover_5min_data()
        
        # Get stats from both databases
        conn_raw = get_db_connection(DB_FILE_RAW)
        c_raw = conn_raw.cursor()
        c_raw.execute('SELECT COUNT(*) FROM energy_data')
        raw_count = c_raw.fetchone()[0]
        conn_raw.close()
        
        conn_avg = get_db_connection(DB_FILE_AVG)
        c_avg = conn_avg.cursor()
        c_avg.execute('SELECT count(*) FROM energy_data_5min')
        avg_count = c_avg.fetchone()[0]
        conn_avg.close()
        
        return jsonify({
            "status": "success",
            "message": "Data recovery completed",
            "raw_records": raw_count,
            "5min_averages": avg_count,
            "graphs_should_be": "populated" if avg_count > 0 else "empty"
        })
    except Exception as e:
        print(f"Recovery error: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/test_notification", methods=['POST'])
def test_notification():
    """Send a test notification"""
    auth_error = require_admin_token()
    if auth_error:
        return auth_error

    sent, detail = send_notification("Test bericht - P1 Dashboard notificatie systeem werkt!")
    if not sent:
        return jsonify({
            "status": "error",
            "message": f"Test notification failed: {detail}"
        }), 500

    return jsonify({
        "status": "success",
        "message": "Test notification sent to configured email address"
    })

@app.route("/db_stats")
def get_db_stats():
    """Get database statistics from primary and backup databases"""
    try:
        import os
        
        # RAW database stats
        conn_raw = get_db_connection(DB_FILE_RAW)
        c_raw = conn_raw.cursor()
        c_raw.execute('SELECT COUNT(*) FROM energy_data')
        raw_count = c_raw.fetchone()[0]
        c_raw.execute('SELECT MIN(timestamp), MAX(timestamp) FROM energy_data')
        raw_min_ts, raw_max_ts = c_raw.fetchone()
        conn_raw.close()
        raw_size_mb = os.path.getsize(DB_FILE_RAW) / (1024 * 1024) if os.path.exists(DB_FILE_RAW) else 0
        
        # AVERAGES database stats
        conn_avg = get_db_connection(DB_FILE_AVG)
        c_avg = conn_avg.cursor()
        c_avg.execute('SELECT COUNT(*) FROM energy_data_5min')
        avg_count = c_avg.fetchone()[0]
        c_avg.execute('SELECT MIN(timestamp), MAX(timestamp) FROM energy_data_5min')
        avg_min_ts, avg_max_ts = c_avg.fetchone()
        conn_avg.close()
        avg_size_mb = os.path.getsize(DB_FILE_AVG) / (1024 * 1024) if os.path.exists(DB_FILE_AVG) else 0

        # DAILY database stats
        conn_daily = get_db_connection(DB_FILE_DAILY)
        c_daily = conn_daily.cursor()
        c_daily.execute('SELECT COUNT(*) FROM daily_consumption')
        daily_count = c_daily.fetchone()[0]
        c_daily.execute('SELECT MIN(date), MAX(date) FROM daily_consumption')
        daily_min_date, daily_max_date = c_daily.fetchone()
        c_daily.execute('SELECT MAX(peak_consumption_w) FROM daily_consumption')
        daily_peak_max = c_daily.fetchone()[0]
        conn_daily.close()
        daily_size_mb = os.path.getsize(DB_FILE_DAILY) / (1024 * 1024) if os.path.exists(DB_FILE_DAILY) else 0

        # BACKUP database stats
        backup_raw_count = 0
        backup_avg_count = 0
        backup_raw_min_ts = backup_raw_max_ts = None
        backup_avg_min_ts = backup_avg_max_ts = None
        backup_size_mb = os.path.getsize(DB_FILE_BACKUP) / (1024 * 1024) if os.path.exists(DB_FILE_BACKUP) else 0

        if is_postgres_enabled() or os.path.exists(DB_FILE_BACKUP):
            conn_backup = get_db_connection(DB_FILE_BACKUP)
            c_backup = conn_backup.cursor()
            c_backup.execute('SELECT COUNT(*) FROM energy_data')
            backup_raw_count = c_backup.fetchone()[0]
            c_backup.execute('SELECT MIN(timestamp), MAX(timestamp) FROM energy_data')
            backup_raw_min_ts, backup_raw_max_ts = c_backup.fetchone()

            c_backup.execute('SELECT COUNT(*) FROM energy_data_5min')
            backup_avg_count = c_backup.fetchone()[0]
            c_backup.execute('SELECT MIN(timestamp), MAX(timestamp) FROM energy_data_5min')
            backup_avg_min_ts, backup_avg_max_ts = c_backup.fetchone()
            conn_backup.close()
        
        return jsonify({
            "raw_database": {
                "file": DB_FILE_RAW,
                "records": raw_count,
                "oldest": datetime.fromtimestamp(raw_min_ts).isoformat() if raw_min_ts else None,
                "newest": datetime.fromtimestamp(raw_max_ts).isoformat() if raw_max_ts else None,
                "size_mb": round(raw_size_mb, 2),
                "retention_days": DATA_RETENTION_DAYS
            },
            "averages_database": {
                "file": DB_FILE_AVG,
                "records": avg_count,
                "oldest": datetime.fromtimestamp(avg_min_ts).isoformat() if avg_min_ts else None,
                "newest": datetime.fromtimestamp(avg_max_ts).isoformat() if avg_max_ts else None,
                "size_mb": round(avg_size_mb, 2),
                "retention": "Permanent"
            },
            "daily_database": {
                "file": DB_FILE_DAILY,
                "records": daily_count,
                "oldest": daily_min_date,
                "newest": daily_max_date,
                "size_mb": round(daily_size_mb, 2),
                "max_peak_consumption_w": round(float(daily_peak_max or 0.0), 2),
                "source": "Derived from raw import/export cumulative counters"
            },
            "backup_database": {
                "file": DB_FILE_BACKUP,
                "raw_records": backup_raw_count,
                "raw_oldest": datetime.fromtimestamp(backup_raw_min_ts).isoformat() if backup_raw_min_ts else None,
                "raw_newest": datetime.fromtimestamp(backup_raw_max_ts).isoformat() if backup_raw_max_ts else None,
                "avg_records": backup_avg_count,
                "avg_oldest": datetime.fromtimestamp(backup_avg_min_ts).isoformat() if backup_avg_min_ts else None,
                "avg_newest": datetime.fromtimestamp(backup_avg_max_ts).isoformat() if backup_avg_max_ts else None,
                "size_mb": round(backup_size_mb, 2),
                "sync_interval_seconds": BACKUP_INTERVAL_SECONDS
            },
            "total_size_mb": round(raw_size_mb + avg_size_mb + daily_size_mb + backup_size_mb, 2),
            "save_interval_seconds": SAVE_INTERVAL,
            "backup_interval_seconds": BACKUP_INTERVAL_SECONDS
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/daily_peak_data")
def get_daily_peak_data():
    """Get daily fixed 15-minute average consumption peaks (W) for the selected month."""
    try:
        date_str = request.args.get('date')
        if date_str:
            try:
                target_date = datetime.strptime(date_str, '%Y-%m-%d')
            except Exception:
                target_date = datetime.now()
        else:
            target_date = datetime.now()

        start_of_month = target_date.replace(day=1)
        if start_of_month.month == 12:
            next_month = start_of_month.replace(year=start_of_month.year + 1, month=1)
        else:
            next_month = start_of_month.replace(month=start_of_month.month + 1)

        start_timestamp = start_of_month.timestamp()
        end_timestamp = next_month.timestamp()

        conn = get_db_connection(DB_FILE_DAILY)
        c = conn.cursor()
        c.execute(
                '''SELECT date, peak_consumption_w, peak_consumption_interval
               FROM daily_consumption
               WHERE day_start_ts >= ? AND day_start_ts < ?
               ORDER BY date''',
            (start_timestamp, end_timestamp),
        )
        rows = c.fetchall()
        conn.close()

        peak_map = {
            row[0]: {
                'peak_consumption_w': float(row[1] or 0.0),
                'peak_consumption_interval': row[2],
            }
            for row in rows
        }

        payload = []
        current_day = start_of_month
        while current_day < next_month:
            day = current_day.strftime('%Y-%m-%d')
            peak_entry = peak_map.get(day, {})
            peak_value = peak_entry.get('peak_consumption_w')
            payload.append(
                {
                    'date': day,
                    'peak_consumption_w': peak_value,
                    'peak_consumption_interval': peak_entry.get('peak_consumption_interval'),
                    'peak_threshold_w': PEAK_THRESHOLD_W,
                    'peak_deviation_pct': round(
                        max(0.0, ((peak_value - PEAK_THRESHOLD_W) / PEAK_THRESHOLD_W) * 100.0),
                        2,
                    ) if isinstance(peak_value, (int, float)) else None,
                }
            )
            current_day += timedelta(days=1)

        return jsonify(payload)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route("/weekly_data")
def get_weekly_data():
    """Get monthly data aggregated per day, returning all calendar days in the month."""
    # Get date parameter, default to current month
    date_str = request.args.get('date')
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except:
            target_date = datetime.now()
    else:
        target_date = datetime.now()

    start_of_month = target_date.replace(day=1)
    if start_of_month.month == 12:
        next_month = start_of_month.replace(year=start_of_month.year + 1, month=1)
    else:
        next_month = start_of_month.replace(month=start_of_month.month + 1)

    now_dt = datetime.now()
    now_ts = time.time()
    month_cache_key = start_of_month.strftime('%Y-%m')
    is_current_month = (start_of_month.year == now_dt.year and start_of_month.month == now_dt.month)
    force_refresh = str(request.args.get('force', '0')).lower() in ('1', 'true', 'yes', 'on')
    allow_repair = str(request.args.get('repair', '0')).lower() in ('1', 'true', 'yes', 'on')

    # Fast path: serve cached month payload to avoid repeated DB scans and joins.
    if not force_refresh:
        with _weekly_data_cache_lock:
            cached_month = _weekly_data_cache.get(month_cache_key)
            if cached_month and now_ts < float(cached_month.get('expires', 0.0) or 0.0):
                return _json_nocache(cached_month.get('payload', {'days': [], 'summary': {}}))

    # Convert to timestamps. Use an exclusive upper bound and rely on the
    # derived daily table built from raw cumulative import/export counters.
    start_timestamp = start_of_month.timestamp()
    end_timestamp = next_month.timestamp()

    # Expensive repair/rebuild checks are only useful for the in-progress month.
    # Keep the request path fast by running them only when explicitly requested.
    if is_current_month and allow_repair:
        cache_entry = _monthly_recover_check_cache.get(month_cache_key)

        # Rebuild solar rollups for the requested month at most once per 5 minutes
        # so missing daily solar totals are backfilled without rebuilding on every hit.
        solar_rebuild_entry = _solar_monthly_rebuild_cache.get(month_cache_key)
        if not solar_rebuild_entry or now_ts >= solar_rebuild_entry.get('expires', 0.0):
            try:
                rebuilt_rows = rebuild_solar_rollups_from_history(start_timestamp, end_timestamp)
                _solar_monthly_rebuild_cache[month_cache_key] = {
                    'expires': now_ts + 300.0,
                    'rebuilt_rows': int(rebuilt_rows or 0),
                }
            except Exception as e:
                print(f"Monthly solar rollup auto-rebuild failed: {e}")

        if not cache_entry or now_ts >= cache_entry.get('expires', 0.0):
            try:
                conn_raw = get_db_connection(DB_FILE_RAW)
                c_raw = conn_raw.cursor()
                c_raw.execute(
                    '''SELECT COUNT(DISTINCT strftime('%Y-%m-%d', datetime(timestamp, 'unixepoch', 'localtime')))
                       FROM energy_data
                       WHERE timestamp >= ? AND timestamp < ?''',
                    (start_timestamp, end_timestamp),
                )
                raw_days_in_month = int(c_raw.fetchone()[0] or 0)
                conn_raw.close()

                conn_daily_check = get_db_connection(DB_FILE_DAILY)
                c_daily_check = conn_daily_check.cursor()
                c_daily_check.execute(
                    '''SELECT COUNT(*)
                       FROM daily_consumption
                       WHERE day_start_ts >= ? AND day_start_ts < ?''',
                    (start_timestamp, end_timestamp),
                )
                daily_days_in_month = int(c_daily_check.fetchone()[0] or 0)
                conn_daily_check.close()

                if raw_days_in_month > 0 and daily_days_in_month < raw_days_in_month:
                    rebuild_daily_consumption_from_raw_range(start_timestamp, end_timestamp)

                _monthly_recover_check_cache[month_cache_key] = {
                    'expires': now_ts + 60.0,
                    'raw_days': raw_days_in_month,
                    'daily_days': daily_days_in_month,
                }
            except Exception as e:
                print(f"Monthly daily-data auto-recover check failed: {e}")

    conn_daily = get_db_connection(DB_FILE_DAILY)
    c_daily = conn_daily.cursor()
    prepare_manual_tables_best_effort(c_daily, seed_monthly=True)
    month_key = start_of_month.strftime('%Y-%m')
    monthly_manual_override = None
    c_daily.execute(
        '''SELECT consumption_kwh,
                  injection_kwh,
                  consumption_offpeak_kwh,
                  consumption_peak_kwh,
                  injection_offpeak_kwh,
                  injection_peak_kwh,
                  solar_yield_kwh
           FROM monthly_manual_totals
           WHERE month = ?''',
        (month_key,),
    )
    monthly_row = c_daily.fetchone()
    if monthly_row:
        monthly_manual_override = {
            'consumption_kwh': float(monthly_row[0]) if monthly_row[0] is not None else None,
            'injection_kwh': float(monthly_row[1]) if monthly_row[1] is not None else None,
            'consumption_offpeak_kwh': float(monthly_row[2]) if monthly_row[2] is not None else None,
            'consumption_peak_kwh': float(monthly_row[3]) if monthly_row[3] is not None else None,
            'injection_offpeak_kwh': float(monthly_row[4]) if monthly_row[4] is not None else None,
            'injection_peak_kwh': float(monthly_row[5]) if monthly_row[5] is not None else None,
            'solar_yield_kwh': float(monthly_row[6]) if monthly_row[6] is not None else None,
        }

    c_daily.execute(
        '''SELECT dc.date,
                  dc.consumption_kwh,
                  dc.injection_kwh,
                  dc.peak_consumption_w,
                  dc.peak_consumption_interval,
                  smd.solar_yield_kwh,
                  dc.consumption_offpeak_kwh,
                  dc.consumption_peak_kwh,
                  dc.injection_offpeak_kwh,
                  dc.injection_peak_kwh
           FROM daily_consumption dc
           LEFT JOIN solar_manual_data smd ON smd.date = dc.date
           WHERE dc.day_start_ts >= ? AND dc.day_start_ts < ?''',
        (start_timestamp, end_timestamp),
    )
    daily_map = {
        row[0]: {
            'consumption': float(row[1]) if row[1] is not None else None,
            'injection': float(row[2]) if row[2] is not None else None,
            'peak_consumption_w': float(row[3]) if row[3] is not None else None,
            'peak_consumption_interval': row[4],
            'solar_yield_kwh': float(row[5]) if row[5] is not None else None,
            'consumption_offpeak_kwh': float(row[6]) if row[6] is not None else None,
            'consumption_peak_kwh': float(row[7]) if row[7] is not None else None,
            'injection_offpeak_kwh': float(row[8]) if row[8] is not None else None,
            'injection_peak_kwh': float(row[9]) if row[9] is not None else None,
        }
        for row in c_daily.fetchall()
    }

    auto_solar_daily_map = get_solar_daily_totals(
        start_of_month.strftime('%Y-%m-%d'),
        next_month.strftime('%Y-%m-%d'),
    )
    for date_value, solar_yield_kwh in auto_solar_daily_map.items():
        day_values = daily_map.setdefault(date_value, {})
        day_values['solar_yield_kwh'] = float(solar_yield_kwh) if solar_yield_kwh is not None else None

    battery_daily_charge_map = get_battery_daily_charge_totals(
        start_of_month.strftime('%Y-%m-%d'),
        next_month.strftime('%Y-%m-%d'),
    )
    for date_value, battery_charge_kwh in battery_daily_charge_map.items():
        day_values = daily_map.setdefault(date_value, {})
        day_values['battery_charge_kwh'] = float(battery_charge_kwh) if battery_charge_kwh is not None else 0.0

    battery_daily_discharge_map = get_battery_daily_discharge_totals(
        start_of_month.strftime('%Y-%m-%d'),
        next_month.strftime('%Y-%m-%d'),
    )
    for date_value, battery_discharge_kwh in battery_daily_discharge_map.items():
        day_values = daily_map.setdefault(date_value, {})
        day_values['battery_discharge_kwh'] = float(battery_discharge_kwh) if battery_discharge_kwh is not None else 0.0

    battery_daily_net_map = get_battery_daily_net_totals(
        start_of_month.strftime('%Y-%m-%d'),
        next_month.strftime('%Y-%m-%d'),
    )
    for date_value, battery_net_kwh in battery_daily_net_map.items():
        day_values = daily_map.setdefault(date_value, {})
        day_values['battery_net_kwh'] = float(battery_net_kwh) if battery_net_kwh is not None else 0.0

    # Override today's row with a live reading from raw samples so the monthly
    # table always shows the current state rather than the last 5-min snapshot.
    today_str = datetime.now().strftime('%Y-%m-%d')
    if today_str in daily_map or (start_of_month <= datetime.now() < next_month):
        breakdown_key = (today_str, int(now_ts // 15))
        if (
            _live_daily_breakdown_cache.get('key') == breakdown_key
            and now_ts < _live_daily_breakdown_cache.get('expires', 0.0)
        ):
            live = _live_daily_breakdown_cache.get('data')
        else:
            live = get_live_daily_breakdown(datetime.now())
            _live_daily_breakdown_cache['key'] = breakdown_key
            _live_daily_breakdown_cache['expires'] = now_ts + 15.0
            _live_daily_breakdown_cache['data'] = live
        if live is not None:
            existing = daily_map.get(today_str, {})
            daily_map[today_str] = {
                **existing,
                'consumption': live['consumption_kwh'],
                'injection': live['injection_kwh'],
                'consumption_offpeak_kwh': live['consumption_offpeak_kwh'],
                'consumption_peak_kwh': live['consumption_peak_kwh'],
                'injection_offpeak_kwh': live['injection_offpeak_kwh'],
                'injection_peak_kwh': live['injection_peak_kwh'],
            }

    # Keep monthly table totals internally consistent when an exact split exists.
    for day_values in daily_map.values():
        cons_offpeak = day_values.get('consumption_offpeak_kwh')
        cons_peak = day_values.get('consumption_peak_kwh')
        if isinstance(cons_offpeak, (int, float)) and isinstance(cons_peak, (int, float)):
            cons_split_sum = float(cons_offpeak) + float(cons_peak)
            if cons_split_sum > 0.0 or float(day_values.get('consumption') or 0.0) <= 0.0:
                day_values['consumption'] = cons_split_sum

        inj_offpeak = day_values.get('injection_offpeak_kwh')
        inj_peak = day_values.get('injection_peak_kwh')
        if isinstance(inj_offpeak, (int, float)) and isinstance(inj_peak, (int, float)):
            inj_split_sum = float(inj_offpeak) + float(inj_peak)
            if inj_split_sum > 0.0 or float(day_values.get('injection') or 0.0) <= 0.0:
                day_values['injection'] = inj_split_sum

    c_daily.execute(
        '''SELECT date, solar_yield_kwh
           FROM solar_manual_data
           WHERE date >= ? AND date < ?''',
        (start_of_month.strftime('%Y-%m-%d'), next_month.strftime('%Y-%m-%d')),
    )
    for date_value, solar_yield_kwh in c_daily.fetchall():
        day_values = daily_map.setdefault(date_value, {})
        # Only override auto solar data with a manual value when the manual value
        # is explicitly set. A null in solar_manual_data means "not entered", so
        # the auto inverter data (applied above) should not be cleared.
        if solar_yield_kwh is not None:
            day_values['solar_yield_kwh'] = float(solar_yield_kwh)

    conn_daily.close()

    result = []
    current_day = start_of_month
    while current_day < next_month:
        day = current_day.strftime('%Y-%m-%d')
        day_values = daily_map.get(day, {})
        solar_yield_value = day_values.get('solar_yield_kwh')
        if not isinstance(solar_yield_value, (int, float)):
            solar_yield_value = 0.0
        battery_charge_value = day_values.get('battery_charge_kwh')
        if not isinstance(battery_charge_value, (int, float)):
            battery_charge_value = 0.0
        battery_discharge_value = day_values.get('battery_discharge_kwh')
        if not isinstance(battery_discharge_value, (int, float)):
            battery_discharge_value = 0.0
        battery_net_value = day_values.get('battery_net_kwh')
        if not isinstance(battery_net_value, (int, float)):
            battery_net_value = float(battery_discharge_value) - float(battery_charge_value)
        direct_consumption_kwh = (
            max(0.0, solar_yield_value - day_values.get('injection'))
            if isinstance(day_values.get('injection'), (int, float))
            else None
        )
        direct_consumption_pct = (
            (direct_consumption_kwh / solar_yield_value) * 100.0
            if isinstance(direct_consumption_kwh, (int, float)) and solar_yield_value > 0
            else None
        )
        result.append({
            'date': day,
            'consumption': day_values.get('consumption'),
            'injection': day_values.get('injection'),
            'peak_consumption_w': day_values.get('peak_consumption_w'),
            'peak_consumption_interval': day_values.get('peak_consumption_interval'),
            'solar_yield_kwh': solar_yield_value,
            'battery_charge_kwh': battery_charge_value,
            'battery_discharge_kwh': battery_discharge_value,
            'battery_net_kwh': float(battery_net_value),
            'direct_consumption_kwh': direct_consumption_kwh,
            'direct_consumption_pct': direct_consumption_pct,
            'consumption_offpeak_kwh': day_values.get('consumption_offpeak_kwh'),
            'consumption_peak_kwh': day_values.get('consumption_peak_kwh'),
            'injection_offpeak_kwh': day_values.get('injection_offpeak_kwh'),
            'injection_peak_kwh': day_values.get('injection_peak_kwh'),
        })
        current_day += timedelta(days=1)

    summary = {
        'consumption_kwh': sum(
            float(item.get('consumption'))
            for item in result
            if isinstance(item.get('consumption'), (int, float))
        ),
        'injection_kwh': sum(
            float(item.get('injection'))
            for item in result
            if isinstance(item.get('injection'), (int, float))
        ),
        'consumption_offpeak_kwh': sum(
            float(item.get('consumption_offpeak_kwh'))
            for item in result
            if isinstance(item.get('consumption_offpeak_kwh'), (int, float))
        ),
        'consumption_peak_kwh': sum(
            float(item.get('consumption_peak_kwh'))
            for item in result
            if isinstance(item.get('consumption_peak_kwh'), (int, float))
        ),
        'injection_offpeak_kwh': sum(
            float(item.get('injection_offpeak_kwh'))
            for item in result
            if isinstance(item.get('injection_offpeak_kwh'), (int, float))
        ),
        'injection_peak_kwh': sum(
            float(item.get('injection_peak_kwh'))
            for item in result
            if isinstance(item.get('injection_peak_kwh'), (int, float))
        ),
        'solar_yield_kwh': sum(
            float(item.get('solar_yield_kwh'))
            for item in result
            if isinstance(item.get('solar_yield_kwh'), (int, float))
        ),
        'battery_charge_kwh': sum(
            float(item.get('battery_charge_kwh'))
            for item in result
            if isinstance(item.get('battery_charge_kwh'), (int, float))
        ),
        'battery_discharge_kwh': sum(
            float(item.get('battery_discharge_kwh'))
            for item in result
            if isinstance(item.get('battery_discharge_kwh'), (int, float))
        ),
        'battery_net_kwh': sum(
            float(item.get('battery_net_kwh'))
            for item in result
            if isinstance(item.get('battery_net_kwh'), (int, float))
        ),
        'source': 'derived-daily',
    }

    if monthly_manual_override and any(
        monthly_manual_override.get(field) is not None
        for field in ('consumption_kwh', 'injection_kwh', 'solar_yield_kwh')
    ):
        for field, value in monthly_manual_override.items():
            if value is not None:
                summary[field] = value
        summary['source'] = 'manual-monthly'

    solar_total = summary.get('solar_yield_kwh')
    injection_total = summary.get('injection_kwh')
    battery_charge_total_daily = float(summary.get('battery_charge_kwh') or 0.0)
    battery_charge_total_monthly = get_battery_monthly_charge_totals(
        month_key,
        next_month.strftime('%Y-%m'),
    ).get(month_key, None)
    if isinstance(battery_charge_total_monthly, (int, float)) and float(battery_charge_total_monthly) > 0.0:
        summary['battery_charge_kwh'] = float(battery_charge_total_monthly)
    else:
        summary['battery_charge_kwh'] = battery_charge_total_daily

    battery_net_total_monthly = get_battery_monthly_net_totals(
        month_key,
        next_month.strftime('%Y-%m'),
    ).get(month_key, None)
    if isinstance(battery_net_total_monthly, (int, float)):
        summary['battery_net_kwh'] = float(battery_net_total_monthly)
    else:
        summary['battery_net_kwh'] = float(summary.get('battery_net_kwh') or 0.0)

    # Keep battery discharge totals consistent with the per-day rows shown in
    # the monthly tab table (and daily tab validation): always use the day-sum.
    summary['battery_discharge_kwh'] = float(summary.get('battery_discharge_kwh') or 0.0)
    if isinstance(solar_total, (int, float)) and isinstance(injection_total, (int, float)):
        direct_kwh = max(0.0, float(solar_total) - float(injection_total))
        summary['direct_consumption_kwh'] = direct_kwh
        summary['direct_consumption_pct'] = (direct_kwh / float(solar_total) * 100.0) if float(solar_total) > 0 else None
    else:
        summary['direct_consumption_kwh'] = None
        summary['direct_consumption_pct'] = None

    cons_total = summary.get('consumption_kwh')
    direct_total = summary.get('direct_consumption_kwh')
    discharge_total = summary.get('battery_discharge_kwh')
    summary['total_energy_need_kwh'] = (
        float(cons_total) + float(direct_total) + float(discharge_total if discharge_total is not None else 0.0)
        if isinstance(cons_total, (int, float)) and isinstance(direct_total, (int, float))
        else None
    )

    payload = {'days': result, 'summary': summary}

    # Keep current month reasonably fresh while avoiding repeated heavy recomputes.
    cache_ttl_seconds = WEEKLY_CURRENT_MONTH_CACHE_TTL_SECONDS if is_current_month else 1800.0
    with _weekly_data_cache_lock:
        _weekly_data_cache[month_cache_key] = {
            'expires': now_ts + cache_ttl_seconds,
            'payload': payload,
        }
        if len(_weekly_data_cache) > 24:
            stale_keys = [
                key for key, entry in _weekly_data_cache.items()
                if now_ts >= float(entry.get('expires', 0.0) or 0.0)
            ]
            for key in stale_keys:
                _weekly_data_cache.pop(key, None)

    return _json_nocache(payload)


@app.route('/solar_daily_data', methods=['POST'])
def save_solar_daily_data():
    """Store or clear manual daily solar yield values."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'message': 'Invalid JSON payload'}), 400

    date_str = str(data.get('date', '')).strip()
    solar_yield_raw = data.get('solar_yield_kwh')

    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return jsonify({'message': 'Date must be in YYYY-MM-DD format'}), 400

    if solar_yield_raw in (None, ''):
        conn_daily = get_db_connection(DB_FILE_DAILY, write=True)
        c_daily = conn_daily.cursor()
        ensure_solar_manual_table(c_daily)
        c_daily.execute('DELETE FROM solar_manual_data WHERE date = ?', (date_str,))
        conn_daily.commit()
        conn_daily.close()
        month_key = date_str[:7]
        with _weekly_data_cache_lock:
            _weekly_data_cache.pop(month_key, None)
        return jsonify({'message': 'Solar yield cleared', 'date': date_str, 'solar_yield_kwh': None})

    try:
        solar_yield_kwh = float(solar_yield_raw)
    except (TypeError, ValueError):
        return jsonify({'message': 'solar_yield_kwh must be a valid number'}), 400

    if solar_yield_kwh < 0:
        return jsonify({'message': 'solar_yield_kwh must be zero or greater'}), 400

    conn_daily = get_db_connection(DB_FILE_DAILY, write=True)
    c_daily = conn_daily.cursor()
    prepare_manual_tables_best_effort(c_daily)
    c_daily.execute(
        '''INSERT OR REPLACE INTO solar_manual_data (date, solar_yield_kwh, updated_at)
           VALUES (?, ?, ?)''',
        (date_str, solar_yield_kwh, time.time()),
    )
    conn_daily.commit()
    conn_daily.close()

    month_key = date_str[:7]
    with _weekly_data_cache_lock:
        _weekly_data_cache.pop(month_key, None)

    return jsonify({
        'message': 'Solar yield saved',
        'date': date_str,
        'solar_yield_kwh': solar_yield_kwh,
    })

@app.route("/yearly_data")
def get_yearly_data():
    """Get full calendar year overview with 12 months, zero-filling missing months."""
    year_param = request.args.get('year')
    try:
        target_year = int(year_param) if year_param else datetime.now().year
    except Exception:
        target_year = datetime.now().year

    now_ts = time.time()
    current_year = datetime.now().year
    yearly_cache_ttl_seconds = 60.0 if target_year == current_year else 6 * 3600.0
    with _yearly_data_cache_lock:
        cache_entry = _yearly_data_cache.get(target_year)
        if cache_entry and now_ts < cache_entry.get('expires', 0.0):
            return _json_nocache(cache_entry['payload'])

    start_dt = datetime(target_year, 1, 1)
    end_dt = datetime(target_year + 1, 1, 1)
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()

    if target_year == current_year:
        projection_end_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        projection_end_ts = min(projection_end_dt.timestamp(), end_ts)
    else:
        projection_end_ts = end_ts

    conn_daily = get_db_connection(DB_FILE_DAILY)
    c_daily = conn_daily.cursor()
    prepare_manual_tables_best_effort(c_daily)
    c_daily.execute(
        '''SELECT strftime('%Y-%m', datetime(day_start_ts, 'unixepoch', 'localtime')) AS month_key,
                  SUM(consumption_kwh),
                  SUM(injection_kwh),
                  SUM(consumption_offpeak_kwh),
                  SUM(consumption_peak_kwh),
                  SUM(injection_offpeak_kwh),
                  SUM(injection_peak_kwh),
                  MAX(peak_consumption_w)
           FROM daily_consumption
           WHERE day_start_ts >= ? AND day_start_ts < ?
           GROUP BY month_key''',
        (start_ts, end_ts),
    )
    monthly_summary_map = {
        row[0]: {
            'consumption': float(row[1]) if row[1] is not None else 0.0,
            'injection': float(row[2]) if row[2] is not None else 0.0,
            'consumption_offpeak': float(row[3]) if row[3] is not None else None,
            'consumption_peak': float(row[4]) if row[4] is not None else None,
            'injection_offpeak': float(row[5]) if row[5] is not None else None,
            'injection_peak': float(row[6]) if row[6] is not None else None,
            'highest_peak_w': float(row[7]) if row[7] is not None else None,
        }
        for row in c_daily.fetchall()
    }
    daily_monthly_summary_map = {month_key: values.copy() for month_key, values in monthly_summary_map.items()}

    monthly_solar_map = get_solar_monthly_totals(start_dt.strftime('%Y-%m'), end_dt.strftime('%Y-%m'))
    battery_monthly_charge_map = get_battery_monthly_charge_totals(start_dt.strftime('%Y-%m'), end_dt.strftime('%Y-%m'))

    c_daily.execute(
        '''SELECT substr(date, 1, 7) AS month_key,
                  SUM(solar_yield_kwh)
           FROM solar_manual_data
           WHERE date >= ? AND date < ?
           GROUP BY month_key''',
        (start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')),
    )
    for row in c_daily.fetchall():
        if row[0] is None:
            continue
        month_key = row[0]
        if month_key not in monthly_solar_map or monthly_solar_map[month_key] is None:
            monthly_solar_map[month_key] = float(row[1]) if row[1] is not None else None

    csv_monthly_overview = load_csv_monthly_overview(start_dt.strftime('%Y-%m'), end_dt.strftime('%Y-%m'))
    for month_key, overview_values in csv_monthly_overview.items():
        monthly_summary_map[month_key] = {
            'consumption': overview_values.get('consumption', 0.0),
            'injection': overview_values.get('injection', 0.0),
            'consumption_offpeak': overview_values.get('consumption_offpeak'),
            'consumption_peak': overview_values.get('consumption_peak'),
            'injection_offpeak': overview_values.get('injection_offpeak'),
            'injection_peak': overview_values.get('injection_peak'),
            'highest_peak_w': monthly_summary_map.get(month_key, {}).get('highest_peak_w'),
            'source': 'csv-overview',
            'covered_days': overview_values.get('covered_days', 0),
        }

    c_daily.execute(
        '''SELECT month,
                  consumption_kwh,
                  injection_kwh,
                  consumption_offpeak_kwh,
                  consumption_peak_kwh,
                  injection_offpeak_kwh,
                  injection_peak_kwh,
                  solar_yield_kwh
           FROM monthly_manual_totals
           WHERE month >= ? AND month < ?''',
        (start_dt.strftime('%Y-%m'), end_dt.strftime('%Y-%m')),
    )
    manual_monthly_totals = {
        row[0]: {
            'consumption': float(row[1]) if row[1] is not None else None,
            'injection': float(row[2]) if row[2] is not None else None,
            'consumption_offpeak': float(row[3]) if row[3] is not None else None,
            'consumption_peak': float(row[4]) if row[4] is not None else None,
            'injection_offpeak': float(row[5]) if row[5] is not None else None,
            'injection_peak': float(row[6]) if row[6] is not None else None,
            'solar_yield_kwh': float(row[7]) if row[7] is not None else None,
        }
        for row in c_daily.fetchall()
    }

    c_daily.execute(
        '''SELECT COUNT(*),
                  SUM(consumption_kwh),
                  SUM(injection_kwh),
                  SUM(consumption_offpeak_kwh),
                  SUM(consumption_peak_kwh),
                  SUM(injection_offpeak_kwh),
                  SUM(injection_peak_kwh)
           FROM daily_consumption
           WHERE day_start_ts >= ? AND day_start_ts < ?''',
        (start_ts, projection_end_ts),
    )
    (
        measured_days,
        measured_consumption,
        measured_injection,
        measured_consumption_offpeak,
        measured_consumption_peak,
        measured_injection_offpeak,
        measured_injection_peak,
    ) = c_daily.fetchone()

    measured_solar_yield = 0.0

    c_daily.execute(
        '''SELECT strftime('%Y-%m', datetime(day_start_ts, 'unixepoch', 'localtime')) AS month_key,
                  COUNT(*)
           FROM daily_consumption
           WHERE day_start_ts >= ? AND day_start_ts < ?
           GROUP BY month_key''',
        (start_ts, projection_end_ts),
    )
    measured_days_by_month = {
        row[0]: int(row[1] or 0)
        for row in c_daily.fetchall()
        if row[0] is not None
    }

    today_str = datetime.now().strftime('%Y-%m-%d')
    today_db_values = None
    if target_year == current_year:
        c_daily.execute(
            '''SELECT consumption_kwh,
                      injection_kwh,
                      consumption_offpeak_kwh,
                      consumption_peak_kwh,
                      injection_offpeak_kwh,
                      injection_peak_kwh
               FROM daily_consumption
               WHERE date = ?''',
            (today_str,),
        )
        today_db_values = c_daily.fetchone()

    conn_daily.close()

    total_days_in_year = (end_dt - start_dt).days
    measured_days = int(measured_days or 0)
    measured_consumption = float(measured_consumption or 0.0)
    measured_injection = float(measured_injection or 0.0)
    measured_consumption_offpeak = float(measured_consumption_offpeak) if measured_consumption_offpeak is not None else None
    measured_consumption_peak = float(measured_consumption_peak) if measured_consumption_peak is not None else None
    measured_injection_offpeak = float(measured_injection_offpeak) if measured_injection_offpeak is not None else None
    measured_injection_peak = float(measured_injection_peak) if measured_injection_peak is not None else None
    measured_solar_yield = float(measured_solar_yield or 0.0)

    if target_year == current_year:
        live_today = get_live_daily_breakdown(datetime.now())
        if live_today:
            current_month_key = datetime.now().strftime('%Y-%m')

            db_consumption = float(today_db_values[0] or 0.0) if today_db_values else 0.0
            db_injection = float(today_db_values[1] or 0.0) if today_db_values else 0.0
            db_consumption_offpeak = float(today_db_values[2] or 0.0) if today_db_values else 0.0
            db_consumption_peak = float(today_db_values[3] or 0.0) if today_db_values else 0.0
            db_injection_offpeak = float(today_db_values[4] or 0.0) if today_db_values else 0.0
            db_injection_peak = float(today_db_values[5] or 0.0) if today_db_values else 0.0

            live_consumption = float(live_today.get('consumption_kwh') or 0.0)
            live_injection = float(live_today.get('injection_kwh') or 0.0)
            live_consumption_offpeak = float(live_today.get('consumption_offpeak_kwh') or 0.0)
            live_consumption_peak = float(live_today.get('consumption_peak_kwh') or 0.0)
            live_injection_offpeak = float(live_today.get('injection_offpeak_kwh') or 0.0)
            live_injection_peak = float(live_today.get('injection_peak_kwh') or 0.0)

            measured_consumption += (live_consumption - db_consumption)
            measured_injection += (live_injection - db_injection)

            if measured_consumption_offpeak is not None:
                measured_consumption_offpeak += (live_consumption_offpeak - db_consumption_offpeak)
            if measured_consumption_peak is not None:
                measured_consumption_peak += (live_consumption_peak - db_consumption_peak)
            if measured_injection_offpeak is not None:
                measured_injection_offpeak += (live_injection_offpeak - db_injection_offpeak)
            if measured_injection_peak is not None:
                measured_injection_peak += (live_injection_peak - db_injection_peak)

            month_summary = monthly_summary_map.setdefault(current_month_key, {})
            month_summary['consumption'] = float(month_summary.get('consumption') or 0.0) + (live_consumption - db_consumption)
            month_summary['injection'] = float(month_summary.get('injection') or 0.0) + (live_injection - db_injection)
            month_summary['consumption_offpeak'] = float(month_summary.get('consumption_offpeak') or 0.0) + (live_consumption_offpeak - db_consumption_offpeak)
            month_summary['consumption_peak'] = float(month_summary.get('consumption_peak') or 0.0) + (live_consumption_peak - db_consumption_peak)
            month_summary['injection_offpeak'] = float(month_summary.get('injection_offpeak') or 0.0) + (live_injection_offpeak - db_injection_offpeak)
            month_summary['injection_peak'] = float(month_summary.get('injection_peak') or 0.0) + (live_injection_peak - db_injection_peak)

            daily_monthly_summary_map[current_month_key] = month_summary.copy()

    def normalize_tariff_split(total_value, offpeak_value, peak_value):
        """Return a reliable T1/T2 pair aligned with total_value or (None, None)."""
        if not isinstance(total_value, (int, float)):
            return offpeak_value, peak_value
        if not isinstance(offpeak_value, (int, float)) or not isinstance(peak_value, (int, float)):
            return None, None

        total = float(total_value)
        offpeak = float(offpeak_value)
        peak = float(peak_value)
        split_sum = offpeak + peak

        if total <= 0.0:
            return 0.0, 0.0
        if split_sum <= 0.0:
            return None, None

        mismatch_ratio = abs(split_sum - total) / max(total, 1e-9)
        if mismatch_ratio <= 0.02:
            return offpeak, peak

        # Very low split coverage indicates partial/missing T1/T2 counters.
        if (split_sum / total) < 0.20:
            return None, None

        scale = total / split_sum
        return offpeak * scale, peak * scale

    measurement_end_month_key = (projection_end_dt if target_year == current_year else end_dt).strftime('%Y-%m')
    measured_solar_yield = sum(
        float(value or 0.0)
        for month_key, value in monthly_solar_map.items()
        if start_dt.strftime('%Y-%m') <= month_key < measurement_end_month_key
    )

    for month_key, overview_values in csv_monthly_overview.items():
        if month_key < start_dt.strftime('%Y-%m') or month_key >= measurement_end_month_key:
            continue
        month_summary = daily_monthly_summary_map.get(month_key, {})
        measured_consumption += float(overview_values.get('consumption') or 0.0) - float(month_summary.get('consumption') or 0.0)
        measured_injection += float(overview_values.get('injection') or 0.0) - float(month_summary.get('injection') or 0.0)
        if measured_consumption_offpeak is not None:
            measured_consumption_offpeak += float(overview_values.get('consumption_offpeak') or 0.0) - float(month_summary.get('consumption_offpeak') or 0.0)
        if measured_consumption_peak is not None:
            measured_consumption_peak += float(overview_values.get('consumption_peak') or 0.0) - float(month_summary.get('consumption_peak') or 0.0)
        if measured_injection_offpeak is not None:
            measured_injection_offpeak += float(overview_values.get('injection_offpeak') or 0.0) - float(month_summary.get('injection_offpeak') or 0.0)
        if measured_injection_peak is not None:
            measured_injection_peak += float(overview_values.get('injection_peak') or 0.0) - float(month_summary.get('injection_peak') or 0.0)
        measured_days += int(overview_values.get('covered_days') or 0) - measured_days_by_month.get(month_key, 0)

    for month_key, manual_values in manual_monthly_totals.items():
        if month_key < start_dt.strftime('%Y-%m') or month_key >= measurement_end_month_key:
            continue
        month_summary = monthly_summary_map.get(month_key, {})
        measured_consumption += float(manual_values.get('consumption') or 0.0) - float(monthly_summary_map.get(month_key, {}).get('consumption') or 0.0)
        measured_injection += float(manual_values.get('injection') or 0.0) - float(monthly_summary_map.get(month_key, {}).get('injection') or 0.0)
        if measured_consumption_offpeak is not None:
            measured_consumption_offpeak += float(manual_values.get('consumption_offpeak') or 0.0) - float(month_summary.get('consumption_offpeak') or 0.0)
        if measured_consumption_peak is not None:
            measured_consumption_peak += float(manual_values.get('consumption_peak') or 0.0) - float(month_summary.get('consumption_peak') or 0.0)
        if measured_injection_offpeak is not None:
            measured_injection_offpeak += float(manual_values.get('injection_offpeak') or 0.0) - float(month_summary.get('injection_offpeak') or 0.0)
        if measured_injection_peak is not None:
            measured_injection_peak += float(manual_values.get('injection_peak') or 0.0) - float(month_summary.get('injection_peak') or 0.0)
        measured_solar_yield += float(manual_values.get('solar_yield_kwh') or 0.0) - float(monthly_solar_map.get(month_key) or 0.0)
        year_value, month_value = month_key.split('-')
        month_start = datetime(int(year_value), int(month_value), 1)
        if month_start.month == 12:
            next_month_start = datetime(month_start.year + 1, 1, 1)
        else:
            next_month_start = datetime(month_start.year, month_start.month + 1, 1)
        manual_month_days = (next_month_start - month_start).days
        measured_days += manual_month_days - measured_days_by_month.get(month_key, 0)

    measured_days = max(0, min(measured_days, total_days_in_year))
    elapsed_days_in_year = (projection_end_dt - start_dt).days if target_year == current_year else total_days_in_year
    effective_measured_days = measured_days

    # For historical years we can trust persisted yearly solar totals more than
    # recomposed month maps (which may have gaps). Apply an explicit override
    # for legacy years when a DB yearly total exists.
    if target_year in (2022, 2023, 2024, 2025):
        solar_year_override = get_solar_yearly_totals(str(target_year), str(target_year + 1)).get(str(target_year))
        if isinstance(solar_year_override, (int, float)):
            measured_solar_yield = float(solar_year_override)

    if target_year == current_year:
        # CSV/manual monthly totals can represent full elapsed months even when
        # day-level covered_days metadata is incomplete. Avoid over-projecting.
        effective_measured_days = max(effective_measured_days, elapsed_days_in_year)

    if effective_measured_days > 0:
        estimated_consumption = (measured_consumption / effective_measured_days) * total_days_in_year
        estimated_injection = (measured_injection / effective_measured_days) * total_days_in_year
        estimated_consumption_offpeak = (measured_consumption_offpeak / effective_measured_days) * total_days_in_year if measured_consumption_offpeak is not None else None
        estimated_consumption_peak = (measured_consumption_peak / effective_measured_days) * total_days_in_year if measured_consumption_peak is not None else None
        estimated_injection_offpeak = (measured_injection_offpeak / effective_measured_days) * total_days_in_year if measured_injection_offpeak is not None else None
        estimated_injection_peak = (measured_injection_peak / effective_measured_days) * total_days_in_year if measured_injection_peak is not None else None
    else:
        estimated_consumption = 0.0
        estimated_injection = 0.0
        estimated_consumption_offpeak = None
        estimated_consumption_peak = None
        estimated_injection_offpeak = None
        estimated_injection_peak = None

    estimated_solar_yield = (measured_solar_yield / effective_measured_days) * total_days_in_year if effective_measured_days > 0 else 0.0

    measured_consumption_offpeak, measured_consumption_peak = normalize_tariff_split(
        measured_consumption,
        measured_consumption_offpeak,
        measured_consumption_peak,
    )
    measured_injection_offpeak, measured_injection_peak = normalize_tariff_split(
        measured_injection,
        measured_injection_offpeak,
        measured_injection_peak,
    )
    estimated_consumption_offpeak, estimated_consumption_peak = normalize_tariff_split(
        estimated_consumption,
        estimated_consumption_offpeak,
        estimated_consumption_peak,
    )
    estimated_injection_offpeak, estimated_injection_peak = normalize_tariff_split(
        estimated_injection,
        estimated_injection_offpeak,
        estimated_injection_peak,
    )

    result = []
    for month_num in range(1, 13):
        month_key = f'{target_year}-{month_num:02d}'
        month_summary = monthly_summary_map.get(month_key, {}).copy()
        if month_key in csv_monthly_overview:
            month_summary.update(csv_monthly_overview[month_key])
        manual_month_summary = manual_monthly_totals.get(month_key)
        if manual_month_summary:
            if manual_month_summary.get('consumption') is not None:
                month_summary['consumption'] = manual_month_summary.get('consumption')
            if manual_month_summary.get('injection') is not None:
                month_summary['injection'] = manual_month_summary.get('injection')
            if manual_month_summary.get('consumption_offpeak') is not None:
                month_summary['consumption_offpeak'] = manual_month_summary.get('consumption_offpeak')
            if manual_month_summary.get('consumption_peak') is not None:
                month_summary['consumption_peak'] = manual_month_summary.get('consumption_peak')
            if manual_month_summary.get('injection_offpeak') is not None:
                month_summary['injection_offpeak'] = manual_month_summary.get('injection_offpeak')
            if manual_month_summary.get('injection_peak') is not None:
                month_summary['injection_peak'] = manual_month_summary.get('injection_peak')
        solar_yield = (
            manual_month_summary.get('solar_yield_kwh')
            if manual_month_summary and manual_month_summary.get('solar_yield_kwh') is not None
            else monthly_solar_map.get(month_key)
        )
        battery_charge_kwh = float(battery_monthly_charge_map.get(month_key, 0.0) or 0.0)
        injection = month_summary.get('injection', 0.0)
        normalized_cons_offpeak, normalized_cons_peak = normalize_tariff_split(
            month_summary.get('consumption', 0.0),
            month_summary.get('consumption_offpeak'),
            month_summary.get('consumption_peak'),
        )
        normalized_inj_offpeak, normalized_inj_peak = normalize_tariff_split(
            injection,
            month_summary.get('injection_offpeak'),
            month_summary.get('injection_peak'),
        )
        direct_consumption_kwh = (
            min(float(solar_yield), max(0.0, float(solar_yield) - float(injection) + battery_charge_kwh))
            if isinstance(solar_yield, (int, float)) and isinstance(injection, (int, float))
            else None
        )
        direct_consumption_pct = ((direct_consumption_kwh / solar_yield) * 100.0) if isinstance(solar_yield, (int, float)) and solar_yield > 0 and isinstance(direct_consumption_kwh, (int, float)) else None

        result.append({
            'month': month_key,
            'consumption': month_summary.get('consumption', 0.0),
            'injection': injection,
            'solar_yield_kwh': solar_yield,
            'battery_charge_kwh': battery_charge_kwh,
            'direct_consumption_kwh': direct_consumption_kwh,
            'direct_consumption_pct': direct_consumption_pct,
            'consumption_offpeak': normalized_cons_offpeak,
            'consumption_peak': normalized_cons_peak,
            'injection_offpeak': normalized_inj_offpeak,
            'injection_peak': normalized_inj_peak,
            'highest_peak_w': month_summary.get('highest_peak_w'),
            'peak_threshold_w': PEAK_THRESHOLD_W,
            'peak_deviation_pct': round(
                max(0.0, ((month_summary.get('highest_peak_w') - PEAK_THRESHOLD_W) / PEAK_THRESHOLD_W) * 100.0),
                2,
            ) if isinstance(month_summary.get('highest_peak_w'), (int, float)) else None,
        })

    # Recalculate direct consumption from months that have solar values.
    measured_months_with_solar = [
        month_entry
        for month_entry in result
        if month_entry.get('month') >= start_dt.strftime('%Y-%m')
        and month_entry.get('month') < measurement_end_month_key
        and isinstance(month_entry.get('solar_yield_kwh'), (int, float))
        and isinstance(month_entry.get('injection'), (int, float))
    ]
    measured_solar_for_direct = sum(month_entry['solar_yield_kwh'] for month_entry in measured_months_with_solar)
    measured_direct_consumption_kwh = sum(
        float(month_entry['direct_consumption_kwh'])
        for month_entry in measured_months_with_solar
        if isinstance(month_entry.get('direct_consumption_kwh'), (int, float))
    ) if measured_months_with_solar else None
    measured_direct_consumption_pct = (
        (measured_direct_consumption_kwh / measured_solar_for_direct) * 100.0
        if measured_direct_consumption_kwh is not None and measured_solar_for_direct > 0
        else None
    )

    estimated_direct_consumption_pct = measured_direct_consumption_pct
    estimated_direct_consumption_kwh = (
        (estimated_solar_yield * estimated_direct_consumption_pct / 100.0)
        if estimated_direct_consumption_pct is not None and estimated_solar_yield > 0
        else None
    )

    payload = {
        'months': result,
        'projection': {
            'year': target_year,
            'measured_days': measured_days,
            'effective_measured_days': effective_measured_days,
            'elapsed_days_in_year': elapsed_days_in_year,
            'days_in_year': total_days_in_year,
            'measured_solar_yield_kwh': round(measured_solar_yield, 2),
            'measured_consumption_kwh': round(measured_consumption, 2),
            'measured_injection_kwh': round(measured_injection, 2),
            'measured_direct_consumption_kwh': round(measured_direct_consumption_kwh, 2) if measured_direct_consumption_kwh is not None else None,
            'measured_direct_consumption_pct': round(measured_direct_consumption_pct, 2) if measured_direct_consumption_pct is not None else None,
            'estimated_solar_yield_kwh': round(estimated_solar_yield, 2),
            'estimated_consumption_kwh': round(estimated_consumption, 2),
            'estimated_injection_kwh': round(estimated_injection, 2),
            'estimated_direct_consumption_kwh': round(estimated_direct_consumption_kwh, 2) if estimated_direct_consumption_kwh is not None else None,
            'estimated_direct_consumption_pct': round(estimated_direct_consumption_pct, 2) if estimated_direct_consumption_pct is not None else None,
            'measured_consumption_offpeak_kwh': round(measured_consumption_offpeak, 2) if measured_consumption_offpeak is not None else None,
            'measured_consumption_peak_kwh': round(measured_consumption_peak, 2) if measured_consumption_peak is not None else None,
            'measured_injection_offpeak_kwh': round(measured_injection_offpeak, 2) if measured_injection_offpeak is not None else None,
            'measured_injection_peak_kwh': round(measured_injection_peak, 2) if measured_injection_peak is not None else None,
            'estimated_consumption_offpeak_kwh': round(estimated_consumption_offpeak, 2) if estimated_consumption_offpeak is not None else None,
            'estimated_consumption_peak_kwh': round(estimated_consumption_peak, 2) if estimated_consumption_peak is not None else None,
            'estimated_injection_offpeak_kwh': round(estimated_injection_offpeak, 2) if estimated_injection_offpeak is not None else None,
            'estimated_injection_peak_kwh': round(estimated_injection_peak, 2) if estimated_injection_peak is not None else None,
        }
    }

    with _yearly_data_cache_lock:
        _yearly_data_cache[target_year] = {
            'expires': now_ts + yearly_cache_ttl_seconds,
            'payload': payload,
        }
        if len(_yearly_data_cache) > 12:
            stale_keys = [
                year_key for year_key, entry in _yearly_data_cache.items()
                if now_ts >= float(entry.get('expires', 0.0) or 0.0)
            ]
            for year_key in stale_keys:
                _yearly_data_cache.pop(year_key, None)

    return _json_nocache(payload)


@app.route('/lifetime_data')
def get_lifetime_data():
    """Return yearly lifetime totals in MWh, merged with provided historical values."""
    now_ts = time.time()
    with _lifetime_data_cache_lock:
        if _lifetime_data_cache.get('payload') is not None and now_ts < float(_lifetime_data_cache.get('expires', 0.0) or 0.0):
            return _json_nocache(_lifetime_data_cache['payload'])

    conn_daily = get_db_connection(DB_FILE_DAILY)
    c_daily = conn_daily.cursor()
    prepare_manual_tables_best_effort(c_daily, seed_monthly=True)

    c_daily.execute(
        '''SELECT strftime('%Y-%m', datetime(day_start_ts, 'unixepoch', 'localtime')) AS month_key,
                  SUM(consumption_kwh),
                  SUM(injection_kwh)
           FROM daily_consumption
           GROUP BY month_key
           ORDER BY month_key'''
    )
    monthly_energy_rows = c_daily.fetchall()

    monthly_solar_map = {
        month_key: (float(value) / 1000.0) if isinstance(value, (int, float)) else None
        for month_key, value in get_solar_monthly_totals().items()
    }

    monthly_manual_solar_map = {}
    c_daily.execute(
        '''SELECT substr(date, 1, 7) AS month_key,
                  SUM(solar_yield_kwh)
           FROM solar_manual_data
           GROUP BY month_key
           ORDER BY month_key'''
    )
    for row in c_daily.fetchall():
        if row[0] is not None:
            month_key = row[0]
            monthly_manual_solar_map[month_key] = (float(row[1]) / 1000.0) if row[1] is not None else None

    c_daily.execute(
        '''SELECT month,
                  SUM(consumption_kwh),
                SUM(injection_kwh),
                  SUM(solar_yield_kwh)
           FROM monthly_manual_totals
           GROUP BY month
           ORDER BY month'''
    )
    monthly_manual_rows = c_daily.fetchall()
    conn_daily.close()
    manual_solar_months = {
        month_key
        for month_key, _, _, solar_yield_kwh in monthly_manual_rows
        if month_key is not None and solar_yield_kwh is not None
    }

    csv_monthly_overview = load_csv_monthly_overview()

    monthly_combined_map = {
        row[0]: {
            'consumption_mwh': (float(row[1]) / 1000.0) if row[1] is not None else None,
            'injection_mwh': (float(row[2]) / 1000.0) if row[2] is not None else None,
            'solar_yield_mwh': monthly_solar_map.get(row[0]),
            'source': 'database',
        }
        for row in monthly_energy_rows
        if row[0] is not None
    }

    for month_key, solar_value in monthly_solar_map.items():
        month_entry = monthly_combined_map.setdefault(
            month_key,
            {
                'consumption_mwh': None,
                'injection_mwh': None,
                'solar_yield_mwh': None,
                'source': 'database',
            },
        )
        month_entry['solar_yield_mwh'] = solar_value

    for month_key, overview_values in csv_monthly_overview.items():
        monthly_combined_map[month_key] = {
            'consumption_mwh': (float(overview_values.get('consumption')) / 1000.0) if overview_values.get('consumption') is not None else None,
            'injection_mwh': (float(overview_values.get('injection')) / 1000.0) if overview_values.get('injection') is not None else None,
            'solar_yield_mwh': monthly_solar_map.get(month_key),
            'source': 'csv-overview',
        }

    for month_key, consumption_kwh, injection_kwh, solar_yield_kwh in monthly_manual_rows:
        if month_key is None:
            continue
        if month_key in csv_monthly_overview:
            month_entry = monthly_combined_map.setdefault(
                month_key,
                {
                    'consumption_mwh': None,
                    'injection_mwh': None,
                    'solar_yield_mwh': None,
                    'source': 'csv-overview',
                },
            )
            continue
        monthly_combined_map[month_key] = {
            'consumption_mwh': (float(consumption_kwh) / 1000.0) if consumption_kwh is not None else None,
            'injection_mwh': (float(injection_kwh) / 1000.0) if injection_kwh is not None else None,
            'solar_yield_mwh': None,
            'source': 'manual-monthly',
        }

    for month_key, solar_value in monthly_manual_solar_map.items():
        if month_key is None:
            continue
        month_entry = monthly_combined_map.setdefault(
            month_key,
            {
                'consumption_mwh': None,
                'injection_mwh': None,
                'solar_yield_mwh': None,
                'source': 'manual-solar',
            },
        )
        if isinstance(solar_value, (int, float)):
            month_entry['solar_yield_mwh'] = float(month_entry.get('solar_yield_mwh') or 0.0) + float(solar_value)
            if month_entry.get('source') == 'database':
                month_entry['source'] = 'manual-solar'

    yearly_aggregates = {}
    for month_key, values in monthly_combined_map.items():
        year = int(month_key[:4])
        year_entry = yearly_aggregates.setdefault(
            year,
            {
                'year': year,
                'solar_yield_mwh': 0.0,
                'injection_mwh': 0.0,
                'injection_with_solar_mwh': 0.0,
                'consumption_mwh': 0.0,
                'source': 'database',
            },
        )
        solar_value = values.get('solar_yield_mwh')
        injection_value = values.get('injection_mwh')

        if isinstance(solar_value, (int, float)):
            year_entry['solar_yield_mwh'] += solar_value
            if isinstance(injection_value, (int, float)):
                year_entry['injection_with_solar_mwh'] += injection_value
        if isinstance(values.get('injection_mwh'), (int, float)):
            year_entry['injection_mwh'] += values['injection_mwh']
        if isinstance(values.get('consumption_mwh'), (int, float)):
            year_entry['consumption_mwh'] += values['consumption_mwh']
        if values.get('source') == 'csv-overview':
            year_entry['source'] = 'csv-overview'
        elif values.get('source') == 'manual-monthly' and year_entry.get('source') != 'historical':
            year_entry['source'] = 'manual-monthly'

    combined = {
        year: {
            'year': year,
            'solar_yield_mwh': values['solar_yield_mwh'],
            'injection_mwh': values['injection_mwh'],
            'injection_with_solar_mwh': values['injection_mwh'],
            'consumption_mwh': values['consumption_mwh'],
            'source': 'historical',
        }
        for year, values in HISTORICAL_LIFETIME_TOTALS_MWH.items()
    }

    for year, values in yearly_aggregates.items():
        if year in combined:
            if values.get('source') == 'csv-overview':
                combined[year].update({
                    'consumption_mwh': values.get('consumption_mwh', combined[year].get('consumption_mwh')),
                    'injection_mwh': values.get('injection_mwh', combined[year].get('injection_mwh')),
                    'source': 'csv-overview',
                })
            continue
        combined[year] = values

    solar_yearly_map = {
        int(year): (float(value) / 1000.0) if isinstance(value, (int, float)) else None
        for year, value in get_solar_yearly_totals().items()
    }
    for year, solar_value in solar_yearly_map.items():
        year_entry = combined.setdefault(
            year,
            {
                'year': year,
                'solar_yield_mwh': None,
                'injection_mwh': None,
                'injection_with_solar_mwh': None,
                'consumption_mwh': None,
                'source': 'solar-history',
            },
        )
        if year_entry.get('solar_yield_mwh') is None:
            year_entry['solar_yield_mwh'] = solar_value
        if year_entry.get('injection_with_solar_mwh') is None:
            year_entry['injection_with_solar_mwh'] = year_entry.get('injection_mwh')

    years = []
    for year in sorted(combined.keys()):
        item = combined[year]
        solar_yield_mwh = item.get('solar_yield_mwh')
        injection_mwh = item.get('injection_mwh')
        direct_consumption_mwh = (
            max(0.0, float(solar_yield_mwh) - float(injection_mwh or 0.0))
            if isinstance(solar_yield_mwh, (int, float))
            else None
        )
        direct_consumption_pct = (
            ((float(solar_yield_mwh) - float(injection_mwh or 0.0)) / float(solar_yield_mwh)) * 100.0
            if isinstance(solar_yield_mwh, (int, float)) and float(solar_yield_mwh) > 0
            else None
        )
        item['direct_consumption_mwh'] = direct_consumption_mwh
        item['direct_consumption_pct'] = direct_consumption_pct
        item.pop('injection_with_solar_mwh', None)
        years.append(item)

    total_solar = sum(item['solar_yield_mwh'] for item in years if isinstance(item.get('solar_yield_mwh'), (int, float)))
    total_injection = sum(item['injection_mwh'] for item in years if isinstance(item.get('injection_mwh'), (int, float)))
    total_consumption = sum(item['consumption_mwh'] for item in years if isinstance(item.get('consumption_mwh'), (int, float)))
    total_direct_consumption = max(0.0, total_solar - total_injection) if total_solar > 0 else 0.0
    total_direct_consumption_pct = (((total_solar - total_injection) / total_solar) * 100.0) if total_solar > 0 else None

    payload = {
        'years': years,
        'totals': {
            'solar_yield_mwh': round(total_solar, 3),
            'injection_mwh': round(total_injection, 3),
            'consumption_mwh': round(total_consumption, 3),
            'direct_consumption_mwh': round(total_direct_consumption, 3),
            'direct_consumption_pct': round(total_direct_consumption_pct, 2) if total_direct_consumption_pct is not None else None,
        }
    }

    with _lifetime_data_cache_lock:
        _lifetime_data_cache['expires'] = now_ts + 6 * 3600.0
        _lifetime_data_cache['payload'] = payload

    return _json_nocache(payload)

@app.route("/cost_data")
def get_cost_data():
    """Get 5-minute cost data using Elia public quarter-hour imbalance prices."""
    date_str = request.args.get('date')
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except:
            target_date = datetime.now()
    else:
        target_date = datetime.now()
    
    midnight_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_end = midnight_start + timedelta(days=1)
    timestamp_start = midnight_start.timestamp()
    timestamp_end = midnight_end.timestamp()
    
    conn = get_db_connection(DB_FILE_AVG)
    c = conn.cursor()
    # Read from 5-minute averages table (permanent storage)
    c.execute('SELECT timestamp, power_avg FROM energy_data_5min WHERE timestamp >= ? AND timestamp < ? ORDER BY timestamp', 
             (timestamp_start, timestamp_end))
    rows = c.fetchall()
    conn.close()

    date_str_formatted = target_date.strftime('%Y-%m-%d')
    quarter_hour_prices, price_source = get_elia_imbalance_prices(date_str_formatted)

    # Convert to dict for quick lookup
    data_dict = {row[0]: row[1] for row in rows}

    result = []
    current = (timestamp_start // 300) * 300
    end = (timestamp_end // 300) * 300
    while current < end:
        local_dt = datetime.fromtimestamp(current)
        time_str = local_dt.strftime('%H:%M')
        quarter_hour_key = (local_dt.hour, (local_dt.minute // 15) * 15)
        price_mwh = quarter_hour_prices.get(quarter_hour_key)

        if price_mwh is None:
            price_mwh = quarter_hour_prices.get((local_dt.hour, 0), 50)
        
        avg_power_w = data_dict.get(current)  # None if no data for this interval
        
        if avg_power_w is not None:
            cost_eur = abs(avg_power_w) / 1_000_000 * price_mwh * (5/60)
            if avg_power_w < 0:
                cost_eur = -cost_eur
        else:
            cost_eur = None
        
        result.append({
            'time': time_str,
            'cost': cost_eur,
            'price_mwh': price_mwh,
            'price_source': price_source,
        })
        current += 300

    return jsonify(result)


@app.route("/gas_daily_data")
def get_gas_daily_data():
    """Get daily gas usage in kWh/day for the selected month (defaults to current month)."""
    try:
        year_param = request.args.get('year')
        month_param = request.args.get('month')

        now_local = datetime.now()
        try:
            target_year = int(year_param) if year_param is not None else now_local.year
        except Exception:
            target_year = now_local.year

        try:
            target_month = int(month_param) if month_param is not None else now_local.month
        except Exception:
            target_month = now_local.month

        target_month = max(1, min(12, target_month))
        refresh_gas_totals_from_raw_if_stale(target_year=target_year, target_month=target_month)

        conn = get_db_connection(DB_FILE_GAS_TOTALS)
        c = conn.cursor()
        c.execute('''
            SELECT date, usage_kwh
            FROM gas_daily_totals
            WHERE date >= ? AND date < ?
            ORDER BY date
        ''', (
            f'{target_year:04d}-{target_month:02d}-01',
            f'{(target_year + 1):04d}-01-01' if target_month == 12 else f'{target_year:04d}-{(target_month + 1):02d}-01',
        ))
        rows = c.fetchall()
        conn.close()

        usage_map = {
            str(day): float(usage_kwh or 0.0)
            for day, usage_kwh in rows
            if day
        }

        month_start = datetime(target_year, target_month, 1)
        month_end = datetime(target_year + 1, 1, 1) if target_month == 12 else datetime(target_year, target_month + 1, 1)
        result = []
        current_day = month_start
        while current_day < month_end:
            day_key = current_day.strftime('%Y-%m-%d')
            usage = float(usage_map.get(day_key, 0.0))

            result.append({
                'date': day_key,
                'usage_kwh': round(usage, 1)
            })
            current_day += timedelta(days=1)

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route("/gas_monthly_data")
def get_gas_monthly_data():
    """Get full calendar year gas usage in kWh with zero-filled missing months."""
    try:
        year_param = request.args.get('year')
        try:
            target_year = int(year_param) if year_param else datetime.now().year
        except Exception:
            target_year = datetime.now().year

        refresh_gas_totals_from_raw_if_stale(target_year=target_year, target_month=None)

        start_month = f'{target_year}-01'
        end_month = f'{target_year + 1}-01'

        conn = get_db_connection(DB_FILE_GAS_TOTALS)
        c = conn.cursor()
        c.execute('''
            SELECT month, usage_kwh
            FROM gas_monthly_totals
            WHERE month >= ? AND month < ?
            ORDER BY month
        ''', (start_month, end_month))
        rows = c.fetchall()
        conn.close()

        month_usage = {}
        for month, usage_kwh in rows:
            month_usage[month] = round(float(usage_kwh or 0.0), 1)

        result = []
        for month_num in range(1, 13):
            month = f'{target_year}-{month_num:02d}'
            result.append({
                'month': month,
                'usage_kwh': month_usage.get(month, 0.0)
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def refresh_gas_totals_from_raw_if_stale(target_year=None, target_month=None, force=False):
    """Refresh gas totals for a requested month/year scope in the dedicated gas database."""
    now_ts = time.time()
    now_local = datetime.now()
    year_value = int(target_year) if isinstance(target_year, int) else now_local.year
    month_value = None
    if target_month is not None:
        try:
            month_value = max(1, min(12, int(target_month)))
        except Exception:
            month_value = now_local.month

    if month_value is not None:
        range_start = datetime(year_value, month_value, 1)
        range_end = datetime(year_value + 1, 1, 1) if month_value == 12 else datetime(year_value, month_value + 1, 1)
        cache_key = f'month:{year_value:04d}-{month_value:02d}'
    else:
        range_start = datetime(year_value, 1, 1)
        range_end = datetime(year_value + 1, 1, 1)
        cache_key = f'year:{year_value:04d}'

    expires_by_key = _gas_totals_refresh_state.setdefault('expires_by_key', {})
    if not force and now_ts < float(expires_by_key.get(cache_key, 0.0) or 0.0):
        return

    raw_conn = None
    gas_conn = None
    try:
        raw_conn = get_db_connection(DB_FILE_RAW)
        raw_cursor = raw_conn.cursor()
        start_ts = range_start.timestamp()
        end_ts = range_end.timestamp()
        raw_cursor.execute(
            '''SELECT strftime('%Y-%m-%d', datetime(timestamp, 'unixepoch', 'localtime')) AS day,
                      MIN(gas_m3) AS min_gas,
                      MAX(gas_m3) AS max_gas,
                      COUNT(*) AS sample_count
               FROM energy_data
               WHERE gas_m3 IS NOT NULL
                 AND timestamp >= ?
                 AND timestamp < ?
               GROUP BY day
               ORDER BY day''',
            (start_ts, end_ts),
        )
        raw_rows = raw_cursor.fetchall()

        daily_rows = []
        month_keys = set()
        for day, min_gas, max_gas, sample_count in raw_rows:
            if not day:
                continue
            usage_m3 = 0.0
            if min_gas is not None and max_gas is not None:
                usage_m3 = max(0.0, float(max_gas) - float(min_gas))
            if usage_m3 > GAS_MAX_DAILY_USAGE_M3:
                usage_m3 = 0.0
            usage_kwh = usage_m3 * GAS_KWH_PER_M3
            day_key = str(day)
            month_key = day_key[:7]
            month_keys.add(month_key)
            daily_rows.append((day_key, usage_m3, usage_kwh, sample_count, now_ts))

        gas_conn = get_db_connection(DB_FILE_GAS_TOTALS, write=True)
        gas_cursor = gas_conn.cursor()
        gas_cursor.execute('''CREATE TABLE IF NOT EXISTS gas_daily_totals (
            date TEXT PRIMARY KEY,
            usage_m3 REAL,
            usage_kwh REAL,
            sample_count INTEGER,
            updated_at REAL
        )''')
        gas_cursor.execute('CREATE INDEX IF NOT EXISTS idx_gas_daily_date ON gas_daily_totals(date)')
        gas_cursor.execute('''CREATE TABLE IF NOT EXISTS gas_monthly_totals (
            month TEXT PRIMARY KEY,
            usage_m3 REAL,
            usage_kwh REAL,
            day_count INTEGER,
            updated_at REAL
        )''')
        gas_cursor.execute('CREATE INDEX IF NOT EXISTS idx_gas_monthly_month ON gas_monthly_totals(month)')

        if daily_rows:
            gas_cursor.executemany(
                '''INSERT OR REPLACE INTO gas_daily_totals (date, usage_m3, usage_kwh, sample_count, updated_at)
                   VALUES (?, ?, ?, ?, ?)''',
                daily_rows,
            )

        # Rebuild month totals only for months inside the refreshed window.
        month_cursor = datetime(range_start.year, range_start.month, 1)
        while month_cursor < range_end:
            month_key = month_cursor.strftime('%Y-%m')
            month_keys.add(month_key)
            if month_cursor.month == 12:
                month_cursor = datetime(month_cursor.year + 1, 1, 1)
            else:
                month_cursor = datetime(month_cursor.year, month_cursor.month + 1, 1)

        for month_key in sorted(month_keys):
            month_start = f'{month_key}-01'
            month_year = int(month_key[:4])
            month_num = int(month_key[5:7])
            next_month = (
                f'{month_year + 1:04d}-01-01'
                if month_num == 12
                else f'{month_year:04d}-{month_num + 1:02d}-01'
            )

            gas_cursor.execute(
                '''SELECT COALESCE(SUM(usage_m3), 0.0),
                          COALESCE(SUM(usage_kwh), 0.0),
                          COUNT(*)
                   FROM gas_daily_totals
                   WHERE date >= ? AND date < ?''',
                (month_start, next_month),
            )
            row = gas_cursor.fetchone() or (0.0, 0.0, 0)
            gas_cursor.execute(
                '''INSERT OR REPLACE INTO gas_monthly_totals (month, usage_m3, usage_kwh, day_count, updated_at)
                   VALUES (?, ?, ?, ?, ?)''',
                (month_key, float(row[0] or 0.0), float(row[1] or 0.0), int(row[2] or 0), now_ts),
            )

        gas_conn.commit()
        expires_by_key[cache_key] = now_ts + GAS_TOTALS_REFRESH_SECONDS
    except Exception as e:
        print(f"Gas totals refresh failed: {e}")
        expires_by_key = _gas_totals_refresh_state.setdefault('expires_by_key', {})
        expires_by_key[cache_key] = now_ts + 15.0
    finally:
        if raw_conn:
            raw_conn.close()
        if gas_conn:
            gas_conn.close()

@app.route("/all_data")
def get_all_data():
    """Get recent raw data from the RAW database with a safe result cap."""
    try:
        limit = int(request.args.get('limit', 1000))
    except (TypeError, ValueError):
        limit = 1000
    limit = max(1, min(limit, 10000))

    conn = get_db_connection(DB_FILE_RAW)
    c = conn.cursor()
    c.execute(
        'SELECT timestamp, power, import_kwh, export_kwh, gas_m3 FROM energy_data ORDER BY timestamp DESC LIMIT ?',
        (limit,),
    )
    rows = list(reversed(c.fetchall()))
    conn.close()
    data_history = [{'timestamp': row[0], 'power': row[1], 'import': row[2], 'export': row[3], 'gas': row[4]} for row in rows]
    return jsonify(data_history)

# Cache for the rolling 365-day avg profile.
_avg_profile_cache = {'key': None, 'data': None, 'day_count': 0, 'expires': 0.0}
_monthly_recover_check_cache = {}
_solar_monthly_rebuild_cache = {}
_solar_daily_rollup_rebuild_cache = {}
_solar_daily_totals_persist_cache = {}
_battery_daily_totals_cache = {}
_battery_daily_totals_cache_lock = threading.Lock()
_weekly_data_cache = {}
_weekly_data_cache_lock = threading.Lock()
_minute_data_cache = {}
_minute_data_cache_lock = threading.Lock()
# Must stay >= the frontend's 30s poll interval (see templates/index.html
# setInterval near the minute-tab refresh) or every poll is a guaranteed
# cache miss and reruns the full solar/battery/live-breakdown query chain.
MINUTE_DATA_TODAY_CACHE_TTL_SECONDS = 25.0
_yearly_data_cache = {}
_yearly_data_cache_lock = threading.Lock()
_lifetime_data_cache = {'expires': 0.0, 'payload': None}
_lifetime_data_cache_lock = threading.Lock()
_live_daily_breakdown_cache = {'key': None, 'expires': 0.0, 'data': None}
_gas_totals_refresh_state = {'expires': 0.0}


def _query_day_5min_from_raw(timestamp_start, timestamp_end):
    """Fast fallback: derive one-day 5-minute averages directly from raw DB without writes."""
    query = '''SELECT CAST(timestamp / 300 AS INTEGER) * 300 AS bucket_ts,
                      AVG(power) AS power_avg
               FROM energy_data
               WHERE timestamp >= ? AND timestamp < ?
               GROUP BY CAST(timestamp / 300 AS INTEGER) * 300
               ORDER BY bucket_ts'''

    for db_path in (DB_FILE_RAW, DB_FILE_BACKUP):
        conn_local = None
        try:
            conn_local = get_db_connection(db_path)
            c_local = conn_local.cursor()
            c_local.execute(query, (timestamp_start, timestamp_end))
            rows = c_local.fetchall()
            if rows:
                return [(float(row[0]), float(row[1])) for row in rows if row[0] is not None and row[1] is not None]
        except Exception as e:
            print(f"Raw 5-min fallback query failed ({db_path}): {e}")
        finally:
            if conn_local:
                conn_local.close()

    return []

@app.route("/minute_data")
def get_minute_data():
    """Get the selected day 5-minute series plus a rolling 365-day 15-minute average profile."""
    date_str = request.args.get('date')
    include_profile = str(request.args.get('include_profile', '')).strip().lower() in {'1', 'true', 'yes'}
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except:
            target_date = datetime.now()
    else:
        target_date = datetime.now()

    midnight_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_end = midnight_start + timedelta(days=1)
    timestamp_start = midnight_start.timestamp()
    timestamp_end = midnight_end.timestamp()
    is_today = target_date.date() == datetime.now().date()
    cache_key = target_date.strftime('%Y-%m-%d')
    now_ts = time.time()
    cache_ttl_seconds = MINUTE_DATA_TODAY_CACHE_TTL_SECONDS if is_today else 1800.0

    with _minute_data_cache_lock:
        cache_entry = _minute_data_cache.get(cache_key)
        if cache_entry and now_ts < cache_entry.get('expires', 0.0):
            return _json_nocache(cache_entry['payload'])

    # --- Daily 5-min series (fast: small result set from avg DB) ---
    rows = []
    conn = None
    try:
        conn = get_db_connection(DB_FILE_AVG)
        c = conn.cursor()
        c.execute(
            'SELECT timestamp, power_avg FROM energy_data_5min WHERE timestamp >= ? AND timestamp < ? ORDER BY timestamp',
            (timestamp_start, timestamp_end),
        )
        rows = c.fetchall()
    except Exception as e:
        print(f"Daily 5-min query failed: {e}")
    finally:
        if conn:
            conn.close()

    # Fast fallback for missing derived rows: query raw data directly.
    # Avoid in-request rebuild work so daily graphs open immediately.
    if not rows:
        try:
            rows = _query_day_5min_from_raw(timestamp_start, timestamp_end)
        except Exception as e:
            print(f"Raw 5-min fallback failed: {e}")

    # For today, also compute the current in-progress 5-minute bucket directly
    # from raw samples so the daily chart updates before the interval closes.
    if is_today:
        try:
            now_ts = time.time()
            live_bucket_start = int(now_ts // 300) * 300
            if timestamp_start <= live_bucket_start < timestamp_end:
                conn_live = get_db_connection(DB_FILE_RAW)
                c_live = conn_live.cursor()
                c_live.execute(
                    '''SELECT AVG(power)
                       FROM energy_data
                       WHERE timestamp >= ? AND timestamp < ?''',
                    (live_bucket_start, min(now_ts, timestamp_end)),
                )
                live_avg_row = c_live.fetchone()
                conn_live.close()

                if live_avg_row and live_avg_row[0] is not None:
                    live_avg_power = float(live_avg_row[0])
                    rows = [row for row in rows if int(row[0]) != live_bucket_start]
                    rows.append((live_bucket_start, live_avg_power))
                    rows.sort(key=lambda row: row[0])
        except Exception as e:
            print(f"Live 5-min bucket query failed: {e}")

    # --- Rolling 365-day avg profile (optional; daily graph does not use it) ---
    profile_dict = {}
    aggregate_day_count = 0
    if include_profile:
        now_ts = time.time()
        profile_window_end = timestamp_start
        profile_window_start = (midnight_start - timedelta(days=AVG_PROFILE_WINDOW_DAYS)).timestamp()
        profile_cache_key = (int(profile_window_start), int(profile_window_end))

        if (
            _avg_profile_cache['data'] is None
            or _avg_profile_cache.get('key') != profile_cache_key
            or now_ts >= _avg_profile_cache['expires']
        ):
            conn_avg_profile = None
            try:
                conn_avg_profile = get_db_connection(DB_FILE_AVG)
                c_profile = conn_avg_profile.cursor()
                c_profile.execute(
                    '''SELECT CAST(((CAST(strftime('%H', datetime(timestamp, 'unixepoch', 'localtime')) AS INTEGER) * 60 +
                                     CAST(strftime('%M', datetime(timestamp, 'unixepoch', 'localtime')) AS INTEGER)) / 15) AS INTEGER) * 15 AS minute_of_day,
                            AVG(power_avg) AS avg_power
                       FROM energy_data_5min
                       WHERE timestamp >= ? AND timestamp < ?
                       GROUP BY minute_of_day
                       ORDER BY minute_of_day''',
                    (profile_window_start, profile_window_end)
                )
                profile_rows = c_profile.fetchall()
                c_profile.execute(
                    '''SELECT COUNT(DISTINCT strftime('%Y-%m-%d', datetime(timestamp, 'unixepoch', 'localtime')))
                       FROM energy_data_5min
                       WHERE timestamp >= ? AND timestamp < ?''',
                    (profile_window_start, profile_window_end)
                )
                aggregate_day_count = c_profile.fetchone()[0] or 0
                profile_dict = {int(row[0]): float(row[1]) for row in profile_rows if row[0] is not None and row[1] is not None}
                _avg_profile_cache['key'] = profile_cache_key
                _avg_profile_cache['data'] = profile_dict
                _avg_profile_cache['day_count'] = aggregate_day_count
                _avg_profile_cache['expires'] = now_ts + 300
            except Exception as e:
                print(f"365-day profile query failed: {e}")
                profile_dict = _avg_profile_cache['data'] or {}
                aggregate_day_count = _avg_profile_cache['day_count'] if _avg_profile_cache['data'] is not None else 0
            finally:
                if conn_avg_profile:
                    conn_avg_profile.close()
        else:
            profile_dict = _avg_profile_cache['data']
            aggregate_day_count = _avg_profile_cache['day_count']

    # Build daily series
    expanded_rows = list(rows)
    if rows and len(rows) <= 96:
        # Fluvius history provides quarter-hour averages. Expand them across the
        # three 5-minute slots of the same quarter so the chart layout matches
        # the denser post-2026-04-07 P1 series.
        expanded_dict = {int(row[0]): row[1] for row in rows}
        for index, row in enumerate(rows):
            slot_ts = int(row[0])
            power_avg = row[1]
            next_ts = int(rows[index + 1][0]) if index + 1 < len(rows) else None
            for offset in (300, 600):
                fill_ts = slot_ts + offset
                if fill_ts >= timestamp_end:
                    break
                if next_ts is not None and fill_ts >= next_ts:
                    break
                expanded_dict.setdefault(fill_ts, power_avg)
        expanded_rows = sorted(expanded_dict.items())

    data_dict = {int(row[0]): row[1] for row in expanded_rows}
    result = []
    current = int((timestamp_start // 300) * 300)
    end = int((timestamp_end // 300) * 300)
    while current < end:
        result.append({'timestamp': current * 1000, 'consumption': data_dict.get(current)})
        current += 300

    # Build profile series using integer arithmetic (no datetime in loop)
    base_ts = int(midnight_start.timestamp())
    profile_result = [
        {'timestamp': (base_ts + m * 60) * 1000, 'avg_power': profile_dict.get(m)}
        for m in range(0, 24 * 60, 15)
    ]

    solar_result = get_solar_5min_series(target_date)
    solar_result = _correct_legacy_solar_series_timestamps(solar_result, target_date=target_date)
    battery_result = get_battery_5min_series(target_date)
    synced_5min_result = _build_synced_power_solar_series(result, solar_result, bucket_seconds=300)

    solar_yield_kwh = None
    try:
        solar_day_key = target_date.strftime('%Y-%m-%d')
        solar_next_day_key = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')
        solar_daily_map = get_solar_daily_totals(solar_day_key, solar_next_day_key)
        if solar_day_key in solar_daily_map and solar_daily_map[solar_day_key] is not None:
            solar_yield_kwh = float(solar_daily_map[solar_day_key])
    except Exception as e:
        print(f"Daily solar total lookup failed: {e}")

    if solar_yield_kwh is None and solar_result:
        try:
            solar_yield_kwh = sum(
                max(0.0, float(point.get('avg_power_w') or 0.0)) * (5.0 / 60.0) / 1000.0
                for point in solar_result
                if isinstance(point, dict)
            )
        except Exception:
            solar_yield_kwh = None

    # Fetch daily off-peak/peak totals.
    # For today: compute live from raw samples so the value always reflects the
    # most recent P1 reading (daily_consumption is only updated every 5 minutes).
    # For past days: read the pre-built daily_consumption row.
    today_str = datetime.now().strftime('%Y-%m-%d')
    daily_breakdown = None
    if target_date.strftime('%Y-%m-%d') == today_str:
        breakdown_key = (today_str, int(now_ts // 15))
        if (
            _live_daily_breakdown_cache.get('key') == breakdown_key
            and now_ts < _live_daily_breakdown_cache.get('expires', 0.0)
        ):
            daily_breakdown = _live_daily_breakdown_cache.get('data')
        else:
            daily_breakdown = get_live_daily_breakdown(target_date)
            _live_daily_breakdown_cache['key'] = breakdown_key
            _live_daily_breakdown_cache['expires'] = now_ts + 15.0
            _live_daily_breakdown_cache['data'] = daily_breakdown
    if daily_breakdown is None:
        try:
            conn_daily = get_db_connection(DB_FILE_DAILY)
            c_daily = conn_daily.cursor()
            c_daily.execute(
                '''SELECT consumption_kwh, injection_kwh,
                          consumption_offpeak_kwh, consumption_peak_kwh,
                          injection_offpeak_kwh, injection_peak_kwh
                   FROM daily_consumption WHERE date = ?''',
                (target_date.strftime('%Y-%m-%d'),),
            )
            row_d = c_daily.fetchone()
            if row_d:
                cons_total = row_d[0]
                inj_total = row_d[1]
                cons_offpeak = row_d[2]
                cons_peak = row_d[3]
                inj_offpeak = row_d[4]
                inj_peak = row_d[5]
                # When split T1/T2 counters are available their sum is authoritative;
                # the total counter and split counters update at different rates so
                # they can diverge slightly. Use T1+T2 as the displayed total so the
                # three values are always internally consistent.
                if isinstance(cons_offpeak, (int, float)) and isinstance(cons_peak, (int, float)):
                    cons_split_sum = float(cons_offpeak) + float(cons_peak)
                    if cons_split_sum > 0.0 or float(cons_total or 0.0) <= 0.0:
                        cons_total = cons_split_sum
                if isinstance(inj_offpeak, (int, float)) and isinstance(inj_peak, (int, float)):
                    inj_split_sum = float(inj_offpeak) + float(inj_peak)
                    if inj_split_sum > 0.0 or float(inj_total or 0.0) <= 0.0:
                        inj_total = inj_split_sum
                daily_breakdown = {
                    'consumption_kwh': cons_total,
                    'injection_kwh': inj_total,
                    'consumption_offpeak_kwh': cons_offpeak,
                    'consumption_peak_kwh': cons_peak,
                    'injection_offpeak_kwh': inj_offpeak,
                    'injection_peak_kwh': inj_peak,
                }
            conn_daily.close()
        except Exception as e:
            print(f"Daily breakdown query failed: {e}")

    if daily_breakdown is None:
        daily_breakdown = {}
    if solar_yield_kwh is not None:
        daily_breakdown['solar_yield_kwh'] = float(solar_yield_kwh)

    payload = {
        'daily_5min': result,
        'solar_5min': solar_result,
        'battery_5min': battery_result,
        'synced_5min': synced_5min_result,
        'all_days_avg_5min': profile_result,
        'all_days_avg_15min': profile_result,
        'aggregate_day_count': aggregate_day_count,
        'aggregate_window_days': AVG_PROFILE_WINDOW_DAYS,
        'daily_breakdown': daily_breakdown,
    }

    with _minute_data_cache_lock:
        _minute_data_cache[cache_key] = {
            'expires': now_ts + cache_ttl_seconds,
            'payload': payload,
        }
        if len(_minute_data_cache) > 12:
            stale_keys = [
                key for key, entry in _minute_data_cache.items()
                if now_ts >= entry.get('expires', 0.0)
            ]
            for key in stale_keys:
                _minute_data_cache.pop(key, None)

    return _json_nocache(payload)

@app.route("/network_info")
def get_network_info():
    try:
        res = requests.get(P1_URL, timeout=P1_HTTP_TIMEOUT)
        d = res.json()

        return jsonify({
            "wifi_ssid": d.get("wifi_ssid", "N/A"),
            "wifi_strength": d.get("wifi_strength", "N/A"),
            "smr_version": d.get("smr_version", "N/A"),
            "meter_model": d.get("meter_model", "N/A"),
            "unique_id": d.get("unique_id", "N/A"),
            "active_tariff": d.get("active_tariff", "N/A"),
            "marstek": _get_marstek_status()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _extract_first_numeric_value(payload, key_candidates):
    """Return the first numeric value found in payload for any candidate key."""
    if not isinstance(payload, dict):
        return None

    for key in key_candidates:
        value = payload.get(key)
        if isinstance(value, (int, float)):
            return float(value)

    for nested_key in ('data', 'result', 'payload', 'battery'):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            nested_value = _extract_first_numeric_value(nested, key_candidates)
            if isinstance(nested_value, (int, float)):
                return float(nested_value)

    return None


def _marstek_window_average(window_values):
    """Return a rounded moving average for a Marstek numeric window."""
    values = [float(value) for value in window_values if isinstance(value, (int, float))]
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _stabilize_marstek_status(result):
    """Apply lightweight smoothing to battery values to reduce jitter."""
    stabilized = dict(result or {})
    if stabilized.get('status') != 'ok':
        return stabilized

    consumption_w = stabilized.get('battery_consumption_w')
    if isinstance(consumption_w, (int, float)):
        _marstek_consumption_window.append(float(consumption_w))

    power_w = stabilized.get('power_w')
    if isinstance(power_w, (int, float)):
        _marstek_power_window.append(float(power_w))

    soc_pct = stabilized.get('soc_pct')
    if isinstance(soc_pct, (int, float)):
        _marstek_soc_window.append(float(soc_pct))

    stable_consumption = _marstek_window_average(_marstek_consumption_window)
    stable_power = _marstek_window_average(_marstek_power_window)
    stable_soc = _marstek_window_average(_marstek_soc_window)

    if stable_consumption is not None:
        stabilized['battery_consumption_w'] = stable_consumption
    if stable_power is not None:
        stabilized['power_w'] = stable_power
    if stable_soc is not None:
        stabilized['soc_pct'] = stable_soc

    return stabilized


def _cache_marstek_runtime_point(timestamp_value, power_w=None, consumption_w=None, soc_pct=None):
    """Cache Marstek runtime state and append point for live-window charts."""
    global _marstek_runtime_state, _marstek_runtime_series

    try:
        point_ts = float(timestamp_value)
    except Exception:
        point_ts = time.time()

    _marstek_runtime_state = {
        'timestamp': point_ts,
        'power_w': float(power_w) if isinstance(power_w, (int, float)) else None,
        'consumption_w': float(consumption_w) if isinstance(consumption_w, (int, float)) else None,
        'soc_pct': float(soc_pct) if isinstance(soc_pct, (int, float)) else None,
    }

    _marstek_runtime_series.append({
        'timestamp': point_ts,
        'power': _marstek_runtime_state['consumption_w'],
    })

    cutoff = time.time() - max(600.0, P1_RUNTIME_WINDOW_SECONDS)
    _marstek_runtime_series = [
        point for point in _marstek_runtime_series
        if point.get('timestamp') is not None and float(point['timestamp']) >= cutoff
    ]


def _get_marstek_runtime_points(start_timestamp):
    """Return Marstek runtime points from the requested start timestamp."""
    return [
        {
            'timestamp': float(point['timestamp']),
            'power': float(point['power']) if point.get('power') is not None else None,
        }
        for point in _marstek_runtime_series
        if point.get('timestamp') is not None and float(point['timestamp']) >= float(start_timestamp)
    ]


def _read_marstek_udp_status_builtin(device_ip, port, timeout_seconds):
    """Read Marstek status via UDP JSON-RPC without external dependencies."""
    request_payload = {
        'id': int(time.time()),
        'method': 'ES.GetMode',
        'params': {'id': 0},
    }
    request_bytes = json.dumps(request_payload).encode('utf-8')

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(float(timeout_seconds))
        sock.sendto(request_bytes, (str(device_ip), int(port)))
        response_bytes, _ = sock.recvfrom(65535)
    finally:
        sock.close()

    response_text = response_bytes.decode('utf-8', errors='replace').strip()
    if not response_text:
        raise RuntimeError('empty UDP response')
    return json.loads(response_text)


def _get_marstek_status(force_refresh=False):
    """Fetch Marstek battery status from configured URL or auto-discovered host paths."""
    now_ts = time.time()
    if not force_refresh and now_ts < float(_marstek_status_cache.get('expires', 0.0) or 0.0):
        cached = _marstek_status_cache.get('data')
        if isinstance(cached, dict):
            return dict(cached)

    endpoint_candidates = []
    if MARSTEK_STATUS_URL:
        endpoint_candidates.append(MARSTEK_STATUS_URL)
    elif MARSTEK_IP:
        ip = MARSTEK_IP
        endpoint_candidates.extend([
            f'http://{ip}/api/v1/status',
            f'http://{ip}/api/status',
            f'http://{ip}/status',
            f'http://{ip}/api/battery/status',
            f'http://{ip}/api/device/status',
            f'http://{ip}/',
        ])

    if not endpoint_candidates and not MARSTEK_IP:
        result = {
            'enabled': False,
            'source': None,
            'status': 'not-configured',
            'message': 'Set MARSTEK_IP or MARSTEK_STATUS_URL to enable battery data',
            'timestamp': datetime.fromtimestamp(now_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'soc_pct': None,
            'power_w': None,
            'battery_consumption_w': None,
            'capacity_kwh': None,
        }
        _marstek_status_cache['expires'] = now_ts + MARSTEK_CACHE_TTL_SECONDS
        _marstek_status_cache['data'] = dict(result)
        return result

    headers = {'Accept': 'application/json'}
    if MARSTEK_API_TOKEN:
        headers['Authorization'] = f'Bearer {MARSTEK_API_TOKEN}'

    last_error = None
    if not _marstek_fetch_lock.acquire(timeout=MARSTEK_FETCH_LOCK_TIMEOUT_SECONDS):
        cached = _marstek_status_cache.get('data')
        if isinstance(cached, dict):
            return dict(cached)
        runtime_ts = _marstek_runtime_state.get('timestamp')
        runtime_soc = _marstek_runtime_state.get('soc_pct')
        runtime_power = _marstek_runtime_state.get('power_w')
        runtime_consumption = _marstek_runtime_state.get('consumption_w')
        if isinstance(runtime_ts, (int, float)):
            return {
                'enabled': True,
                'source': endpoint_candidates[0] if endpoint_candidates else None,
                'status': 'stale',
                'message': 'Marstek fetch lock timeout; using last runtime sample',
                'timestamp': datetime.fromtimestamp(float(runtime_ts), timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'soc_pct': runtime_soc if isinstance(runtime_soc, (int, float)) else None,
                'power_w': runtime_power if isinstance(runtime_power, (int, float)) else None,
                'battery_consumption_w': runtime_consumption if isinstance(runtime_consumption, (int, float)) else None,
                'capacity_kwh': None,
            }
        return {
            'enabled': True,
            'source': endpoint_candidates[0],
            'status': 'error',
            'message': 'Marstek fetch lock timeout',
            'timestamp': datetime.fromtimestamp(now_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'soc_pct': None,
            'power_w': None,
            'battery_consumption_w': None,
            'capacity_kwh': None,
        }

    try:
        # Preferred path: direct UDP status read.
        if MARSTEK_IP:
            try:
                udp_payload = _read_marstek_udp_status_builtin(MARSTEK_IP, MARSTEK_UDP_PORT, MARSTEK_UDP_TIMEOUT_SECONDS)
                soc_pct = _extract_first_numeric_value(
                    udp_payload,
                    ('battery_soc', 'bat_soc', 'soc', 'soc_pct', 'state_of_charge', 'stateOfCharge')
                )
                power_w = _extract_first_numeric_value(
                    udp_payload,
                    ('battery_power', 'ongrid_power', 'offgrid_power', 'power_w', 'power', 'active_power')
                )
                discharge_w = _extract_first_numeric_value(
                    udp_payload,
                    (
                        'battery_consumption_w',
                        'consumption_w',
                        'battery_discharge_w',
                        'discharge_power',
                        'ongrid_power',
                    )
                )
                if discharge_w is None and isinstance(power_w, (int, float)):
                    # Preserve sign so charging (negative) is visible in live graph.
                    discharge_w = float(power_w)
                capacity_kwh = _extract_first_numeric_value(
                    udp_payload,
                    ('capacity_kwh', 'battery_capacity_kwh', 'capacity')
                )

                result = {
                    'enabled': True,
                    'source': f'udp://{MARSTEK_IP}:{MARSTEK_UDP_PORT} (builtin)',
                    'status': 'ok',
                    'message': None,
                    'timestamp': datetime.fromtimestamp(now_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    'soc_pct': round(float(soc_pct), 2) if isinstance(soc_pct, (int, float)) else None,
                    'power_w': round(float(power_w), 2) if isinstance(power_w, (int, float)) else None,
                    'battery_consumption_w': round(float(discharge_w), 2) if isinstance(discharge_w, (int, float)) else None,
                    'capacity_kwh': round(float(capacity_kwh), 3) if isinstance(capacity_kwh, (int, float)) else None,
                }
                result = _stabilize_marstek_status(result)
                _marstek_status_cache['expires'] = now_ts + MARSTEK_CACHE_TTL_SECONDS
                _marstek_status_cache['data'] = dict(result)
                _cache_marstek_runtime_point(
                    now_ts,
                    power_w=result.get('power_w'),
                    consumption_w=result.get('battery_consumption_w'),
                    soc_pct=result.get('soc_pct'),
                )
                return result
            except Exception as e:
                last_error = f'udp://{MARSTEK_IP}:{MARSTEK_UDP_PORT}: {e}'

        # HTTP status endpoint(s).
        for endpoint in endpoint_candidates:
            try:
                response = MARSTEK_HTTP.get(endpoint, headers=headers, timeout=MARSTEK_TIMEOUT_SECONDS)
                response.raise_for_status()
                payload = response.json()

                soc_pct = _extract_first_numeric_value(
                    payload,
                    ('soc', 'soc_pct', 'state_of_charge', 'stateOfCharge', 'battery_soc', 'batterySoc')
                )
                power_w = _extract_first_numeric_value(
                    payload,
                    ('power_w', 'power', 'battery_power', 'batteryPower', 'p_batt', 'active_power')
                )
                discharge_w = _extract_first_numeric_value(
                    payload,
                    (
                        'battery_consumption_w',
                        'consumption_w',
                        'battery_discharge_w',
                        'discharge_power',
                        'dischargePower',
                        'output_power',
                    )
                )
                capacity_kwh = _extract_first_numeric_value(
                    payload,
                    ('capacity_kwh', 'capacityKwh', 'battery_capacity_kwh', 'batteryCapacityKwh', 'capacity')
                )
                if discharge_w is None and isinstance(power_w, (int, float)):
                    # Preserve sign so charging (negative) is visible in live graph.
                    discharge_w = float(power_w)

                result = {
                    'enabled': True,
                    'source': endpoint,
                    'status': 'ok',
                    'message': None,
                    'timestamp': datetime.fromtimestamp(now_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    'soc_pct': round(float(soc_pct), 2) if isinstance(soc_pct, (int, float)) else None,
                    'power_w': round(float(power_w), 2) if isinstance(power_w, (int, float)) else None,
                    'battery_consumption_w': round(float(discharge_w), 2) if isinstance(discharge_w, (int, float)) else None,
                    'capacity_kwh': round(float(capacity_kwh), 3) if isinstance(capacity_kwh, (int, float)) else None,
                }
                result = _stabilize_marstek_status(result)
                _marstek_status_cache['expires'] = now_ts + MARSTEK_CACHE_TTL_SECONDS
                _marstek_status_cache['data'] = dict(result)
                _cache_marstek_runtime_point(
                    now_ts,
                    power_w=result.get('power_w'),
                    consumption_w=result.get('battery_consumption_w'),
                    soc_pct=result.get('soc_pct'),
                )
                return result
            except Exception as e:
                last_error = f'{endpoint}: {e}'

        # Fetch failed: prefer last known good cache/runtime values to avoid
        # transient blanks in the live dashboard.
        cached = _marstek_status_cache.get('data')
        runtime_ts = _marstek_runtime_state.get('timestamp')
        runtime_soc = _marstek_runtime_state.get('soc_pct')
        runtime_power = _marstek_runtime_state.get('power_w')
        runtime_consumption = _marstek_runtime_state.get('consumption_w')

        if isinstance(cached, dict) and any(
            isinstance(cached.get(field), (int, float))
            for field in ('soc_pct', 'power_w', 'battery_consumption_w')
        ):
            stale = dict(cached)
            stale['status'] = 'stale'
            stale['message'] = str(last_error) if last_error else 'Unable to read Marstek status'
            _marstek_status_cache['expires'] = now_ts + min(1.0, MARSTEK_CACHE_TTL_SECONDS)
            _marstek_status_cache['data'] = dict(stale)
            return stale

        if isinstance(runtime_ts, (int, float)):
            stale = {
                'enabled': True,
                'source': endpoint_candidates[0] if endpoint_candidates else None,
                'status': 'stale',
                'message': str(last_error) if last_error else 'Unable to read Marstek status',
                'timestamp': datetime.fromtimestamp(float(runtime_ts), timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'soc_pct': runtime_soc if isinstance(runtime_soc, (int, float)) else None,
                'power_w': runtime_power if isinstance(runtime_power, (int, float)) else None,
                'battery_consumption_w': runtime_consumption if isinstance(runtime_consumption, (int, float)) else None,
                'capacity_kwh': None,
            }
            _marstek_status_cache['expires'] = now_ts + min(1.0, MARSTEK_CACHE_TTL_SECONDS)
            _marstek_status_cache['data'] = dict(stale)
            return stale

        result = {
            'enabled': True,
            'source': endpoint_candidates[0] if endpoint_candidates else None,
            'status': 'error',
            'message': str(last_error) if last_error else 'Unable to read Marstek status',
            'timestamp': datetime.fromtimestamp(now_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'soc_pct': None,
            'power_w': None,
            'battery_consumption_w': None,
            'capacity_kwh': None,
        }
        _marstek_status_cache['expires'] = now_ts + MARSTEK_CACHE_TTL_SECONDS
        _marstek_status_cache['data'] = dict(result)
        return result
    finally:
        _marstek_fetch_lock.release()


@app.route('/marstek/status')
def marstek_status():
    """Return current Marstek battery status (if configured)."""
    return _json_nocache(_get_marstek_status())

def _json_nocache(payload, status=200):
    response = make_response(jsonify(payload), status)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


def _fetch_direct_live_power_sample():
    """Fetch the current P1 reading directly when the stored live sample is stale."""
    try:
        res = P1_HTTP.get(P1_URL, timeout=P1_HTTP_TIMEOUT)
        res.raise_for_status()
        d = res.json()
        sample = {
            'timestamp': time.time(),
            'live_power': float(d.get('active_power_w')) if d.get('active_power_w') is not None else None,
            'raw_reading': {
                'import_kwh': d.get('total_power_import_kwh'),
                'export_kwh': d.get('total_power_export_kwh'),
            }
        }
        _cache_p1_runtime_point(sample['timestamp'], sample['live_power'], sample['raw_reading'])
        return sample
    except Exception as e:
        print(f"Direct P1 live fetch failed: {e}")
        return None


def _merge_live_window_points(points, window_start_ts, window_end_ts, step_seconds=2.0):
    """Return recent live points on a stable 2-second grid and preserve the latest pre-window value as carry-in."""
    if not points:
        return []

    merged = {}
    carry_in_point = None
    for point in points:
        raw_ts = point.get('timestamp')
        try:
            point_ts = _normalize_live_window_timestamp(raw_ts, step_seconds=step_seconds)
        except Exception:
            point_ts = None
        if point_ts is None:
            continue

        point_value = point.get('power')
        normalized_point = {
            'timestamp': float(point_ts),
            'power': float(point_value) if point_value is not None else None,
        }

        if point_ts < float(window_start_ts):
            if carry_in_point is None or point_ts > carry_in_point['timestamp']:
                carry_in_point = normalized_point
            continue

        if point_ts > float(window_end_ts) + 0.001:
            continue

        existing = merged.get(point_ts)
        if existing is None or (existing.get('power') is None and normalized_point.get('power') is not None):
            merged[point_ts] = normalized_point

    if carry_in_point is not None:
        merged[carry_in_point['timestamp']] = carry_in_point

    return [merged[key] for key in sorted(merged.keys())]


def _densify_live_series(points, window_start_ts, window_end_ts, step_seconds=2, max_hold_seconds=None):
    """Fill a rolling live window with carry-forward values so charts stay continuous."""
    if not points:
        return []

    step_seconds = max(1.0, float(step_seconds or 2))
    max_hold_seconds = None if max_hold_seconds is None else max(0.0, float(max_hold_seconds))
    sorted_points = []
    carry_value = None
    carry_timestamp = None

    for point in sorted(points, key=lambda item: float(item.get('timestamp', 0) or 0)):
        raw_ts = point.get('timestamp')
        try:
            point_ts = float(raw_ts)
        except (TypeError, ValueError):
            continue

        point_value = point.get('power')
        if point_ts < window_start_ts:
            if point_value is not None:
                carry_value = point_value
                carry_timestamp = point_ts
            continue

        if point_ts > window_end_ts:
            continue

        if point_value is not None:
            carry_value = point_value
            carry_timestamp = point_ts
        sorted_points.append({'timestamp': point_ts, 'power': point_value})

    if not sorted_points and carry_value is None:
        return []

    if not sorted_points and carry_value is not None:
        if max_hold_seconds is not None and carry_timestamp is not None and (float(window_start_ts) - float(carry_timestamp)) > max_hold_seconds:
            return []
        return [
            {'timestamp': float(window_start_ts), 'power': carry_value},
            {'timestamp': float(window_end_ts), 'power': carry_value},
        ]

    dense_points = []
    current_ts = float(window_start_ts)
    current_value = carry_value
    current_value_timestamp = carry_timestamp
    point_index = 0

    while current_ts <= float(window_end_ts) + 0.001:
        while point_index < len(sorted_points) and sorted_points[point_index]['timestamp'] <= current_ts + (step_seconds / 2.0):
            if sorted_points[point_index]['power'] is not None:
                current_value = sorted_points[point_index]['power']
                current_value_timestamp = sorted_points[point_index]['timestamp']
            point_index += 1

        output_value = current_value
        if max_hold_seconds is not None and current_value_timestamp is not None:
            if (current_ts - float(current_value_timestamp)) > max_hold_seconds:
                output_value = None

        dense_points.append({
            'timestamp': current_ts,
            'power': output_value,
        })
        current_ts += step_seconds

    return dense_points


def _get_recent_nonzero_solar_point(lookback_seconds=120):
    """Return the most recent non-zero solar reading from realtime history."""
    conn = None
    try:
        conn = open_solar_history_connection(kind='raw')
        if conn is None:
            return None

        cutoff = datetime.fromtimestamp(time.time() - max(10, int(lookback_seconds or 120)), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        cursor = conn.cursor()
        cursor.execute(
            '''SELECT CAST(strftime('%s', timestamp) AS INTEGER) AS point_ts,
                      power_w
               FROM solar_realtime
               WHERE timestamp >= ?
                 AND power_w IS NOT NULL
                 AND ABS(power_w) >= 1
               ORDER BY timestamp DESC
               LIMIT 1''',
            (cutoff,),
        )
        row = cursor.fetchone()
        if row and row['point_ts'] is not None:
            return {
                'timestamp': float(row['point_ts']),
                'power_w': float(row['power_w']) if row['power_w'] is not None else None,
            }
    except Exception as e:
        print(f"Recent solar DB read failed: {e}")
    finally:
        if conn:
            conn.close()

    return None


def get_live_battery_points(start_timestamp, max_point_age_seconds=LIVE_DENSIFY_BATTERY_HOLD_SECONDS):
    """Return recent battery consumption points from persisted history for the live chart."""
    conn = None
    try:
        conn = open_battery_history_connection(kind='raw')
        if conn is None:
            return []

        cutoff_ts = max(
            0.0,
            float(start_timestamp) - max(10.0, float(max_point_age_seconds or LIVE_DENSIFY_BATTERY_HOLD_SECONDS)),
        )
        cursor = conn.cursor()
        cursor.execute(
            '''SELECT timestamp,
                      consumption_w,
                      power_w
               FROM battery_raw_data
               WHERE timestamp >= ?
               ORDER BY timestamp ASC''',
            (float(cutoff_ts),),
        )
        rows = cursor.fetchall()

        if not rows:
            cutoff_text = datetime.fromtimestamp(cutoff_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                '''SELECT CAST(strftime('%s', timestamp) AS REAL) AS point_ts,
                          consumption_w,
                          power_w
                   FROM battery_realtime
                   WHERE timestamp >= ?
                   ORDER BY timestamp ASC''',
                (cutoff_text,),
            )
            rows = cursor.fetchall()

        points = []
        for row in rows:
            point_ts = row[0]
            if point_ts is None:
                continue

            power_value = row[1] if row[1] is not None else row[2]
            points.append({
                'timestamp': float(point_ts),
                'power': float(power_value) if power_value is not None else None,
            })

        return points
    except Exception as e:
        print(f"Battery live history query failed: {e}")
        return []
    finally:
        if conn:
            conn.close()


def _get_latest_live_power_sample():
    runtime_sample = dict(_p1_runtime_state)
    if _is_fresh_sample_timestamp(runtime_sample.get('timestamp'), LIVE_SAMPLE_STALE_AFTER_SECONDS):
        return runtime_sample

    freshest_sample = None

    for db_path in (DB_FILE_RAW, DB_FILE_BACKUP):
        conn = None
        try:
            conn = get_db_connection(db_path)
            c = conn.cursor()
            c.execute('SELECT timestamp, power, import_kwh, export_kwh FROM energy_data ORDER BY timestamp DESC LIMIT 1')
            row = c.fetchone()
            if row:
                sample = {
                    'timestamp': float(row[0]) if row[0] is not None else None,
                    'live_power': float(row[1]) if row[1] is not None else None,
                    'raw_reading': {
                        'import_kwh': row[2],
                        'export_kwh': row[3],
                    }
                }
                freshest_sample = sample
                _cache_p1_runtime_point(sample['timestamp'], sample['live_power'], sample['raw_reading'])
                if sample['timestamp'] is not None and (time.time() - sample['timestamp']) <= LIVE_SAMPLE_STALE_AFTER_SECONDS:
                    return sample
        except Exception as e:
            print(f"Live power DB read failed for {db_path}: {e}")
        finally:
            if conn:
                conn.close()

    return _fetch_direct_live_power_sample() or freshest_sample


@app.route('/live_power')
def live_power():
    """Fetch the latest live power value and raw reading from the database."""
    try:
        latest_sample = _get_latest_live_power_sample()
        solar_point = get_latest_solar_point(allow_direct_refresh=True)
        marstek_status = _get_marstek_status(force_refresh=False)
        runtime_battery_power = _marstek_runtime_state.get('power_w') if isinstance(_marstek_runtime_state.get('power_w'), (int, float)) else None
        runtime_battery_consumption = _marstek_runtime_state.get('consumption_w') if isinstance(_marstek_runtime_state.get('consumption_w'), (int, float)) else None
        runtime_battery_soc = _marstek_runtime_state.get('soc_pct') if isinstance(_marstek_runtime_state.get('soc_pct'), (int, float)) else None

        battery_power_value = marstek_status.get('power_w') if isinstance(marstek_status.get('power_w'), (int, float)) else runtime_battery_power
        battery_consumption_value = (
            marstek_status.get('battery_consumption_w')
            if isinstance(marstek_status.get('battery_consumption_w'), (int, float))
            else runtime_battery_consumption
        )
        battery_soc_value = marstek_status.get('soc_pct') if isinstance(marstek_status.get('soc_pct'), (int, float)) else runtime_battery_soc
        battery_timestamp = _marstek_runtime_state.get('timestamp')
        if battery_timestamp is None:
            status_ts = marstek_status.get('timestamp') if isinstance(marstek_status, dict) else None
            if isinstance(status_ts, str):
                try:
                    battery_timestamp = datetime.strptime(status_ts, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp()
                except Exception:
                    battery_timestamp = None

        if latest_sample:
            latest_sample.update({
                'solar_power': solar_point.get('power_w'),
                'solar_timestamp': solar_point.get('timestamp'),
                'battery_power': battery_power_value,
                'battery_consumption_w': battery_consumption_value,
                'battery_timestamp': battery_timestamp,
                'battery_soc_pct': battery_soc_value,
            })
            _append_live_power_temp_point(latest_sample.get('timestamp'), live_power=latest_sample.get('live_power'))
            _append_live_power_temp_point(solar_point.get('timestamp'), solar_power=solar_point.get('power_w'))
            return _json_nocache(latest_sample)

        return _json_nocache({
            'timestamp': solar_point.get('timestamp'),
            'live_power': None,
            'solar_power': solar_point.get('power_w'),
            'solar_timestamp': solar_point.get('timestamp'),
            'battery_power': battery_power_value,
            'battery_consumption_w': battery_consumption_value,
            'battery_timestamp': battery_timestamp,
            'battery_soc_pct': battery_soc_value,
            'raw_reading': None,
        })
    except Exception as e:
        print(f"Error fetching live power: {e}")
        return _json_nocache({'error': 'Failed to fetch live power'}, status=500)


@app.route('/live_power_window')
def live_power_window():
    """Return recent live power points for the requested rolling window (default 5 minutes)."""
    try:
        seconds_param = request.args.get('seconds', default='300')
        try:
            window_seconds = int(seconds_param)
        except (TypeError, ValueError):
            window_seconds = 300

        # Clamp to a safe range to prevent very heavy queries from the browser.
        window_seconds = max(60, min(window_seconds, 3600))
        end_timestamp = time.time()
        start_timestamp = end_timestamp - window_seconds

        rows = []
        for db_path in (DB_FILE_RAW, DB_FILE_BACKUP):
            conn = None
            try:
                conn = get_db_connection(db_path)
                c = conn.cursor()
                c.execute(
                    '''SELECT timestamp, power
                       FROM energy_data
                       WHERE timestamp >= ?
                       ORDER BY timestamp ASC''',
                    (start_timestamp,),
                )
                rows = c.fetchall()
                if rows:
                    break
            except Exception as db_error:
                print(f"Live power window DB read failed for {db_path}: {db_error}")
            finally:
                if conn:
                    conn.close()

        db_points = [
            {
                'timestamp': float(row[0]) if row[0] is not None else None,
                'power': float(row[1]) if row[1] is not None else None,
            }
            for row in rows
            if row[0] is not None
        ]

        runtime_points = _get_p1_runtime_points(start_timestamp)
        latest_sample = _get_latest_live_power_sample()
        latest_points = []
        if latest_sample and latest_sample.get('timestamp') is not None:
            latest_points.append({
                'timestamp': float(latest_sample['timestamp']),
                'power': latest_sample.get('live_power'),
            })

        live_step_seconds = 2.0
        points = _merge_live_window_points(
            db_points + runtime_points + latest_points,
            start_timestamp,
            end_timestamp,
            step_seconds=live_step_seconds,
        )
        points = _densify_live_series(
            points,
            start_timestamp,
            end_timestamp,
            step_seconds=live_step_seconds,
            max_hold_seconds=LIVE_DENSIFY_POWER_HOLD_SECONDS,
        )

        solar_points = get_live_solar_points(
            start_timestamp,
            max_point_age_seconds=SOLAR_POINT_MAX_AGE_SECONDS,
            allow_direct_refresh=True,
        )
        solar_points = _merge_live_window_points(
            solar_points,
            start_timestamp,
            end_timestamp,
            step_seconds=live_step_seconds,
        )
        solar_points = _densify_live_series(
            solar_points,
            start_timestamp,
            end_timestamp,
            step_seconds=live_step_seconds,
            max_hold_seconds=LIVE_DENSIFY_SOLAR_HOLD_SECONDS,
        )

        battery_history_points = get_live_battery_points(
            start_timestamp,
            max_point_age_seconds=LIVE_DENSIFY_BATTERY_HOLD_SECONDS,
        )
        marstek_status = _get_marstek_status(force_refresh=False)
        marstek_runtime_points = _get_marstek_runtime_points(start_timestamp)
        marstek_latest_points = []
        runtime_ts = _marstek_runtime_state.get('timestamp')
        if runtime_ts is not None:
            runtime_battery_power = _marstek_runtime_state.get('consumption_w')
            if not isinstance(runtime_battery_power, (int, float)):
                runtime_battery_power = _marstek_runtime_state.get('power_w')
            marstek_latest_points.append({
                'timestamp': float(runtime_ts),
                'power': runtime_battery_power,
            })
        elif isinstance(marstek_status.get('battery_consumption_w'), (int, float)):
            marstek_latest_points.append({
                'timestamp': float(end_timestamp),
                'power': marstek_status.get('battery_consumption_w'),
            })
        elif isinstance(marstek_status.get('power_w'), (int, float)):
            marstek_latest_points.append({
                'timestamp': float(end_timestamp),
                'power': marstek_status.get('power_w'),
            })

        battery_points = _merge_live_window_points(
            battery_history_points + marstek_runtime_points + marstek_latest_points,
            start_timestamp,
            end_timestamp,
            step_seconds=live_step_seconds,
        )
        battery_points = _densify_live_series(
            battery_points,
            start_timestamp,
            end_timestamp,
            step_seconds=live_step_seconds,
            max_hold_seconds=LIVE_DENSIFY_BATTERY_HOLD_SECONDS,
        )

        _write_live_power_temp_window(points, solar_points, start_timestamp, end_timestamp)

        return _json_nocache({
            'window_seconds': window_seconds,
            'points': points,
            'solar_points': solar_points,
            'battery_points': battery_points,
            'battery_status': marstek_status,
        })
    except Exception as e:
        print(f"Error fetching live power window: {e}")
        return _json_nocache({'error': 'Failed to fetch live power window'}, status=500)

@app.route('/cache_recovery')
def cache_recovery():
    """Serve the browser cache recovery tool"""
    return render_template('cache_recovery.html')

@app.route('/recover_cached_data', methods=['POST'])
def recover_cached_data():
    """Recover data from browser cache and insert into database"""
    try:
        payload = request.get_json(silent=True) or {}
        local_storage = payload.get('localStorage', {})
        
        recovered_count = 0
        errors = []
        
        # Process chart data from localStorage
        for key, value in local_storage.items():
            try:
                # Skip non-data keys
                if not key.startswith('chart'):
                    continue
                
                # Parse JSON data if it's a string
                if isinstance(value, str):
                    try:
                        data = json.loads(value)
                    except json.JSONDecodeError:
                        continue
                else:
                    data = value
                
                # Data might be in various formats - try to insert
                if isinstance(data, list):
                    # Array of measurements
                    for item in data:
                        if isinstance(item, dict):
                            normalized = normalize_measurement_record(item)
                            if normalized:
                                insert_raw_entry(normalized)
                                recovered_count += 1
                elif isinstance(data, dict):
                    # Single measurement
                    normalized = normalize_measurement_record(data)
                    if normalized:
                        insert_raw_entry(normalized)
                        recovered_count += 1
            except Exception as e:
                errors.append(f"Error processing {key}: {str(e)}")
        
        # Rebuild 5-minute averages if we recovered anything
        if recovered_count > 0:
            conn = get_db_connection(DB_FILE_RAW)
            c = conn.cursor()
            c.execute('SELECT MIN(timestamp), MAX(timestamp) FROM energy_data')
            min_ts, max_ts = c.fetchone()
            conn.close()
            
            if min_ts and max_ts:
                start_ts = (int(min_ts) // 300) * 300
                end_ts = ((int(max_ts) // 300) + 1) * 300
                rebuilt = rebuild_5min_from_raw_range(start_ts, end_ts)
                rebuild_daily_consumption_from_raw_range(start_ts, end_ts)
                
                # Sync backup
                sync_backup_db()
        
        return jsonify({
            'status': 'success',
            'message': f'Successfully recovered {recovered_count} data points from browser cache',
            'recovered': recovered_count,
            'errors': errors if errors else None
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'message': 'Failed to recover cache data'
        }), 500

# ============================================================================
# SUN2000 SOLAR INVERTER ENDPOINTS
# ============================================================================

@app.route("/sun2000/current")
def get_sun2000_current():
    """Get current real-time power output from Sun2000 inverter"""
    if not SUN2000_AVAILABLE:
        return jsonify({'error': 'Sun2000 module not available'}), 503

    try:
        latest = fetch_and_store_current_power(force=True, persist=None)
        if latest:
            latest_ts = latest.get('timestamp')
            if not _is_fresh_sample_timestamp(latest_ts, SOLAR_SAMPLE_STALE_AFTER_SECONDS):
                latest = None

        if latest:
            return jsonify({
                'timestamp': latest.get('timestamp'),
                'device_id': latest.get('device_id', SUN2000_DEVICE_ID),
                'current_power_w': latest.get('current_power_w'),
                'status': latest.get('status', 'Unknown'),
            })

        return jsonify({
            'timestamp': None,
            'device_id': SUN2000_DEVICE_ID,
            'current_power_w': None,
            'status': 'Unavailable',
        })
    except Exception as e:
        print(f"Error getting Sun2000 current data: {e}")
        return jsonify({'error': str(e)}), 500


@app.route("/sun2000/daily")
def get_sun2000_daily():
    """Daily yield tracking is disabled in live-power-only mode."""
    return jsonify({
        'data': [],
        'count': 0,
        'unit': 'kWh',
        'message': 'Disabled: app runs in live-power-only solar mode',
    })


@app.route("/sun2000/sync", methods=['POST'])
def sync_sun2000_data():
    """Manually trigger sync from Sun2000 inverter"""
    auth_error = require_admin_token()
    if auth_error:
        return auth_error

    if not SUN2000_AVAILABLE:
        return jsonify({'error': 'Sun2000 module not available'}), 503

    try:
        data = fetch_and_store_current_power(force=True, persist=None)
        if data:
            return jsonify({
                'status': 'success',
                'message': 'Data synced from Sun2000',
                'power': data.get('current_power_w'),
                'timestamp': data.get('timestamp'),
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch data from Sun2000'
            }), 500
    except Exception as e:
        print(f"Error syncing Sun2000 data: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route("/sun2000/info")
def get_sun2000_info():
    """Get Sun2000 inverter information and device details"""
    if not SUN2000_AVAILABLE:
        return jsonify({'error': 'Sun2000 module not available'}), 503

    try:
        return jsonify({
            'status': 'success',
            'device_info': {
                'device_id': SUN2000_DEVICE_ID,
                'host': SUN2000_IP,
                'port': SUN2000_PORT,
                'mode': 'live-power-only',
            },
        })
    except Exception as e:
        print(f"Error getting Sun2000 info: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route("/sun2000/health")
def get_sun2000_health():
    """Check Sun2000 connection health and status"""
    if not SUN2000_AVAILABLE:
        return jsonify({
            'available': False,
            'message': 'Sun2000 module not available'
        }), 503

    try:
        data = fetch_and_store_current_power(force=True, persist=None)

        if data and data.get('current_power_w') is not None:
            return jsonify({
                'available': True,
                'status': data.get('status', 'Unknown'),
                'last_update': data.get('timestamp'),
                'message': 'Connected'
            })
        else:
            return jsonify({
                'available': False,
                'message': 'Failed to retrieve data'
            }), 500
    except Exception as e:
        return jsonify({
            'available': False,
            'message': str(e),
            'error': str(e)
        }), 500


if __name__ == "__main__":
    if DEBUG_MODE:
        # Flask's dev server handles one request at a time by default, which
        # is fine for local debugging but serializes every request behind
        # whatever else is running (background collector threads, other open
        # tabs) in production.
        app.run(host=SERVER_HOST, port=SERVER_PORT, debug=True)
    else:
        from waitress import serve
        print(f"✓ Serving with waitress on {SERVER_HOST}:{SERVER_PORT} (threads=8)")
        serve(app, host=SERVER_HOST, port=SERVER_PORT, threads=8)
