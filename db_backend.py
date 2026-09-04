import os
import re
import sqlite3
from urllib.parse import urlsplit, urlunsplit


DATABASE_URL = str(os.getenv('DATABASE_URL', '') or '').strip()
POSTGRES_ENABLED = DATABASE_URL.lower().startswith('postgres')

try:
    if POSTGRES_ENABLED:
        import psycopg
    else:
        psycopg = None
except Exception:
    psycopg = None


SCHEMA_BY_DB_BASENAME = {
    'data_raw.db': 'p1_raw',
    'data_avg.db': 'p1_avg',
    'data_daily.db': 'p1_daily',
    'data_backup.db': 'p1_backup',
    'data_overview.db': 'p1_overview',
    'p1_2sec.db': 'p1_raw',
    'p1_5min_avg.db': 'p1_avg',
    'p1_totals.db': 'p1_daily',
    'solar_history.db': 'p1_solar',
    'solar_2sec.db': 'p1_solar',
    'solar_5min_avg.db': 'p1_solar',
    'solar_totals.db': 'p1_solar',
    'battery_2sec.db': 'p1_battery',
    'battery_5min_avg.db': 'p1_battery',
    'battery_totals.db': 'p1_battery',
    'data.db': 'p1_legacy',
    'p1_data_complete.db': 'p1_legacy',
}

TABLE_PRIMARY_KEYS = {
    'energy_data': ['timestamp'],
    'energy_data_5min': ['timestamp'],
    'daily_consumption': ['date'],
    'solar_manual_data': ['date'],
    'monthly_manual_totals': ['month'],
    'monthly_overview': ['month'],
    'yearly_overview': ['year'],
    'overview_build_info': ['id'],
    'solar_realtime': ['timestamp'],
    'battery_realtime': ['timestamp'],
    'battery_raw_data': ['timestamp'],
    'five_minute_averages': ['bucket_start'],
    'daily_totals': ['date'],
    'monthly_totals': ['month'],
    'yearly_totals': ['year'],
}

TABLE_DEFAULT_COLUMNS = {
    'energy_data_5min': ['timestamp', 'power_avg', 'power_min', 'power_max'],
}

_SIMPLE_TIME_EXPR = r"(?:to_timestamp\([^)]+\)|CAST\([^)]+\)|[A-Za-z_][\w.]*)"


class CompatRow:
    def __init__(self, values, columns):
        self._values = tuple(values)
        self._columns = list(columns or [])
        self._mapping = {name: self._values[idx] for idx, name in enumerate(self._columns)}

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._mapping[key]
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __repr__(self):
        return repr(dict(self._mapping)) if self._mapping else repr(self._values)

    def keys(self):
        return self._mapping.keys()

    def get(self, key, default=None):
        return self._mapping.get(key, default)


def is_postgres_enabled():
    return bool(POSTGRES_ENABLED and psycopg is not None)


def current_db_backend():
    return 'postgresql' if is_postgres_enabled() else 'sqlite'


def masked_database_url():
    if not DATABASE_URL:
        return ''
    try:
        split = urlsplit(DATABASE_URL)
        netloc = split.netloc
        if '@' in netloc:
            creds, _, host_part = netloc.rpartition('@')
            user = creds.split(':', 1)[0] if creds else ''
            safe_creds = f'{user}:***' if user else '***'
            safe_netloc = f'{safe_creds}@{host_part}'
        else:
            safe_netloc = netloc
        return urlunsplit((split.scheme, safe_netloc, split.path, split.query, split.fragment))
    except Exception:
        return 'postgresql://***'


def schema_for_db(db_identifier):
    basename = os.path.basename(str(db_identifier or '')).lower()
    return SCHEMA_BY_DB_BASENAME.get(basename, 'p1_app')


def connect_database(db_identifier, timeout_seconds=30, write=False):
    if not is_postgres_enabled():
        raise RuntimeError('PostgreSQL backend is not enabled')

    schema = schema_for_db(db_identifier)
    try:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=max(1, int(timeout_seconds or 30)))
        return PostgresConnectionWrapper(conn, schema=schema)
    except Exception as exc:
        raise sqlite3.OperationalError(str(exc)) from exc


class PostgresConnectionWrapper:
    def __init__(self, connection, schema):
        self._connection = connection
        self.schema = self._sanitize_schema(schema)
        self.row_factory = None
        self._prepare_schema()

    def _sanitize_schema(self, schema):
        text = str(schema or 'p1_app').strip().lower()
        if not re.fullmatch(r'[a-z_][a-z0-9_]*', text):
            return 'p1_app'
        return text

    def _prepare_schema(self):
        cursor = self._connection.cursor()
        try:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {self.schema}')
            cursor.execute(f'SET search_path TO {self.schema}, public')
            self._connection.commit()
        finally:
            cursor.close()

    def cursor(self):
        return PostgresCursorWrapper(self._connection.cursor(), schema=self.schema)

    def execute(self, query, params=None):
        cursor = self.cursor()
        cursor.execute(query, params)
        return cursor

    def commit(self):
        try:
            self._connection.commit()
        except Exception as exc:
            raise sqlite3.OperationalError(str(exc)) from exc

    def rollback(self):
        try:
            self._connection.rollback()
        except Exception as exc:
            raise sqlite3.OperationalError(str(exc)) from exc

    def close(self):
        try:
            self._connection.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()
        return False


class PostgresCursorWrapper:
    def __init__(self, cursor, schema):
        self._cursor = cursor
        self.schema = schema
        self._mock_rows = None
        self.description = None

    @property
    def rowcount(self):
        return getattr(self._cursor, 'rowcount', -1)

    def close(self):
        try:
            self._cursor.close()
        except Exception:
            pass

    def execute(self, query, params=None):
        query_text = str(query or '')
        special = self._special_sql(query_text, params)
        if self._mock_rows is not None and special is None:
            return self
        if special is not None:
            sql, special_params = special
            return self._execute_real(sql, special_params)

        translated = translate_query(query_text)
        translated_params = _normalize_params(params)
        return self._execute_real(translated, translated_params)

    def executemany(self, query, seq_of_params):
        translated = translate_query(str(query or ''))
        try:
            total = 0
            for params in seq_of_params:
                self._cursor.execute(translated, _normalize_params(params))
                if getattr(self._cursor, 'rowcount', -1) and self._cursor.rowcount > 0:
                    total += self._cursor.rowcount
            self._mock_rows = None
            self.description = getattr(self._cursor, 'description', None)
            return total
        except Exception as exc:
            raise sqlite3.OperationalError(str(exc)) from exc

    def fetchone(self):
        if self._mock_rows is not None:
            if not self._mock_rows:
                return None
            return self._mock_rows.pop(0)

        row = self._cursor.fetchone()
        return self._wrap_row(row)

    def fetchall(self):
        if self._mock_rows is not None:
            rows = self._mock_rows
            self._mock_rows = []
            return rows

        rows = self._cursor.fetchall()
        return [self._wrap_row(row) for row in rows]

    def _wrap_row(self, row):
        if row is None:
            return None
        if isinstance(row, CompatRow):
            return row
        columns = [desc[0] for desc in (self.description or getattr(self._cursor, 'description', None) or [])]
        return CompatRow(row, columns)

    def _execute_real(self, sql, params):
        try:
            self._mock_rows = None
            self._cursor.execute(sql, params)
            self.description = getattr(self._cursor, 'description', None)
            return self
        except Exception as exc:
            raise sqlite3.OperationalError(str(exc)) from exc

    def _special_sql(self, query, params):
        normalized = ' '.join(query.strip().split())
        lower = normalized.lower()

        if lower == 'pragma integrity_check':
            self._mock_rows = [CompatRow(('ok',), ['integrity_check'])]
            self.description = [('integrity_check', None, None, None, None, None, None)]
            return None

        if lower.startswith('pragma wal_checkpoint'):
            self._mock_rows = [CompatRow((0, 0, 0), ['busy', 'log', 'checkpointed'])]
            self.description = [('busy', None, None, None, None, None, None), ('log', None, None, None, None, None, None), ('checkpointed', None, None, None, None, None, None)]
            return None

        if lower.startswith('pragma busy_timeout') or lower.startswith('pragma synchronous') or lower.startswith('pragma temp_store') or lower.startswith('pragma foreign_keys') or lower.startswith('pragma journal_mode'):
            self._mock_rows = []
            self.description = []
            return None

        pragma_table = re.match(r"pragma\s+table_info\(([^)]+)\)", lower, flags=re.IGNORECASE)
        if pragma_table:
            table_name = pragma_table.group(1).strip().strip('"\'')
            sql = """
                SELECT
                    ordinal_position - 1 AS cid,
                    column_name AS name,
                    data_type AS type,
                    CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
                    column_default AS dflt_value,
                    0 AS pk
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                ORDER BY ordinal_position
            """
            return sql, (table_name,)

        if 'sqlite_master' in lower:
            table_name = None
            match = re.search(r"name\s*=\s*'([^']+)'", query, flags=re.IGNORECASE)
            if match:
                table_name = match.group(1)
            elif params:
                seq = _normalize_params(params)
                table_name = seq[0] if seq else None
            sql = """
                SELECT table_name AS name
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = %s
            """
            return sql, (table_name,)

        return ('SELECT 1 WHERE FALSE', None) if lower == 'pragma foreign_keys=on' else ('SELECT 1 WHERE FALSE', None) if lower == 'pragma temp_store=memory' else None


def _normalize_params(params):
    if params is None:
        return None
    if isinstance(params, tuple):
        return params
    if isinstance(params, list):
        return tuple(params)
    return (params,)


def translate_query(query):
    sql = str(query or '')
    sql = _translate_insert_or_replace(sql)
    sql = _translate_insert_or_ignore(sql)
    sql = _translate_datetime_and_strftime(sql)
    sql = sql.replace('?', '%s')
    return sql


def _translate_insert_or_ignore(sql):
    if not re.search(r'INSERT\s+OR\s+IGNORE\s+INTO', sql, flags=re.IGNORECASE):
        return sql
    sql = re.sub(r'INSERT\s+OR\s+IGNORE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)
    if 'ON CONFLICT' not in sql.upper():
        sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
    return sql


def _translate_insert_or_replace(sql):
    if not re.search(r'INSERT\s+OR\s+REPLACE\s+INTO', sql, flags=re.IGNORECASE):
        return sql

    compact = sql.strip().rstrip(';')
    match = re.match(
        r'(?is)INSERT\s+OR\s+REPLACE\s+INTO\s+([a-zA-Z_][\w]*)\s*(\((.*?)\))?\s*VALUES\s*\((.*)\)\s*$',
        compact,
    )
    if not match:
        return re.sub(r'INSERT\s+OR\s+REPLACE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)

    table_name = match.group(1)
    raw_columns = match.group(3)
    values_sql = match.group(4)
    columns = [item.strip() for item in raw_columns.split(',')] if raw_columns else list(TABLE_DEFAULT_COLUMNS.get(table_name, []))
    if not columns:
        return re.sub(r'INSERT\s+OR\s+REPLACE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)

    pk_columns = TABLE_PRIMARY_KEYS.get(table_name)
    if not pk_columns:
        return re.sub(r'INSERT\s+OR\s+REPLACE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)

    update_columns = [column for column in columns if column not in pk_columns]
    if update_columns:
        update_sql = ', '.join(f'{column} = EXCLUDED.{column}' for column in update_columns)
        conflict_sql = f'ON CONFLICT ({", ".join(pk_columns)}) DO UPDATE SET {update_sql}'
    else:
        conflict_sql = f'ON CONFLICT ({", ".join(pk_columns)}) DO NOTHING'

    return f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({values_sql}) {conflict_sql}'


def _translate_datetime_and_strftime(sql):
    translated = sql
    translated = re.sub(
        r"datetime\(\s*([A-Za-z_][\w.]*)\s*,\s*'unixepoch'\s*,\s*'localtime'\s*\)",
        r'to_timestamp(\1)',
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(
        r"datetime\(\s*([A-Za-z_][\w.]*)\s*,\s*'unixepoch'\s*\)",
        r'to_timestamp(\1)',
        translated,
        flags=re.IGNORECASE,
    )

    patterns = [
        (rf"CAST\(strftime\('%s',\s*({_SIMPLE_TIME_EXPR})(?:,\s*'utc')?\)\s+AS\s+INTEGER\)", lambda m: f"CAST(EXTRACT(EPOCH FROM {_coerce_time_expr(m.group(1))}) AS INTEGER)"),
        (rf"strftime\('%Y-%m-%d %H:',\s*({_SIMPLE_TIME_EXPR})\)", lambda m: f"TO_CHAR({_coerce_time_expr(m.group(1))}, 'YYYY-MM-DD HH24:')"),
        (rf"strftime\('%Y-%m-%d %H',\s*({_SIMPLE_TIME_EXPR})\)", lambda m: f"TO_CHAR({_coerce_time_expr(m.group(1))}, 'YYYY-MM-DD HH24')"),
        (rf"strftime\('%Y-%m-%d',\s*({_SIMPLE_TIME_EXPR})\)", lambda m: f"TO_CHAR({_coerce_time_expr(m.group(1))}, 'YYYY-MM-DD')"),
        (rf"strftime\('%Y-%m',\s*({_SIMPLE_TIME_EXPR})\)", lambda m: f"TO_CHAR({_coerce_time_expr(m.group(1))}, 'YYYY-MM')"),
        (rf"strftime\('%H',\s*({_SIMPLE_TIME_EXPR})\)", lambda m: f"TO_CHAR({_coerce_time_expr(m.group(1))}, 'HH24')"),
        (rf"strftime\('%M',\s*({_SIMPLE_TIME_EXPR})\)", lambda m: f"TO_CHAR({_coerce_time_expr(m.group(1))}, 'MI')"),
        (rf"strftime\('%s',\s*({_SIMPLE_TIME_EXPR})(?:,\s*'utc')?\)", lambda m: f"EXTRACT(EPOCH FROM {_coerce_time_expr(m.group(1))})"),
    ]

    for pattern, repl in patterns:
        translated = re.sub(pattern, repl, translated, flags=re.IGNORECASE)

    return translated


def _coerce_time_expr(expr):
    text = str(expr or '').strip()
    lowered = text.lower()
    if lowered.startswith('to_timestamp(') or lowered.startswith('cast(') or lowered == 'now()':
        return text
    return f'CAST({text} AS timestamp)'
