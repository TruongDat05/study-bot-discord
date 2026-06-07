from __future__ import annotations

import shutil
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse


class DatabaseService:
    def __init__(self, database_url: str):
        self.database_url = database_url.strip() or 'sqlite:///data/bot.db'
        self.backend = self._detect_backend(self.database_url)
        self.sqlite_path = self._sqlite_path(self.database_url) if self.backend == 'sqlite' else None
        self._memory_uri = (
            f'file:discord_bot_{id(self)}?mode=memory&cache=shared'
            if self.sqlite_path is not None and str(self.sqlite_path) == ':memory:'
            else None
        )
        self._memory_keeper = None

    @staticmethod
    def _detect_backend(database_url: str) -> str:
        if database_url.startswith('sqlite'):
            return 'sqlite'
        if database_url.startswith(('postgresql://', 'postgres://')):
            return 'postgresql'
        raise ValueError(f'Unsupported DATABASE_URL: {database_url}')

    @staticmethod
    def _sqlite_path(database_url: str) -> Path:
        parsed = urlparse(database_url)
        raw_path = parsed.path or ''
        if parsed.netloc:
            raw_path = f'{parsed.netloc}{raw_path}'
        if database_url.startswith('sqlite:///'):
            raw_path = database_url[len('sqlite:///'):]
        elif database_url.startswith('sqlite://'):
            raw_path = database_url[len('sqlite://'):]
        if raw_path in ('', ':memory:'):
            return Path(':memory:')
        path = Path(raw_path)
        return path if path.is_absolute() else Path.cwd() / path

    def connect(self):
        if self.backend != 'sqlite':
            raise NotImplementedError(
                'PostgreSQL DATABASE_URL is recognized, but this deployment currently '
                'uses the SQLite driver. Add a PostgreSQL driver-backed adapter here.'
            )
        assert self.sqlite_path is not None
        if self._memory_uri:
            if self._memory_keeper is None:
                self._memory_keeper = sqlite3.connect(self._memory_uri, timeout=30, uri=True)
                self._configure_connection(self._memory_keeper)
            conn = sqlite3.connect(self._memory_uri, timeout=30, uri=True)
        else:
            self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self.sqlite_path), timeout=30)
        self._configure_connection(conn)
        return conn

    @staticmethod
    def _configure_connection(conn):
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode = WAL')

    @contextmanager
    def read_connection(self):
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            conn.execute('BEGIN IMMEDIATE')
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self):
        if self.backend != 'sqlite':
            raise NotImplementedError(
                'PostgreSQL schema support is prepared conceptually, but SQLite is '
                'the active implementation for this bot runtime.'
            )
        conn = self.connect()
        try:
            conn.executescript(SCHEMA_SQL)
            conn.commit()
        finally:
            conn.close()

    def status(self) -> dict:
        if self.backend != 'sqlite':
            return {'backend': self.backend, 'database_url': self.database_url}
        assert self.sqlite_path is not None
        exists = self.sqlite_path.exists() if str(self.sqlite_path) != ':memory:' else True
        size = self.sqlite_path.stat().st_size if exists and str(self.sqlite_path) != ':memory:' else 0
        counts = {}
        with self.read_connection() as conn:
            for table in (
                'guild_configs', 'users', 'study_sessions', 'daily_stats',
                'economy_accounts', 'transactions', 'loans', 'loan_offers',
                'user_notifications', 'class_roles', 'sent_milestones',
                'runtime_sessions', 'runtime_snapshots',
            ):
                counts[table] = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        return {
            'backend': self.backend,
            'path': str(self.sqlite_path),
            'exists': exists,
            'size_bytes': size,
            'counts': counts,
        }

    def backup(self, backup_dir: Path) -> Path:
        if self.backend != 'sqlite':
            raise NotImplementedError('Database backup is implemented for SQLite only.')
        assert self.sqlite_path is not None
        if str(self.sqlite_path) == ':memory:':
            raise ValueError('Cannot backup in-memory SQLite database.')
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        dest = backup_dir / f'bot_db_{ts}.sqlite3'
        shutil.copy2(self.sqlite_path, dest)
        wal = Path(f'{self.sqlite_path}-wal')
        shm = Path(f'{self.sqlite_path}-shm')
        if wal.exists():
            shutil.copy2(wal, backup_dir / f'bot_db_{ts}.sqlite3-wal')
        if shm.exists():
            shutil.copy2(shm, backup_dir / f'bot_db_{ts}.sqlite3-shm')
        return dest


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS guild_configs (
    guild_id INTEGER PRIMARY KEY,
    create_room_channel_id INTEGER,
    temp_room_category_id INTEGER,
    report_channel_id INTEGER,
    admin_role_id INTEGER,
    coins_per_minute INTEGER NOT NULL DEFAULT 10,
    focus_channel_ids_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    display_name TEXT NOT NULL,
    class_level INTEGER NOT NULL DEFAULT 0,
    class_name TEXT,
    streak INTEGER NOT NULL DEFAULT 0,
    longest_streak INTEGER NOT NULL DEFAULT 0,
    notifications_enabled INTEGER NOT NULL DEFAULT 1,
    profile_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, user_id)
);

CREATE TABLE IF NOT EXISTS economy_accounts (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    balance INTEGER NOT NULL DEFAULT 0,
    total_earned INTEGER NOT NULL DEFAULT 0,
    debt INTEGER NOT NULL DEFAULT 0,
    credit_score INTEGER NOT NULL DEFAULT 600,
    coins_acc_secs INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (guild_id, user_id),
    FOREIGN KEY (guild_id, user_id) REFERENCES users(guild_id, user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS daily_stats (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    study_seconds INTEGER NOT NULL DEFAULT 0,
    earned_coins INTEGER NOT NULL DEFAULT 0,
    sessions_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (guild_id, user_id, date),
    FOREIGN KEY (guild_id, user_id) REFERENCES users(guild_id, user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    amount INTEGER NOT NULL DEFAULT 0,
    balance_after INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS loans (
    guild_id INTEGER NOT NULL,
    id TEXT NOT NULL,
    lender_id TEXT,
    borrower_id TEXT,
    principal INTEGER NOT NULL DEFAULT 0,
    interest_percent REAL NOT NULL DEFAULT 0,
    total_due INTEGER NOT NULL DEFAULT 0,
    repaid_amount INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    due_at TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, id)
);

CREATE TABLE IF NOT EXISTS loan_offers (
    guild_id INTEGER NOT NULL,
    id TEXT NOT NULL,
    lender_id TEXT,
    borrower_id TEXT,
    amount INTEGER NOT NULL DEFAULT 0,
    interest_percent REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, id)
);

CREATE TABLE IF NOT EXISTS user_notifications (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, user_id, kind, key)
);

CREATE TABLE IF NOT EXISTS class_roles (
    guild_id INTEGER NOT NULL,
    class_level INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    role_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, class_level)
);

CREATE TABLE IF NOT EXISTS sent_milestones (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    milestone_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, user_id, milestone_key)
);

CREATE TABLE IF NOT EXISTS study_sessions (
    id TEXT PRIMARY KEY,
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    channel_id INTEGER,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    duration_seconds INTEGER NOT NULL DEFAULT 0,
    earned_coins INTEGER NOT NULL DEFAULT 0,
    ended_reason TEXT
);

CREATE TABLE IF NOT EXISTS runtime_sessions (
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    channel_id INTEGER,
    joined_at TEXT NOT NULL,
    last_checkpoint TEXT,
    media_active INTEGER NOT NULL DEFAULT 0,
    milestones_json TEXT NOT NULL DEFAULT '[]',
    session_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, user_id)
);

CREATE TABLE IF NOT EXISTS runtime_snapshots (
    guild_id INTEGER PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""
