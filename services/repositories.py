from __future__ import annotations

import json
import logging
import shutil
import uuid
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Callable

from .database import DatabaseService

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def _json_loads(raw: str | None, default):
    if not raw:
        return deepcopy(default)
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return deepcopy(default)


def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _new_fallback_id(prefix: str) -> str:
    return f'{prefix}_{uuid.uuid4().hex}'


class BotRepository:
    def __init__(self, db: DatabaseService, default_coins_per_minute: int = 10):
        self.db = db
        self.default_coins_per_minute = int(default_coins_per_minute)

    def initialize(self):
        self.db.initialize()

    def db_status(self) -> dict:
        return self.db.status()

    def backup_db(self, backup_dir: Path) -> Path:
        return self.db.backup(backup_dir)

    def is_empty(self) -> bool:
        with self.db.read_connection() as conn:
            users = conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
            configs = conn.execute('SELECT COUNT(*) FROM guild_configs').fetchone()[0]
        return users == 0 and configs == 0

    def get_guild_config(self, guild_id: int) -> dict:
        with self.db.read_connection() as conn:
            return self._get_guild_config(conn, guild_id)

    def _default_guild_config(self, guild_id: int) -> dict:
        return {
            'guild_id': int(guild_id),
            'create_room_channel_id': None,
            'temp_room_category_id': None,
            'report_channel_id': None,
            'admin_role_id': None,
            'coins_per_minute': self.default_coins_per_minute,
            'focus_channel_ids': [],
        }

    def _guild_config_from_row(self, row) -> dict:
        if not row:
            return {}
        return {
            'guild_id': int(row['guild_id']),
            'create_room_channel_id': row['create_room_channel_id'],
            'temp_room_category_id': row['temp_room_category_id'],
            'report_channel_id': row['report_channel_id'],
            'admin_role_id': row['admin_role_id'],
            'coins_per_minute': row['coins_per_minute'],
            'focus_channel_ids': _json_loads(row['focus_channel_ids_json'], []),
        }

    def _get_guild_config(self, conn, guild_id: int) -> dict:
        row = conn.execute(
            'SELECT * FROM guild_configs WHERE guild_id = ?',
            (int(guild_id),),
        ).fetchone()
        if not row:
            return self._default_guild_config(guild_id)
        return self._guild_config_from_row(row)

    def save_guild_config(self, guild_id: int, config: dict) -> dict:
        with self.db.transaction() as conn:
            current = self._get_guild_config(conn, guild_id)
            current.update({k: v for k, v in config.items() if k != 'guild_id'})
            self._save_guild_config(conn, guild_id, current)
            return current

    def set_guild_config(self, guild_id: int, key: str, value) -> dict:
        with self.db.transaction() as conn:
            config = self._get_guild_config(conn, guild_id)
            config[key] = value
            self._save_guild_config(conn, guild_id, config)
            return config

    def _save_guild_config(self, conn, guild_id: int, config: dict):
        now = _now()
        existing = conn.execute(
            'SELECT created_at FROM guild_configs WHERE guild_id = ?',
            (int(guild_id),),
        ).fetchone()
        conn.execute(
            """
            INSERT OR REPLACE INTO guild_configs (
                guild_id, create_room_channel_id, temp_room_category_id,
                report_channel_id, admin_role_id, coins_per_minute,
                focus_channel_ids_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(guild_id),
                config.get('create_room_channel_id'),
                config.get('temp_room_category_id'),
                config.get('report_channel_id'),
                config.get('admin_role_id'),
                _as_int(config.get('coins_per_minute'), self.default_coins_per_minute),
                _json_dumps(config.get('focus_channel_ids') or []),
                existing['created_at'] if existing else now,
                now,
            ),
        )

    def list_guild_configs(self) -> list[dict]:
        with self.db.read_connection() as conn:
            rows = conn.execute('SELECT * FROM guild_configs ORDER BY guild_id').fetchall()
            return [self._guild_config_from_row(row) for row in rows]

    def load_guild_data(self, guild_id: int) -> dict:
        with self.db.read_connection() as conn:
            return self._load_guild_data(conn, guild_id)

    def _load_guild_data(self, conn, guild_id: int) -> dict:
        rows = conn.execute(
            'SELECT user_id, profile_json FROM users WHERE guild_id = ?',
            (int(guild_id),),
        ).fetchall()
        data = {}
        for row in rows:
            data[str(row['user_id'])] = _json_loads(row['profile_json'], {})
        return data

    def save_guild_data(self, guild_id: int, data: dict):
        with self.db.transaction() as conn:
            self._replace_guild_data(conn, guild_id, data)

    def update_guild_data(
        self,
        guild_id: int,
        mutator: Callable[[dict], object],
        normalize_fn: Callable[[dict], dict] | None = None,
    ) -> tuple[object, dict]:
        with self.db.transaction() as conn:
            data = self._load_guild_data(conn, guild_id)
            if normalize_fn:
                normalize_fn(data)
            result = mutator(data)
            if normalize_fn:
                normalize_fn(data)
            self._replace_guild_data(conn, guild_id, data)
            return result, deepcopy(data)

    def _replace_guild_data(self, conn, guild_id: int, data: dict):
        guild_id = int(guild_id)
        try:
            for table in (
                'sent_milestones', 'user_notifications', 'loan_offers', 'loans',
                'transactions', 'daily_stats', 'economy_accounts', 'users',
            ):
                conn.execute(f'DELETE FROM {table} WHERE guild_id = ?', (guild_id,))

            now = _now()
            used_tx_ids: set[str] = set()
            used_loan_ids: set[str] = set()
            used_offer_ids: set[str] = set()
            for uid, info in (data or {}).items():
                if not isinstance(info, dict):
                    continue
                user_id = _as_int(uid)
                if not user_id:
                    continue
                display_name = str(info.get('name') or f'User {uid}')
                class_level = _as_int(info.get('class', info.get('level', 0)))
                conn.execute(
                    """
                    INSERT INTO users (
                        guild_id, user_id, display_name, class_level, class_name,
                        streak, longest_streak, notifications_enabled, profile_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        guild_id, user_id, display_name, class_level,
                        str(info.get('class_name') or ''),
                        _as_int(info.get('streak', 0)),
                        _as_int(info.get('longest_streak', 0)),
                        1 if info.get('notifications_enabled', True) else 0,
                        _json_dumps(info),
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO economy_accounts (
                        guild_id, user_id, balance, total_earned, debt,
                        credit_score, coins_acc_secs
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        guild_id, user_id,
                        _as_int(info.get('balance', 0)),
                        _as_int(info.get('total_earned', 0)),
                        _as_int(info.get('debt', 0)),
                        _as_int(info.get('credit_score', 600)),
                        _as_int(info.get('coins_acc_secs', 0)),
                    ),
                )
                daily = info.get('daily') if isinstance(info.get('daily'), dict) else {}
                earnings = info.get('daily_earnings') if isinstance(info.get('daily_earnings'), dict) else {}
                for day, seconds in daily.items():
                    conn.execute(
                        """
                        INSERT INTO daily_stats (
                            guild_id, user_id, date, study_seconds, earned_coins, sessions_count
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            guild_id, user_id, str(day), _as_int(seconds),
                            _as_int(earnings.get(day, 0)),
                            0,
                        ),
                    )
                for tx in info.get('transactions', []) or []:
                    if not isinstance(tx, dict):
                        continue
                    tx_id = str(tx.get('id') or '').strip()
                    if not tx_id or tx_id in used_tx_ids:
                        tx_id = _new_fallback_id('tx')
                    used_tx_ids.add(tx_id)
                    conn.execute(
                        """
                        INSERT INTO transactions (
                            id, guild_id, user_id, type, amount, balance_after,
                            description, payload_json, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            tx_id, guild_id, user_id, str(tx.get('type', 'tx')),
                            _as_int(tx.get('amount', 0)),
                            _as_int(tx.get('balance_after', tx.get('balance', 0))),
                            str(tx.get('description', '')),
                            _json_dumps(tx),
                            str(tx.get('ts') or now),
                        ),
                    )
                for loan in info.get('active_loans', []) or []:
                    if not isinstance(loan, dict):
                        continue
                    loan_id = str(loan.get('id') or '').strip()
                    if not loan_id or loan_id in used_loan_ids:
                        loan_id = _new_fallback_id('loan')
                    used_loan_ids.add(loan_id)
                    total_due = _as_int(loan.get('total_due', loan.get('remaining', 0)))
                    remaining = _as_int(loan.get('remaining', total_due))
                    conn.execute(
                        """
                        INSERT INTO loans (
                            guild_id, id, lender_id, borrower_id, principal,
                            interest_percent, total_due, repaid_amount, status,
                            due_at, payload_json, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            guild_id, loan_id, str(loan.get('lender_id', '')),
                            str(loan.get('borrower_id', user_id)),
                            _as_int(loan.get('principal', 0)),
                            float(loan.get('interest_percent', 0) or 0),
                            total_due,
                            max(0, total_due - remaining),
                            str(loan.get('status', 'active')),
                            str(loan.get('due_date') or ''),
                            _json_dumps(loan),
                            str(loan.get('borrowed_at') or now),
                        ),
                    )
                for offer in info.get('loan_offers', []) or []:
                    if not isinstance(offer, dict):
                        continue
                    offer_id = str(offer.get('id') or '').strip()
                    if not offer_id or offer_id in used_offer_ids:
                        offer_id = _new_fallback_id('offer')
                    used_offer_ids.add(offer_id)
                    conn.execute(
                        """
                        INSERT INTO loan_offers (
                            guild_id, id, lender_id, borrower_id, amount,
                            interest_percent, status, payload_json, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            guild_id, offer_id, str(offer.get('lender_id', user_id)),
                            str(offer.get('borrower_id', '')),
                            _as_int(offer.get('amount', 0)),
                            float(offer.get('interest_percent', 0) or 0),
                            str(offer.get('status', 'pending')),
                            _json_dumps(offer),
                            str(offer.get('created_at') or now),
                        ),
                    )
                notification_fields = (
                    'notified_classes', 'notified_study_milestones',
                    'notified_coin_milestones', 'notified_loan_overdue',
                )
                for field in notification_fields:
                    for key in info.get(field, []) or []:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO user_notifications (
                                guild_id, user_id, kind, key, created_at
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (guild_id, user_id, field, str(key), now),
                        )
        except Exception:
            log.error('[DB] Failed to replace guild data for guild_id=%s; transaction will roll back.', guild_id, exc_info=True)
            raise

    def get_class_roles(self, guild_id: int) -> dict[int, int]:
        with self.db.read_connection() as conn:
            rows = conn.execute(
                'SELECT class_level, role_id FROM class_roles WHERE guild_id = ?',
                (int(guild_id),),
            ).fetchall()
            return {int(row['class_level']): int(row['role_id']) for row in rows}

    def save_class_roles(self, guild_id: int, roles: dict[int, tuple[int, str] | int]):
        with self.db.transaction() as conn:
            now = _now()
            for level, value in roles.items():
                if isinstance(value, tuple):
                    role_id, role_name = value
                else:
                    role_id, role_name = value, ''
                existing = conn.execute(
                    'SELECT created_at FROM class_roles WHERE guild_id = ? AND class_level = ?',
                    (int(guild_id), int(level)),
                ).fetchone()
                conn.execute(
                    """
                    INSERT OR REPLACE INTO class_roles (
                        guild_id, class_level, role_id, role_name, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(guild_id), int(level), int(role_id), str(role_name),
                        existing['created_at'] if existing else now,
                        now,
                    ),
                )

    def save_runtime_state(self, guild_id: int, state: dict):
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO runtime_snapshots (guild_id, state_json, updated_at)
                VALUES (?, ?, ?)
                """,
                (int(guild_id), _json_dumps(state), _now()),
            )

    def load_runtime_states(self) -> dict[int, dict]:
        with self.db.read_connection() as conn:
            rows = conn.execute('SELECT guild_id, state_json FROM runtime_snapshots').fetchall()
            return {int(row['guild_id']): _json_loads(row['state_json'], {}) for row in rows}

    def migrate_json_to_db(
        self,
        guild_id: int,
        *,
        study_data_path: Path,
        guild_config_path: Path | None = None,
        runtime_state_path: Path | None = None,
        role_config_path: Path | None = None,
        backup_dir: Path,
        normalize_fn: Callable[[dict], dict] | None = None,
    ) -> dict:
        backup_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        backed_up = []
        for path in (study_data_path, guild_config_path, runtime_state_path, role_config_path):
            if path and path.exists():
                dest = backup_dir / f'pre_db_migration_{path.stem}_{ts}{path.suffix}'
                shutil.copy2(path, dest)
                backed_up.append(str(dest))

        inserted_users = 0
        skipped_users = 0
        if study_data_path.exists():
            with open(study_data_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            if not isinstance(raw_data, dict):
                raw_data = {}
            if normalize_fn:
                normalize_fn(raw_data)
            with self.db.transaction() as conn:
                existing = self._load_guild_data(conn, guild_id)
                for uid, info in raw_data.items():
                    if uid in existing:
                        skipped_users += 1
                        continue
                    existing[str(uid)] = info
                    inserted_users += 1
                if normalize_fn:
                    normalize_fn(existing)
                self._replace_guild_data(conn, guild_id, existing)

        migrated_guild_config = False
        if guild_config_path and guild_config_path.exists():
            with open(guild_config_path, 'r', encoding='utf-8') as f:
                config_raw = json.load(f)
            guild_key = str(guild_id)
            if isinstance(config_raw, dict) and isinstance(config_raw.get('guilds'), dict):
                config = config_raw['guilds'].get(guild_key, {})
            else:
                config = config_raw.get(guild_key, config_raw) if isinstance(config_raw, dict) else {}
            if isinstance(config, dict):
                self.save_guild_config(guild_id, config)
                migrated_guild_config = True

        migrated_roles = 0
        if role_config_path and role_config_path.exists():
            with open(role_config_path, 'r', encoding='utf-8') as f:
                role_raw = json.load(f)
            role_meta = (
                role_raw.get('guilds', {}).get(str(guild_id), {})
                if isinstance(role_raw, dict) else {}
            )
            class_roles = role_meta.get('class_roles', {}) if isinstance(role_meta, dict) else {}
            roles = {
                _as_int(level): _as_int(role_id)
                for level, role_id in class_roles.items()
                if _as_int(level) and _as_int(role_id)
            }
            if roles:
                self.save_class_roles(guild_id, roles)
                migrated_roles = len(roles)

        migrated_runtime = False
        if runtime_state_path and runtime_state_path.exists():
            with open(runtime_state_path, 'r', encoding='utf-8') as f:
                runtime_raw = json.load(f)
            if isinstance(runtime_raw, dict):
                self.save_runtime_state(guild_id, runtime_raw)
                migrated_runtime = True

        return {
            'inserted_users': inserted_users,
            'skipped_users': skipped_users,
            'migrated_guild_config': migrated_guild_config,
            'migrated_roles': migrated_roles,
            'migrated_runtime': migrated_runtime,
            'backups': backed_up,
        }
