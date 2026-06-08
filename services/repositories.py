from __future__ import annotations

import json
import logging
import shutil
import uuid
from copy import deepcopy
from datetime import datetime, timezone
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

    @staticmethod
    def _table_columns(conn, table: str) -> set[str]:
        return {str(row['name']) for row in conn.execute(f'PRAGMA table_info({table})').fetchall()}

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
            task_stats: dict[tuple[int, str], dict[str, int]] = {}
            if self._table_columns(conn, 'tasks'):
                rows = conn.execute(
                    """
                    SELECT
                        user_id,
                        substr(completed_at, 1, 10) AS date,
                        COUNT(*) AS completed_tasks,
                        COALESCE(SUM(reward_coins), 0) AS task_rewarded_coins
                    FROM tasks
                    WHERE guild_id = ? AND completed = 1 AND completed_at IS NOT NULL
                    GROUP BY user_id, substr(completed_at, 1, 10)
                    """,
                    (guild_id,),
                ).fetchall()
                task_stats = {
                    (int(row['user_id']), str(row['date'])): {
                        'completed_tasks': _as_int(row['completed_tasks'], 0),
                        'task_rewarded_coins': _as_int(row['task_rewarded_coins'], 0),
                    }
                    for row in rows
                    if row['date']
                }

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
                inserted_days: set[str] = set()
                for day, seconds in daily.items():
                    inserted_days.add(str(day))
                    task_day = task_stats.get((user_id, str(day)), {})
                    task_rewarded = _as_int(task_day.get('task_rewarded_coins', 0))
                    conn.execute(
                        """
                        INSERT INTO daily_stats (
                            guild_id, user_id, date, study_seconds, earned_coins,
                            sessions_count, completed_tasks, task_rewarded_coins
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            guild_id, user_id, str(day), _as_int(seconds),
                            _as_int(earnings.get(day, 0)) + task_rewarded,
                            0,
                            _as_int(task_day.get('completed_tasks', 0)),
                            task_rewarded,
                        ),
                    )
                for (task_user_id, day), task_day in task_stats.items():
                    if task_user_id != user_id or day in inserted_days:
                        continue
                    task_rewarded = _as_int(task_day.get('task_rewarded_coins', 0))
                    conn.execute(
                        """
                        INSERT INTO daily_stats (
                            guild_id, user_id, date, study_seconds, earned_coins,
                            sessions_count, completed_tasks, task_rewarded_coins
                        ) VALUES (?, ?, ?, 0, ?, 0, ?, ?)
                        """,
                        (
                            guild_id,
                            user_id,
                            day,
                            task_rewarded,
                            _as_int(task_day.get('completed_tasks', 0)),
                            task_rewarded,
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

    @staticmethod
    def _row_dict(row) -> dict:
        return dict(row) if row else {}

    def _minimal_profile(self, display_name: str) -> dict:
        return {
            'name': str(display_name or 'Unknown'),
            'total': 0,
            'balance': 0,
            'total_earned': 0,
            'debt': 0,
            'credit_score': 600,
            'coins_acc_secs': 0,
            'class': 0,
            'level': 0,
            'class_name': '',
            'streak': 0,
            'longest_streak': 0,
            'daily': {},
            'daily_earnings': {},
            'transactions': [],
            'active_loans': [],
            'loan_offers': [],
            'badges': [],
            'special_flags': [],
            'notifications_enabled': True,
        }

    def _ensure_user_account_conn(
        self,
        conn,
        guild_id: int,
        user_id: int,
        display_name: str | None = None,
    ) -> tuple[dict, dict]:
        guild_id = int(guild_id)
        user_id = int(user_id)
        now = _now()
        row = conn.execute(
            'SELECT profile_json FROM users WHERE guild_id = ? AND user_id = ?',
            (guild_id, user_id),
        ).fetchone()
        if row:
            profile = _json_loads(row['profile_json'], {})
        else:
            profile = self._minimal_profile(display_name or f'User {user_id}')

        if display_name:
            profile['name'] = str(display_name)
        profile.setdefault('balance', 0)
        profile.setdefault('total_earned', 0)
        profile.setdefault('debt', 0)
        profile.setdefault('credit_score', 600)
        profile.setdefault('coins_acc_secs', 0)
        profile.setdefault('class', _as_int(profile.get('level', 0)))
        profile.setdefault('level', _as_int(profile.get('class', 0)))
        profile.setdefault('transactions', [])

        class_level = _as_int(profile.get('class', profile.get('level', 0)))
        conn.execute(
            """
            INSERT INTO users (
                guild_id, user_id, display_name, class_level, class_name,
                streak, longest_streak, notifications_enabled, profile_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET
                display_name = excluded.display_name,
                class_level = excluded.class_level,
                class_name = excluded.class_name,
                streak = excluded.streak,
                longest_streak = excluded.longest_streak,
                notifications_enabled = excluded.notifications_enabled,
                profile_json = excluded.profile_json,
                updated_at = excluded.updated_at
            """,
            (
                guild_id, user_id, str(profile.get('name') or display_name or f'User {user_id}'),
                class_level, str(profile.get('class_name') or ''),
                _as_int(profile.get('streak', 0)),
                _as_int(profile.get('longest_streak', 0)),
                1 if profile.get('notifications_enabled', True) else 0,
                _json_dumps(profile), now, now,
            ),
        )

        account_row = conn.execute(
            'SELECT * FROM economy_accounts WHERE guild_id = ? AND user_id = ?',
            (guild_id, user_id),
        ).fetchone()
        if not account_row:
            conn.execute(
                """
                INSERT INTO economy_accounts (
                    guild_id, user_id, balance, total_earned, debt,
                    credit_score, coins_acc_secs
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    guild_id, user_id,
                    _as_int(profile.get('balance', 0)),
                    _as_int(profile.get('total_earned', 0)),
                    _as_int(profile.get('debt', 0)),
                    _as_int(profile.get('credit_score', 600)),
                    _as_int(profile.get('coins_acc_secs', 0)),
                ),
            )
            account_row = conn.execute(
                'SELECT * FROM economy_accounts WHERE guild_id = ? AND user_id = ?',
                (guild_id, user_id),
            ).fetchone()
        account = self._row_dict(account_row)
        return profile, account

    def _write_profile_conn(self, conn, guild_id: int, user_id: int, profile: dict) -> None:
        now = _now()
        class_level = _as_int(profile.get('class', profile.get('level', 0)))
        conn.execute(
            """
            UPDATE users
            SET display_name = ?, class_level = ?, class_name = ?, streak = ?,
                longest_streak = ?, notifications_enabled = ?, profile_json = ?,
                updated_at = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (
                str(profile.get('name') or f'User {user_id}'),
                class_level,
                str(profile.get('class_name') or ''),
                _as_int(profile.get('streak', 0)),
                _as_int(profile.get('longest_streak', 0)),
                1 if profile.get('notifications_enabled', True) else 0,
                _json_dumps(profile),
                now,
                int(guild_id),
                int(user_id),
            ),
        )

    def _append_profile_transaction(
        self,
        profile: dict,
        *,
        tx_id: str,
        tx_type: str,
        amount: int,
        balance_after: int,
        description: str,
        created_at: str,
        counterparty: int | str | None = None,
        meta: dict | None = None,
    ) -> None:
        tx = {
            'id': tx_id,
            'type': tx_type,
            'amount': int(amount),
            'balance': int(balance_after),
            'balance_after': int(balance_after),
            'description': str(description),
            'ts': created_at,
        }
        if counterparty is not None:
            tx['counterparty'] = str(counterparty)
        if meta:
            tx.update(meta)
        profile.setdefault('transactions', []).append(tx)
        profile['transactions'] = profile.get('transactions', [])[-100:]

    def _record_transaction_conn(
        self,
        conn,
        *,
        guild_id: int,
        user_id: int,
        tx_type: str,
        amount: int,
        balance_after: int,
        description: str,
        payload: dict | None = None,
        created_at: str | None = None,
    ) -> str:
        tx_id = f'tx_{uuid.uuid4().hex}'
        created_at = created_at or _now()
        conn.execute(
            """
            INSERT INTO transactions (
                id, guild_id, user_id, type, amount, balance_after,
                description, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tx_id, int(guild_id), int(user_id), str(tx_type), int(amount),
                int(balance_after), str(description), _json_dumps(payload or {}),
                created_at,
            ),
        )
        return tx_id

    def _change_balance_conn(
        self,
        conn,
        *,
        guild_id: int,
        user_id: int,
        display_name: str | None,
        amount: int,
        tx_type: str,
        description: str,
        count_as_earned: bool = False,
        allow_negative: bool = False,
        payload: dict | None = None,
    ) -> dict:
        amount = int(amount)
        profile, account = self._ensure_user_account_conn(conn, guild_id, user_id, display_name)
        balance_before = _as_int(account.get('balance', profile.get('balance', 0)))
        total_earned_before = _as_int(account.get('total_earned', profile.get('total_earned', 0)))
        balance_after = balance_before + amount
        if balance_after < 0 and not allow_negative:
            raise ValueError(f'Balance không đủ. Hiện có {balance_before:,} coins.')
        total_earned_after = total_earned_before + (amount if count_as_earned and amount > 0 else 0)
        conn.execute(
            """
            UPDATE economy_accounts
            SET balance = ?, total_earned = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (balance_after, total_earned_after, int(guild_id), int(user_id)),
        )
        profile['balance'] = balance_after
        profile['total_earned'] = total_earned_after
        tx_id = self._record_transaction_conn(
            conn,
            guild_id=guild_id,
            user_id=user_id,
            tx_type=tx_type,
            amount=amount,
            balance_after=balance_after,
            description=description,
            payload=payload,
        )
        self._append_profile_transaction(
            profile,
            tx_id=tx_id,
            tx_type=tx_type,
            amount=amount,
            balance_after=balance_after,
            description=description,
            created_at=_now(),
            meta=payload,
        )
        self._write_profile_conn(conn, guild_id, user_id, profile)
        return {
            'balance': balance_after,
            'total_earned': total_earned_after,
            'transaction_id': tx_id,
        }

    def create_task(self, guild_id: int, user_id: int, display_name: str, content: str) -> int:
        content = str(content or '').strip()
        if not content:
            raise ValueError('Task content is required.')
        with self.db.transaction() as conn:
            self._ensure_user_account_conn(conn, guild_id, user_id, display_name)
            cur = conn.execute(
                """
                INSERT INTO tasks (guild_id, user_id, content, completed, created_at)
                VALUES (?, ?, ?, 0, ?)
                """,
                (int(guild_id), int(user_id), content[:500], _now()),
            )
            return int(cur.lastrowid)

    def list_tasks(self, guild_id: int, user_id: int, *, include_completed: bool = False, limit: int = 50) -> list[dict]:
        query = """
            SELECT * FROM tasks
            WHERE guild_id = ? AND user_id = ?
        """
        params: list = [int(guild_id), int(user_id)]
        if not include_completed:
            query += ' AND completed = 0'
        query += ' ORDER BY completed ASC, id ASC LIMIT ?'
        params.append(int(limit))
        with self.db.read_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_dict(row) for row in rows]

    def complete_task(
        self,
        *,
        guild_id: int,
        user_id: int,
        display_name: str,
        task_id: int,
        reward_coins: int = 5,
        daily_reward_cap: int = 10,
    ) -> dict:
        with self.db.transaction() as conn:
            row = conn.execute(
                """
                SELECT * FROM tasks
                WHERE guild_id = ? AND user_id = ? AND id = ?
                """,
                (int(guild_id), int(user_id), int(task_id)),
            ).fetchone()
            if not row:
                return {'ok': False, 'message': 'Không tìm thấy task.'}
            task = self._row_dict(row)
            if task.get('completed'):
                return {'ok': False, 'message': 'Task này đã hoàn thành rồi.'}

            today = datetime.now().date().isoformat()
            reward_count = conn.execute(
                """
                SELECT COUNT(*) FROM transactions
                WHERE guild_id = ? AND user_id = ? AND type = 'task_reward'
                  AND created_at LIKE ?
                """,
                (int(guild_id), int(user_id), f'{today}%'),
            ).fetchone()[0]
            reward = int(reward_coins) if reward_count < int(daily_reward_cap) else 0
            completed_at = _now()
            conn.execute(
                """
                UPDATE tasks
                SET completed = 1, completed_at = ?, reward_coins = ?, reward_claimed = 1
                WHERE guild_id = ? AND user_id = ? AND id = ?
                """,
                (completed_at, reward, int(guild_id), int(user_id), int(task_id)),
            )
            self._ensure_user_account_conn(conn, guild_id, user_id, display_name)
            conn.execute(
                """
                INSERT OR IGNORE INTO daily_stats (
                    guild_id, user_id, date, study_seconds, earned_coins,
                    sessions_count, completed_tasks, task_rewarded_coins
                ) VALUES (?, ?, ?, 0, 0, 0, 0, 0)
                """,
                (int(guild_id), int(user_id), today),
            )
            conn.execute(
                """
                UPDATE daily_stats
                SET completed_tasks = completed_tasks + 1,
                    earned_coins = earned_coins + ?,
                    task_rewarded_coins = task_rewarded_coins + ?
                WHERE guild_id = ? AND user_id = ? AND date = ?
                """,
                (reward, reward, int(guild_id), int(user_id), today),
            )
            balance = None
            if reward:
                change = self._change_balance_conn(
                    conn,
                    guild_id=guild_id,
                    user_id=user_id,
                    display_name=display_name,
                    amount=reward,
                    tx_type='task_reward',
                    description=f'Task completed: {task.get("content", "")[:80]}',
                    count_as_earned=True,
                    payload={'task_id': int(task_id)},
                )
                balance = change['balance']
            message = f'Đã hoàn thành task #{task_id}.'
            if reward:
                message += f' +{reward:,} coins. Balance: {balance:,}.'
            else:
                message += ' Hôm nay bạn đã đạt giới hạn reward task.'
            return {'ok': True, 'message': message, 'reward': reward, 'balance': balance}

    def delete_task(self, guild_id: int, user_id: int, task_id: int) -> bool:
        with self.db.transaction() as conn:
            cur = conn.execute(
                'DELETE FROM tasks WHERE guild_id = ? AND user_id = ? AND id = ?',
                (int(guild_id), int(user_id), int(task_id)),
            )
            return cur.rowcount > 0

    def clear_tasks(self, guild_id: int, user_id: int, *, completed_only: bool = False) -> int:
        query = 'DELETE FROM tasks WHERE guild_id = ? AND user_id = ?'
        params: list = [int(guild_id), int(user_id)]
        if completed_only:
            query += ' AND completed = 1'
        with self.db.transaction() as conn:
            cur = conn.execute(query, params)
            return int(cur.rowcount)

    def get_private_room(self, guild_id: int, channel_id: int) -> dict:
        with self.db.read_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM private_rooms
                WHERE guild_id = ? AND channel_id = ? AND deleted_at IS NULL
                """,
                (int(guild_id), int(channel_id)),
            ).fetchone()
            return self._row_dict(row)

    def list_active_private_rooms(self, guild_id: int | None = None) -> list[dict]:
        query = 'SELECT * FROM private_rooms WHERE deleted_at IS NULL'
        params: list = []
        if guild_id is not None:
            query += ' AND guild_id = ?'
            params.append(int(guild_id))
        query += ' ORDER BY created_at ASC'
        with self.db.read_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_dict(row) for row in rows]

    def create_private_room(
        self,
        *,
        guild_id: int,
        channel_id: int,
        owner_id: int,
        owner_name: str,
        expires_at: str | None = None,
        rent_paid_coins: int = 0,
    ) -> dict:
        with self.db.transaction() as conn:
            if rent_paid_coins:
                self._change_balance_conn(
                    conn,
                    guild_id=guild_id,
                    user_id=owner_id,
                    display_name=owner_name,
                    amount=-abs(int(rent_paid_coins)),
                    tx_type='room_rent',
                    description='Private study room rent',
                    payload={'channel_id': int(channel_id), 'expires_at': expires_at},
                )
            else:
                self._ensure_user_account_conn(conn, guild_id, owner_id, owner_name)
            conn.execute(
                """
                INSERT OR REPLACE INTO private_rooms (
                    guild_id, channel_id, owner_id, created_at, expires_at,
                    locked, rent_paid_coins, deleted_at
                ) VALUES (?, ?, ?, ?, ?, 0, ?, NULL)
                """,
                (
                    int(guild_id), int(channel_id), int(owner_id), _now(),
                    expires_at, int(rent_paid_coins),
                ),
            )
            return self.get_private_room_in_conn(conn, guild_id, channel_id)

    def get_private_room_in_conn(self, conn, guild_id: int, channel_id: int) -> dict:
        row = conn.execute(
            """
            SELECT * FROM private_rooms
            WHERE guild_id = ? AND channel_id = ? AND deleted_at IS NULL
            """,
            (int(guild_id), int(channel_id)),
        ).fetchone()
        return self._row_dict(row)

    def set_private_room_locked(self, guild_id: int, channel_id: int, locked: bool) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE private_rooms
                SET locked = ?
                WHERE guild_id = ? AND channel_id = ? AND deleted_at IS NULL
                """,
                (1 if locked else 0, int(guild_id), int(channel_id)),
            )

    def delete_private_room(self, guild_id: int, channel_id: int) -> bool:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE private_rooms
                SET deleted_at = ?
                WHERE guild_id = ? AND channel_id = ? AND deleted_at IS NULL
                """,
                (_now(), int(guild_id), int(channel_id)),
            )
            return cur.rowcount > 0

    def create_scheduled_session(
        self,
        *,
        guild_id: int,
        user_id: int,
        display_name: str,
        start_at: str,
        duration_minutes: int,
        deposit_coins: int = 0,
    ) -> dict:
        with self.db.transaction() as conn:
            if deposit_coins:
                self._change_balance_conn(
                    conn,
                    guild_id=guild_id,
                    user_id=user_id,
                    display_name=display_name,
                    amount=-abs(int(deposit_coins)),
                    tx_type='schedule_deposit',
                    description='Study accountability session deposit',
                    payload={'start_at': start_at, 'duration_minutes': int(duration_minutes)},
                )
            else:
                self._ensure_user_account_conn(conn, guild_id, user_id, display_name)
            cur = conn.execute(
                """
                INSERT INTO scheduled_sessions (
                    guild_id, user_id, start_at, duration_minutes, attended,
                    completed, deposit_coins, status, created_at
                ) VALUES (?, ?, ?, ?, 0, 0, ?, 'booked', ?)
                """,
                (
                    int(guild_id), int(user_id), str(start_at), int(duration_minutes),
                    int(deposit_coins), _now(),
                ),
            )
            session_id = int(cur.lastrowid)
            return self.get_scheduled_session_in_conn(conn, guild_id, session_id)

    def get_scheduled_session_in_conn(self, conn, guild_id: int, session_id: int) -> dict:
        row = conn.execute(
            'SELECT * FROM scheduled_sessions WHERE guild_id = ? AND id = ?',
            (int(guild_id), int(session_id)),
        ).fetchone()
        return self._row_dict(row)

    def list_scheduled_sessions(self, guild_id: int, user_id: int | None = None, *, include_done: bool = False) -> list[dict]:
        query = 'SELECT * FROM scheduled_sessions WHERE guild_id = ?'
        params: list = [int(guild_id)]
        if user_id is not None:
            query += ' AND user_id = ?'
            params.append(int(user_id))
        if not include_done:
            query += " AND status = 'booked'"
        query += ' ORDER BY start_at ASC LIMIT 50'
        with self.db.read_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_dict(row) for row in rows]

    def cancel_scheduled_session(
        self,
        *,
        guild_id: int,
        user_id: int,
        display_name: str,
        session_id: int,
        admin_override: bool = False,
    ) -> dict:
        with self.db.transaction() as conn:
            session = self.get_scheduled_session_in_conn(conn, guild_id, session_id)
            if not session:
                return {'ok': False, 'message': 'Không tìm thấy lịch học.'}
            if int(session['user_id']) != int(user_id) and not admin_override:
                return {'ok': False, 'message': 'Bạn không sở hữu lịch học này.'}
            if session.get('status') != 'booked':
                return {'ok': False, 'message': 'Lịch học này không còn đang booked.'}
            conn.execute(
                """
                UPDATE scheduled_sessions
                SET status = 'cancelled', cancelled_at = ?
                WHERE guild_id = ? AND id = ?
                """,
                (_now(), int(guild_id), int(session_id)),
            )
            deposit = _as_int(session.get('deposit_coins', 0))
            if deposit > 0:
                self._change_balance_conn(
                    conn,
                    guild_id=guild_id,
                    user_id=int(session['user_id']),
                    display_name=display_name,
                    amount=deposit,
                    tx_type='schedule_refund',
                    description='Cancelled study session deposit refund',
                    payload={'scheduled_session_id': int(session_id)},
                )
            message = 'Đã hủy lịch học.'
            if deposit > 0:
                message += f' Đã hoàn lại {deposit:,} coins.'
            return {'ok': True, 'message': message, 'refunded': deposit}

    def create_reminder(
        self,
        *,
        guild_id: int,
        user_id: int,
        display_name: str,
        remind_at: str,
        message: str,
        channel_id: int | None = None,
    ) -> int:
        with self.db.transaction() as conn:
            self._ensure_user_account_conn(conn, guild_id, user_id, display_name)
            cur = conn.execute(
                """
                INSERT INTO reminders (
                    guild_id, user_id, remind_at, message, channel_id, sent, created_at
                ) VALUES (?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    int(guild_id), int(user_id), str(remind_at), str(message)[:500],
                    int(channel_id) if channel_id else None, _now(),
                ),
            )
            return int(cur.lastrowid)

    def list_reminders(self, guild_id: int, user_id: int, *, include_sent: bool = False) -> list[dict]:
        query = 'SELECT * FROM reminders WHERE guild_id = ? AND user_id = ?'
        params: list = [int(guild_id), int(user_id)]
        if not include_sent:
            query += ' AND sent = 0'
        query += ' ORDER BY remind_at ASC LIMIT 50'
        with self.db.read_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_dict(row) for row in rows]

    def cancel_reminder(self, guild_id: int, user_id: int, reminder_id: int) -> bool:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                DELETE FROM reminders
                WHERE guild_id = ? AND user_id = ? AND id = ? AND sent = 0
                """,
                (int(guild_id), int(user_id), int(reminder_id)),
            )
            return cur.rowcount > 0

    def claim_due_reminders(self, due_at: str, *, limit: int = 25) -> list[dict]:
        with self.db.transaction() as conn:
            rows = conn.execute(
                """
                SELECT * FROM reminders
                WHERE sent = 0 AND remind_at <= ?
                ORDER BY remind_at ASC
                LIMIT ?
                """,
                (str(due_at), int(limit)),
            ).fetchall()
            reminders = [self._row_dict(row) for row in rows]
            if reminders:
                ids = [int(item['id']) for item in reminders]
                placeholders = ','.join('?' for _ in ids)
                conn.execute(
                    f"UPDATE reminders SET sent = 1, sent_at = ? WHERE id IN ({placeholders})",
                    [_now(), *ids],
                )
            return reminders

    def record_study_session_chunk(
        self,
        *,
        guild_id: int,
        user_id: int,
        channel_id: int | None,
        started_at: str,
        ended_at: str,
        duration_seconds: int,
        active_seconds: int,
        earned_coins: int = 0,
        used_camera: bool = False,
        used_stream: bool = False,
        ended_reason: str = 'checkpoint',
    ) -> str:
        session_id = f'study_{uuid.uuid4().hex}'
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO study_sessions (
                    id, guild_id, user_id, channel_id, started_at, ended_at,
                    duration_seconds, active_seconds, used_camera, used_stream,
                    earned_coins, ended_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    int(guild_id),
                    int(user_id),
                    int(channel_id) if channel_id else None,
                    str(started_at),
                    str(ended_at),
                    max(0, int(duration_seconds)),
                    max(0, int(active_seconds)),
                    1 if used_camera else 0,
                    1 if used_stream else 0,
                    max(0, int(earned_coins)),
                    str(ended_reason),
                ),
            )
        return session_id

    @staticmethod
    def _parse_iso_dt(value: str) -> datetime:
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _study_seconds_in_window_conn(
        self,
        conn,
        *,
        guild_id: int,
        user_id: int,
        start_at: datetime,
        end_at: datetime,
    ) -> int:
        rows = conn.execute(
            """
            SELECT started_at, ended_at, duration_seconds, active_seconds
            FROM study_sessions
            WHERE guild_id = ? AND user_id = ?
              AND ended_at IS NOT NULL
              AND started_at < ?
              AND ended_at > ?
            """,
            (
                int(guild_id),
                int(user_id),
                end_at.isoformat(timespec='seconds'),
                start_at.isoformat(timespec='seconds'),
            ),
        ).fetchall()
        total = 0
        for row in rows:
            try:
                session_start = self._parse_iso_dt(row['started_at'])
                session_end = self._parse_iso_dt(row['ended_at'])
            except (TypeError, ValueError):
                continue
            overlap = max(0, int((min(session_end, end_at) - max(session_start, start_at)).total_seconds()))
            if not overlap:
                continue
            duration = max(1, _as_int(row['duration_seconds'], overlap))
            active = _as_int(row['active_seconds'], duration) or duration
            total += min(overlap, int(overlap * min(active, duration) / duration))
        return total

    def process_due_scheduled_sessions(
        self,
        *,
        due_at: str,
        attendance_ratio: float = 0.8,
        completion_bonus_coins: int = 10,
        grace_minutes: int = 5,
        limit: int = 25,
    ) -> list[dict]:
        due_dt = self._parse_iso_dt(due_at)
        rows = []
        with self.db.transaction() as conn:
            candidates = conn.execute(
                """
                SELECT ss.*, u.display_name
                FROM scheduled_sessions ss
                LEFT JOIN users u
                  ON u.guild_id = ss.guild_id AND u.user_id = ss.user_id
                WHERE ss.status = 'booked'
                ORDER BY ss.start_at ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            for row in candidates:
                session = self._row_dict(row)
                try:
                    start_at = self._parse_iso_dt(session['start_at'])
                except (TypeError, ValueError):
                    continue
                duration_seconds = max(60, _as_int(session.get('duration_minutes'), 60) * 60)
                scheduled_end = start_at.timestamp() + duration_seconds + max(0, int(grace_minutes)) * 60
                if due_dt.timestamp() < scheduled_end:
                    continue

                end_at = datetime.fromtimestamp(start_at.timestamp() + duration_seconds, tz=timezone.utc)
                studied = self._study_seconds_in_window_conn(
                    conn,
                    guild_id=session['guild_id'],
                    user_id=session['user_id'],
                    start_at=start_at,
                    end_at=end_at,
                )
                required = int(duration_seconds * max(0.0, min(float(attendance_ratio), 1.0)))

                # Older deployments may not have session rows for every study
                # chunk yet. Fall back to the scheduled day's total so deposits
                # are not unfairly lost immediately after migration.
                if studied < required:
                    day = start_at.date().isoformat()
                    daily = conn.execute(
                        """
                        SELECT study_seconds FROM daily_stats
                        WHERE guild_id = ? AND user_id = ? AND date = ?
                        """,
                        (int(session['guild_id']), int(session['user_id']), day),
                    ).fetchone()
                    studied = max(studied, _as_int(daily['study_seconds'], 0) if daily else 0)

                completed = studied >= required
                deposit = _as_int(session.get('deposit_coins'), 0)
                bonus = max(0, int(completion_bonus_coins)) if completed else 0
                status = 'completed' if completed else 'missed'
                now = _now()
                conn.execute(
                    """
                    UPDATE scheduled_sessions
                    SET status = ?, attended = ?, completed = 1, completed_at = ?
                    WHERE guild_id = ? AND id = ? AND status = 'booked'
                    """,
                    (
                        status,
                        1 if completed else 0,
                        now,
                        int(session['guild_id']),
                        int(session['id']),
                    ),
                )
                refunded = 0
                if completed and (deposit > 0 or bonus > 0):
                    display_name = session.get('display_name') or f"User {session['user_id']}"
                    refunded = deposit + bonus
                    if deposit > 0:
                        self._change_balance_conn(
                            conn,
                            guild_id=session['guild_id'],
                            user_id=session['user_id'],
                            display_name=display_name,
                            amount=deposit,
                            tx_type='schedule_refund',
                            description='Accountability study session deposit refund',
                            payload={
                                'scheduled_session_id': int(session['id']),
                                'studied_seconds': studied,
                                'required_seconds': required,
                            },
                        )
                    if bonus > 0:
                        self._change_balance_conn(
                            conn,
                            guild_id=session['guild_id'],
                            user_id=session['user_id'],
                            display_name=display_name,
                            amount=bonus,
                            tx_type='schedule_bonus',
                            description='Completed accountability study session bonus',
                            count_as_earned=True,
                            payload={
                                'scheduled_session_id': int(session['id']),
                                'studied_seconds': studied,
                                'required_seconds': required,
                            },
                        )
                rows.append({
                    **session,
                    'status': status,
                    'studied_seconds': studied,
                    'required_seconds': required,
                    'refunded_coins': refunded,
                    'bonus_coins': bonus,
                })
        return rows

    def completed_task_leaderboard(self, guild_id: int, *, limit: int = 10) -> list[dict]:
        with self.db.read_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    t.user_id,
                    COALESCE(u.display_name, 'Unknown') AS display_name,
                    COUNT(*) AS completed_tasks
                FROM tasks t
                LEFT JOIN users u
                  ON u.guild_id = t.guild_id AND u.user_id = t.user_id
                WHERE t.guild_id = ? AND t.completed = 1
                GROUP BY t.user_id, u.display_name
                ORDER BY completed_tasks DESC, display_name ASC
                LIMIT ?
                """,
                (int(guild_id), int(limit)),
            ).fetchall()
            return [self._row_dict(row) for row in rows]

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
