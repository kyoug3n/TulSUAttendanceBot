import logging
import json
from pathlib import Path
from typing import Any

import aiosqlite


class StorageManager:
    DB_FILE = Path('db.sqlite3')

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.conn: aiosqlite.Connection | None = None

    async def connect(self):
        self.conn = await aiosqlite.connect(self.DB_FILE)
        self.conn.row_factory = aiosqlite.Row
        self.logger.info('DB connection established')
        await self._init_db()
        self.logger.info('DB initialized')

    async def _init_db(self):
        assert self.conn

        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS user_data (
                user_id    TEXT PRIMARY KEY,
                username   TEXT,
                last_name  TEXT CHECK(last_name GLOB '[А-Яа-яЁё -]*'),
                first_name TEXT CHECK(first_name GLOB '[А-Яа-яЁё -]*'),
                registered INTEGER DEFAULT 0
            ) WITHOUT ROWID;
        ''')

        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS active_polls (
                poll_id     TEXT PRIMARY KEY,
                message_id  INTEGER UNIQUE,
                date        TEXT,
                start_time  TEXT,
                end_time    TEXT,
                class_name  TEXT,
                prof        TEXT,
                room        TEXT,
                class_type  TEXT,
                close_time  TIMESTAMP,
                responses   TEXT DEFAULT '[]'
            );
        ''')

        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS past_polls(
                poll_id      TEXT PRIMARY KEY,
                message_id   INTEGER UNIQUE,
                date         TEXT,
                start_time   TEXT,
                end_time     TEXT,
                class_name   TEXT,
                prof         TEXT,
                room         TEXT,
                class_type   TEXT,
                close_time   TIMESTAMP,
                responses    TEXT DEFAULT '[]'
            );         
        ''')

        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS discipline_settings (
                class_name   TEXT PRIMARY KEY,
                class_type   TEXT,
                alias        TEXT,
                is_nmg       INTEGER NOT NULL DEFAULT 0,
                is_excluded  INTEGER NOT NULL DEFAULT 0
            );
        ''')

        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS schedule_cache (
                group_id    INTEGER PRIMARY KEY,
                raw_json    TEXT NOT NULL,
                fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        await self.conn.commit()

    # ----- User data -----
    async def get_user(self, user_id: str) -> dict[str, Any] | None:
        assert self.conn

        async with self.conn.execute(
            'SELECT * FROM user_data WHERE user_id = ?', (user_id,)
        ) as cursor:
            row = await cursor.fetchone()

        return dict(row) if row else None

    async def update_user(self, user_id: str, data: dict[str, Any]) -> None:
        assert self.conn

        await self.conn.execute('''
            INSERT OR REPLACE INTO user_data 
            (user_id, username, last_name, first_name, registered)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            user_id,
            data.get('username'),
            data.get('last_name'),
            data.get('first_name'),
            1 if data.get('registered') else 0
        ))

        await self.conn.commit()

    # ----- Polls -----
    async def get_active_polls(self) -> dict[str, Any]:
        assert self.conn

        polls: dict[str, Any] = {}
        async with self.conn.execute('SELECT * FROM active_polls') as cursor:
            async for row in cursor:
                data = dict(row)
                data['class_info'] = {
                    k: data[k] for k in
                    ('date', 'start_time', 'end_time', 'class_name', 'prof', 'room', 'class_type')
                }
                polls[data['poll_id']] = data

        return polls

    async def save_active_polls(self, poll_id: str, data: dict[str, Any]) -> None:
        assert self.conn

        class_info = data['class_info']

        await self.conn.execute('''
            INSERT OR REPLACE INTO active_polls (
                poll_id, message_id, date, start_time, end_time,
                class_name, prof, room, class_type, close_time,
                responses
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
                poll_id,
                data['message_id'],
                class_info['date'],
                class_info['start_time'],
                class_info['end_time'],
                class_info['class_name'],
                class_info['prof'],
                class_info['room'],
                class_info['class_type'],
                data['close_time'],
                data.get('responses', '[]')
            )
        )

        await self.conn.commit()

    async def update_poll_response(
            self,
            poll_id: str,
            user_id: str,
            option_ids: list[int],
            first_name: str = '',
            last_name: str = '',
            username: str = ''
    ) -> None:
        assert self.conn

        async with self.conn.execute(
            'SELECT responses FROM active_polls WHERE poll_id = ?', (poll_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        try:
            responses = json.loads(row['responses'])
        except json.JSONDecodeError:
            responses = []

        responses.append({
            'user_id': user_id,
            'option_ids': option_ids,
            'first_name': first_name,
            'last_name': last_name,
            'username': username,
        })

        await self.conn.execute(
            'UPDATE active_polls SET responses = ? WHERE poll_id = ?',
            (json.dumps(responses, ensure_ascii=False), poll_id)
        )

        return await self.conn.commit()

    async def archive_poll(self, poll_id: str):
        assert self.conn

        async with self.conn.execute('''
            SELECT poll_id, message_id, date, start_time, end_time,
            class_name, prof, room, class_type, close_time, responses
            FROM active_polls WHERE poll_id = ?
        ''', (poll_id,)) as cursor:
            poll_data = await cursor.fetchone()

        if not poll_data:
            return None

        await self.conn.execute('''
            INSERT INTO past_polls (
                poll_id, message_id, date, start_time, end_time,
                class_name, prof, room, class_type, close_time,
                responses
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(poll_data))

        await self.conn.execute('''
            DELETE FROM active_polls WHERE poll_id = ?
        ''', (poll_id,))

        return await self.conn.commit()

    async def delete_poll(self, poll_id: str) -> None:
        assert self.conn

        await self.conn.execute(
            'DELETE FROM active_polls WHERE poll_id = ?', (poll_id,)
        )
        await self.conn.execute(
            'DELETE FROM past_polls WHERE poll_id = ?', (poll_id,)
        )

        await self.conn.commit()

    async def get_past_polls_by_month(self, year: int, month: int) -> list[dict]:
        assert self.conn

        start = f'{year:04d}-{month:02d}-01'
        end = f'{year + 1:04d}-01-01' if month == 12 else f'{year:04d}-{month + 1:02d}-01'

        query = '''
            SELECT date, start_time, end_time, class_name, room, responses 
            FROM past_polls
            WHERE close_time >= ? AND close_time < ?
            ORDER BY date, start_time
        '''
        rows = []
        async with self.conn.execute(query, (start, end)) as cursor:
            async for row in cursor:
                rows.append(dict(row))

        return rows

    # ----- Discipline settings -----
    async def get_discipline_settings(self) -> tuple[list, dict, dict]:
        assert self.conn

        query = '''
            SELECT class_name, class_type, alias, is_nmg, is_excluded
            FROM discipline_settings
        '''
        async with self.conn.execute(query) as cursor:
            rows = await cursor.fetchall()

        excluded = []
        nmg = {}
        aliases = {}

        for row in rows:
            class_name = row['class_name']
            class_type = row['class_type']
            alias = row['alias']
            is_nmg = row['is_nmg']
            is_excluded = row['is_excluded']

            if is_excluded:
                excluded.append(class_name)
            if is_nmg:
                nmg[class_name] = class_type
            if alias:
                aliases[class_name] = alias

        return excluded, nmg, aliases

    async def set_discipline_setting(
            self,
            class_name: str,
            *,
            class_type: str | None = None,
            alias: str | None = None,
            is_nmg: bool | None = None,
            is_excluded: bool | None = None
    ) -> None:
        assert self.conn

        to_insert: dict[str, str | int] = {'class_name': class_name}
        if alias is not None:
            to_insert['alias'] = alias
        if class_type is not None:
            to_insert['class_type'] = class_type
        if is_nmg is not None:
            to_insert['is_nmg'] = int(is_nmg)
        if is_excluded is not None:
            to_insert['is_excluded'] = int(is_excluded)
        if len(to_insert) == 1:
            return None

        cols = list(to_insert.keys())
        placeholders = ','.join('?' for _ in cols)
        insert_cols = ','.join(cols)

        update_clauses = ', '.join(f"{col}=excluded.{col}" for col in cols if col != 'class_name')

        sql = f'''
            INSERT INTO discipline_settings ({insert_cols})
            VALUES ({placeholders})
            ON CONFLICT(class_name) DO UPDATE SET {update_clauses}
        '''

        await self.conn.execute(sql, tuple(to_insert[col] for col in cols))
        await self.conn.commit()

    async def delete_discipline_settings(
            self,
            class_name: str,
            *,
            remove_class_type: bool = False,
            remove_alias: bool = False,
            remove_nmg: bool = False,
            remove_excluded: bool = False,
            remove_all: bool = False
    ) -> None:
        assert self.conn

        if remove_all:
            await self.conn.execute(
                'DELETE FROM discipline_settings WHERE class_name = ?',
                (class_name,)
            )
        else:
            updates = []
            if remove_class_type:
                updates.append('class_type = NULL')
            if remove_alias:
                updates.append('alias = NULL')
            if remove_nmg:
                updates.append('is_nmg = 0')
            if remove_excluded:
                updates.append('is_excluded = 0')

            if updates:
                sql = f'''
                    UPDATE discipline_settings SET {', '.join(updates)}
                    WHERE class_name = ?
                '''
                await self.conn.execute(sql, (class_name,))

        await self.conn.commit()

    # ----- Schedule cache -----
    async def save_last_schedule(self, group_id: int, raw_list: list[dict]) -> None:
        assert self.conn

        raw_json = json.dumps(raw_list, ensure_ascii=False)
        await self.conn.execute('''
            INSERT INTO schedule_cache(group_id, raw_json)
            VALUES (?, ?)
            ON CONFLICT(group_id) DO UPDATE SET
                raw_json = excluded.raw_json,
                fetched_at = CURRENT_TIMESTAMP
        ''', (group_id, raw_json))

        await self.conn.commit()

    async def get_last_schedule(self, group_id: int) -> list[dict]:
        assert self.conn

        async with self.conn.execute(
            'SELECT raw_json FROM schedule_cache WHERE group_id = ?',
            (group_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return []

        try:
            return json.loads(row['raw_json'])
        except json.JSONDecodeError:
            return []

    async def close(self):
        if self.conn:
            if self.conn.in_transaction:
                await self.conn.rollback()
            await self.conn.close()
            self.conn = None
            self.logger.info('DB connection closed')
