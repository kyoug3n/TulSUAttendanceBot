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
        await self._init_db()
        self.logger.info('DB connection established')

    async def _init_db(self):
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

        self.logger.info('DB initialized')

    async def get_user(self, user_id: str) -> dict[str, Any] | None:
        async with self.conn.execute('''
            SELECT * FROM user_data WHERE user_id = ?
        ''', (user_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def update_user(self, user_id: str, data: dict[str, Any]) -> None:
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

    async def get_active_polls(self) -> dict[str, Any]:
        polls: dict[str, Any] = {}
        async with self.conn.execute('SELECT * FROM active_polls') as cursor:
            async for row in cursor:
                rowdict = dict(row)

                class_info = {
                    'date': rowdict['date'],
                    'start_time': rowdict['start_time'],
                    'end_time': rowdict['end_time'],
                    'class_name': rowdict['class_name'],
                    'prof': rowdict['prof'],
                    'room': rowdict['room'],
                    'class_type': rowdict['class_type'],
                }
                rowdict['class_info'] = class_info
                polls[rowdict['poll_id']] = rowdict
        return polls

    async def save_active_polls(self, poll_id: str, data: dict[str, Any]) -> None:
        class_info = data['class_info']
        responses = data.get('responses', '[]')

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
                responses
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
    ):
        async with self.conn.execute('''
            SELECT responses FROM active_polls WHERE poll_id = ?
        ''', (poll_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return
        try:
            responses = json.loads(row["responses"])
        except json.JSONDecodeError:
            responses = []

        entry = {
            "user_id": user_id,
            "option_ids": option_ids,
            "first_name": first_name,
            "last_name": last_name,
            "username": username,
        }
        responses.append(entry)

        await self.conn.execute('''
            UPDATE active_polls SET responses = ? WHERE poll_id = ?
        ''', (json.dumps(responses), poll_id)
        )
        await self.conn.commit()

    async def archive_poll(self, poll_id: str):
        async with self.conn.execute('''
            SELECT poll_id, message_id, date, start_time, end_time,
            class_name, prof, room, class_type, close_time, responses
            FROM active_polls WHERE poll_id = ?
        ''', (poll_id,)) as cursor:
            poll_data = await cursor.fetchone()

        if not poll_data:
            return

        await self.conn.execute('''
            INSERT INTO past_polls (
                poll_id, message_id, date, start_time, end_time,
                class_name, prof, room, class_type, close_time,
                responses
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            poll_data['poll_id'],
            poll_data['message_id'],
            poll_data['date'],
            poll_data['start_time'],
            poll_data['end_time'],
            poll_data['class_name'],
            poll_data['prof'],
            poll_data['room'],
            poll_data['class_type'],
            poll_data['close_time'],
            poll_data['responses']
        ))

        await self.conn.execute('''
            DELETE FROM active_polls WHERE poll_id = ?
        ''', (poll_id,))

        await self.conn.commit()

    async def delete_poll(self, poll_id: str) -> None:
        await self.conn.execute('''
            DELETE FROM active_polls WHERE poll_id = ?
        ''', (poll_id,))

        await self.conn.execute('''
            DELETE FROM past_polls WHERE poll_id = ?
        ''', (poll_id,))

        await self.conn.commit()

    async def get_past_polls_by_month(self, year: int, month: int) -> list[dict]:
        start = f"{year:04d}-{month:02d}-01"

        if month == 12:
            end = f"{year+1:04d}-01-01"
        else:
            end = f"{year:04d}-{month+1:02d}-01"
        query = """
            SELECT date, start_time, end_time, class_name, room, responses 
            FROM past_polls
            WHERE close_time >= ? AND close_time < ?
            ORDER BY date, start_time
        """
        rows = []
        async with self.conn.execute(query, (start, end)) as cursor:
            async for row in cursor:
                rows.append(dict(row))
        return rows

    async def close(self):
        if self.conn:
            if self.conn.in_transaction:
                await self.conn.rollback()
            await self.conn.close()
            self.conn = None
            self.logger.info('DB connection closed')

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
