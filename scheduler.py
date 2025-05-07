import asyncio
import datetime
import logging
from typing import Any

from aiogram import exceptions
from parser import ScheduleParser
from storage import StorageManager


class Scheduler:
    def __init__(
            self, bot, chat_id: int, group_id: int,
            storage: StorageManager,
            poll_interval: float, update_interval: float, poll_window: float
    ):
        self.bot = bot
        self.chat_id = chat_id
        self.storage = storage
        self.parser = ScheduleParser(group_id)
        self.poll_interval = poll_interval
        self.update_interval = update_interval
        self.poll_window = poll_window

        self.logger = logging.getLogger(__name__)
        self.active_polls: dict[str, Any] = {}
        self._semaphore = asyncio.Semaphore(3)
        self._running = False

    async def _load_active_polls(self) -> None:
        try:
            rows = await self.storage.get_active_polls()
            self.active_polls.clear()
            for poll_id, row in rows.items():

                class_info = {
                    k: row[k] for k in
                    ['date', 'start_time', 'end_time', 'class_name', 'prof', 'room', 'class_type']
                }
                raw_close = row.get('close_time')
                if isinstance(raw_close, str):
                    try:
                        # noinspection PyUnusedLocal
                        close_time = datetime.datetime.fromisoformat(raw_close)
                    except ValueError:
                        close_time = datetime.datetime.strptime(raw_close, "%Y-%m-%d %H:%M:%S")
                else:
                    close_time = raw_close

                record = {
                    'poll_id': row['poll_id'],
                    'message_id': row['message_id'],
                    'class_info': class_info,
                    'close_time': close_time,
                    'responses': row.get('responses', '[]')
                }
                key = self._generate_key(class_info)
                self.active_polls[key] = record
            self.logger.info(f"Loaded {len(self.active_polls)} active polls from DB")
        except Exception as e:
            self.logger.error(f"Failed loading active polls: {e}")
            self.active_polls.clear()

    def _generate_key(self, cls: dict[str, Any]) -> str:
        return f"{cls['date']}|{cls['start_time']}|{cls['class_name']}"

    async def _check_and_send_polls(self, now: datetime.datetime) -> None:
        today = now.strftime("%d.%m.%Y")
        classes = self.parser.schedule.get(today, [])

        for cls in classes:
            key = self._generate_key(cls)
            if key in self.active_polls:
                continue
            class_dt = datetime.datetime.strptime(f"{cls['date']} {cls['start_time']}", "%d.%m.%Y %H:%M")
            if 0 <= (class_dt - now).total_seconds() <= self.poll_window:
                await self._send_poll(cls, key)

        await self._close_expired(now)

    async def _send_poll(self, cls: dict[str, Any], key: str) -> None:
        async with self._semaphore:
            try:
                question = f"{cls['class_name']} в {cls['start_time']} - {cls['end_time']}"
                msg = await self.bot.send_poll(
                    chat_id=self.chat_id,
                    question=question,
                    options=['Да', 'Нет', 'Пикнулся', 'На больничном'],
                    is_anonymous=False,
                    allows_multiple_answers=False,
                )
                close_time = self._calculate_close_time(cls)
                info = {
                    'poll_id': msg.poll.id,
                    'message_id': msg.message_id,
                    'class_info': cls,
                    'close_time': close_time,
                    'responses': '[]'
                }
                self.active_polls[key] = info
                await self.storage.save_active_polls(info['poll_id'], info)
                self.logger.info(f"Sent poll: {key}")
            except Exception as e:
                self.logger.error(f"Error sending poll {key}: {e}")

    def _calculate_close_time(self, cls: dict[str, Any]) -> datetime.datetime:
        date = datetime.datetime.strptime(cls['date'], "%d.%m.%Y").date()
        end = datetime.datetime.strptime(cls['end_time'], "%H:%M").time()
        close = datetime.datetime.combine(date, end)
        start = datetime.datetime.strptime(cls['start_time'], "%H:%M").time()

        if end <= start:
            close += datetime.timedelta(days=1)
        return close

    async def _close_expired(self, now: datetime.datetime):
        expired_keys = []
        for key, info in list(self.active_polls.items()):
            if now >= info['close_time'] and await self._close_poll(info):
                expired_keys.append(key)
        for key in expired_keys:
            self.active_polls.pop(key, None)

    async def _close_poll(self, info: dict[str, Any]) -> bool:
        pid = info['poll_id']
        async with self._semaphore:
            try:
                await self.bot.stop_poll(chat_id=self.chat_id, message_id=info['message_id'])
            except exceptions.TelegramBadRequest as e:
                msg = str(e)
                if 'has already been closed' in msg:
                    self.logger.warning(f"Poll {pid} already closed: {msg}")
                    try:
                        await self.storage.archive_poll(pid)
                        self.logger.info(f"Archived already closed poll {pid}")
                    except Exception as e:
                        self.logger.error(f"Error archiving already closed poll {pid}: {e}")
                    return True
                if 'message with poll to stop not found' in msg:
                    self.logger.warning(f"Poll {pid} not found; deleting from storage")
                    try:
                        await self.storage.delete_poll(pid)
                        self.logger.info(f"Deleted poll {pid} from both active and past tables")
                    except Exception as e:
                        self.logger.error(f"Error deleting poll {pid}: {e}")
                    return True
                self.logger.error(f"BadRequest closing poll {pid}: {msg}")
                return False

            try:
                await self.storage.archive_poll(pid)
                self.logger.info(f"Closed and archived poll {pid}")
                return True
            except Exception as e:
                self.logger.error(f"Error archiving poll {pid}: {e}")
                return False

    async def start(self) -> None:
        self._running = True

        await self._load_active_polls()
        await self.parser.fetch()
        last_update = datetime.datetime.now()
        self.logger.info("Initial schedule fetched")

        while self._running:
            try:
                now = datetime.datetime.now()

                if (now - last_update).total_seconds() >= self.update_interval:
                    await self.parser.fetch()
                    last_update = now
                    self.logger.info("Schedule updated")

                await self._check_and_send_polls(now)

            except Exception as e:
                self.logger.error(f"Scheduler loop error: {e}")

            await asyncio.sleep(self.poll_interval)

    async def close(self):
        self._running = False
        await self.parser.close()
        await self.storage.close()
