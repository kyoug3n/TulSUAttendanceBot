import asyncio
import datetime
import logging
from typing import Any, Final

import aiohttp
from aiogram import Bot, exceptions
from parser import ScheduleParser
from storage import StorageManager


logger = logging.getLogger(__name__)


class Scheduler:
    TIMEGROUPS_ENDPOINT: Final[str] = 'https://tulsu.ru/schedule/queries/GetTimeGroups.php'

    def __init__(self, bot: Bot, config, storage: StorageManager, discipline_settings: tuple[list, dict, dict]):
        self.bot = bot
        self.config = config
        self.storage = storage

        self.chat_id = self.config.chat_id
        self.group_id = self.config.group_id
        self.poll_interval = self.config.poll_interval
        self.poll_window = self.config.poll_window
        self.prefetch_offset = self.config.prefetch_offset
        self.discipline_settings = discipline_settings

        self.session = aiohttp.ClientSession()
        self.parser = ScheduleParser(self.config, self.session, self.storage, self.discipline_settings)
        self.active_polls: dict[str, Any] = {}

        self._semaphore = asyncio.Semaphore(3)
        self._running = False
        self.start_times: list[datetime.time] = []
        self._next_fetch: datetime.datetime | None = None

    async def start(self) -> None:
        self._running = True

        await self._load_active_polls()
        await self.parser.fetch()
        await self._load_time_groups()
        self._compute_next_fetch(datetime.datetime.now())
        logger.info(f'Initial schedule fetched, next refresh at {self._next_fetch}')

        while self._running:
            try:
                now = datetime.datetime.now()
                await self._refresh_sched_disc_settings(now)
                await self._check_and_send_polls(now)
            except Exception as e:
                logger.error(f'Scheduler loop error: {e}')

            await asyncio.sleep(self.poll_interval)

    async def _load_active_polls(self) -> None:
        try:
            rows = await self.storage.get_active_polls()
            self.active_polls.clear()
            for poll_id, row in rows.items():
                class_info = {
                    k: row[k] for k in
                    ['date', 'start_time', 'end_time', 'class_name', 'prof', 'room', 'class_type']
                }
                key = self._generate_key(class_info)
                raw_close = row.get('close_time')
                if isinstance(raw_close, str):
                    try:
                        # noinspection PyUnusedLocal
                        close_time = datetime.datetime.fromisoformat(raw_close)
                    except ValueError:
                        close_time = datetime.datetime.strptime(raw_close, '%Y-%m-%d %H:%M:%S')
                else:
                    close_time = raw_close

                self.active_polls[key] = {
                    'poll_id': row['poll_id'],
                    'message_id': row['message_id'],
                    'class_info': class_info,
                    'close_time': close_time,
                    'responses': row.get('responses', '[]')
                }
            logger.info(f'Loaded {len(self.active_polls)} active polls from DB')
        except Exception as e:
            logger.error(f'Failed loading active polls: {e}')
            self.active_polls.clear()

    @staticmethod
    def _generate_key(cls: dict[str, Any]) -> str:
        return f"{cls['date']}|{cls['start_time']}|{cls['class_name']}"

    async def _load_time_groups(self) -> None:
        params = {'search_field': 'GROUP_P', 'search_value': self.group_id}
        try:
            async with self.session.get(self.TIMEGROUPS_ENDPOINT, params=params, timeout=10) as response:
                response.raise_for_status()
                timegroups_raw = await response.json()
            times = set()
            for item in timegroups_raw:
                hh, mm = map(int, item['TIME_START'].split(':'))
                times.add(datetime.time(hour=hh, minute=mm))
            self.start_times = sorted(times)

            # format start times as HH:MM
            formatted_times = [t.strftime('%H:%M') for t in self.start_times]
            logger.info(f'Loaded schedule time groups: {formatted_times}')
        except Exception as e:
            logger.error(f'Failed loading schedule time groups: {e}')
            self.start_times = []

    def _compute_next_fetch(self, now: datetime.datetime) -> None:
        today = now.date()
        future = []

        for t in self.start_times:
            candidate = datetime.datetime.combine(today, t) - datetime.timedelta(
                seconds=self.prefetch_offset
            )
            if candidate > now:
                future.append(candidate)

        if not future:
            tomorrow = today + datetime.timedelta(days=1)
            for t in self.start_times:
                future.append(datetime.datetime.combine(tomorrow, t) -
                              datetime.timedelta(seconds=self.prefetch_offset))

        self._next_fetch = min(future) if future else None

    async def _refresh_sched_disc_settings(self, now: datetime.datetime) -> None:
        if self._next_fetch and now >= self._next_fetch:
            try:
                self.discipline_settings = await self.storage.get_discipline_settings()
                await self.parser.fetch()
                logger.info(f'Schedule and discipline settings refreshed at {now}')
            except Exception as e:
                logger.error(f'Error refreshing schedule and discipline settings: {e}')
            finally:
                self._compute_next_fetch(now)

    async def _check_and_send_polls(self, now: datetime.datetime) -> None:
        today = now.strftime('%d.%m.%Y')
        classes = self.parser.schedule.get(today, [])

        for cls in classes:
            key = self._generate_key(cls)
            if key not in self.active_polls:
                class_dt = datetime.datetime.strptime(f"{cls['date']} {cls['start_time']}", '%d.%m.%Y %H:%M')
                if 0 <= (class_dt - now).total_seconds() <= self.poll_window:
                    await self._send_poll(cls, key)

        await self._close_expired_polls(now)

    async def _send_poll(self, cls: dict[str, Any], key: str) -> None:
        # extract info from class dict
        api_class_name = cls['class_name']
        api_class_type = cls['class_type']
        start_time = cls['start_time']
        end_time = cls['end_time']

        options_default = ['Да', 'Нет', 'Пикма', 'На больничном']
        options_nmg = [*options_default, 'Не моя группа']

        # resolve class name to be used in the poll
        alias_map = self.discipline_settings[2]
        final_class_name = alias_map.get(api_class_name, api_class_name)

        # determine if poll will contain "Не моя группа" option
        nmg_map = self.discipline_settings[1]
        is_nmg = api_class_name in nmg_map and nmg_map.get(api_class_name) == api_class_type
        options = options_nmg if is_nmg else options_default

        question = f'{final_class_name} в {start_time} - {end_time}'

        async with self._semaphore:
            try:
                msg = await self.bot.send_poll(
                    chat_id=self.chat_id,
                    question=question,
                    options=options,
                    is_anonymous=False,
                    allows_multiple_answers=False,
                )
                close_time = self._calculate_close_time(cls)
                record = {
                    'poll_id': msg.poll.id,
                    'message_id': msg.message_id,
                    'class_info': cls,
                    'close_time': close_time,
                    'responses': '[]'
                }
                self.active_polls[key] = record
                await self.storage.save_active_polls(record['poll_id'], record)
                logger.info(f'Sent poll: {key}')
            except Exception as e:
                logger.error(f'Error sending poll {key}: {e}')

    def _calculate_close_time(self, cls: dict[str, Any]) -> datetime.datetime:
        date = datetime.datetime.strptime(cls['date'], '%d.%m.%Y').date()
        start = datetime.datetime.strptime(cls['start_time'], '%H:%M').time()
        end = datetime.datetime.strptime(cls['end_time'], '%H:%M').time()

        if end <= start:
            date += datetime.timedelta(days=1)

        return datetime.datetime.combine(date, datetime.time(23, 59))

    async def _close_expired_polls(self, now: datetime.datetime) -> None:
        expired_keys = []
        for key, info in list(self.active_polls.items()):
            if now >= info['close_time']:
                async with self._semaphore:
                    try:
                        await self._close_poll(info)
                    except exceptions.TelegramBadRequest:
                        pass
                    await self.storage.archive_poll(info['poll_id'])

                expired_keys.append(key)

        for key in expired_keys:
            self.active_polls.pop(key, None)

    async def _close_poll(self, info: dict[str, Any]) -> bool:
        poll_id = info['poll_id']
        try:
            async with self._semaphore:
                await self.bot.stop_poll(chat_id=self.chat_id, message_id=info['message_id'])
        except exceptions.TelegramBadRequest as e:
            err_msg = str(e)

            # poll already closed -> archive
            if 'has already been closed' in err_msg:
                logger.warning(f'Poll {poll_id} already closed: {err_msg}')
                try:
                    await self.storage.archive_poll(poll_id)
                    logger.info(f'Archived already closed poll {poll_id}')
                except Exception as err:
                    logger.error(f'Error archiving already closed poll {poll_id}: {err}')
                return True

            # message not found -> delete poll from db
            if 'message with poll to stop not found' in err_msg:
                logger.warning(f'Poll {poll_id} not found; deleting from storage')
                try:
                    await self.storage.delete_poll(poll_id)
                    logger.info(f'Deleted poll {poll_id} from both active and past tables')
                except Exception as err:
                    logger.error(f'Error deleting poll {poll_id}: {err}')
                return True

            logger.error(f'BadRequest closing poll {poll_id}: {err_msg}')
            return False

        try:
            await self.storage.archive_poll(poll_id)
            logger.info(f'Closed and archived poll {poll_id}')
            return True
        except Exception as err:
            logger.error(f'Error archiving poll {poll_id}: {err}')
            return False

    async def close(self):
        self._running = False
        await self.session.close()
        await self.storage.close()
