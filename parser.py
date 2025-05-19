import logging
from collections import defaultdict
from datetime import datetime as dt, time
from typing import Any, Final

import aiohttp

from storage import StorageManager
from test_schedule import get_test_schedule


logger = logging.getLogger(__name__)


class ScheduleParser:
    SCHEDULE_ENDPOINT: Final[str] = 'https://tulsu.ru/schedule/queries/GetSchedule.php'
    DATE_FORMAT: Final[str] = '%d.%m.%Y'
    TIME_FORMAT: Final[str] = '%H:%M'

    def __init__(
            self, config,
            session: aiohttp.ClientSession, storage: StorageManager,
            discipline_settings: tuple[list, dict, dict]
    ):
        self.config = config
        self.session = session
        self.storage = storage
        self.discipline_settings = discipline_settings

        self.group_id = self.config.group_id
        self.schedule: dict[str, list[dict[str, Any]]] = defaultdict(list)

    async def fetch(self) -> dict[str, list[dict[str, Any]]]:
        logger.info(f'Fetching schedule for group {self.group_id}...')

        raw_list: list[dict[str, Any]] = await self._retrieve_raw()
        parsed = []

        for item in raw_list:
            entry = self._parse_sched_entry(item)
            if entry is not None:
                parsed.append(entry)
        parsed.sort(key=self._sort_key)

        grouped: dict[str, list[dict[str, Any]]] = {}
        for entry in parsed:
            if self.config.include_exams or entry['class_type'] != 'default':  # "default" = exams
                grouped.setdefault(entry['date'], []).append(entry)

        self.schedule = grouped
        logger.info('Schedule updated.')

        return grouped

    async def _retrieve_raw(self) -> list[dict[str, Any]]:
        # classes 2 min from bot launch or periodic schedule refresh
        if self.config.test_mode:
            logger.info('TEST MODE ENABLED! Using mock schedule.')
            return get_test_schedule()

        params = {'search_field': 'GROUP_P', 'search_value': self.group_id}
        try:
            async with self.session.get(self.SCHEDULE_ENDPOINT, params=params, timeout=10) as resp:
                resp.raise_for_status()
                if data := await resp.json():
                    await self.storage.save_last_schedule(self.group_id, data)
                return data

        except aiohttp.web.HTTPError as e:
            logger.error(f'Schedule fetch failed: {e}')
            if cache := await self.storage.get_last_schedule(self.group_id):
                logger.info('Loaded schedule fallback from DB cache')
                return cache
            return []

    def _parse_sched_entry(self, raw: dict[str, Any]) -> dict[str, str] | None:
        time_range: str = raw.get('TIME_Z', '')
        start_time, end_time = time_range.split(' - ')
        class_name = raw.get('DISCIP', '').strip()

        if class_name in self.discipline_settings[0]:
            return None

        return {
            'date': raw.get('DATE_Z', ''),
            'start_time': start_time,
            'end_time': end_time,
            'class_name': class_name,
            'prof': raw.get('PREP', ''),
            'room': raw.get('AUD', ''),
            'class_type': raw.get('CLASS', '')  # values: "lecture", "practice", "lab", "default". "default" = exams
        }

    def _sort_key(self, entry: dict[str, Any]) -> tuple[dt, time, time]:
        try:
            date = dt.strptime(entry['date'], self.DATE_FORMAT)
            start = dt.strptime(entry['start_time'], self.TIME_FORMAT).time()
            end = dt.strptime(entry['end_time'], self.TIME_FORMAT).time()
            return date, start, end
        except (ValueError, KeyError) as e:
            logger.warning(f'Sorting error for entry {entry}: {e}')
            return dt.max, time.max, time.max
