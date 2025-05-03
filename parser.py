import os
from collections import defaultdict
import logging
from datetime import datetime as dt, time
from typing import Any, Final

import aiohttp

import test_schedule


class ScheduleParser:
    API_ENDPOINT: Final[str] = 'https://tulsu.ru/schedule/queries/GetSchedule.php'
    DATE_FORMAT: Final[str] = '%d.%m.%Y'
    TIME_FORMAT: Final[str] = '%H:%M'

    def __init__(self, group_id: int, session: aiohttp.ClientSession):
        self.logger = logging.getLogger(__name__)
        self.group_id = group_id
        self.session = session
        self.schedule = defaultdict(list)

    async def fetch(self) -> dict[str, list[dict[str, Any]]]:
        self.logger.info('Fetching schedule...')
        raw_list: list[dict[str, Any]] = []  # TODO: add last schedule saving

        # TEST MODE (classes 2 min from bot launch)
        if os.getenv('TEST_MODE', 'False').lower() == 'true':
            raw_list = test_schedule.get_test_schedule()
        else:
            # real API fetch
            params = {'search_field': 'GROUP_P', 'search_value': self.group_id}
            try:
                async with self.session.get(self.API_ENDPOINT, params=params, timeout=10) as response:
                    response.raise_for_status()
                    raw_list = await response.json()
            except aiohttp.web.HTTPException as e:
                self.logger.error(f'Schedule fetch failed: {e}')

        parsed = [self._parse_raw(item) for item in raw_list]
        parsed.sort(key=self._sort_key)

        grouped: dict[str, list[dict[str, Any]]] = {}
        for entry in parsed:
            grouped.setdefault(entry['date'], []).append(entry)

        self.schedule = grouped
        self.logger.info('Schedule updated.')

        return grouped

    def _parse_raw(self, raw: dict[str, Any]) -> dict[str, Any]:
        date = raw.get('DATE_Z', '')
        time_range = raw.get('TIME_Z', '')
        start_time, end_time = time_range.split(' - ')

        class_name = raw.get('DISCIP', '')
        shortened_class_name_map = {
            'Иностранный язык': 'Ин. яз.',
            'Современные информационные системы и технологии': 'СИСИТ',
            'Физическая культура и спорт (элективные модули)': 'Физра',
            'Программирование': 'Прога',
            'Тайм-менеджмент и селф-менеджмент': 'ТМ',
            'Технологии самоорганизации и саморазвития личности': 'Самоорганизация',
            'Введение в математический анализ': 'Матан',
            'Основы информационной безопасности': 'ОиБ',
            'Психология лидерства и командной работы': 'Психология',
            'История России': 'История',
            'Алгебра и геометрия': 'Алгебра',
            'Культура речи и нормы делового взаимодействия': 'Культура речи',
            'Теория алгоритмов и структуры данных': 'Алгоритмы',
            'Философия и методология мышления': 'Философия'
        }
        class_name = shortened_class_name_map.get(class_name, class_name)

        return {
            'date': date,
            'start_time': start_time,
            'end_time': end_time,
            'class_name': class_name,
            'prof': raw.get('PREP', ''),
            'room': raw.get('AUD', ''),
            'class_type': raw.get('KOW', ''),
        }

    def _sort_key(self, entry: dict[str, Any]) -> tuple[dt, time, time]:
        try:
            date = dt.strptime(entry['date'], self.DATE_FORMAT)
            start = dt.strptime(entry['start_time'], self.TIME_FORMAT).time()
            end = dt.strptime(entry['end_time'], self.TIME_FORMAT).time()
            return date, start, end
        except (ValueError, KeyError) as e:
            self.logger.warning(f'Sorting error for entry {entry}: {e}')
            return dt.max, time.max, time.max
