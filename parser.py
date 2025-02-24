import asyncio
import os
from collections import defaultdict
import datetime
import logging
from typing import Any

import requests

import test_schedule


class ScheduleParser:
    def __init__(self, group_id: int):
        self.logger = logging.getLogger(__name__)
        self.api_endpoint = 'https://tulsu.ru/schedule/queries/GetSchedule.php'
        self.group_id = group_id
        self.raw_schedule: list[dict[str, Any]] = []
        self.sorted_schedule: list[dict[str, Any]] = []
        self.schedule: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)

    async def fetch_schedule(self):
        try:
            if os.getenv('TEST_MODE', 'False').lower() in ('true', '1'):
                web_schedule = test_schedule.get_test_schedule()
                self.raw_schedule = [self.parse_class(class_data) for class_data in web_schedule]
                print(self.raw_schedule)
                self.logger.info('Test schedule loaded.')
            else:
                params = {
                    'search_field': 'GROUP_P',
                    'search_value': self.group_id
                }
                response = await asyncio.to_thread(requests.get, self.api_endpoint, timeout=10, params=params)
                response.raise_for_status()
                web_schedule = response.json()
                print(web_schedule)
                self.raw_schedule = [self.parse_class(class_data) for class_data in web_schedule]
                self.logger.info('Schedule updated successfully.')
            self.sort_and_group_classes()
        except Exception as e:
            self.logger.error(f'Error updating schedule: {e}')


    def sort_and_group_classes(self):
        try:
            self.sorted_schedule = sorted(
                self.raw_schedule,
                key=lambda cls: (
                    datetime.datetime.strptime(cls['date'], '%d.%m.%Y'),
                    datetime.datetime.strptime(cls['start_time'], '%H:%M'),
                    datetime.datetime.strptime(cls['end_time'], '%H:%M')
                )
            )
        except Exception as e:
            self.logger.error(f'Error sorting schedule: {e}')
            return

        self.schedule.clear()
        for class_dict in self.sorted_schedule:
            date = class_dict.get('date')
            if date:
                self.schedule[date].append(class_dict)
                del class_dict['date']

    def parse_class(self, class_data: dict[str, Any]) -> dict[str, Any]:
        time_range = class_data.get('TIME_Z', '')
        time_range = time_range.split(' - ') if time_range else ''
        start_time = time_range[0]
        end_time = time_range[1]

        return {
            'date': class_data.get('DATE_Z', ''),
            'start_time': start_time,
            'end_time': end_time,
            'class_name': class_data.get('DISCIP', ''),
            'prof': class_data.get('PREP', ''),
            'room': class_data.get('AUD', ''),
            'class_type': class_data.get('KOW', '')
        }
