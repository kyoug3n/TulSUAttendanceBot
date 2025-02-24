import os
import json
import asyncio
import datetime
import logging

from parser import ScheduleParser


class Scheduler:
    def __init__(self, bot, chat_id):
        self.bot = bot
        self.chat_id: int = chat_id
        self.logger = logging.getLogger(__name__)
        self.active_polls: dict[str, dict] = {} # format: date|start_time|class_name
        self.polls_file: str = 'active_polls.json'
        self.schedule_parser = ScheduleParser(self.chat_id)
        self.load_active_polls()

    def load_active_polls(self):
        if os.path.exists(self.polls_file):
            try:
                with open(self.polls_file, 'r') as f:
                    data = json.load(f)

                for key, poll_info in data.items():
                    if 'close_time' in poll_info:
                        poll_info['close_time'] = datetime.datetime.fromisoformat(poll_info['close_time'])
                self.active_polls = data
                self.logger.info('Active polls loaded.')
            except Exception as e:
                self.logger.error(f'Error loading active polls: {e}')
                self.active_polls = {}
        else:
            self.active_polls = {}

    def save_active_polls(self):
        try:
            save_buffer = {}

            for key, poll_info in self.active_polls.items():
                poll_info_copy = poll_info.copy()
                if 'close_time' in poll_info_copy and isinstance(poll_info_copy['close_time'], datetime.datetime):
                    poll_info_copy['close_time'] = poll_info_copy['close_time'].isoformat()
                save_buffer[key] = poll_info_copy

            with open(self.polls_file, 'w') as f:
                json.dump(save_buffer, f)
        except (IOError, OSError) as e:
            self.logger.error(f'Error writing to file: {e}')
        except Exception as e:
            self.logger.error(f'Error saving active polls: {e}')

    async def fetch_schedule_task(self):
        while True:
            await self.schedule_parser.fetch_schedule()
            await asyncio.sleep(1800)

    async def check_classes_task(self):
        while True:
            now = datetime.datetime.now()
            current_date = now.strftime('%d.%m.%Y')
            await self._process_upcoming_classes(now, current_date)
            await self._update_expired_polls(now)
            await asyncio.sleep(60)

    async def _process_upcoming_classes(self, now: datetime.datetime, current_date: str):
        classes = self.schedule_parser.schedule.get(current_date, [])
        for cls in classes:
            key = f'{current_date}|{cls['start_time']}|{cls['class_name']}'
            if key in self.active_polls:
                continue

            try:
                class_time = datetime.datetime.strptime(
                    f'{current_date} {cls['start_time']}', '%d.%m.%Y %H:%M'
                )
            except Exception as e:
                self.logger.error(f'Error parsing class time: {e}')
                continue

            if 0 <= (class_time - now).total_seconds() <= 300 and key not in self.active_polls:
                await self._send_poll(current_date, cls, key)


    async def _send_poll(self, current_date: str, cls: dict, key: str):
        question = f'{cls['class_name']} в {cls['start_time']} - {cls['end_time']}'
        options = ['Да', 'Сосал']
        try:
            message = await self.bot.send_poll(
                chat_id=self.chat_id,
                question=question,
                options=options,
                is_anonymous=False,
                allows_multiple_answers=False,
            )

            class_date = datetime.datetime.strptime(current_date, '%d.%m.%Y').date()
            start_time_obj = datetime.datetime.strptime(cls['start_time'], "%H:%M").time()
            end_time_obj = datetime.datetime.strptime(cls['end_time'], "%H:%M").time()

            start_datetime = datetime.datetime.combine(class_date, start_time_obj)
            close_datetime = datetime.datetime.combine(class_date, end_time_obj)

            if close_datetime <= start_datetime:
                close_datetime += datetime.timedelta(days=1)

            poll_info = {
                'poll_id': message.poll.id,
                'message_id': message.message_id,
                'class_info': cls,
                'close_time': close_datetime,
                'date': current_date
            }
            self.active_polls[key] = poll_info
            self.save_active_polls()
            self.logger.info(f'Poll created for class {key}')
        except Exception as e:
            self.logger.error(f'Error sending poll for class {key}: {e}')

    async def _update_expired_polls(self, now: datetime.datetime):
        expired_keys = []
        for poll_key, poll_info in self.active_polls.items():
            if now >= poll_info['close_time']:
                await self._close_poll(poll_key, poll_info)
                expired_keys.append(poll_key)
        for key in expired_keys:
            del self.active_polls[key]
        if expired_keys:
            self.save_active_polls()

    async def _close_poll(self, poll_key: str, poll_info: dict):
        try:
            stopped_poll = await self.bot.stop_poll(
                chat_id=self.chat_id,
                message_id=poll_info['message_id']
            )
            total_members = await self.bot.get_chat_member_count(chat_id=self.chat_id)
            votes = {option.text: option.voter_count for option in stopped_poll.options}
            answered = sum(votes.values())
            didnt_attend = votes.get('Сосал', 0)
            not_answered = total_members - answered

            stats_text = (
                f'Результаты опроса для пары:\n'
                f'{poll_info['class_info']['class_name']} в {poll_info['class_info']['start_time']}\n\n'
                f'Проголосовало: {answered} из {total_members}\n'
                f'Не посетили: {didnt_attend}\n'
                f'Не ответили: {not_answered}'
            )
            await self.bot.send_message(
                chat_id=self.chat_id,
                reply_to_message_id=poll_info['message_id'],
                text=stats_text
            )
            self.logger.info(f'Poll closed for class {poll_key}')
        except Exception as e:
            self.logger.error(f'Error closing poll for class {poll_key}: {e}')

    async def start(self):
        await asyncio.gather(
            self.fetch_schedule_task(),
            self.check_classes_task()
        )