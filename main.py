import asyncio
import contextlib
import io
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
from aiogram import Bot, Dispatcher, Router, exceptions, types
from aiogram.filters import Command, Filter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message
from dotenv import load_dotenv
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

from scheduler import Scheduler
from storage import StorageManager


# ----- Configuration -----
@dataclass(frozen=True)
class Config:
    token: str
    chat_id: int
    group_id: int
    admin_ids: list[int]
    poll_interval: float = 60.0
    update_interval: float = 3600.0
    poll_window: float = 300.0

    @staticmethod
    def _parse_admin_ids(raw: str) -> list[int]:
        try:
            return [int(x.strip()) for x in raw.strip('[]').split(',') if x.strip()]
        except ValueError:
            return []

    @classmethod
    def from_env(cls) -> 'Config':
        load_dotenv()
        token = os.getenv('TOKEN', '')
        if not token or token == 'token':
            raise RuntimeError('Environment variable TOKEN is invalid.')

        chat_id = int(os.getenv('CHAT_ID', '0'))
        group_id = int(os.getenv('GROUP_ID', '0'))
        admin_ids = cls._parse_admin_ids(os.getenv('ATTENDANCE_ACCESS', '[]'))
        poll_interval = float(os.getenv('POLL_CHECK_INTERVAL', '60'))
        update_interval = float(os.getenv('SCHEDULE_UPDATE_INTERVAL', '3600'))
        poll_window = float(os.getenv('POLL_CLOSURE_WINDOW', '300'))

        return cls(
            token=token,
            chat_id=chat_id,
            group_id=group_id,
            admin_ids=admin_ids,
            poll_interval=poll_interval,
            update_interval=update_interval,
            poll_window=poll_window
        )


# ----- Logging -----
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----- Validators -----
NAME_PATTERN = re.compile(r'^[А-Яа-яЁё-]+$')


def valid_name(name: str) -> bool:
    name = name.strip()
    return bool(NAME_PATTERN.fullmatch(name))


async def validate_chat_id(bot: Bot, config: Config) -> None:
    try:
        chat = await bot.get_chat(config.chat_id)
        if chat.type not in ('group', 'supergroup'):
            raise RuntimeError(
                f'Only groups and supergroups are supported. Invalid chat type: {chat.type}, ID: {chat.id}'
            )
    except exceptions.AiogramError as e:
        raise RuntimeError(f'Could not fetch chat info: {e}')

    return None


class PrivateChatFilter(Filter):
    async def __call__(self, message: types.Message) -> bool:
        if message.chat.type != 'private':
            await message.answer('Эта команда доступна только в личных сообщениях.')
            return False
        return True


# ----- FSM States -----
class Registration(StatesGroup):
    last_name = State()
    first_name = State()


# ----- Main Bot Class -----
class AttendanceBot:
    def __init__(self, config: Config):
        self.config = config
        self.router = Router()
        self.dispatcher = Dispatcher()
        self.dispatcher.include_router(self.router)
        self.bot: Bot | None = None
        self.scheduler: Scheduler | None = None
        self.storage = StorageManager()

        self._setup_routes()

    def _setup_routes(self) -> None:
        self.router.message(Command('start'))(self._on_start)
        self.router.message(Command('edit_name'), PrivateChatFilter())(self._on_edit_name)
        self.router.message(Command('display_name'), PrivateChatFilter())(self._on_display_name)
        self.router.message(Registration.last_name)(self._on_last_name)
        self.router.message(Registration.first_name)(self._on_first_name)
        self.router.message(Command('export_attendance'), PrivateChatFilter())(self._on_export_attendance)

        self.router.poll_answer()(self._on_poll_answer)

        self.router.errors()(self._on_error)

    # ----- Helper Methods -----
    @staticmethod
    def _build_report(polls: list[dict[str, Any]], year: int, month: int) -> types.InputFile:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:  # type: ignore
            df = pd.DataFrame(polls)
            for date_val, group in df.groupby('date'):
                sheet_name = str(date_val).replace('.', '-')[:31]

                records: dict[str, dict[str, str]] = {}
                classes: set[str] = set()

                for _, row in group.iterrows():
                    cls = f"{row['class_name']} ({row['start_time']} - {row['end_time']})"

                    if cls in classes:
                        cls = f"{cls} ({row['class_room']})"

                    classes.add(cls)
                    for resp in json.loads(row['responses']):
                        name = f"{resp['last_name']} {resp['first_name']}"
                        opt = resp['option_ids'][0] if resp['option_ids'] else None
                        mark = {0: 'Д', 1: 'Н', 2: 'П', 3: 'Б'}.get(opt, '')
                        records.setdefault(name, {})[cls] = mark

                classes_list = sorted(classes)
                table = []
                for student_name, answers in records.items():
                    row_dict = {'Имя': student_name}
                    for cls in classes_list:
                        row_dict[cls] = answers.get(cls, '')
                    table.append(row_dict)

                df_day = pd.DataFrame(table, columns=['Имя'] + classes_list)
                df_day = df_day.sort_values(by='Имя', ascending=False)
                df_day.to_excel(writer, sheet_name=sheet_name, index=False)

                ws = writer.sheets[sheet_name]
                for idx, col in enumerate(df_day.columns, 1):
                    max_length = max(
                        df_day[col].astype(str).map(len).max(),
                        len(str(col))
                    )
                    adjusted_width = max_length + 3
                    col_letter = get_column_letter(idx)
                    ws.column_dimensions[col_letter].width = adjusted_width

                align = Alignment(horizontal='center', vertical='center')
                for row in ws.iter_rows(1, ws.max_row, 1, ws.max_column):
                    for cell in row:
                        cell.alignment = align

        output.seek(0)
        return types.BufferedInputFile(output.getvalue(), filename=f"attendance_{year}-{month:02d}.xlsx")

    # ----- Route Handlers -----
    async def _on_start(self, message: types.Message, state: FSMContext) -> None:
        await state.clear()
        user_id = str(message.from_user.id)
        user = await self.storage.get_user(user_id)

        if message.chat.type == 'private' and not (user and user.get('registered')):
            await message.answer('Введите вашу фамилию (одно слово на кириллице):')
            await state.set_state(Registration.last_name)
        else:
            start_time = time.monotonic()
            response = await message.answer('Бот запущен!')
            ping = (time.monotonic() - start_time) * 1000

            await response.edit_text(
                f'Бот запущен!\n'
                f'Пинг: {ping:.0f} мс\n'
                f'(Жду следующей пары...)'
            )

    async def _on_edit_name(self, message: types.Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer('Введите вашу фамилию (одно слово на кириллице):')
        return await state.set_state(Registration.last_name)

    async def _on_display_name(self, message: types.Message, state: FSMContext) -> Message:
        last_not_specified = 'не указана'
        first_not_specified = 'не указано'

        await state.clear()
        user_id = str(message.from_user.id)
        user = await self.storage.get_user(user_id)

        last = user.get('last_name', last_not_specified) if user else last_not_specified
        first = user.get('first_name', first_not_specified) if user else first_not_specified

        if last == last_not_specified and first == first_not_specified:
            return await message.answer('Ваше имя не указано.')

        return await message.answer(f'Ваше имя: {last} {first}.')

    async def _on_last_name(self, message: types.Message, state: FSMContext) -> types.Message | None:
        last = message.text
        if not valid_name(last):
            return await message.answer(
                'Фамилия должна состоять из одного слова на кириллице. Пожалуйста, введите корректную фамилию:'
            )

        await state.update_data(last_name=last)
        await message.answer('Отлично! Теперь введите ваше имя (одно слово на кириллице):')

        return await state.set_state(Registration.first_name)

    async def _on_first_name(self, message: types.Message, state: FSMContext) -> Message | None:
        first = message.text
        if not valid_name(first):
            return await message.answer(
                'Имя должно состоять из одного слова на кириллице. Пожалуйста, введите корректное имя:'
            )

        data = await state.get_data()
        user_id = str(message.from_user.id)

        await self.storage.update_user(user_id, {
            "username": message.from_user.username,
            "last_name": data['last_name'],
            "first_name": first,
            "registered": True
        })

        await message.answer(f"Спасибо, {data['last_name']} {first}! Ваши данные сохранены.")
        return await state.clear()

    async def _on_export_attendance(self, message: types.Message, state: FSMContext) -> Message | None:
        if state:
            await state.clear()

        user_id = message.from_user.id
        if user_id not in self.config.admin_ids:
            await message.answer('Эта команда доступна только администраторам.')
            return await state.clear()

        parts = message.text.strip().split()
        now = datetime.now()
        year, month = now.year, now.month

        if len(parts) > 1:
            try:
                year, month = map(int, parts[1].split('-', 1))
            except ValueError:
                return await message.answer("Используйте формат: /export_attendance YYYY-MM")

        await message.answer(f'Генерирую отчёт за {year}-{month:02d}…')

        polls = await self.storage.get_past_polls_by_month(year, month)
        if not polls:
            return await message.answer('Нет данных за этот период.')

        file = self._build_report(polls, year, month)
        return await message.answer_document(file)

    async def _on_poll_answer(self, poll_answer: types.PollAnswer) -> None:
        user = poll_answer.user

        rec = {
            'user_id': str(user.id),
            'first_name': user.first_name,
            'last_name': user.last_name or '',
            'username': f"@{user.username}" if user.username else '',
            'option_ids': poll_answer.option_ids
        }
        await self.storage.update_poll_response(
            rec['user_id'], rec['option_ids'], rec['first_name'], rec['last_name'], rec['username']
        )
        for entry in self.scheduler.active_polls.values():
            if entry['poll_id'] == poll_answer.poll_id:
                responses = json.loads(entry['responses'])
                responses.append(rec)
                entry['responses'] = json.dumps(responses)
                break

    async def _on_error(self, update: types.Update, exception: Exception | None = None) -> None:
        logger.exception(f'Error for update {update}: {exception!r}')

    async def run(self) -> None:
        await self.storage.connect()
        self.bot = Bot(token=self.config.token)
        self.scheduler = Scheduler(
            bot=self.bot,
            chat_id=self.config.chat_id,
            group_id=self.config.group_id,
            storage=self.storage,
            poll_interval=self.config.poll_interval,
            update_interval=self.config.update_interval,
            poll_window=self.config.poll_window
        )

        try:
            await validate_chat_id(self.bot, self.config)
        except RuntimeError as e:
            logger.critical(e)
            return None

        scheduler_task = asyncio.create_task(self.scheduler.start())
        try:
            await self.dispatcher.start_polling(self.bot)
        finally:
            scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await scheduler_task

        await self.scheduler.shutdown()
        await self.bot.session.close()
        await self.storage.close()


if __name__ == '__main__':
    try:
        attendance_bot = AttendanceBot(Config.from_env())
        asyncio.run(attendance_bot.run())
    except KeyboardInterrupt:
        logger.info('Bot stopped by user.')
