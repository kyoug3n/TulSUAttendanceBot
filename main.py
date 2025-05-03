import asyncio
import contextlib
import io
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Optional

import pandas as pd
from aiogram import Bot, Dispatcher, Router, exceptions, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from dotenv import load_dotenv
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

from scheduler import Scheduler
from storage import StorageManager

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()
dp = Dispatcher()
dp.include_router(router)
storage = StorageManager()

NAME_PATTERN = re.compile(r'^[А-Яа-яЁё-]+$')


class Registration(StatesGroup):
    last_name = State()
    first_name = State()


def valid_name(name: str) -> bool:
    return bool(
        NAME_PATTERN.fullmatch(name.strip()) and
        len(name.strip().split()) == 1
    )


@router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    await state.clear()

    user_id = str(message.from_user.id)
    user = await storage.get_user(user_id)

    if message.chat.type == 'private' and (not user or not user.get('registered', False)):
        await message.answer('Введите вашу фамилию (одно слово на кириллице):')
        await state.set_state(Registration.last_name)
    else:
        start_time = time.monotonic()
        response = await message.answer('Bot running!')
        ping = (time.monotonic() - start_time) * 1000

        await response.edit_text(
            f'Bot running!\n'
            f'Current ping: {ping:.0f} ms\n'
            f'(Жду следующей пары...)'
        )


@router.message(Command('edit_name'))
async def cmd_edit_name(message: types.Message, state: FSMContext) -> None:
    await state.clear()

    if message.chat.type == 'private':
        await message.answer('Введите вашу фамилию (одно слово на кириллице):')
        await state.set_state(Registration.first_name)


@router.message(Command('display_name'))
async def cmd_display_name(message: types.Message, state: FSMContext) -> None:
    await state.clear()

    user_id = str(message.from_user.id)
    user = await storage.get_user(user_id)

    ln = user.get('last_name', 'не указана') if user else 'не указана'
    fn = user.get('first_name', 'не указано') if user else 'не указано'

    await message.answer(f'Ваше имя: {ln} {fn}.')


@router.message(Registration.last_name)
async def handle_last_name(message: types.Message, state: FSMContext) -> Optional[types.Message]:
    ln = message.text.strip()
    if not valid_name(ln):
        return await message.answer(
            'Фамилия должна состоять из одного слова на кириллице. Пожалуйста, введите корректную фамилию:'
        )

    await state.update_data(first_name=ln)
    await message.answer('Отлично! Теперь введите ваше имя (одно слово на кириллице):')

    return await state.set_state(Registration.first_name)


@router.message(Registration.first_name)
async def handle_first_name(message: types.Message, state: FSMContext) -> Optional[types.Message]:
    fn = message.text.strip()
    if not valid_name(fn):
        return await message.answer(
            'Имя должно состоять из одного слова на кириллице. Пожалуйста, введите корректное имя:'
        )

    data = await state.get_data()
    user_id = str(message.from_user.id)

    await storage.update_user(user_id, {
        "username": message.from_user.username,
        "last_name": data['last_name'],
        "first_name": fn,
        "registered": True
    })

    await message.answer(f"Спасибо, {data['last_name']} {fn}! Ваши данные сохранены.")
    await state.clear()


@router.message(Command('export_attendance'))
async def cmd_export_attendance(message: types.Message, state: FSMContext) -> Optional[types.Message]:
    admins: list[int] = [int(x.strip()) for x in str(os.getenv('ATTENDANCE_ACCESS'))[1:-1].split(',')]

    if not (message.chat.type == 'private' and message.from_user.id in admins):
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

    await message.answer(f"Генерирую отчёт за {year}-{month:02d}…")

    polls = await storage.get_past_polls_by_month(year, month)
    if not polls:
        return await message.answer("Нет данных за этот период.")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:  # type: ignore
        df_polls = pd.DataFrame(polls)
        for date_val, group in df_polls.groupby('date', sort=True):
            date_str = str(date_val)
            sheet_name = date_str.replace('.', '-')[:31]

            records: dict[str, dict[str,str]] = {}
            classes: set[str] = set()

            for _, row in group.iterrows():
                cls = f'{row['class_name']} ({row["start_time"]} - {row["end_time"]})'
                classes.add(cls)
                for resp in json.loads(row['responses']):
                    name = resp['last_name'] + ' ' + resp['first_name']
                    opt = resp['option_ids'][0] if resp['option_ids'] else None
                    mark = {0: 'Д', 1: 'Н', 2: 'П', 3: 'Б'}.get(opt, '')
                    records.setdefault(name, {})[cls] = mark

            classes_list = sorted(classes)
            table = []
            for student, answers in records.items():
                row_dict = {'Имя': student}
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
                adjusted_width = max_length + 6
                col_letter = get_column_letter(idx)
                ws.column_dimensions[col_letter].width = adjusted_width

            align = Alignment(horizontal='center', vertical='center')
            for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
                for cell in row:
                    cell.alignment = align

    output.seek(0)
    data = output.getvalue()
    file = types.BufferedInputFile(data, filename=f"attendance_{year}-{month:02d}.xlsx")
    return await message.answer_document(file)


def register_handlers(router_instance: Router, storage_instance: StorageManager, scheduler_instance: Scheduler):
    @router_instance.poll_answer()
    async def handle_poll_answer(poll_answer: types.PollAnswer):
        pid = poll_answer.poll_id
        uid = poll_answer.user.id
        uid_str = str(uid)
        opts = poll_answer.option_ids

        if local_user_info := await storage_instance.get_user(uid_str):
            fn, ln = local_user_info.get('first_name', ''), local_user_info.get('last_name', '')
        else:
            fn, ln = poll_answer.user.first_name, poll_answer.user.last_name or ''
        username = f'@{poll_answer.user.username}' if poll_answer.user.username else ''

        await storage_instance.update_poll_response(
            poll_id=pid,
            user_id=uid_str,
            option_ids=opts,
            first_name=fn,
            last_name=ln,
            username=username
        )

        for rec in scheduler_instance.active_polls.values():
            if rec['poll_id'] == pid:
                resps = json.loads(rec['responses'])
                resps.append({
                    'user_id': uid_str,
                    'first_name': fn,
                    'last_name': ln,
                    'username': username,
                    'option_ids': opts
                })
                rec['responses'] = json.dumps(resps)
                break


@router.errors()
async def error_handler(update: types.Update, exception: Exception = None) -> None:
    logger.exception(f'Update {update} caused error {exception!r}')


async def main() -> None:
    load_dotenv()
    token = os.getenv('TOKEN')
    chat_id = int(os.getenv('CHAT_ID'))
    group_id = int(os.getenv('GROUP_ID'))
    poll_interval = float(os.getenv('POLL_CHECK_INTERVAL', '60'))
    update_interval = float(os.getenv('SCHEDULE_UPDATE_INTERVAL', '3600'))
    poll_window = float(os.getenv('POLL_CLOSURE_WINDOW', '300'))

    if not token or token == 'token':
        logger.critical('Environment variable TOKEN is not set.')
        return

    await storage.connect()

    bot = Bot(token=token)
    scheduler = Scheduler(
        bot=bot,
        chat_id=chat_id,
        group_id=group_id,
        storage=storage,
        poll_interval=poll_interval,
        update_interval=update_interval,
        poll_window=poll_window
    )
    register_handlers(router, storage, scheduler)

    async def validate_chat_id() -> bool:
        try:
            chat = await bot.get_chat(chat_id)
            return chat.type in ('group', 'supergroup')
        except exceptions.AiogramError:
            return False

    if not await validate_chat_id():
        logger.critical(f'Invalid chat ID: {chat_id}')
        return

    task: Optional[asyncio.Task] = None

    try:
        task = asyncio.create_task(scheduler.start())
        await dp.start_polling(bot)
    except Exception as e:
        logger.exception(f'Unexpected error: {e}')
    finally:
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        await scheduler.shutdown()
        await bot.session.close()
        await storage.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Bot stopped by user.')
