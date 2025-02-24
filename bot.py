import asyncio
import logging
import os
import time

from scheduler import Scheduler

from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from dotenv import load_dotenv


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()
dp = Dispatcher()
dp.include_router(router)


@router.message(Command('start'))
async def start(message: types.Message) -> None:
    start_time = time.monotonic()
    response = await message.answer('Bot running!')
    end_time = time.monotonic()

    ping = (end_time - start_time) * 1000

    await response.edit_text(f'Bot running!\n'
                             f'Current ping: {ping:.0f} ms\n'
                             f'(Жду следующей пары...)')


@router.errors()
async def error_handler(update: types.Update, exception: Exception):
    logger.exception(f'Update {update} caused error {exception}')


async def main():
    load_dotenv()
    token = os.getenv('TOKEN')
    chat_id = int(os.getenv('CHAT_ID'))
    if not token or token == 'token':
        logger.critical('Environment variable TOKEN is not set.')
        return

    bot = Bot(token=token)

    scheduler_instance = Scheduler(bot, chat_id)
    asyncio.create_task(scheduler_instance.start())

    logger.info('Bot is running...')

    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.exception(f'Unexpected error: {e}')
    finally:
        await bot.session.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Bot stopped by user.')
