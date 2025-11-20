import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramRetryAfter

from config import BOT_TOKEN
from database import init_db
from executor_handlers import router as executor_router
from logging_config import setup_logging
from planfix_client import planfix_client
from services.status_registry import ensure_status_registry_loaded
from rate_limit_middleware import RateLimitMiddleware
from user_handlers import router as user_router

setup_logging()
logger = logging.getLogger(__name__)

async def on_startup(bot: Bot):
    """Инициализация при старте бота."""
    logger.info("Bot startup complete. Running in polling mode.")
    logger.info("⚠️ Автоматический polling Planfix отключен. Запросы выполняются только при действиях пользователей.")
    try:
        await ensure_status_registry_loaded()
        logger.info("Status registry loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load status registry: {e}", exc_info=True)

async def on_shutdown(bot: Bot):
    """Закрытие всех ресурсов при остановке бота."""
    logger.info("Shutting down bot, closing resources...")
    
    # Закрываем единую aiohttp сессию в planfix клиенте
    await planfix_client.close()
    
    logger.info("All resources closed.")

async def main():
    logger.info("Starting bot in polling mode...")

    # Инициализация базы данных
    init_db()
    logger.info("Database initialized.")

    # Инициализация бота и диспетчера
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    # Регистрация middleware для обработки rate limit
    dp.message.middleware(RateLimitMiddleware())
    dp.callback_query.middleware(RateLimitMiddleware())

    # Регистрация обработчиков
    # ВАЖНО: executor_router регистрируется первым, чтобы обработчики кнопок меню исполнителей
    # имели приоритет над общими обработчиками в user_router
    dp.include_router(executor_router)
    dp.include_router(user_router)
    
    # Запуск синхронизации при старте
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Запуск бота в режиме polling (без webhook)
    max_retries = 10  # Максимальное количество повторных попыток
    retry_count = 0
    
    try:
        while retry_count < max_retries:
            try:
                logger.info("Starting polling...")
                await dp.start_polling(
                    bot, 
                    allowed_updates=dp.resolve_used_update_types(),
                    relax_timeout=0.1,  # Минимальный timeout между запросами
                    timeout=30,  # Timeout для long polling
                    skip_updates=False  # Не пропускаем обновления при старте
                )
                # Если polling завершился нормально, выходим из цикла
                break
            except TelegramRetryAfter as e:
                retry_count += 1
                logger.error(f"Telegram flood control: need to wait {e.retry_after} seconds (attempt {retry_count}/{max_retries})")
                logger.info(f"Sleeping for {e.retry_after} seconds...")
                await asyncio.sleep(e.retry_after)
                # Продолжаем цикл для повторной попытки
                continue
            except KeyboardInterrupt:
                logger.info("Bot stopped by user (KeyboardInterrupt)")
                break
            except Exception as e:
                logger.error(f"Unexpected error in polling: {e}", exc_info=True)
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(60, retry_count * 5)  # Экспоненциальная задержка до 60 секунд
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("Max retries reached, stopping bot")
                    break
    finally:
        await bot.session.close()
        logger.info("Bot stopped.")

if __name__ == "__main__":
    asyncio.run(main())
