#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Единая точка входа для запуска бота и webhook сервера.

Использование:
    # Запуск только бота (polling)
    python run.py --mode polling
    python run.py -m polling

    # Запуск только webhook сервера
    python run.py --mode webhook
    python run.py -m webhook

    # Запуск бота и webhook сервера одновременно
    python run.py --mode both
    python run.py -m both

    # По умолчанию запускается polling
    python run.py
"""

import argparse
import asyncio
import logging
import signal
import sys

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramRetryAfter
from aiohttp import web

from config import BOT_TOKEN
from config.settings import settings
from database import init_db
from admin_handlers import router as admin_router
from executor_handlers import router as executor_router
from logging_config import setup_logging
from planfix_client import planfix_client
from rate_limit_middleware import RateLimitMiddleware
from services.status_registry import ensure_status_registry_loaded
from user_handlers import router as user_router
from webhook_server import create_webhook_app, run_webhook_server as run_webhook_server_original

setup_logging()
logger = logging.getLogger(__name__)


async def on_startup(bot: Bot):
    """Инициализация при старте бота."""
    logger.info("Bot startup complete.")
    try:
        await ensure_status_registry_loaded()
        logger.info("Status registry loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load status registry: {e}", exc_info=True)


async def on_shutdown(bot: Bot):
    """Закрытие всех ресурсов при остановке бота."""
    logger.info("Shutting down bot, closing resources...")
    await planfix_client.close()
    logger.info("All resources closed.")


def create_dispatcher() -> Dispatcher:
    """Создает и настраивает диспетчер бота."""
    dp = Dispatcher()
    
    # Регистрация middleware для обработки rate limit
    dp.message.middleware(RateLimitMiddleware())
    dp.callback_query.middleware(RateLimitMiddleware())
    
    # Регистрация обработчиков
    # ВАЖНО: executor_router регистрируется первым, чтобы обработчики кнопок меню исполнителей
    # имели приоритет над общими обработчиками в user_router
    # admin_router регистрируется первым для приоритета админ-команд
    dp.include_router(admin_router)
    dp.include_router(executor_router)
    dp.include_router(user_router)
    
    # Регистрация startup/shutdown обработчиков
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    return dp


async def run_polling(bot: Bot, dp: Dispatcher):
    """Запускает бота в режиме polling."""
    logger.info("Starting bot in polling mode...")
    
    max_retries = 10
    retry_count = 0
    
    try:
        while retry_count < max_retries:
            try:
                logger.info("Starting polling...")
                await dp.start_polling(
                    bot,
                    allowed_updates=dp.resolve_used_update_types(),
                    relax_timeout=0.1,
                    timeout=30,
                    skip_updates=False
                )
                break
            except TelegramRetryAfter as e:
                retry_count += 1
                logger.error(
                    f"Telegram flood control: need to wait {e.retry_after} seconds "
                    f"(attempt {retry_count}/{max_retries})"
                )
                logger.info(f"Sleeping for {e.retry_after} seconds...")
                await asyncio.sleep(e.retry_after)
                continue
            except KeyboardInterrupt:
                logger.info("Bot stopped by user (KeyboardInterrupt)")
                break
            except Exception as e:
                logger.error(f"Unexpected error in polling: {e}", exc_info=True)
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(60, retry_count * 5)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("Max retries reached, stopping bot")
                    break
    finally:
        await bot.session.close()
        logger.info("Polling stopped.")


async def run_webhook_server(bot: Bot, host: str = '0.0.0.0', port: int = 8080):
    """Запускает webhook сервер."""
    await run_webhook_server_original(bot, host, port)


async def run_both(bot: Bot, dp: Dispatcher, webhook_host: str = '0.0.0.0', webhook_port: int = 8080):
    """Запускает бота и webhook сервер одновременно."""
    logger.info("Starting bot in polling mode and webhook server...")
    
    # Создаем webhook приложение
    app = create_webhook_app(bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, webhook_host, webhook_port)
    await site.start()
    logger.info(f"🚀 Webhook server started on {webhook_host}:{webhook_port}")
    
    # Показываем правильный URL в зависимости от хоста
    if webhook_host == '0.0.0.0':
        logger.info(f"📡 Webhook доступен на всех интерфейсах: http://<your-ip>:{webhook_port}/planfix/webhook")
        logger.info(f"📡 Локальный URL: http://127.0.0.1:{webhook_port}/planfix/webhook")
    elif webhook_host == '127.0.0.1':
        logger.info(f"📡 Webhook URL (только локальный доступ): http://127.0.0.1:{webhook_port}/planfix/webhook")
        logger.info(f"💡 Для получения webhook от Planfix используйте nginx или другой прокси")
    else:
        logger.info(f"📡 Webhook URL: http://{webhook_host}:{webhook_port}/planfix/webhook")
    
    # Запускаем polling в фоне
    polling_task = asyncio.create_task(run_polling(bot, dp))
    
    try:
        # Ждем завершения polling или KeyboardInterrupt
        await polling_task
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass
    finally:
        await runner.cleanup()
        logger.info("All services stopped.")


async def main():
    """Основная функция запуска."""
    parser = argparse.ArgumentParser(
        description='Запуск Telegram бота и/или webhook сервера',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python run.py                    # Запуск только polling (по умолчанию)
  python run.py -m polling         # Запуск только polling
  python run.py -m webhook         # Запуск только webhook сервера
  python run.py -m both            # Запуск polling и webhook одновременно
  python run.py -m webhook -p 9000 # Запуск webhook на порту 9000
        """
    )
    
    parser.add_argument(
        '-m', '--mode',
        choices=['polling', 'webhook', 'both'],
        default='polling',
        help='Режим запуска: polling (только бот), webhook (только сервер), both (оба)'
    )
    
    parser.add_argument(
        '--webhook-host',
        default=None,
        help=f'Хост для webhook сервера (по умолчанию: {settings.webhook_host} из .env или 127.0.0.1)'
    )
    
    parser.add_argument(
        '-p', '--webhook-port',
        type=int,
        default=None,
        help=f'Порт для webhook сервера (по умолчанию: {settings.webhook_port} из .env или 8080)'
    )
    
    args = parser.parse_args()
    
    # Определяем хост и порт для webhook (приоритет: аргументы командной строки > .env > значения по умолчанию)
    webhook_host = args.webhook_host if args.webhook_host is not None else settings.webhook_host
    webhook_port = args.webhook_port if args.webhook_port is not None else settings.webhook_port
    
    # Предупреждение о безопасности, если используется 0.0.0.0
    if webhook_host == '0.0.0.0':
        logger.warning("=" * 80)
        logger.warning("⚠️  ВНИМАНИЕ: Webhook сервер запущен на 0.0.0.0 (все интерфейсы)")
        logger.warning("⚠️  Это означает, что сервер доступен извне!")
        logger.warning("⚠️  Для безопасности рекомендуется использовать 127.0.0.1")
        logger.warning("⚠️  Если нужен публичный доступ, используйте nginx или другой прокси")
        logger.warning("=" * 80)
    
    # Инициализация базы данных
    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized.")
    
    # Инициализация бота
    bot = Bot(token=BOT_TOKEN)
    dp = create_dispatcher()
    
    try:
        if args.mode == 'polling':
            logger.info("=" * 80)
            logger.info("Starting in POLLING mode")
            logger.info("=" * 80)
            await run_polling(bot, dp)
            
        elif args.mode == 'webhook':
            logger.info("=" * 80)
            logger.info("Starting in WEBHOOK SERVER mode")
            logger.info("=" * 80)
            logger.info("⚠️  Note: Bot polling is not started. Only webhook server is running.")
            logger.info("⚠️  Make sure to set webhook URL in Telegram Bot API if needed.")
            await run_webhook_server(bot, webhook_host, webhook_port)
            
        elif args.mode == 'both':
            logger.info("=" * 80)
            logger.info("Starting in BOTH mode (Polling + Webhook Server)")
            logger.info("=" * 80)
            await run_both(bot, dp, webhook_host, webhook_port)
            
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await bot.session.close()
        logger.info("Application stopped.")


def setup_signal_handlers():
    """Настраивает обработчики сигналов для корректного завершения."""
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()
    
    def signal_handler(sig):
        logger.info(f"Received signal {sig}, initiating graceful shutdown...")
        shutdown_event.set()
    
    # Обработка SIGTERM (от systemd) и SIGINT (Ctrl+C)
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
    
    return shutdown_event


if __name__ == "__main__":
    # Настраиваем обработчики сигналов только в Linux
    if sys.platform != 'win32':
        try:
            shutdown_event = setup_signal_handlers()
        except NotImplementedError:
            # Windows не поддерживает add_signal_handler
            shutdown_event = None
    else:
        shutdown_event = None
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

