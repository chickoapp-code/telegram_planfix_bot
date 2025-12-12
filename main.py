#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Главный файл для запуска Telegram бота и webhook сервера.

Использование:
    python main.py                    # Запуск бота + webhook сервера
    python main.py --webhook-port 8080 # Указать порт webhook
    python main.py --webhook-host 127.0.0.1  # Указать хост webhook
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import socket
import sys
from datetime import datetime
from typing import Optional

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
from webhook_server import create_webhook_app

# Настройка логирования с выводом в консоль
setup_logging()
logger = logging.getLogger(__name__)


async def on_startup(bot: Bot):
    """Инициализация при старте бота."""
    logger.info("=" * 80)
    logger.info("🚀 Bot startup complete")
    logger.info("=" * 80)
    try:
        await ensure_status_registry_loaded()
        logger.info("✅ Status registry loaded successfully")
    except Exception as e:
        logger.error(f"❌ Failed to load status registry: {e}", exc_info=True)


async def on_shutdown(bot: Bot):
    """Закрытие всех ресурсов при остановке бота."""
    logger.info("=" * 80)
    logger.info("🛑 Shutting down bot, closing resources...")
    logger.info("=" * 80)
    await planfix_client.close()
    logger.info("✅ All resources closed")


def create_dispatcher() -> Dispatcher:
    """Создает и настраивает диспетчер бота."""
    dp = Dispatcher()
    
    # Регистрация middleware для обработки rate limit
    dp.message.middleware(RateLimitMiddleware())
    dp.callback_query.middleware(RateLimitMiddleware())
    
    # Регистрация обработчиков
    # ВАЖНО: порядок регистрации важен для приоритета обработчиков
    dp.include_router(admin_router)      # Админ-команды (высший приоритет)
    dp.include_router(executor_router)   # Команды исполнителей
    dp.include_router(user_router)       # Общие команды пользователей
    
    # Регистрация startup/shutdown обработчиков
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    return dp


async def run_polling(bot: Bot, dp: Dispatcher):
    """Запускает бота в режиме polling."""
    logger.info("📡 Starting bot in polling mode...")
    
    max_retries = 10
    retry_count = 0
    
    try:
        while retry_count < max_retries:
            try:
                logger.info("🔄 Starting polling...")
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
                    f"⚠️ Telegram flood control: need to wait {e.retry_after} seconds "
                    f"(attempt {retry_count}/{max_retries})"
                )
                logger.info(f"⏳ Sleeping for {e.retry_after} seconds...")
                await asyncio.sleep(e.retry_after)
                continue
            except KeyboardInterrupt:
                logger.info("🛑 Bot stopped by user (KeyboardInterrupt)")
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error in polling: {e}", exc_info=True)
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(60, retry_count * 5)
                    logger.info(f"⏳ Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("❌ Max retries reached, stopping bot")
                    break
    finally:
        await bot.session.close()
        logger.info("✅ Polling stopped")


def find_available_port(host: str, start_port: int, max_attempts: int = 10) -> Optional[int]:
    """Находит свободный порт, начиная с start_port."""
    for port in range(start_port, start_port + max_attempts):
        if is_port_available(host, port):
            return port
    return None


def is_port_available(host: str, port: int) -> bool:
    """Проверяет, доступен ли порт для использования."""
    # #region agent log
    try:
        with open(r'b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log', 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A,B,C","location":"main.py:129","message":"is_port_available entry","data":{"host":host,"port":port},"timestamp":int(datetime.now().timestamp()*1000)})+'\n')
    except: pass
    # #endregion
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            # #region agent log
            try:
                with open(r'b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log', 'a', encoding='utf-8') as f:
                    f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A,B,C","location":"main.py:134","message":"socket.connect_ex result","data":{"host":host,"port":port,"result":result,"port_available":result!=0},"timestamp":int(datetime.now().timestamp()*1000)})+'\n')
            except: pass
            # #endregion
            return result != 0  # Порт доступен, если соединение не удалось
    except Exception as e:
        # #region agent log
        try:
            with open(r'b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log', 'a', encoding='utf-8') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"main.py:137","message":"is_port_available exception","data":{"host":host,"port":port,"error":str(e)},"timestamp":int(datetime.now().timestamp()*1000)})+'\n')
        except: pass
        # #endregion
        return False


async def run_both(bot: Bot, dp: Dispatcher, webhook_host: str = '127.0.0.1', webhook_port: int = 8080):
    """Запускает бота и webhook сервер одновременно."""
    # #region agent log
    try:
        with open(r'b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log', 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A,B,C","location":"main.py:140","message":"run_both entry","data":{"webhook_host":webhook_host,"webhook_port":webhook_port},"timestamp":int(datetime.now().timestamp()*1000)})+'\n')
    except: pass
    # #endregion
    logger.info("=" * 80)
    logger.info("🚀 Starting bot in polling mode + webhook server")
    logger.info("=" * 80)
    
    # Проверяем доступность порта перед запуском
    port_check_result = is_port_available(webhook_host, webhook_port)
    # #region agent log
    try:
        with open(r'b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log', 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A,B,C","location":"main.py:147","message":"port check result","data":{"webhook_host":webhook_host,"webhook_port":webhook_port,"port_available":port_check_result},"timestamp":int(datetime.now().timestamp()*1000)})+'\n')
    except: pass
    # #endregion
    if not port_check_result:
        logger.error("=" * 80)
        logger.error(f"❌ Port {webhook_port} is already in use on {webhook_host}")
        logger.error("=" * 80)
        logger.error("💡 Solutions:")
        logger.error(f"   1. Stop the process using port {webhook_port}:")
        logger.error(f"      sudo lsof -ti:{webhook_port} | xargs kill -9")
        logger.error(f"      OR: sudo netstat -tulpn | grep :{webhook_port}")
        logger.error(f"   2. Use a different port:")
        logger.error(f"      python main.py --webhook-port 8081")
        logger.error(f"   3. Check if another instance of the bot is running:")
        logger.error(f"      ps aux | grep 'python.*main.py'")
        logger.error("=" * 80)
        raise OSError(f"Port {webhook_port} is already in use on {webhook_host}")
    
    # Создаем webhook приложение
    app = create_webhook_app(bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, webhook_host, webhook_port)
    
    try:
        await site.start()
        # #region agent log
        try:
            with open(r'b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log', 'a', encoding='utf-8') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"main.py:169","message":"site.start success","data":{"webhook_host":webhook_host,"webhook_port":webhook_port},"timestamp":int(datetime.now().timestamp()*1000)})+'\n')
        except: pass
        # #endregion
    except OSError as e:
        # #region agent log
        try:
            with open(r'b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log', 'a', encoding='utf-8') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"main.py:171","message":"site.start OSError","data":{"webhook_host":webhook_host,"webhook_port":webhook_port,"errno":e.errno,"error":str(e)},"timestamp":int(datetime.now().timestamp()*1000)})+'\n')
        except: pass
        # #endregion
        if e.errno == 98 or "address already in use" in str(e).lower():
            logger.error("=" * 80)
            logger.error(f"❌ Failed to bind to {webhook_host}:{webhook_port}")
            logger.error(f"   Error: {e}")
            logger.error("=" * 80)
            logger.error("💡 Solutions:")
            logger.error(f"   1. Stop the process using port {webhook_port}:")
            logger.error(f"      sudo lsof -ti:{webhook_port} | xargs kill -9")
            logger.error(f"      OR: sudo netstat -tulpn | grep :{webhook_port}")
            logger.error(f"   2. Use a different port:")
            logger.error(f"      python main.py --webhook-port 8081")
            logger.error("=" * 80)
        raise
    
    logger.info("=" * 80)
    logger.info(f"✅ Webhook server started on {webhook_host}:{webhook_port}")
    
    # Показываем правильный URL в зависимости от хоста
    if webhook_host == '0.0.0.0':
        logger.info(f"📡 Webhook доступен на всех интерфейсах: http://<your-ip>:{webhook_port}/planfix/webhook")
        logger.info(f"📡 Локальный URL: http://127.0.0.1:{webhook_port}/planfix/webhook")
    elif webhook_host == '127.0.0.1':
        logger.info(f"📡 Webhook URL (только локальный доступ): http://127.0.0.1:{webhook_port}/planfix/webhook")
        logger.info(f"💡 Для получения webhook от Planfix используйте nginx или другой прокси")
    else:
        logger.info(f"📡 Webhook URL: http://{webhook_host}:{webhook_port}/planfix/webhook")
    
    logger.info("=" * 80)
    
    # Запускаем polling в фоне
    polling_task = asyncio.create_task(run_polling(bot, dp))
    
    try:
        # Ждем завершения polling или KeyboardInterrupt
        await polling_task
    except KeyboardInterrupt:
        logger.info("=" * 80)
        logger.info("🛑 Shutting down...")
        logger.info("=" * 80)
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass
    finally:
        await runner.cleanup()
        logger.info("✅ All services stopped")


async def main():
    """Основная функция запуска."""
    parser = argparse.ArgumentParser(
        description='Запуск Telegram бота и webhook сервера',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        '--webhook-host',
        default=None,
        help=f'Хост для webhook сервера (по умолчанию: {settings.webhook_host} из .env или 127.0.0.1)'
    )
    
    parser.add_argument(
        '--webhook-port',
        type=int,
        default=None,
        help=f'Порт для webhook сервера (по умолчанию: {settings.webhook_port} из .env или 8080)'
    )
    
    parser.add_argument(
        '--auto-port',
        action='store_true',
        help='Автоматически найти свободный порт, если указанный занят'
    )
    
    args = parser.parse_args()
    
    # Определяем хост и порт для webhook
    webhook_host = args.webhook_host if args.webhook_host is not None else settings.webhook_host
    webhook_port = args.webhook_port if args.webhook_port is not None else settings.webhook_port
    auto_port = args.auto_port
    
    # Предупреждение о безопасности, если используется 0.0.0.0
    if webhook_host == '0.0.0.0':
        logger.warning("=" * 80)
        logger.warning("⚠️  ВНИМАНИЕ: Webhook сервер запущен на 0.0.0.0 (все интерфейсы)")
        logger.warning("⚠️  Это означает, что сервер доступен извне!")
        logger.warning("⚠️  Для безопасности рекомендуется использовать 127.0.0.1")
        logger.warning("⚠️  Если нужен публичный доступ, используйте nginx или другой прокси")
        logger.warning("=" * 80)
    
    # Инициализация базы данных
    logger.info("=" * 80)
    logger.info("📦 Initializing database...")
    logger.info("=" * 80)
    init_db()
    logger.info("✅ Database initialized")
    
    # Инициализация бота
    bot = Bot(token=BOT_TOKEN)
    dp = create_dispatcher()
    
    try:
        logger.info("=" * 80)
        logger.info("🚀 Starting in BOTH mode (Polling + Webhook Server)")
        logger.info("=" * 80)
        await run_both(bot, dp, webhook_host, webhook_port)
    except KeyboardInterrupt:
        logger.info("=" * 80)
        logger.info("🛑 Shutdown requested by user")
        logger.info("=" * 80)
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        logger.error("=" * 80)
        sys.exit(1)
    finally:
        await bot.session.close()
        logger.info("=" * 80)
        logger.info("✅ Application stopped")
        logger.info("=" * 80)


def setup_signal_handlers():
    """Настраивает обработчики сигналов для корректного завершения."""
    def signal_handler(sig, frame):
        logger.info(f"📶 Received signal {sig}, initiating graceful shutdown...")
        sys.exit(0)
    
    # Обработка SIGTERM (от systemd) и SIGINT (Ctrl+C)
    if sys.platform != 'win32':
        try:
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)
        except NotImplementedError:
            pass


if __name__ == "__main__":
    # Настраиваем обработчики сигналов
    setup_signal_handlers()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("=" * 80)
        logger.info("🛑 Shutdown requested by user")
        logger.info("=" * 80)
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        logger.error("=" * 80)
        sys.exit(1)
