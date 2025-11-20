"""
Обработчик rate limit для Telegram бота
Версия: 1.0 
"""

import logging
import asyncio
from functools import wraps
from aiogram.types import CallbackQuery, Message

logger = logging.getLogger(__name__)


def handle_rate_limit(func):
    """
    Декоратор для обработки rate limit в обработчиках Telegram.
    Немедленно отвечает пользователю, что запрос обрабатывается,
    а затем выполняет долгую операцию.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Находим callback_query или message в аргументах
        callback_query = None
        message = None
        
        for arg in args:
            if isinstance(arg, CallbackQuery):
                callback_query = arg
                message = callback_query.message
                break
            elif isinstance(arg, Message):
                message = arg
                break
        
        try:
            # Выпол��яем функцию
            return await func(*args, **kwargs)
        except Exception as e:
            error_msg = str(e)
            
            # Проверяем, является ли это ошибкой rate limit
            if "rate limit" in error_msg.lower() or "403" in error_msg:
                logger.warning(f"Rate limit detected in handler {func.__name__}: {error_msg}")
                
                # Отвечаем пользователю
                if callback_query and not callback_query.message.from_user.is_bot:
                    try:
                        await callback_query.answer(
                            "⏳ Сервер перегружен, пожалуйста подождите...",
                            show_alert=True
                        )
                    except Exception:
                        pass
                
                if message:
                    try:
                        await message.answer(
                            "⏳ Сервер Planfix временно перегружен.\n\n"
                            "Пожалуйста, подождите 1-2 минуты и попробуйте снова.\n\n"
                            "Приносим извинения за неудобства."
                        )
                    except Exception:
                        pass
                
                return None
            else:
                # Если это не rate limit, пробрасываем ошибку дальше
                raise
    
    return wrapper


async def notify_rate_limit_wait(message: Message, wait_seconds: int):
    """
    Уведомляет пользователя о необходимости подождать из-за rate limit.
    
    Args:
        message: Сообщение Telegram
        wait_seconds: Количество секунд ожидания
    """
    try:
        await message.answer(
            f"⏳ Сервер Planfix временно перегружен.\n\n"
            f"Пожалуйста, подождите примерно {wait_seconds} секунд и попробуйте снова.\n\n"
            "Приносим извинения за неудобства."
        )
    except Exception as e:
        logger.error(f"Failed to send rate limit notification: {e}")
