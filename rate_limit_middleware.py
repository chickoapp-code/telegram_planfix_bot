"""
Middleware для обработки rate limit ошибок Planfix API
Версия: 1.1 
"""

import logging
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, CallbackQuery, Message
from planfix_api import PlanfixRateLimitError

logger = logging.getLogger(__name__)


class RateLimitMiddleware(BaseMiddleware):
    """
    Middleware для обработки ошибок rate limit от Planfix API.
    Перехватывает исключения и показывает пользователю понятное сообщение.
    """
    
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        try:
            # Выполняем обработчик
            return await handler(event, data)
        except PlanfixRateLimitError as e:
            # Специальная обработка для нашего исключения rate limit
            logger.warning(f"Rate limit error caught by middleware: {e.message}, wait {e.wait_seconds}s")
            
            # Определяем, откуда пришел запрос
            message = None
            if isinstance(event, CallbackQuery):
                message = event.message
                # Отвечаем на callback, чтобы убрать "часики"
                try:
                    await event.answer(
                        "⏳ Сервер перегружен",
                        show_alert=False
                    )
                except Exception:
                    pass
            elif isinstance(event, Message):
                message = event
            
            # Отправляем сообщение пользователю с точным временем ожидания
            if message:
                try:
                    minutes = e.wait_seconds // 60
                    seconds = e.wait_seconds % 60
                    
                    if minutes > 0:
                        time_str = f"{minutes} мин {seconds} сек" if seconds > 0 else f"{minutes} мин"
                    else:
                        time_str = f"{seconds} сек"
                    
                    await message.answer(
                        f"⏳ Сервер Planfix временно перегружен.\n\n"
                        f"Пожалуйста, подождите примерно {time_str} и попробуйте снова.\n\n"
                        "Приносим извинения за неудобства."
                    )
                except Exception as send_error:
                    logger.error(f"Failed to send rate limit message: {send_error}")
            
            return None
        except Exception as e:
            error_msg = str(e)
            
            # Проверяем, является ли это другой ошибкой rate limit (на всякий случай)
            if "403" in error_msg or "rate limit" in error_msg.lower() or "code 22" in error_msg.lower():
                logger.warning(f"Generic rate limit error caught by middleware: {error_msg}")
                
                # Определяем, откуда пришел запрос
                message = None
                if isinstance(event, CallbackQuery):
                    message = event.message
                    # Отвечаем на callback, чтобы убрать "часики"
                    try:
                        await event.answer(
                            "⏳ Сервер перегружен",
                            show_alert=False
                        )
                    except Exception:
                        pass
                elif isinstance(event, Message):
                    message = event
                
                # Отправляем сообщение пользователю
                if message:
                    try:
                        await message.answer(
                            "⏳ Сервер Planfix временно перегружен.\n\n"
                            "Пожалуйста, подождите 1-2 минуты и попробуйте снова.\n\n"
                            "Приносим извинения за неудобства."
                        )
                    except Exception as send_error:
                        logger.error(f"Failed to send rate limit message: {send_error}")
                
                return None
            else:
                # Если это не rate limit, пробрасываем ошибку дальше
                raise
