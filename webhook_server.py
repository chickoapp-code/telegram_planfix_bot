"""
Webhook сервер для получения уведомлений от Planfix
Версия: 2.0 - Улучшенная обработка событий
"""

import asyncio
import hashlib
import hmac
import json
import logging
import re
from datetime import datetime
from typing import Optional, Set

from aiohttp import web
from aiogram import Bot

from config import (
    BOT_TOKEN,
    FRANCHISE_GROUPS,
    PLANFIX_TASK_PROCESS_ID,
    PLANFIX_WEBHOOK_SECRET,
    PLANFIX_WEBHOOK_USERNAME,
    PLANFIX_WEBHOOK_PASSWORD,
    WEBHOOK_MAX_BODY_SIZE,
)
from db_manager import DBManager
from keyboards import get_executor_main_menu_keyboard
from logging_config import setup_logging
from notifications import NotificationService
from planfix_client import planfix_client
from services.status_registry import StatusKey, is_status, status_in
from task_notification_service import TaskNotificationService

setup_logging()
logger = logging.getLogger(__name__)

class PlanfixWebhookHandler:
    """Обработчик webhook от Planfix."""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.db_manager = DBManager()
        self.notification_service = NotificationService(bot)
        self.task_notification_service = TaskNotificationService(bot)
        # Кэш для отслеживания предыдущих статусов задач
        self._task_status_cache = {}  # {task_id: status_id}
        # Кэш для предотвращения дубликатов событий
        self._processed_events = set()  # {(event_type, task_id, timestamp)}
    
    async def check_pending_registration_tasks(self):
        """Проверяет все незавершенные задачи регистрации при старте. да"""
        try:
            logger.info("Checking pending registration tasks on startup...")
            
            # Убеждаемся, что status registry загружен
            from services.status_registry import ensure_status_registry_loaded
            await ensure_status_registry_loaded()
            logger.info("Status registry loaded for registration tasks check")
            
            with self.db_manager.get_db() as db:
                from database import ExecutorProfile
                executors = db.query(ExecutorProfile).filter(
                    ExecutorProfile.registration_task_id.isnot(None),
                    ExecutorProfile.profile_status == "ожидает подтверждения"
                ).all()
                
                if not executors:
                    logger.info("No pending registration tasks found")
                    return
                
                logger.info(f"Found {len(executors)} pending registration tasks, checking their status...")
                
                for executor in executors:
                    task_id = executor.registration_task_id
                    if not task_id:
                        continue
                    
                    try:
                        # Получаем статус задачи из Planfix
                        task_response = await planfix_client.get_task_by_id(
                            task_id,
                            fields="id,status"
                        )
                        
                        if not task_response or task_response.get('result') != 'success':
                            logger.warning(f"Failed to get registration task {task_id} for executor {executor.telegram_id}")
                            continue
                        
                        task = task_response.get('task', {})
                        status_raw = task.get('status', {})
                        # Проверяем все возможные варианты получения статуса
                        status_id_raw = (
                            status_raw.get('id') or 
                            status_raw.get('task.status.id') or 
                            status_raw.get('task.status.Идентификатор')
                        )
                        status_id = self._normalize_status_id(status_id_raw)
                        status_name = (
                            status_raw.get('name') or 
                            status_raw.get('task.status.name') or 
                            status_raw.get('task.status.Активный') or
                            status_raw.get('task.status.Статус') or
                            'Unknown'
                        )
                        
                        logger.info(f"Registration task {task_id} for executor {executor.telegram_id}: status_id={status_id}, status_name='{status_name}'")
                        
                        if status_id:
                            if status_in(status_id, (StatusKey.COMPLETED, StatusKey.FINISHED)):
                                logger.info(f"Registration task {task_id} is already completed, approving executor {executor.telegram_id}")
                                await self._approve_executor(executor.telegram_id, task_id)
                            elif status_in(status_id, (StatusKey.CANCELLED, StatusKey.REJECTED)):
                                logger.info(f"Registration task {task_id} is cancelled/rejected, rejecting executor {executor.telegram_id}")
                                await self._reject_executor(executor.telegram_id, task_id)
                            else:
                                logger.debug(f"Registration task {task_id} status {status_id} ('{status_name}') is not a terminal status")
                        else:
                            logger.warning(f"Could not normalize status_id for registration task {task_id}, status_raw: {status_raw}")
                    except Exception as e:
                        logger.error(f"Error checking registration task {task_id} for executor {executor.telegram_id}: {e}", exc_info=True)
                        continue
                        
        except Exception as e:
            logger.error(f"Error checking pending registration tasks: {e}", exc_info=True)
        
    def _normalize_status_id(self, status_raw) -> Optional[int]:
        """Нормализует ID статуса из webhook данных."""
        if status_raw is None:
            return None
        try:
            # Если это строка с разделителем (например, "process:123")
            if isinstance(status_raw, str) and ":" in status_raw:
                status_raw = status_raw.split(":")[-1]
            
            # Если это строка с числом, преобразуем в int
            if isinstance(status_raw, str):
                # Пытаемся преобразовать в число
                try:
                    return int(status_raw)
                except ValueError:
                    # Если не число, пытаемся найти статус по имени
                    from services.status_registry import get_status_id, StatusKey
                    # Пробуем найти статус по имени (например, "В работе" -> IN_PROGRESS)
                    status_name_lower = status_raw.lower().strip()
                    # Маппинг русских названий на ключи статусов
                    name_to_key = {
                        "новая": StatusKey.NEW,
                        "в работе": StatusKey.IN_PROGRESS,
                        "завершена": StatusKey.COMPLETED,
                        "завершенная": StatusKey.COMPLETED,
                        "отменена": StatusKey.CANCELLED,
                        "отклонена": StatusKey.REJECTED,
                    }
                    if status_name_lower in name_to_key:
                        status_id = get_status_id(name_to_key[status_name_lower], required=False)
                        if status_id:
                            return status_id
            return int(status_raw)
        except (TypeError, ValueError):
            return None
    
    def _normalize_user_id(self, user_id_raw) -> Optional[str]:
        """Нормализует ID пользователя из webhook данных."""
        if user_id_raw is None:
            return None
        try:
            if isinstance(user_id_raw, str) and ":" in user_id_raw:
                return user_id_raw.split(":")[-1]
            return str(user_id_raw)
        except (TypeError, ValueError):
            return None
    
    def _normalize_int(self, value) -> Optional[int]:
        """Нормализует целочисленное значение из webhook данных."""
        if value is None:
            return None
        try:
            # Если это строка с разделителем (например, "template:123")
            if isinstance(value, str) and ":" in value:
                value = value.split(":")[-1]
            
            # Преобразуем в int
            if isinstance(value, str):
                # Убираем пробелы
                value = value.strip()
                if not value:
                    return None
                return int(value)
            elif isinstance(value, int):
                return value
            else:
                return int(value)
        except (TypeError, ValueError):
            return None
    
    def _is_bot_comment(self, comment: dict) -> bool:
        """Проверяет, является ли комментарий от бота."""
        owner = comment.get('owner', {})
        owner_name = owner.get('name', '').lower()
        # Проверяем по имени владельца (может быть "Telegram Bot" или подобное)
        bot_indicators = ['bot', 'бот', 'telegram', 'автоматическ']
        return any(indicator in owner_name for indicator in bot_indicators)
    
    def _should_process_task(self, task: dict) -> bool:
        """Проверяет, нужно ли обрабатывать задачу (фильтрация по процессу и шаблонам)."""
        try:
            # Проверяем процесс задачи
            process = task.get('process', {})
            process_id = process.get('id') if isinstance(process, dict) else None
            if process_id and PLANFIX_TASK_PROCESS_ID:
                if str(process_id) != str(PLANFIX_TASK_PROCESS_ID):
                    logger.debug(f"Task {task.get('id')} skipped: wrong process {process_id}")
                    return False
            
            # Можно добавить дополнительные фильтры по шаблонам, проектам и т.д.
            return True
        except Exception as e:
            logger.error(f"Error checking if task should be processed: {e}")
            return True  # В случае ошибки обрабатываем задачу
    
    async def handle_task_created(self, data: dict):
        """Обработка создания новой задачи."""
        try:
            task = data.get('task', {})
            # Приоритет: generalId > id (generalId - публичный идентификатор)
            task_identifier = task.get('generalId') or task.get('id')
            project_id_raw = task.get('project', {}).get('id')
            
            if not task_identifier:
                logger.warning(f"Incomplete task data in webhook: {data}")
                return
            
            # Преобразуем task_id в int
            try:
                if isinstance(task_identifier, str):
                    if task_identifier.isdigit():
                        task_id = int(task_identifier)
                    else:
                        parts = task_identifier.split(':')
                        if len(parts) > 1 and parts[-1].isdigit():
                            task_id = int(parts[-1])
                        else:
                            logger.warning(f"Invalid task_id format: {task_identifier}")
                            return
                else:
                    task_id = int(task_identifier)
            except (ValueError, TypeError):
                logger.warning(f"Invalid task_id format: {task_identifier}")
                return
            
            # Обрабатываем project_id и counterparty
            project_id = None
            if project_id_raw:
                if isinstance(project_id_raw, str):
                    try:
                        project_id = int(project_id_raw)
                    except (ValueError, TypeError):
                        logger.debug(f"Skipping task {task_id}: project_id is not a number ({project_id_raw})")
                        return
                else:
                    project_id = project_id_raw
            
            # Обрабатываем counterparty (может быть объект {"id": 5} или строка "contact:5")
            counterparty_id = None
            counterparty_raw = task.get('counterparty')
            if counterparty_raw:
                if isinstance(counterparty_raw, dict):
                    counterparty_id = counterparty_raw.get('id')
                elif isinstance(counterparty_raw, str):
                    if ':' in counterparty_raw:
                        counterparty_id = counterparty_raw.split(':')[-1]
                    else:
                        counterparty_id = counterparty_raw
                if counterparty_id:
                    try:
                        counterparty_id = int(counterparty_id) if str(counterparty_id).isdigit() else counterparty_id
                    except (ValueError, TypeError):
                        counterparty_id = None
            
            if not project_id:
                logger.debug(f"Skipping task {task_id}: no valid project_id")
                return
            
            # Фильтруем только релевантные задачи
            if not self._should_process_task(task):
                logger.debug(f"Task {task_id} creation skipped by filter")
                return
            
            logger.info(f"📋 New task created: {task_id} in project {project_id}" + 
                       (f", counterparty: {counterparty_id}" if counterparty_id else ""))
            
            # Сохраняем задачу в кэш БД
            status_obj = task.get('status', {})
            if isinstance(status_obj, dict):
                status_id_raw = (
                    status_obj.get('id') or
                    status_obj.get('task.status.id') or 
                    status_obj.get('task.status.Идентификатор')
                )
                status_name = status_obj.get('name') or status_obj.get('task.status.name')
            else:
                status_id_raw = None
                status_name = None
            status_id = self._normalize_status_id(status_id_raw)
            
            # Определяем, создана ли задача через бота (проверяем по BotLog)
            created_by_bot = False
            user_telegram_id = None
            try:
                with self.db_manager.get_db() as db:
                    from database import BotLog
                    import json as json_module
                    # Ищем в BotLog задачу с этим task_id
                    logs = db.query(BotLog).filter(
                        BotLog.action == 'create_task'
                    ).all()
                    for log in logs:
                        if log.details:
                            details = log.details if isinstance(log.details, dict) else json_module.loads(log.details) if isinstance(log.details, str) else {}
                            if details.get('task_id') == task_id or details.get('task_id_general') == task_id:
                                created_by_bot = True
                                user_telegram_id = log.telegram_id
                                break
                    
                    # Сохраняем в TaskCache
                    task_name = task.get('name', '')
                    task_id_internal = None
                    if task.get('id') and str(task.get('id')) != str(task_id):
                        try:
                            task_id_internal = int(task.get('id'))
                        except:
                            pass
                    
                    template_id = None
                    template_obj = task.get('template', {})
                    if isinstance(template_obj, dict):
                        template_id_raw = template_obj.get('id')
                        if template_id_raw:
                            try:
                                template_id = int(template_id_raw) if isinstance(template_id_raw, (int, str)) and str(template_id_raw).isdigit() else None
                            except:
                                pass
                    
                    self.db_manager.create_or_update_task_cache(
                        db=db,
                        task_id=task_id,
                        task_id_internal=task_id_internal,
                        name=task_name,
                        status_id=status_id,
                        status_name=status_name,
                        counterparty_id=counterparty_id,
                        project_id=project_id,
                        template_id=template_id,
                        user_telegram_id=user_telegram_id,
                        created_by_bot=created_by_bot,
                        date_of_last_update=datetime.now()
                    )
                    logger.debug(f"✅ Saved task {task_id} to TaskCache")
            except Exception as cache_err:
                logger.warning(f"Failed to save task {task_id} to TaskCache: {cache_err}")
            
            if status_id:
                self._task_status_cache[task_id] = status_id
            
            await self.notification_service.notify_new_task(task_id, project_id)
                
        except Exception as e:
            logger.error(f"Error handling task created: {e}", exc_info=True)
    
    async def handle_task_updated(self, data: dict):
        """Обработка обновления задачи."""
        try:
            # Убеждаемся, что status registry загружен
            from services.status_registry import ensure_status_registry_loaded
            await ensure_status_registry_loaded()
            
            task = data.get('task', {})
            # Приоритет: generalId > id (generalId - публичный идентификатор)
            task_identifier = task.get('generalId') or task.get('id')
            
            if not task_identifier:
                logger.warning(f"Incomplete task data in webhook: {data}")
                return
            
            # Преобразуем task_id в int, если это строка
            try:
                if isinstance(task_identifier, str):
                    # Если это строка с числом, преобразуем
                    if task_identifier.isdigit():
                        task_id = int(task_identifier)
                    else:
                        # Может быть формат "task:123" или другой
                        parts = task_identifier.split(':')
                        if len(parts) > 1 and parts[-1].isdigit():
                            task_id = int(parts[-1])
                        else:
                            logger.warning(f"Invalid task_id format: {task_identifier}")
                            return
                else:
                    task_id = int(task_identifier)
            except (ValueError, TypeError):
                logger.warning(f"Invalid task_id format: {task_identifier}")
                return
            
            # Фильтруем только релевантные задачи
            if not self._should_process_task(task):
                logger.debug(f"Task {task_id} update skipped by filter")
                return
            
            # Получаем новый статус
            # Согласно swagger.json, статус должен быть объектом {"id": 4, "name": "В работе"}
            # Но Planfix может передавать статус в разных форматах в webhook
            status_obj = task.get('status', {})
            if not isinstance(status_obj, dict):
                status_obj = {}
            
            # Приоритет: стандартный формат (id, name) > формат из шаблона
            status_id_raw = (
                status_obj.get('id') or  # Стандартный формат (приоритет)
                status_obj.get('task.status.id') or 
                status_obj.get('task.status.Идентификатор')
            )
            status_name_raw = (
                status_obj.get('name') or  # Стандартный формат (приоритет)
                status_obj.get('task.status.name') or 
                status_obj.get('task.status.Активный') or
                status_obj.get('task.status.Статус')
            )
            new_status_id = self._normalize_status_id(status_id_raw)
            old_status_id = self._task_status_cache.get(task_id)
            
            # Получаем назначенных исполнителей
            assignees = task.get('assignees', {})
            assignee_users_raw = assignees.get('users', []) if isinstance(assignees, dict) else []
            
            # Нормализуем данные исполнителей
            # Согласно swagger.json, assignees.users содержит объекты вида:
            # [{"id": "user:5", "name": "Иван"}, {"id": "contact:1", "name": "Петр"}]
            assignee_users = []
            if isinstance(assignee_users_raw, list):
                for user in assignee_users_raw:
                    if isinstance(user, dict):
                        # Нормализуем ID: "user:123" -> сохраняем как есть, но добавляем normalized_id
                        user_id = user.get('id')
                        if user_id:
                            # Если id - это массив, берем первый элемент
                            if isinstance(user_id, list) and user_id:
                                user_id = user_id[0]
                            
                            # Нормализуем ID
                            if isinstance(user_id, str) and ':' in user_id:
                                prefix, uid = user_id.split(':', 1)
                                if prefix == 'user':
                                    # Для user:ID сохраняем числовой ID
                                    try:
                                        user['normalized_id'] = int(uid) if uid.isdigit() else uid
                                    except (ValueError, TypeError):
                                        user['normalized_id'] = uid
                                else:
                                    # Для contact:ID и других сохраняем как есть
                                    user['normalized_id'] = user_id
                            elif isinstance(user_id, (int, str)):
                                # Если ID без префикса, считаем что это user ID
                                try:
                                    user['normalized_id'] = int(user_id) if str(user_id).isdigit() else user_id
                                except (ValueError, TypeError):
                                    user['normalized_id'] = user_id
                            
                            # Обновляем оригинальный id если он был массивом
                            user['id'] = user_id
                        
                        # Если name - это массив, берем первый элемент
                        if 'name' in user and isinstance(user['name'], list) and user['name']:
                            user['name'] = user['name'][0]
                        
                        assignee_users.append(user)
            elif isinstance(assignee_users_raw, dict):
                # Если users - это объект, преобразуем в массив
                assignee_users = [assignee_users_raw]
            
            logger.info(f"📝 Task {task_id} updated, status: {old_status_id} -> {new_status_id}")
            
            # ВАЖНО: Проверяем задачи регистрации ДО обработки изменения статуса,
            # чтобы обработать случаи, когда задача уже была завершена
            # Проверяем, это задача регистрации исполнителя
            # ВАЖНО: task_id из webhook - это id (внутренний), а не generalId
            # Но в базе может быть сохранен как id, так и generalId
            # Также в webhook может быть generalId, который можно использовать для поиска
            general_id_from_webhook = task.get('generalId')
            if isinstance(general_id_from_webhook, str):
                try:
                    general_id_from_webhook = int(general_id_from_webhook)
                except (ValueError, TypeError):
                    general_id_from_webhook = None
            
            with self.db_manager.get_db() as db:
                from database import ExecutorProfile
                # Сначала ищем по точному совпадению task_id (внутренний id)
                executor = db.query(ExecutorProfile).filter(
                    ExecutorProfile.registration_task_id == task_id,
                    ExecutorProfile.profile_status == "ожидает подтверждения"
                ).first()
                
                # Если не нашли и есть generalId в webhook, ищем по нему
                if not executor and general_id_from_webhook:
                    logger.debug(f"Task {task_id} not found by id, trying to find by generalId={general_id_from_webhook} from webhook")
                    executor = db.query(ExecutorProfile).filter(
                        ExecutorProfile.registration_task_id == general_id_from_webhook,
                        ExecutorProfile.profile_status == "ожидает подтверждения"
                    ).first()
                    if executor:
                        logger.info(f"Found executor {executor.telegram_id} by generalId={general_id_from_webhook}, updating registration_task_id to id={task_id}")
                        # Обновляем registration_task_id на правильный id для будущих поисков
                        executor.registration_task_id = task_id
                        db.commit()
                
                # Если не нашли, пробуем найти по generalId (если в базе сохранен generalId)
                # В базе может быть сохранен generalId, а в webhook приходит id
                # Проверяем все задачи регистрации через API, чтобы найти совпадение
                if not executor:
                    logger.debug(f"Task {task_id} not found by id, trying to find by checking all pending registration tasks")
                    all_pending = db.query(ExecutorProfile).filter(
                        ExecutorProfile.profile_status == "ожидает подтверждения"
                    ).all()
                    logger.debug(f"Checking {len(all_pending)} pending registration tasks for task {task_id}")
                    
                    # Проверяем каждую задачу регистрации через API
                    for pending_executor in all_pending:
                        if not pending_executor.registration_task_id:
                            continue
                        
                        saved_id = pending_executor.registration_task_id
                        logger.debug(f"Checking executor {pending_executor.telegram_id} with registration_task_id={saved_id}")
                        
                        # Пробуем получить задачу по сохраненному ID
                        try:
                            # Если сохранен generalId, запрос по нему вернет задачу с id
                            # Если сохранен id, запрос по нему вернет ту же задачу
                            check_response = await planfix_client.get_task_by_id(
                                saved_id,
                                fields="id"
                            )
                            if check_response and check_response.get('result') == 'success':
                                check_task_data = check_response.get('task', {})
                                check_task_id = check_task_data.get('id')
                                # Если id из запроса совпадает с task_id из webhook, это наша задача
                                if check_task_id and str(check_task_id) == str(task_id):
                                    executor = pending_executor
                                    logger.info(f"Found executor {pending_executor.telegram_id} by matching task: saved_id={saved_id} -> task_id={task_id}")
                                    # Обновляем registration_task_id на правильный id для будущих поисков
                                    executor.registration_task_id = task_id
                                    db.commit()
                                    break
                        except Exception as e:
                            logger.debug(f"Error checking task {saved_id} for executor {pending_executor.telegram_id}: {e}")
                            continue
                
                if not executor:
                    logger.warning(f"No executor found for registration task {task_id}. This may be because the task was created with generalId but webhook sends id.")
                
                if executor:
                    # Получаем имя статуса из всех возможных мест
                    status_obj = task.get('status', {})
                    status_name = (
                        status_obj.get('name') or 
                        status_obj.get('task.status.name') or 
                        status_obj.get('task.status.Активный') or
                        status_obj.get('task.status.Статус') or
                        'Unknown'
                    )
                    logger.info(f"Found registration task {task_id} for executor {executor.telegram_id}, status_id={new_status_id}, status_name='{status_name}'")
                    
                    # Извлекаем planfix_user_id из assignee в webhook данных (приоритет)
                    planfix_user_id_from_webhook = None
                    if assignee_users:
                        for assignee in assignee_users:
                            assignee_id = assignee.get('id')
                            assignee_name = assignee.get('name')
                            
                            if assignee_id:
                                # Нормализуем ID (может быть "user:123" или просто "123")
                                normalized_id = self._normalize_user_id(assignee_id)
                                if normalized_id:
                                    # Проверяем, что это не имя, а ID (должно быть числом)
                                    try:
                                        int(normalized_id)
                                        planfix_user_id_from_webhook = normalized_id
                                        logger.info(f"Found planfix_user_id {planfix_user_id_from_webhook} from assignee in webhook for task {task_id}")
                                        break
                                    except (ValueError, TypeError):
                                        # Это имя, а не ID, попробуем найти по имени
                                        logger.debug(f"Assignee id '{assignee_id}' is a name, trying to find user by name")
                                        if assignee_name:
                                            # Пробуем найти пользователя по имени через API
                                            try:
                                                user_id = await self._find_user_id_by_name(assignee_name)
                                                if user_id:
                                                    planfix_user_id_from_webhook = user_id
                                                    logger.info(f"Found planfix_user_id {planfix_user_id_from_webhook} by name '{assignee_name}' for task {task_id}")
                                                    break
                                            except Exception as e:
                                                logger.debug(f"Failed to find user by name '{assignee_name}': {e}")
                                        continue
                    
                    # Проверяем по ID и по имени статуса (на случай если ID не совпадает)
                    is_completed = False
                    is_cancelled = False
                    
                    if new_status_id:
                        from services.status_registry import get_status_id
                        completed_id = get_status_id(StatusKey.COMPLETED, required=False)
                        finished_id = get_status_id(StatusKey.FINISHED, required=False)
                        cancelled_id = get_status_id(StatusKey.CANCELLED, required=False)
                        rejected_id = get_status_id(StatusKey.REJECTED, required=False)
                        
                        logger.info(f"Checking status {new_status_id} ('{status_name}') against COMPLETED={completed_id}, FINISHED={finished_id}, CANCELLED={cancelled_id}, REJECTED={rejected_id}")
                        
                        # Проверяем по ID
                        is_completed = status_in(new_status_id, (StatusKey.COMPLETED, StatusKey.FINISHED))
                        is_cancelled = status_in(new_status_id, (StatusKey.CANCELLED, StatusKey.REJECTED))
                        
                        # Если по ID не определили, проверяем по имени
                        if not is_completed and not is_cancelled:
                            status_name_lower = status_name.lower().strip()
                            if status_name_lower in ('завершена', 'завершенная', 'completed', 'finished', 'done'):
                                logger.info(f"Status '{status_name}' recognized as completed by name")
                                is_completed = True
                            elif status_name_lower in ('отменена', 'отклонена', 'cancelled', 'canceled', 'rejected'):
                                logger.info(f"Status '{status_name}' recognized as cancelled/rejected by name")
                                is_cancelled = True
                        
                        logger.info(f"Final check: is_completed={is_completed}, is_cancelled={is_cancelled}")
                    
                    if is_completed:
                        logger.info(f"Registration task {task_id} is completed, approving executor {executor.telegram_id}")
                        # Передаем planfix_user_id из webhook, если он был найден
                        await self._approve_executor(executor.telegram_id, task_id, planfix_user_id=planfix_user_id_from_webhook)
                    elif is_cancelled:
                        logger.info(f"Registration task {task_id} is cancelled/rejected, rejecting executor {executor.telegram_id}")
                        await self._reject_executor(executor.telegram_id, task_id)
                    elif new_status_id:
                        logger.warning(f"Registration task {task_id} status {new_status_id} ('{status_name}') is not recognized as a terminal status for executor approval")
                    else:
                        logger.warning(f"Could not determine status for registration task {task_id}, status data: {task.get('status', {})}")
            
            # Обновляем задачу в кэше БД
            try:
                with self.db_manager.get_db() as db:
                    task_name = task.get('name', '')
                    counterparty_id = None
                    counterparty_raw = task.get('counterparty')
                    if counterparty_raw:
                        if isinstance(counterparty_raw, dict):
                            counterparty_id = counterparty_raw.get('id')
                        elif isinstance(counterparty_raw, str) and ':' in counterparty_raw:
                            counterparty_id = int(counterparty_raw.split(':')[-1]) if counterparty_raw.split(':')[-1].isdigit() else None
                        elif isinstance(counterparty_raw, (int, str)) and str(counterparty_raw).isdigit():
                            counterparty_id = int(counterparty_raw)
                    
                    project_id = None
                    project_raw = task.get('project', {})
                    if isinstance(project_raw, dict):
                        project_id_raw = project_raw.get('id')
                        if project_id_raw:
                            try:
                                if isinstance(project_id_raw, str) and ':' in project_id_raw:
                                    project_id = int(project_id_raw.split(':')[-1])
                                elif isinstance(project_id_raw, (int, str)) and str(project_id_raw).isdigit():
                                    project_id = int(project_id_raw)
                            except:
                                pass
                    
                    template_id = None
                    template_obj = task.get('template', {})
                    if isinstance(template_obj, dict):
                        template_id_raw = template_obj.get('id')
                        if template_id_raw:
                            try:
                                template_id = int(template_id_raw) if isinstance(template_id_raw, (int, str)) and str(template_id_raw).isdigit() else None
                            except:
                                pass
                    
                    # Обновляем TaskCache, сохраняя существующие user_telegram_id и created_by_bot
                    existing_cache = self.db_manager.get_task_cache(db, task_id)
                    user_telegram_id = None
                    created_by_bot = True
                    if existing_cache:
                        # Сохраняем существующие значения, если они есть
                        user_telegram_id = existing_cache.user_telegram_id
                        created_by_bot = existing_cache.created_by_bot
                    
                    self.db_manager.create_or_update_task_cache(
                        db=db,
                        task_id=task_id,
                        name=task_name if task_name else None,
                        status_id=new_status_id,
                        status_name=status_name_raw,
                        counterparty_id=counterparty_id,
                        project_id=project_id,
                        template_id=template_id,
                        user_telegram_id=user_telegram_id,
                        created_by_bot=created_by_bot,
                        date_of_last_update=datetime.now()
                    )
                    logger.debug(f"✅ Updated task {task_id} in TaskCache (status: {new_status_id})")
            except Exception as cache_err:
                logger.warning(f"Failed to update task {task_id} in TaskCache: {cache_err}")
            
            # Обрабатываем изменение статуса
            # #region agent log
            import json, os, time
            log_path = r"b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log"
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"STATUS1","location":"webhook_server.py:589","message":"status change check","data":{"task_id":task_id,"old_status_id":old_status_id,"new_status_id":new_status_id,"status_obj":status_obj,"status_id_raw":status_id_raw},"timestamp":int(time.time()*1000)})+"\n")
            except: pass
            # #endregion
            if new_status_id != old_status_id:
                # Обновляем кэш статуса
                if new_status_id:
                    self._task_status_cache[task_id] = new_status_id
                else:
                    self._task_status_cache.pop(task_id, None)
                
                # Отправляем уведомление об изменении статуса (если это реальное изменение)
                if old_status_id is not None and new_status_id is not None:
                    # #region agent log
                    try:
                        with open(log_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"STATUS2","location":"webhook_server.py:597","message":"calling notify_task_status_changed","data":{"task_id":task_id,"old_status_id":old_status_id,"new_status_id":new_status_id},"timestamp":int(time.time()*1000)})+"\n")
                    except: pass
                    # #endregion
                    try:
                        await self.notification_service.notify_task_status_changed(
                            task_id=task_id,
                            old_status_id=old_status_id,
                            new_status_id=new_status_id
                        )
                        # #region agent log
                        try:
                            with open(log_path, "a", encoding="utf-8") as f:
                                f.write(json.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"STATUS2","location":"webhook_server.py:604","message":"notify_task_status_changed completed","data":{"task_id":task_id},"timestamp":int(time.time()*1000)})+"\n")
                        except: pass
                        # #endregion
                    except Exception as e:
                        logger.error(f"Error notifying status change for task {task_id}: {e}")
                        # #region agent log
                        try:
                            with open(log_path, "a", encoding="utf-8") as f:
                                f.write(json.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"STATUS2","location":"webhook_server.py:605","message":"notify_task_status_changed failed","data":{"task_id":task_id,"error":str(e)},"timestamp":int(time.time()*1000)})+"\n")
                        except: pass
                        # #endregion
            else:
                # #region agent log
                try:
                    with open(log_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"STATUS3","location":"webhook_server.py:615","message":"status not changed (same)","data":{"task_id":task_id,"status_id":new_status_id},"timestamp":int(time.time()*1000)})+"\n")
                except: pass
                # #endregion
                
                # Обрабатываем завершение задачи
                if status_in(new_status_id, (StatusKey.COMPLETED, StatusKey.FINISHED)):
                    await self._handle_task_completed(task_id, new_status_id)
            
            # Обрабатываем назначения исполнителей
            if assignee_users:
                await self._handle_task_assignments(task_id, assignee_users)
                        
        except Exception as e:
            logger.error(f"Error handling task updated: {e}", exc_info=True)
    
    async def handle_comment_added(self, data: dict):
        """Обработка добавления комментария."""
        try:
            task = data.get('task', {})
            task_id_raw = task.get('id')
            comment = data.get('comment', {})
            
            if not task_id_raw:
                logger.warning(f"Incomplete comment data in webhook: {data}")
                return
            
            # Преобразуем task_id в int, если это строка
            try:
                task_id = int(task_id_raw) if isinstance(task_id_raw, str) else task_id_raw
            except (ValueError, TypeError):
                logger.warning(f"Invalid task_id format: {task_id_raw}")
                return
            
            # Фильтруем комментарии от бота
            if self._is_bot_comment(comment):
                logger.debug(f"Comment from bot in task {task_id} skipped")
                return
            
            comment_text = comment.get('description', '')
            comment_author = comment.get('owner', {}).get('name', 'Неизвестно')
            comment_id = comment.get('id')
            
            logger.info(f"💬 New comment in task {task_id} from {comment_author}")
            await self.notification_service.notify_new_comment(
                task_id=task_id,
                comment_author=comment_author,
                comment_text=comment_text,
                comment_id=comment_id
            )
        except Exception as e:
            logger.error(f"Error handling comment added: {e}", exc_info=True)
    
    async def _handle_task_assignments(self, task_id: int, assignee_users: list):
        """Обрабатывает назначения исполнителей на задачу."""
        try:
            with self.db_manager.get_db() as db:
                from database import TaskAssignment, ExecutorProfile
                
                # Получаем текущие назначения из БД
                existing_assignments = {
                    (a.task_id, a.executor_telegram_id): a
                    for a in db.query(TaskAssignment).filter(
                        TaskAssignment.task_id == task_id,
                        TaskAssignment.status == "active"
                    ).all()
                }
                
                # Получаем назначенных пользователей из webhook
                assigned_user_ids = set()
                for user in assignee_users:
                    # Проверяем, что user - это словарь
                    if isinstance(user, dict):
                        user_id = self._normalize_user_id(user.get('id'))
                        if user_id:
                            assigned_user_ids.add(user_id)
                    elif isinstance(user, str):
                        # Если user - это строка, пытаемся нормализовать её как ID
                        user_id = self._normalize_user_id(user)
                        if user_id:
                            assigned_user_ids.add(user_id)
                
                # Находим исполнителей по planfix_user_id
                executors = db.query(ExecutorProfile).filter(
                    ExecutorProfile.planfix_user_id.in_(assigned_user_ids),
                    ExecutorProfile.profile_status == "активен"
                ).all()
                
                # Создаем новые назначения
                for executor in executors:
                    key = (task_id, executor.telegram_id)
                    if key not in existing_assignments:
                        # Создаем новое назначение
                        assignment = TaskAssignment(
                            task_id=task_id,
                            executor_telegram_id=executor.telegram_id,
                            planfix_user_id=executor.planfix_user_id,
                            status="active"
                        )
                        db.add(assignment)
                        logger.info(f"✅ Created TaskAssignment: task {task_id} -> executor {executor.telegram_id}")
                
                # Деактивируем назначения для исполнителей, которые больше не назначены
                for key, assignment in existing_assignments.items():
                    executor = db.query(ExecutorProfile).filter(
                        ExecutorProfile.telegram_id == assignment.executor_telegram_id
                    ).first()
                    if not executor or executor.planfix_user_id not in assigned_user_ids:
                        assignment.status = "cancelled"
                        logger.info(f"❌ Deactivated TaskAssignment: task {task_id} -> executor {assignment.executor_telegram_id}")
                
                db.commit()
        except Exception as e:
            logger.error(f"Error handling task assignments for task {task_id}: {e}", exc_info=True)
    
    async def _handle_task_completed(self, task_id: int, status_id: int):
        """Обрабатывает завершение задачи."""
        try:
            with self.db_manager.get_db() as db:
                from database import TaskAssignment
                
                # Деактивируем все активные назначения для этой задачи
                assignments = db.query(TaskAssignment).filter(
                    TaskAssignment.task_id == task_id,
                    TaskAssignment.status == "active"
                ).all()
                
                for assignment in assignments:
                    assignment.status = "completed"
                    logger.info(f"✅ Completed TaskAssignment: task {task_id} -> executor {assignment.executor_telegram_id}")
                
                # Удаляем из кэша статусов
                self._task_status_cache.pop(task_id, None)
                
                db.commit()
        except Exception as e:
            logger.error(f"Error handling task completion for task {task_id}: {e}", exc_info=True)
    
    async def _approve_executor(self, telegram_id: int, task_id: int, planfix_user_id: Optional[str] = None):
        """Подтверждает регистрацию исполнителя."""
        try:
            with self.db_manager.get_db() as db:
                executor = self.db_manager.get_executor_profile(db, telegram_id)
                
                if not executor:
                    logger.warning(f"Executor {telegram_id} not found for approval")
                    return
                
                # Если planfix_user_id не передан, используем planfix_contact_id (ID контакта)
                if not planfix_user_id:
                    if executor.planfix_contact_id:
                        planfix_user_id = str(executor.planfix_contact_id)
                        logger.info(f"Using planfix_contact_id {planfix_user_id} as planfix_user_id")
                    else:
                        # Пытаемся извлечь из задачи (fallback)
                        planfix_user_id = await self._extract_planfix_user_id(task_id)
                
                # Обновляем статус исполнителя
                self.db_manager.update_executor_profile(
                    db,
                    telegram_id,
                    profile_status="активен",
                    confirmation_date=datetime.now(),
                    planfix_user_id=planfix_user_id
                )
                
                concept_names = [FRANCHISE_GROUPS[cid]["name"] for cid in executor.serving_franchise_groups]
                
                message = (
                    f"✅ Ваша регистрация подтверждена!\n\n"
                    f"Теперь вы будете получать заявки по концепциям:\n"
                    f"🏢 {', '.join(concept_names)}\n\n"
                    f"Используйте меню для работы с заявками."
                )
                # Отправляем сообщение с клавиатурой меню исполнителя
                await self.notification_service._send_notification(
                    telegram_id, 
                    message, 
                    reply_markup=get_executor_main_menu_keyboard()
                )
                logger.info(f"✅ Executor {telegram_id} approved via webhook (planfix_user_id: {planfix_user_id})")
        except Exception as e:
            logger.error(f"Error approving executor: {e}", exc_info=True)
    
    async def _find_user_id_by_name(self, user_name: str) -> Optional[str]:
        """Находит Planfix User ID по имени пользователя."""
        try:
            # Пробуем найти пользователя через поиск контактов
            # В Planfix пользователи могут быть контактами
            # Используем get_contact_list_by_group с фильтром по имени
            # Но сначала нужно получить все группы или использовать общий поиск
            # Попробуем использовать метод get_contact_list_by_group с фильтром
            endpoint = "/contact/list"
            data = {
                "filters": [
                    {
                        "type": 4001,  # Фильтр по имени контакта
                        "operator": "equal",
                        "value": user_name
                    }
                ],
                "fields": "id,name,userGeneralId",
                "pageSize": 10
            }
            search_response = await planfix_client._request("POST", endpoint, data=data)
            
            if search_response and search_response.get('result') == 'success':
                contacts = search_response.get('contacts', [])
                for contact in contacts:
                    # Проверяем, есть ли у контакта userGeneralId (это означает, что это пользователь)
                    user_general_id = contact.get('userGeneralId')
                    if user_general_id:
                        logger.info(f"Found user ID {user_general_id} for name '{user_name}'")
                        return str(user_general_id)
            
            return None
        except Exception as e:
            logger.debug(f"Error finding user by name '{user_name}': {e}")
            return None
    
    async def _extract_planfix_user_id(self, task_id: int) -> Optional[str]:
        """Извлекает planfix_user_id из задачи регистрации."""
        try:
            # Пробуем использовать generalId вместо id для запроса задачи
            # Иногда API не принимает id, но принимает generalId
            task_response = None
            try:
                task_response = await planfix_client.get_task_by_id(
                    task_id,
                    fields="id,name,description,customFieldData,comments,assignees"
                )
            except Exception as api_err:
                logger.warning(f"Failed to get task {task_id} by id, error: {api_err}")
                # Если не получилось по id, пробуем найти через другие методы
                return None
            
            if not task_response or task_response.get('result') != 'success':
                return None
            
            task = task_response.get('task', {})
            
            # ПРИОРИТЕТ 1: Извлекаем из назначенных исполнителей
            assignees = task.get('assignees', {})
            if isinstance(assignees, dict):
                users = assignees.get('users', [])
                if users and isinstance(users, list) and len(users) > 0:
                    first_assignee = users[0]
                    assignee_id = first_assignee.get('id')
                    assignee_name = first_assignee.get('name')
                    
                    if assignee_id:
                        planfix_user_id = self._normalize_user_id(assignee_id)
                        if planfix_user_id:
                            # Проверяем, что это ID, а не имя
                            try:
                                int(planfix_user_id)
                                logger.info(f"Found planfix_user_id {planfix_user_id} from assignee in task {task_id}")
                                return planfix_user_id
                            except (ValueError, TypeError):
                                # Это имя, пробуем найти по имени
                                if assignee_name:
                                    user_id = await self._find_user_id_by_name(assignee_name)
                                    if user_id:
                                        logger.info(f"Found planfix_user_id {user_id} by name '{assignee_name}' for task {task_id}")
                                        return user_id
            
            # ПРИОРИТЕТ 2: Ищем в кастомных полях
            # Согласно swagger.json, customFieldData - массив объектов:
            # [{"field": {"id": 10, "type": 0}, "value": "Test value"}]
            custom_fields = task.get('customFieldData', [])
            if isinstance(custom_fields, list):
                for field_data in custom_fields:
                    if not isinstance(field_data, dict):
                        continue
                    
                    field_obj = field_data.get('field', {})
                    if not isinstance(field_obj, dict):
                        continue
                    
                    field_id = field_obj.get('id')
                    field_type = field_obj.get('type')  # 0=Line, 1=Number, 10=Contact, 11=Employee, etc.
                    field_value = field_data.get('value')
                    
                    # Ищем в полях типа Line (0) или Number (1), которые могут содержать User ID
                    if field_id in (85, 86, 87, 88, 89, 90) and field_value:
                        planfix_user_id = str(field_value).strip()
                        if planfix_user_id.isdigit():
                            logger.info(f"Found planfix_user_id {planfix_user_id} in custom field {field_id} (type {field_type})")
                            return planfix_user_id
                    
                    # Также проверяем поля типа Employee (11), которые содержат объект {"id": "user:3", "name": "Petrov"}
                    if field_type == 11 and isinstance(field_value, dict):
                        employee_id = field_value.get('id')
                        if employee_id:
                            normalized_id = self._normalize_user_id(employee_id)
                            if normalized_id and normalized_id.isdigit():
                                logger.info(f"Found planfix_user_id {normalized_id} in custom field {field_id} (type Employee)")
                                return normalized_id
            
            # ПРИОРИТЕТ 3: Ищем в описании
            description = task.get('description', '')
            # Ищем "Planfix User ID" или "Telegram ID" (для задач регистрации)
            match = re.search(r'[Pp]lanfix\s+[Uu]ser\s+ID[:\s]+(\d+)', description)
            if match:
                planfix_user_id = match.group(1)
                logger.info(f"Found planfix_user_id {planfix_user_id} in task description")
                return planfix_user_id
            
            # ПРИОРИТЕТ 4: Ищем в комментариях
            comments = task.get('comments', [])
            if isinstance(comments, list):
                for comment in comments:
                    comment_text = comment.get('description', '') if isinstance(comment, dict) else str(comment)
                    # Ищем "Planfix User ID" или "Telegram ID" в комментариях
                    match = re.search(r'[Pp]lanfix\s+[Uu]ser\s+ID[:\s]+(\d+)', comment_text)
                    if match:
                        planfix_user_id = match.group(1)
                        logger.info(f"Found planfix_user_id {planfix_user_id} in task comment")
                        return planfix_user_id
                    
                    # Также ищем в JSON комментария (если есть)
                    comment_json = comment.get('json', {}) if isinstance(comment, dict) else {}
                    if isinstance(comment_json, dict):
                        comment_json_text = comment_json.get('description', '')
                        if comment_json_text:
                            match = re.search(r'[Pp]lanfix\s+[Uu]ser\s+ID[:\s]+(\d+)', comment_json_text)
                            if match:
                                planfix_user_id = match.group(1)
                                logger.info(f"Found planfix_user_id {planfix_user_id} in task comment JSON")
                                return planfix_user_id
            
            return None
        except Exception as e:
            logger.error(f"Error extracting planfix_user_id from task {task_id}: {e}")
            return None
    
    async def _reject_executor(self, telegram_id: int, task_id: int):
        """Отклоняет регистрацию исполнителя."""
        try:
            with self.db_manager.get_db() as db:
                self.db_manager.update_executor_profile(
                    db,
                    telegram_id,
                    profile_status="отклонен"
                )
                
                message = (
                    f"❌ Ваша регистрация отклонена.\n\n"
                    f"Обратитесь к администратору для выяснения причин."
                )
                await self.notification_service._send_notification(telegram_id, message)
                logger.info(f"Executor {telegram_id} rejected via webhook")
        except Exception as e:
            logger.error(f"Error rejecting executor: {e}", exc_info=True)
    
    async def handle_task_reminder(self, data: dict):
        """
        Обработка напоминания о задаче, которая еще не взята в работу.
        Используется для повторной отправки уведомлений исполнителям.
        """
        try:
            logger.info(f"🔔 Processing task.reminder webhook")
            logger.debug(f"🔔 task.reminder full data: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            task = data.get('task', {})
            if not task:
                logger.warning(f"⚠️ No 'task' field in reminder webhook. Data keys: {list(data.keys())}")
                return
            
            task_id_raw = task.get('id')
            
            if not task_id_raw:
                logger.warning(f"⚠️ Incomplete task data in reminder webhook: task object keys: {list(task.keys())}, full data: {json.dumps(data, ensure_ascii=False)}")
                return
            
            logger.info(f"📋 Task reminder: raw task_id={task_id_raw}, type={type(task_id_raw)}")
            
            # Преобразуем task_id в int, если это строка
            # Может быть "task:123" или просто "123" или число
            try:
                if isinstance(task_id_raw, str):
                    # Если это строка вида "task:123" или "user:123", извлекаем число
                    if ':' in task_id_raw:
                        task_id = int(task_id_raw.split(':')[-1])
                    else:
                        task_id = int(task_id_raw)
                else:
                    task_id = int(task_id_raw)
                logger.info(f"✅ Task reminder: normalized task_id={task_id}")
            except (ValueError, TypeError) as e:
                logger.error(f"❌ Invalid task_id format in reminder: {task_id_raw} (type: {type(task_id_raw)}), error: {e}")
                return
            
            # Если в вебхуке недостаточно данных (только id), получаем задачу через API
            # ВАЖНО: В настройках Planfix webhook должен содержать:
            # - {{Задача.Шаблон.Идентификатор}} (template.id)
            # - {{Задача.Номер}} (generalId или id)
            # - {{Задача.Теги}} (tags)
            task_data_from_webhook = task
            
            # Извлекаем данные из webhook (если они есть)
            template_id_from_webhook = None
            task_number_from_webhook = None
            tags_from_webhook = None
            
            # Пытаемся извлечь шаблон из webhook
            template_obj = task.get('template') or task.get('task.template') or {}
            if isinstance(template_obj, dict):
                template_id_from_webhook = self._normalize_int(template_obj.get('id'))
            elif isinstance(template_obj, (int, str)):
                template_id_from_webhook = self._normalize_int(template_obj)
            
            # Пытаемся извлечь номер задачи из webhook
            task_number_from_webhook = task.get('generalId') or task.get('number') or task.get('task.number')
            
            # Если generalId нет в webhook, пытаемся найти его в BotLog по внутреннему id
            if not task_number_from_webhook:
                logger.debug(f"Task {task_id} reminder: generalId not in webhook, searching in BotLog")
                try:
                    with self.db_manager.get_db() as db:
                        from database import BotLog
                        # Ищем задачу в BotLog по внутреннему id
                        bot_logs = db.query(BotLog).filter(
                            BotLog.action == "create_task",
                            BotLog.success == True
                        ).order_by(BotLog.id.desc()).limit(100).all()
                        
                        for log in bot_logs:
                            if not log.details:
                                continue
                            
                            # Проверяем все возможные ID в BotLog
                            log_internal_id = log.details.get('task_id_internal')
                            log_task_id = log.details.get('task_id')
                            
                            # Нормализуем ID из лога для сравнения
                            log_id_to_check = None
                            if log_internal_id:
                                try:
                                    log_id_to_check = int(log_internal_id)
                                except (ValueError, TypeError):
                                    pass
                            
                            if not log_id_to_check and log_task_id:
                                try:
                                    log_id_to_check = int(log_task_id)
                                except (ValueError, TypeError):
                                    pass
                            
                            # Если нашли совпадение, берем generalId
                            if log_id_to_check == task_id:
                                task_number_from_webhook = log.details.get('task_id_general') or log.details.get('task_id')
                                if task_number_from_webhook:
                                    logger.info(f"Task {task_id} reminder: found generalId {task_number_from_webhook} in BotLog")
                                    break
                except Exception as log_err:
                    logger.warning(f"Task {task_id} reminder: error searching BotLog for generalId: {log_err}")
            
            # Пытаемся извлечь теги из webhook
            tags_from_webhook = task.get('tags') or task.get('task.tags') or []
            
            logger.info(
                f"Task {task_id} reminder webhook data: "
                f"template_id={template_id_from_webhook}, "
                f"task_number={task_number_from_webhook}, "
                f"tags={tags_from_webhook}, "
                f"has_status={bool(task.get('status'))}, "
                f"has_assignees={bool(task.get('assignees'))}"
            )
            
            needs_full_data = not task.get('status') or not task.get('assignees') or not template_id_from_webhook
            
            if needs_full_data:
                logger.debug(f"Task {task_id} reminder: fetching full task data from API (webhook missing some fields)")
                
                # ВАЖНО: Используем generalId для запроса к API, так как внутренний id может не работать
                api_task_id = None
                if task_number_from_webhook:
                    try:
                        api_task_id = int(task_number_from_webhook)
                        logger.info(f"Task {task_id} reminder: using generalId {api_task_id} for API request")
                    except (ValueError, TypeError):
                        logger.warning(f"Task {task_id} reminder: invalid generalId format: {task_number_from_webhook}")
                
                # Если generalId не найден, пробуем использовать внутренний id (может не сработать)
                if not api_task_id:
                    logger.warning(f"Task {task_id} reminder: generalId not found, trying internal id (may fail)")
                    api_task_id = task_id
                
                try:
                    # Запрашиваем все необходимые поля, включая шаблон, теги и номер
                    task_response = await planfix_client.get_task_by_id(
                        api_task_id,
                        fields="id,generalId,status,assignees,process,project,template.id,tags"
                    )
                    if task_response and task_response.get('result') == 'success':
                        task_data_from_webhook = task_response.get('task', {})
                        # Обновляем данные из API, если их не было в webhook
                        if not template_id_from_webhook:
                            template_obj = task_data_from_webhook.get('template', {})
                            if isinstance(template_obj, dict):
                                template_id_from_webhook = self._normalize_int(template_obj.get('id'))
                        if not task_number_from_webhook:
                            task_number_from_webhook = task_data_from_webhook.get('generalId') or task_data_from_webhook.get('id')
                        if not tags_from_webhook:
                            tags_from_webhook = task_data_from_webhook.get('tags', [])
                        logger.info(f"Task {task_id} reminder: got full task data from API (generalId={task_number_from_webhook})")
                    else:
                        logger.warning(f"Task {task_id} reminder: failed to get task from API (response: {task_response}), using webhook data")
                except Exception as api_err:
                    logger.warning(f"Task {task_id} reminder: error fetching task from API: {api_err}, using webhook data")
            
            # Фильтруем только релевантные задачи
            # Для reminder может не быть полных данных, поэтому проверяем после получения полных данных
            if task_data_from_webhook.get('process') or task_data_from_webhook.get('status'):
                # Есть данные для фильтрации
                if not self._should_process_task(task_data_from_webhook):
                    logger.info(f"Task {task_id} reminder skipped by filter (process/status check)")
                    return
            else:
                # Нет данных для фильтрации, пропускаем фильтр (задача будет проверена позже)
                logger.debug(f"Task {task_id} reminder: skipping filter check (no process/status data yet)")
            
            # Проверяем, что задача еще не взята в работу
            # 1. Проверяем статус задачи (согласно swagger.json, статус - объект {"id": 4, "name": "В работе"})
            status_obj = task_data_from_webhook.get('status', {})
            if isinstance(status_obj, dict):
                status_id_raw = (
                    status_obj.get('id') or  # Стандартный формат (приоритет)
                    status_obj.get('task.status.id') or 
                    status_obj.get('task.status.Идентификатор')
                )
            else:
                status_id_raw = None
            status_id = self._normalize_status_id(status_id_raw)
            
            # Если статус не "Новая" или подобный, пропускаем
            from services.status_registry import ensure_status_registry_loaded, get_status_id
            await ensure_status_registry_loaded()
            
            new_status_id = get_status_id(StatusKey.NEW, required=False)
            if status_id and new_status_id and status_id != new_status_id:
                # Проверяем, не в работе ли задача
                in_progress_id = get_status_id(StatusKey.IN_PROGRESS, required=False)
                if status_id == in_progress_id:
                    logger.info(f"Task {task_id} reminder skipped: task is already in progress (status_id={status_id})")
                    return
            
            # 2. Проверяем наличие активных назначений в БД
            # Используем generalId для проверки в БД, если он доступен
            db_task_id = int(task_number_from_webhook) if task_number_from_webhook else task_id
            with self.db_manager.get_db() as db:
                from database import TaskAssignment
                active_assignments = db.query(TaskAssignment).filter(
                    TaskAssignment.task_id == db_task_id,
                    TaskAssignment.status == "active"
                ).count()
                
                if active_assignments > 0:
                    logger.info(f"Task {task_id} reminder skipped: task has {active_assignments} active assignment(s)")
                    return
            
            # 3. Проверяем назначенных исполнителей в Planfix
            # Согласно swagger.json, assignees.users - массив объектов [{"id": "user:5", "name": "Иван"}]
            assignees = task_data_from_webhook.get('assignees', {})
            assignee_users = []
            if isinstance(assignees, dict):
                assignee_users_raw = assignees.get('users', [])
                if isinstance(assignee_users_raw, list):
                    assignee_users = assignee_users_raw
                elif isinstance(assignee_users_raw, dict):
                    assignee_users = [assignee_users_raw]
            
            if assignee_users and len(assignee_users) > 0:
                logger.info(f"Task {task_id} reminder skipped: task has {len(assignee_users)} assignee(s) in Planfix")
                return
            
            # Задача не взята в работу - отправляем повторные уведомления
            # Используем generalId для уведомлений, если он доступен
            notification_task_id = int(task_number_from_webhook) if task_number_from_webhook else task_id
            logger.info(f"🔔 Reminder for unassigned task {task_id} (generalId={task_number_from_webhook}) - resending notifications to executors")
            try:
                await self.task_notification_service.notify_executors_about_new_task(notification_task_id)
                logger.info(f"✅ Successfully sent reminder notifications for task {notification_task_id}")
            except Exception as notify_err:
                logger.error(f"❌ Error sending reminder notifications for task {task_id}: {notify_err}", exc_info=True)
            
        except Exception as e:
            logger.error(f"Error handling task reminder: {e}", exc_info=True)


async def webhook_handler(request):
    """Обработчик входящих webhook от Planfix."""
    try:
        # Логируем ВСЕ входящие запросы на самом раннем этапе
        logger.info(f"🌐 Webhook request received: {request.method} {request.path_qs}, headers: {dict(request.headers)}")
        
        # ПРИМЕЧАНИЕ: Basic Auth проверяется на уровне Nginx (auth_basic)
        # Если запрос дошел до приложения, значит Nginx уже проверил аутентификацию
        # Дополнительная проверка Basic Auth в приложении отключена, чтобы избежать конфликтов
        # Если нужно проверять Basic Auth только в приложении (без Nginx), раскомментируйте код ниже
        
        # Проверка HTTP Basic Authentication (только если НЕ используется Nginx с auth_basic)
        # Раскомментируйте, если хотите проверять Basic Auth только в приложении:
        # if PLANFIX_WEBHOOK_USERNAME and PLANFIX_WEBHOOK_PASSWORD:
        #     import base64
        #     auth_header = request.headers.get('Authorization', '')
        #     if not auth_header.startswith('Basic '):
        #         logger.warning("Webhook authentication required but no Basic Auth header found")
        #         return web.Response(
        #             text='Authentication required',
        #             status=401,
        #             headers={'WWW-Authenticate': 'Basic realm="Planfix Webhook"'}
        #         )
        #     
        #     try:
        #         # Декодируем Basic Auth
        #         encoded = auth_header.split(' ', 1)[1]
        #         decoded = base64.b64decode(encoded).decode('utf-8')
        #         username, password = decoded.split(':', 1)
        #         
        #         # Проверяем учетные данные
        #         if username != PLANFIX_WEBHOOK_USERNAME or password != PLANFIX_WEBHOOK_PASSWORD:
        #             logger.warning(f"Invalid webhook credentials: username={username}")
        #             return web.Response(
        #                 text='Invalid credentials',
        #                 status=401,
        #                 headers={'WWW-Authenticate': 'Basic realm="Planfix Webhook"'}
        #             )
        #         logger.debug(f"Webhook Basic Auth successful for user: {username}")
        #     except Exception as auth_err:
        #         logger.warning(f"Error processing Basic Auth: {auth_err}")
        #         return web.Response(
        #             text='Authentication error',
        #             status=401,
        #             headers={'WWW-Authenticate': 'Basic realm="Planfix Webhook"'}
        #         )
        
        # Получаем сырое тело запроса для диагностики
        raw_body = await request.read()
        
        # Проверка размера тела запроса (защита от DoS)
        if len(raw_body) > WEBHOOK_MAX_BODY_SIZE:
            logger.warning(f"Webhook body too large: {len(raw_body)} bytes (max: {WEBHOOK_MAX_BODY_SIZE})")
            return web.Response(text='Payload too large', status=413)
        
        content_type = request.headers.get('Content-Type', '').lower()
        
        # Проверка подписи webhook (если настроен секрет)
        if PLANFIX_WEBHOOK_SECRET:
            signature_header = request.headers.get('X-Planfix-Signature') or request.headers.get('X-Signature')
            if signature_header:
                # Вычисляем ожидаемую подпись (HMAC-SHA256)
                expected_signature = hmac.new(
                    PLANFIX_WEBHOOK_SECRET.encode('utf-8'),
                    raw_body,
                    hashlib.sha256
                ).hexdigest()
                
                # Сравниваем подписи (защита от timing attacks)
                if not hmac.compare_digest(signature_header, expected_signature):
                    logger.warning("Invalid webhook signature")
                    return web.Response(text='Invalid signature', status=401)
            else:
                logger.warning("Webhook secret configured but no signature header found")
                # Не блокируем, т.к. Planfix может не отправлять подпись
        
        # Логируем информацию о запросе
        logger.info(f"Received webhook: method={request.method}, content_type={content_type}, body_length={len(raw_body)}")
        
        # Логируем первые 200 символов тела запроса для отладки
        if raw_body:
            body_preview = raw_body.decode('utf-8', errors='ignore')[:200]
            logger.debug(f"Webhook body preview: {body_preview}")
        
        # Пытаемся распарсить данные в зависимости от Content-Type
        data = {}
        # #region agent log
        import json as json_module, os, time
        log_path = r"b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log"
        webhook_start = time.time()
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json_module.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"WEBHOOK1","location":"webhook_server.py:1304","message":"webhook received","data":{"method":request.method,"content_type":content_type,"body_length":len(raw_body)},"timestamp":int(time.time()*1000)})+"\n")
        except: pass
        # #endregion
        
        if raw_body:
            try:
                if 'application/json' in content_type:
                    # Парсим JSON из сырого тела
                    body_text = raw_body.decode('utf-8')
                    # Удаляем markdown-код блоки, если они есть (```json ... ```)
                    body_text = body_text.strip()
                    if body_text.startswith('```'):
                        # Удаляем начальный ```json или ```
                        lines = body_text.split('\n')
                        if lines[0].startswith('```'):
                            lines = lines[1:]
                        # Удаляем конечный ```
                        if lines and lines[-1].strip() == '```':
                            lines = lines[:-1]
                        body_text = '\n'.join(lines)
                    
                    # Исправляем проблему с массивами, вставленными как строки
                    # Planfix может вставлять "[]" или "["value"]" как строки вместо массивов
                    import re
                    # Заменяем строки вида "[]" на пустые массивы []
                    body_text = re.sub(r':\s*"\[\]"', ': []', body_text)
                    # Заменяем строки вида "["value"]" на массивы ["value"]
                    # Используем более точное регулярное выражение для обработки вложенных кавычек и массивов
                    def fix_array_strings(match):
                        value = match.group(1)
                        # Если это валидный JSON-массив, заменяем строку на массив
                        try:
                            # Проверяем, что это валидный JSON-массив
                            parsed = json.loads(value)
                            if isinstance(parsed, list):
                                return f': {value}'
                        except:
                            pass
                        return match.group(0)  # Оставляем как есть, если не валидный JSON
                    
                    # Ищем строки вида ": "["value"]" или ": "[]""
                    # Обрабатываем массивы с кавычками внутри, например: "["Робот Бендер"]"
                    # Используем более сложное регулярное выражение для обработки вложенных кавычек
                    # Сначала обрабатываем простые случаи
                    body_text = re.sub(r':\s*"(\[[^\]]*\])"', fix_array_strings, body_text)
                    
                    # Исправляем вложенные JSON-объекты в строках (например, comment.json)
                    # Planfix может вставлять JSON-объекты как строки с неэкранированными кавычками
                    # Упрощенная обработка: пытаемся найти и распарсить JSON-строки после парсинга основного JSON
                    # Это более надежно, чем пытаться исправить до парсинга
                    
                    data = json.loads(body_text)
                    
                    # Постобработка: нормализуем массивы и исправляем вложенные JSON-строки
                    def normalize_webhook_data(obj):
                        """Рекурсивно нормализует данные webhook."""
                        if isinstance(obj, dict):
                            for key, value in obj.items():
                                # Исправляем вложенные JSON-строки (например, comment.json)
                                if isinstance(value, str) and value.strip().startswith('{'):
                                    try:
                                        # Пытаемся распарсить как JSON
                                        parsed = json.loads(value)
                                        obj[key] = normalize_webhook_data(parsed)
                                        continue
                                    except (json.JSONDecodeError, ValueError):
                                        # Если не JSON, оставляем как строку
                                        pass
                                
                                # Если значение - массив с одним элементом, заменяем на элемент
                                if isinstance(value, list) and len(value) == 1:
                                    obj[key] = normalize_webhook_data(value[0])
                                else:
                                    obj[key] = normalize_webhook_data(value)
                        elif isinstance(obj, list):
                            return [normalize_webhook_data(item) for item in obj]
                        return obj
                    
                    data = normalize_webhook_data(data)
                    # #region agent log
                    try:
                        with open(log_path, "a", encoding="utf-8") as f:
                            f.write(json_module.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"WEBHOOK2","location":"webhook_server.py:1389","message":"webhook data parsed","data":{"event_type":data.get("event"),"task_id":data.get("task",{}).get("id") or data.get("task",{}).get("generalId")},"timestamp":int(time.time()*1000)})+"\n")
                    except: pass
                    # #endregion
                elif 'application/x-www-form-urlencoded' in content_type:
                    # Парсим form-urlencoded данные
                    from urllib.parse import parse_qs, unquote
                    form_data = parse_qs(raw_body.decode('utf-8'))
                    # Преобразуем в обычный dict (берем первое значение из списка)
                    for key, value_list in form_data.items():
                        value = value_list[0] if value_list else ''
                        # Пытаемся распарсить JSON из значения
                        try:
                            data[key] = json.loads(unquote(value))
                        except (json.JSONDecodeError, TypeError):
                            data[key] = unquote(value)
                elif 'multipart/form-data' in content_type:
                    # Для multipart нужно использовать request.post(), но тело уже прочитано
                    # Попробуем распарсить как JSON, если это не сработает - вернем OK
                    try:
                        body_text = raw_body.decode('utf-8')
                        # Удаляем markdown-код блоки, если они есть (```json ... ```)
                        body_text = body_text.strip()
                        if body_text.startswith('```'):
                            lines = body_text.split('\n')
                            if lines[0].startswith('```'):
                                lines = lines[1:]
                            if lines and lines[-1].strip() == '```':
                                lines = lines[:-1]
                            body_text = '\n'.join(lines)
                        data = json.loads(body_text)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        logger.warning(f"Could not parse multipart body as JSON. Raw body (first 500 chars): {raw_body[:500]}")
                        return web.Response(text='OK', status=200)
                else:
                    # Пытаемся распарсить как JSON по умолчанию
                    try:
                        body_text = raw_body.decode('utf-8')
                        # Удаляем markdown-код блоки, если они есть (```json ... ```)
                        body_text = body_text.strip()
                        if body_text.startswith('```'):
                            lines = body_text.split('\n')
                            if lines[0].startswith('```'):
                                lines = lines[1:]
                            if lines and lines[-1].strip() == '```':
                                lines = lines[:-1]
                            body_text = '\n'.join(lines)
                        data = json.loads(body_text)
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning(f"Could not parse body as JSON: {e}. Content-Type: {content_type}, Raw body (first 500 chars): {raw_body[:500]}")
                        # Возвращаем успех, чтобы Planfix не повторял запрос
                        return web.Response(text='OK', status=200)
            except Exception as parse_error:
                logger.warning(f"Error parsing request body: {parse_error}. Content-Type: {content_type}, Raw body (first 500 chars): {raw_body[:500]}")
                # Возвращаем успех, чтобы Planfix не повторял запрос
                return web.Response(text='OK', status=200)
        else:
            logger.warning("Received webhook with empty body")
            # Возвращаем успех для пустых запросов (возможно, это проверка доступности)
            return web.Response(text='OK', status=200)
        
        # Логируем распарсенные данные
        if data:
            logger.info(f"Parsed webhook data: {json.dumps(data, ensure_ascii=False, indent=2)}")
        else:
            logger.warning("No data extracted from webhook")
            return web.Response(text='OK', status=200)
        
        handler = request.app['webhook_handler']
        event_type = data.get('event')
        
        # Логируем тип события для всех webhook
        logger.info(f"📥 Webhook event type: '{event_type}' (data keys: {list(data.keys()) if data else 'no data'})")
        
        if not event_type:
            logger.warning(f"Webhook received without event type. Data keys: {list(data.keys())}")
            return web.Response(text='OK', status=200)
        
        if event_type == 'task.create':
            # #region agent log
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json_module.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"WEBHOOK3","location":"webhook_server.py:1465","message":"calling handle_task_created","data":{"event_type":event_type},"timestamp":int(time.time()*1000)})+"\n")
            except: pass
            # #endregion
            await handler.handle_task_created(data)
        elif event_type == 'task.update':
            # #region agent log
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json_module.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"WEBHOOK3","location":"webhook_server.py:1467","message":"calling handle_task_updated","data":{"event_type":event_type},"timestamp":int(time.time()*1000)})+"\n")
            except: pass
            # #endregion
            await handler.handle_task_updated(data)
        elif event_type == 'comment.create':
            await handler.handle_comment_added(data)
        elif event_type == 'task.reminder' or event_type == 'task.remind':
            # Обработка напоминаний о задачах, которые еще не взяты в работу
            logger.info(f"🔔 Received task.reminder webhook")
            logger.debug(f"🔔 task.reminder data: {json.dumps(data, ensure_ascii=False, indent=2)}")
            await handler.handle_task_reminder(data)
        else:
            logger.warning(f"Unknown event type: {event_type}")
        # #region agent log
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json_module.dumps({"sessionId":"debug-session","runId":"webhook","hypothesisId":"WEBHOOK_TOTAL","location":"webhook_server.py:1478","message":"webhook processing completed","data":{"event_type":event_type,"duration_ms":(time.time()-webhook_start)*1000},"timestamp":int(time.time()*1000)})+"\n")
        except: pass
        # #endregion
        
        return web.Response(text='OK', status=200)
    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        # Возвращаем 200 OK даже при ошибке, чтобы Planfix не повторял запрос
        # (если это критическая ошибка, она будет залогирована выше)
        return web.Response(text='Error', status=200)


async def health_check(request):
    """Health check endpoint."""
    return web.Response(text='OK')


def create_webhook_app(bot: Bot) -> web.Application:
    """Создает aiohttp приложение для webhook."""
    app = web.Application()
    handler = PlanfixWebhookHandler(bot)
    app['webhook_handler'] = handler
    
    # Проверяем незавершенные задачи регистрации при старте
    async def on_startup(app):
        await handler.check_pending_registration_tasks()
    
    app.on_startup.append(on_startup)
    
    app.router.add_post('/planfix/webhook', webhook_handler)
    app.router.add_get('/health', health_check)
    
    return app


async def run_webhook_server(bot: Bot, host: str = '0.0.0.0', port: int = 8080):
    """Запускает webhook сервер."""
    app = create_webhook_app(bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"🚀 Webhook server started on {host}:{port}")
    logger.info(f"📡 Webhook URL: http://{host}:{port}/planfix/webhook")
    
    # Держим сервер запущенным
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
