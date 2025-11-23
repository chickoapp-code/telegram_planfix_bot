"""
Webhook сервер для получения уведомлений от Planfix
Версия: 2.0 - Улучшенная обработка событий
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Optional, Set

from aiohttp import web
from aiogram import Bot

from config import BOT_TOKEN, FRANCHISE_GROUPS, PLANFIX_TASK_PROCESS_ID
from db_manager import DBManager
from logging_config import setup_logging
from notifications import NotificationService
from planfix_client import planfix_client
from services.status_registry import StatusKey, is_status, status_in

setup_logging()
logger = logging.getLogger(__name__)

class PlanfixWebhookHandler:
    """Обработчик webhook от Planfix."""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.db_manager = DBManager()
        self.notification_service = NotificationService(bot)
        # Кэш для отслеживания предыдущих статусов задач
        self._task_status_cache = {}  # {task_id: status_id}
        # Кэш для предотвращения дубликатов событий
        self._processed_events = set()  # {(event_type, task_id, timestamp)}
    
    async def check_pending_registration_tasks(self):
        """Проверяет все незавершенные задачи регистрации при старте."""
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
                        status_id = self._normalize_status_id(status_raw.get('id'))
                        status_name = status_raw.get('name', 'Unknown')
                        
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
            if isinstance(status_raw, str) and ":" in status_raw:
                status_raw = status_raw.split(":")[-1]
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
            task_id = task.get('id')
            project_id = task.get('project', {}).get('id')
            
            if not task_id or not project_id:
                logger.warning(f"Incomplete task data in webhook: {data}")
                return
            
            # Фильтруем только релевантные задачи
            if not self._should_process_task(task):
                logger.debug(f"Task {task_id} creation skipped by filter")
                return
            
            logger.info(f"📋 New task created: {task_id} in project {project_id}")
            await self.notification_service.notify_new_task(task_id, project_id)
            
            # Сохраняем начальный статус в кэш
            status_id = self._normalize_status_id(task.get('status', {}).get('id'))
            if status_id:
                self._task_status_cache[task_id] = status_id
                
        except Exception as e:
            logger.error(f"Error handling task created: {e}", exc_info=True)
    
    async def handle_task_updated(self, data: dict):
        """Обработка обновления задачи."""
        try:
            # Убеждаемся, что status registry загружен
            from services.status_registry import ensure_status_registry_loaded
            await ensure_status_registry_loaded()
            
            task = data.get('task', {})
            task_id = task.get('id')
            
            if not task_id:
                logger.warning(f"Incomplete task data in webhook: {data}")
                return
            
            # Фильтруем только релевантные задачи
            if not self._should_process_task(task):
                logger.debug(f"Task {task_id} update skipped by filter")
                return
            
            # Получаем новый статус
            new_status_id = self._normalize_status_id(task.get('status', {}).get('id'))
            old_status_id = self._task_status_cache.get(task_id)
            
            # Получаем назначенных исполнителей
            assignees = task.get('assignees', {})
            assignee_users = assignees.get('users', []) if isinstance(assignees, dict) else []
            
            logger.info(f"📝 Task {task_id} updated, status: {old_status_id} -> {new_status_id}")
            
            # ВАЖНО: Проверяем задачи регистрации ДО обработки изменения статуса,
            # чтобы обработать случаи, когда задача уже была завершена
            # Проверяем, это задача регистрации исполнителя
            with self.db_manager.get_db() as db:
                from database import ExecutorProfile
                executor = db.query(ExecutorProfile).filter(
                    ExecutorProfile.registration_task_id == task_id,
                    ExecutorProfile.profile_status == "ожидает подтверждения"
                ).first()
                
                if executor:
                    status_name = task.get('status', {}).get('name', 'Unknown')
                    logger.info(f"Found registration task {task_id} for executor {executor.telegram_id}, status_id={new_status_id}, status_name='{status_name}'")
                    if new_status_id and status_in(new_status_id, (StatusKey.COMPLETED, StatusKey.FINISHED)):
                        logger.info(f"Registration task {task_id} is completed, approving executor {executor.telegram_id}")
                        await self._approve_executor(executor.telegram_id, task_id)
                    elif new_status_id and status_in(new_status_id, (StatusKey.CANCELLED, StatusKey.REJECTED)):
                        logger.info(f"Registration task {task_id} is cancelled/rejected, rejecting executor {executor.telegram_id}")
                        await self._reject_executor(executor.telegram_id, task_id)
                    elif new_status_id:
                        logger.debug(f"Registration task {task_id} status {new_status_id} ('{status_name}') is not a terminal status for executor approval")
                    else:
                        logger.warning(f"Could not determine status for registration task {task_id}, status data: {task.get('status', {})}")
            
            # Обрабатываем изменение статуса
            if new_status_id != old_status_id:
                # Обновляем кэш статуса
                if new_status_id:
                    self._task_status_cache[task_id] = new_status_id
                else:
                    self._task_status_cache.pop(task_id, None)
                
                # Отправляем уведомление об изменении статуса (если это реальное изменение)
                if old_status_id is not None and new_status_id is not None:
                    try:
                        await self.notification_service.notify_task_status_changed(
                            task_id=task_id,
                            old_status_id=old_status_id,
                            new_status_id=new_status_id
                        )
                    except Exception as e:
                        logger.error(f"Error notifying status change for task {task_id}: {e}")
                
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
            task_id = task.get('id')
            comment = data.get('comment', {})
            
            if not task_id:
                logger.warning(f"Incomplete comment data in webhook: {data}")
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
                    user_id = self._normalize_user_id(user.get('id'))
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
    
    async def _approve_executor(self, telegram_id: int, task_id: int):
        """Подтверждает регистрацию исполнителя."""
        try:
            with self.db_manager.get_db() as db:
                executor = self.db_manager.get_executor_profile(db, telegram_id)
                
                if not executor:
                    logger.warning(f"Executor {telegram_id} not found for approval")
                    return
                
                # Извлекаем planfix_user_id из задачи (используем логику из planfix_sync.py)
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
                await self.notification_service._send_notification(telegram_id, message)
                logger.info(f"✅ Executor {telegram_id} approved via webhook (planfix_user_id: {planfix_user_id})")
        except Exception as e:
            logger.error(f"Error approving executor: {e}", exc_info=True)
    
    async def _extract_planfix_user_id(self, task_id: int) -> Optional[str]:
        """Извлекает planfix_user_id из задачи регистрации."""
        try:
            task_response = await planfix_client.get_task_by_id(
                task_id,
                fields="id,name,description,customFieldData,comments,assignees"
            )
            
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
                    if assignee_id:
                        planfix_user_id = self._normalize_user_id(assignee_id)
                        if planfix_user_id:
                            logger.info(f"Found planfix_user_id {planfix_user_id} from assignee in task {task_id}")
                            return planfix_user_id
            
            # ПРИОРИТЕТ 2: Ищем в кастомных полях
            custom_fields = task.get('customFieldData', [])
            for field in custom_fields:
                field_id = field.get('field', {}).get('id')
                if field_id in (85, 86, 87, 88, 89, 90):
                    value = field.get('value')
                    if value:
                        planfix_user_id = str(value).strip()
                        logger.info(f"Found planfix_user_id {planfix_user_id} in custom field {field_id}")
                        return planfix_user_id
            
            # ПРИОРИТЕТ 3: Ищем в описании
            description = task.get('description', '')
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
                    match = re.search(r'[Pp]lanfix\s+[Uu]ser\s+ID[:\s]+(\d+)', comment_text)
                    if match:
                        planfix_user_id = match.group(1)
                        logger.info(f"Found planfix_user_id {planfix_user_id} in task comment")
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


async def webhook_handler(request):
    """Обработчик входящих webhook от Planfix."""
    try:
        # Получаем сырое тело запроса для диагностики
        raw_body = await request.read()
        content_type = request.headers.get('Content-Type', '').lower()
        
        # Логируем информацию о запросе
        logger.info(f"Received webhook: method={request.method}, content_type={content_type}, body_length={len(raw_body)}")
        
        # Пытаемся распарсить данные в зависимости от Content-Type
        data = {}
        
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
                    data = json.loads(body_text)
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
        
        if not event_type:
            logger.warning(f"Webhook received without event type. Data keys: {list(data.keys())}")
            return web.Response(text='OK', status=200)
        
        if event_type == 'task.create':
            await handler.handle_task_created(data)
        elif event_type == 'task.update':
            await handler.handle_task_updated(data)
        elif event_type == 'comment.create':
            await handler.handle_comment_added(data)
        else:
            logger.warning(f"Unknown event type: {event_type}")
        
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
