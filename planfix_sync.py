"""
Синхронизация данных с Planfix
Версия: 2.0 
"""

import logging
import asyncio
import re
import html
import json
from datetime import datetime, timedelta
from db_manager import DBManager
from database import ExecutorProfile, BotLog
from planfix_client import planfix_client
from planfix_api import PlanfixRateLimitError
from config import (
    PLANFIX_TASK_PROCESS_ID,
    PLANFIX_STATUS_NAME_IN_PROGRESS,
    PLANFIX_STATUS_NAME_PAUSED,
    PLANFIX_STATUS_NAME_WAITING_INFO,
    PLANFIX_STATUS_NAME_COMPLETED,
    FRANCHISE_GROUPS,
    PLANFIX_POLL_INTERVAL,
)
from services.status_registry import (
    StatusKey,
    ensure_status_registry_loaded,
    is_status,
    require_status_id,
    status_in,
)

logger = logging.getLogger(__name__)


def clean_html_text(text: str) -> str:
    """Очищает HTML-теги и сущности из текста."""
    if not text:
        return text
    
    # Декодируем HTML-сущности (&nbsp; -> пробел, &lt; -> <, и т.д.)
    text = html.unescape(text)
    
    # Удаляем HTML-теги
    text = re.sub(r'<[^>]+>', '', text)
    
    # Удаляем множественные пробелы и переводы строк
    text = re.sub(r'\s+', ' ', text)
    
    # Удаляем пробелы в начале и конце
    text = text.strip()
    
    return text


class PlanfixDataSync:
    """Класс для синхронизации данных из Planfix в локальную БД."""
    
    def __init__(self):
        self.db_manager = DBManager()
        # Используем глобальный экземпляр клиента
        self.planfix_client = planfix_client

    async def sync_directories(self):
        """Синхронизирует справочники Planfix с локальной БД."""
        logger.info("Starting Planfix directories synchronization...")
        try:
            with self.db_manager.get_db() as db:
                directories_response = await self.planfix_client.get_directories()
                
                if not directories_response or directories_response.get('result') != 'success':
                    logger.warning("Failed to get directories from Planfix")
                    return
                
                directories = directories_response.get('directories', [])
                logger.info(f"Found {len(directories)} directories in Planfix")
                
                for directory_data in directories:
                    dir_id = directory_data['id']
                    dir_name = directory_data['name']
                    dir_group = directory_data.get('group', {}).get('name')

                    self.db_manager.create_or_update_directory(db, dir_id, dir_name, dir_group)
                    logger.info(f"Syncing directory '{dir_name}' (ID: {dir_id})")

                    # Получаем записи справочника
                    try:
                        entries_response = await self.planfix_client.get_directory_entries(
                            dir_id, 
                            fields="name,key,parentKey",
                            page_size=100
                        )
                        
                        if entries_response and entries_response.get('result') == 'success':
                            entries = entries_response.get('directoryEntries', [])
                            logger.info(f"Found {len(entries)} entries in directory {dir_id}")
                            
                            for entry_data in entries:
                                self.db_manager.create_or_update_directory_entry(
                                    db=db,
                                    directory_id=dir_id,
                                    key=str(entry_data['key']),
                                    name=entry_data.get('name', f"Entry {entry_data['key']}"),
                                    parent_key=str(entry_data['parentKey']) if entry_data.get('parentKey') else None,
                                    custom_fields=entry_data.get('customFields')
                                )
                    except Exception as e:
                        logger.error(f"Error syncing entries for directory {dir_id}: {e}")
                        continue
                        
            logger.info("✅ Planfix directories synchronization completed successfully.")
            
        except Exception as e:
            logger.error(f"❌ Error during Planfix directories synchronization: {e}", exc_info=True)

    async def sync_task_statuses(self):
        """Синхронизирует статусы задач из Planfix."""
        logger.info("Starting Planfix task statuses synchronization...")
        try:
            with self.db_manager.get_db() as db:
                statuses_response = await self.planfix_client.get_process_task_statuses(
                    PLANFIX_TASK_PROCESS_ID,
                    fields="id,name,isFinal"
                )
                
                if not statuses_response or statuses_response.get('result') != 'success':
                    logger.warning("Failed to get task statuses from Planfix")
                    return
                
                statuses = statuses_response.get('statuses', [])
                logger.info(f"Found {len(statuses)} statuses in process {PLANFIX_TASK_PROCESS_ID}")
                
                for status_data in statuses:
                    self.db_manager.create_or_update_task_status(
                        db=db,
                        status_id=status_data['id'],
                        name=status_data['name'],
                        is_final=status_data.get('isFinal', False)
                    )
                    logger.debug(f"Synced status: {status_data['id']} - {status_data['name']}")
                    
            logger.info("✅ Planfix task statuses synchronization completed successfully.")
            # Обновляем кэш статусов после синхронизации
            try:
                await ensure_status_registry_loaded(force_refresh=True)
            except Exception as refresh_error:
                logger.warning("Failed to refresh status registry after sync: %s", refresh_error)
            
        except Exception as e:
            logger.error(f"❌ Error during Planfix task statuses synchronization: {e}", exc_info=True)

    async def sync_all_data(self):
        """Выполняет полную синхронизацию всех данных."""
        logger.info("🔄 Starting full Planfix data synchronization...")
        
        try:
            await self.sync_task_statuses()
            await self.sync_directories()
            logger.info("✅ Full synchronization completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Error during full synchronization: {e}", exc_info=True)


class PlanfixPollingService:
    """
    Сервис для периодического опроса Planfix на предмет новых задач и обновлений.
    
    Note: В будущем рекомендуется заменить на Webhooks для мгновенных уведомлений.
    """
    
    def __init__(self, poll_interval_seconds: int | None = None):
        """
        Args:
            poll_interval_seconds: Интервал опроса в секундах (по умолчанию из PLANFIX_POLL_INTERVAL, обычно 60 секунд).
                                   Можно уменьшить до 30 секунд для более быстрого обновления статусов.
                                   Статусы также обновляются при проверке комментариев для ускорения синхронизации.
        """
        self.db_manager = DBManager()
        # Используем глобальный экземпляр клиента
        self.planfix_client = planfix_client
        self.poll_interval_seconds = poll_interval_seconds or PLANFIX_POLL_INTERVAL
        self.last_check_time = None
        self.notification_service = None
        self.tracked_tasks = {}  # {task_id: {"status_id": X, "last_update": datetime}}
        self.tracked_comments = {}  # {task_id: {"last_comment_id": X, "last_comment_time": datetime}}
        self.registration_tasks = {}  # {task_id: {"executor_telegram_id": X, "status": "pending"}}

    async def _get_bot_created_task_ids(self) -> set:
        """
        Получает множество всех task_id задач, созданных через бота из BotLog.
        
        Returns:
            set: Множество task_id (int)
        """
        try:
            with self.db_manager.get_db() as db:
                logs = db.query(BotLog).filter(
                    BotLog.action == "create_task",
                    BotLog.success == True
                ).all()
            
            task_ids = set()
            for log in logs:
                if log.details:
                    details = log.details
                    if isinstance(details, str):
                        try:
                            details = json.loads(details)
                        except Exception:
                            continue
                    
                    if isinstance(details, dict):
                        task_id = details.get('task_id')
                        if task_id:
                            try:
                                task_id = int(str(task_id).split(":")[-1])
                                task_ids.add(task_id)
                            except (ValueError, TypeError):
                                continue
            
            logger.debug(f"Found {len(task_ids)} bot-created task IDs in BotLog")
            return task_ids
        except Exception as e:
            logger.error(f"Error getting bot-created task IDs: {e}", exc_info=True)
            return set()
    
    async def _include_recent_botlog_tasks(self, limit: int = 300):
        """Добавляет задачи из BotLog в список отслеживаемых (если их ещё нет).
        Уже фильтрует только задачи, созданные через бота (action='create_task')."""
        try:
            with self.db_manager.get_db() as db:
                logs = (
                    db.query(BotLog)
                        .filter(BotLog.action == "create_task")
                        .order_by(BotLog.id.desc())
                        .limit(limit)
                        .all()
                )
            for log in logs:
                details = log.details or {}
                if isinstance(details, str):
                    try:
                        details = json.loads(details)
                    except Exception:
                        details = {}
                raw_task_id = details.get("task_id")
                try:
                    if raw_task_id is None:
                        continue
                    task_id = int(str(raw_task_id).split(":")[-1])
                except Exception:
                    continue

                if task_id not in self.tracked_tasks:
                    self.tracked_tasks[task_id] = {
                        "status_id": None,
                        "last_update": datetime.now()
                    }
                self.tracked_comments.setdefault(task_id, {
                    "last_comment_id": None,
                    "last_comment_time": None
                })
        except Exception as e:
            logger.error(f"Error including BotLog tasks into tracking: {e}", exc_info=True)

    async def check_new_tasks(self):
        """Проверяет наличие новых задач в Planfix."""
        try:
            await self._include_recent_botlog_tasks()
            # ✅ ПРАВИЛЬНЫЙ ФОРМАТ согласно документации Planfix API
            # Операторы д��лжны быть строками: "equal", "notequal", "gt", "lt"
            filters = [
                {
                    "type": 10,  # Фильтр по статусу задачи (type 10, не 3! type 3 = Task auditor)
                    "operator": "equal",  # ✅ Строка, не число!
                    "value": require_status_id(StatusKey.NEW)  # Число - это OK
                }
            ]
            
            # Убираем фильтр по дате - проверяем локально через tracked_tasks
            
            new_tasks_response = await self.planfix_client.get_task_list(
                filters=filters,
                fields="id,name,status,project,counterparty,dateOfLastUpdate",
                page_size=50,
                result_order=[{"field": "dateTime", "direction": "Desc"}]
            )
            
            if new_tasks_response and new_tasks_response.get('result') == 'success':
                tasks = new_tasks_response.get('tasks', [])
                if tasks and self.notification_service:
                    logger.info(f"Found {len(tasks)} new tasks")
                    for task in tasks:
                        task_id = task['id']
                        project_id = task.get('project', {}).get('id')
                        
                        # Отправляем уведомление о новой задаче
                        if project_id and task_id not in self.tracked_tasks:
                            await self.notification_service.notify_new_task(task_id, project_id)
                            self.tracked_tasks[task_id] = {
                                "status_id": task.get('status', {}).get('id'),
                                "last_update": datetime.now()
                            }
                            # Инициализируем отслеживание комментариев для новой задачи
                            self.tracked_comments[task_id] = {
                                "last_comment_id": None,
                                "last_comment_time": None
                            }
                        logger.debug(f"New task: #{task_id} - {task.get('name', 'No name')}")
                        
        except PlanfixRateLimitError as e:
            logger.warning(f"Rate limit encountered while checking new tasks: {e.message}. Will retry on next poll cycle.")
        except Exception as e:
            logger.error(f"Error checking new tasks: {e}", exc_info=True)

    async def check_task_updates(self):
        """Проверяет обновления статусов задач и отправляет уведомления при изменении.
        Источники для отслеживания: активные назначения (TaskAssignment) и ранее добавленные tracked_tasks.
        Фильтрует только задачи, созданные через бота.
        """
        try:
            await self._include_recent_botlog_tasks()
            
            # Получаем список всех task_id, созданных через бота
            bot_created_task_ids = await self._get_bot_created_task_ids()
            
            # Подтягиваем активные назначения из локальной БД в tracked_tasks
            try:
                from database import TaskAssignment
                with self.db_manager.get_db() as db:
                    active_assignments = db.query(TaskAssignment).filter(
                        TaskAssignment.status == "active"
                    ).all()
                for a in active_assignments:
                    # Фильтруем только задачи, созданные через бота
                    if bot_created_task_ids and a.task_id not in bot_created_task_ids:
                        logger.debug(f"Skipping task {a.task_id} in check_task_updates - not created by bot")
                        continue
                    
                    if a.task_id not in self.tracked_tasks:
                        self.tracked_tasks[a.task_id] = {
                            "status_id": None,
                            "last_update": datetime.now()
                        }
                    if a.task_id not in self.tracked_comments:
                        self.tracked_comments[a.task_id] = {
                            "last_comment_id": None,
                            "last_comment_time": None
                        }
            except Exception as e:
                logger.error(f"Error loading active assignments for tracking: {e}")
            
            # Фильтруем tracked_tasks - оставляем только те, что созданы через бота
            if bot_created_task_ids:
                filtered_tasks = {
                    task_id: task_info 
                    for task_id, task_info in self.tracked_tasks.items() 
                    if task_id in bot_created_task_ids
                }
                removed_count = len(self.tracked_tasks) - len(filtered_tasks)
                if removed_count > 0:
                    logger.debug(f"Filtered out {removed_count} tasks not created by bot in check_task_updates (keeping only {len(filtered_tasks)} bot-created tasks)")
                self.tracked_tasks = filtered_tasks

            if not self.tracked_tasks:
                return

            def _status_id(raw):
                if raw is None:
                    return None
                if isinstance(raw, int):
                    return raw
                try:
                    s = str(raw)
                    if ':' in s:
                        s = s.split(':')[-1]
                    return int(s)
                except Exception:
                    return None

            for task_id in list(self.tracked_tasks.keys()):
                try:
                    tr = await self.planfix_client.get_task_by_id(
                        task_id,
                        fields="id,name,status"
                    )
                    if not tr or tr.get('result') != 'success':
                        continue
                    t = tr.get('task', {})
                    new_sid = _status_id((t.get('status') or {}).get('id'))
                    old_sid = self.tracked_tasks.get(task_id, {}).get('status_id')
                    if new_sid != old_sid and old_sid is not None and self.notification_service is not None:
                        try:
                            await self.notification_service.notify_task_status_changed(
                                task_id=task_id,
                                old_status_id=old_sid,
                                new_status_id=new_sid
                            )
                            # Обновляем статус только после успешной отправки уведомления
                            self.tracked_tasks[task_id]['status_id'] = new_sid
                        except Exception as ne:
                            logger.error(f"Notify status change failed for task {task_id}: {ne}")
                            # Не обновляем статус, если уведомление не отправилось
                    elif new_sid != old_sid:
                        # Обновляем статус даже если это первое обнаружение (old_sid is None)
                        self.tracked_tasks[task_id]['status_id'] = new_sid
                    self.tracked_tasks[task_id]['last_update'] = datetime.now()
                except PlanfixRateLimitError as ie:
                    logger.warning(f"Rate limit encountered while checking status for task {task_id}: {ie.message}. Will retry on next poll cycle.")
                except Exception as ie:
                    logger.error(f"Error checking status for task {task_id}: {ie}")
        except PlanfixRateLimitError as e:
            logger.warning(f"Rate limit encountered while checking task updates: {e.message}. Will retry on next poll cycle.")
        except Exception as e:
            logger.error(f"Error checking task updates: {e}", exc_info=True)

    async def check_new_comments(self):
        """Проверяет новые комментарии в отслеживаемых задачах.
        Инициализируется без рассылки истории: фиксируется последний комментарий, затем шлются новые.
        Фильтрует только задачи, созданные через бота (из BotLog).
        """
        try:
            await self._include_recent_botlog_tasks()
            
            # Получаем список всех task_id, созданных через бота
            bot_created_task_ids = await self._get_bot_created_task_ids()
            
            # Фильтруем tracked_tasks - оставляем только те, что созданы через бота
            if bot_created_task_ids:
                filtered_tasks = {
                    task_id: task_info 
                    for task_id, task_info in self.tracked_tasks.items() 
                    if task_id in bot_created_task_ids
                }
                removed_count = len(self.tracked_tasks) - len(filtered_tasks)
                if removed_count > 0:
                    logger.debug(f"Filtered out {removed_count} tasks not created by bot (keeping only {len(filtered_tasks)} bot-created tasks)")
                self.tracked_tasks = filtered_tasks
            else:
                logger.warning("No bot-created tasks found in BotLog, skipping comment check")
                self.tracked_tasks = {}
            
            if not self.tracked_tasks:
                logger.debug("No tracked tasks to check for comments")
                return

            logger.debug(f"Checking comments for {len(self.tracked_tasks)} tracked tasks (all created by bot)")

            def _to_int(raw):
                if raw is None:
                    return None
                if isinstance(raw, int):
                    return raw
                try:
                    s = str(raw)
                    if ':' in s:
                        s = s.split(':')[-1]
                    return int(s)
                except Exception:
                    return None

            for task_id, task_info in list(self.tracked_tasks.items()):
                try:
                    # Сначала проверяем статус задачи (для ускорения обновления статусов)
                    try:
                        tr = await self.planfix_client.get_task_by_id(
                            task_id,
                            fields="id,status"
                        )
                        if tr and tr.get('result') == 'success':
                            t = tr.get('task', {})
                            new_sid = None
                            status_raw = (t.get('status') or {}).get('id')
                            if status_raw is not None:
                                if isinstance(status_raw, int):
                                    new_sid = status_raw
                                else:
                                    try:
                                        s = str(status_raw)
                                        if ':' in s:
                                            s = s.split(':')[-1]
                                        new_sid = int(s)
                                    except Exception:
                                        pass
                            
                            old_sid = self.tracked_tasks.get(task_id, {}).get('status_id')
                            if new_sid != old_sid and old_sid is not None and self.notification_service is not None:
                                try:
                                    await self.notification_service.notify_task_status_changed(
                                        task_id=task_id,
                                        old_status_id=old_sid,
                                        new_status_id=new_sid
                                    )
                                    # Обновляем статус только после успешной отправки уведомления
                                    self.tracked_tasks[task_id]['status_id'] = new_sid
                                except Exception as ne:
                                    logger.error(f"Notify status change failed for task {task_id}: {ne}")
                                    # Не обновляем статус, если уведомление не отправилось
                            elif new_sid != old_sid:
                                # Обновляем статус даже если это первое обнаружение (old_sid is None)
                                if new_sid is not None:
                                    self.tracked_tasks[task_id]['status_id'] = new_sid
                            self.tracked_tasks[task_id]['last_update'] = datetime.now()
                    except Exception as status_err:
                        logger.debug(f"Error checking status for task {task_id} during comment check: {status_err}")
                    
                    comments_response = await self.planfix_client.get_task_comments(
                        task_id,
                        fields="id,description,owner,dateTime",
                        page_size=20
                    )

                    if not comments_response or comments_response.get('result') != 'success':
                        error_payload = comments_response or {}
                        error_code = error_payload.get("code")
                        error_msg = error_payload.get("error")
                        logger.warning(f"Failed to get comments for task {task_id}: code={error_code}, error={error_msg}")

                        if error_code == 1000 or (error_msg and "not found" in error_msg.lower()):
                            logger.warning(f"Removing task {task_id} from tracking (Planfix reports not found)")
                            self.tracked_tasks.pop(task_id, None)
                            self.tracked_comments.pop(task_id, None)
                            await self._remove_local_assignments(task_id)
                        continue

                    comments = comments_response.get('comments', [])
                    if not comments:
                        # Инициализация структуры при отсутствии комментариев
                        self.tracked_comments.setdefault(task_id, {
                            'last_comment_id': None,
                            'last_comment_time': None
                        })
                        continue

                    # Сортируем комментарии (новые первыми)
                    def get_sort_key(comment):
                        dt = comment.get('dateTime', '')
                        if isinstance(dt, dict):
                            return str(dt.get('value', '')) if 'value' in dt else ''
                        return str(dt) if dt else ''

                    comments.sort(key=get_sort_key, reverse=True)

                    tracked_comment_info = self.tracked_comments.get(task_id, {})
                    last_comment_id = _to_int(tracked_comment_info.get('last_comment_id'))

                    # Если нет сохранённого ID — фиксируем последний и продолжаем (без уведомлений)
                    if last_comment_id is None:
                        latest = comments[0]
                        self.tracked_comments[task_id] = {
                            'last_comment_id': _to_int(latest.get('id')),
                            'last_comment_time': latest.get('dateTime')
                        }
                        logger.debug(f"Initialized last comment for task {task_id}: {self.tracked_comments[task_id]['last_comment_id']}")
                        continue

                    # Собираем только новые комментарии
                    new_comments = []
                    for c in comments:
                        cid = _to_int(c.get('id'))
                        if cid is not None and cid > last_comment_id:
                            new_comments.append(c)
                        else:
                            break

                    if new_comments and self.notification_service:
                        logger.info(f"Found {len(new_comments)} new comments for task {task_id}")
                        for c in reversed(new_comments):  # отправляем в хронологическом порядке
                            comment_id = c.get('id')
                            comment_text = c.get('description', '')
                            comment_author = (c.get('owner') or {}).get('name', 'Неизвестно')
                            
                            # Пропускаем комментарии от ботов (Робот Бендера и др.)
                            if 'робот' in comment_author.lower() or 'bot' in comment_author.lower():
                                logger.debug(f"Skipping comment {comment_id} from bot '{comment_author}' in task {task_id}")
                                continue
                            
                            # Очищаем HTML-теги из текста комментария
                            clean_comment_text = clean_html_text(comment_text)
                            
                            await self.notification_service.notify_new_comment(
                                task_id=task_id,
                                comment_author=comment_author,
                                comment_text=clean_comment_text,
                                comment_id=comment_id
                            )
                            logger.info(f"Notified about new comment {comment_id} in task {task_id}")

                        latest = new_comments[0]
                        self.tracked_comments[task_id] = {
                            'last_comment_id': _to_int(latest.get('id')),
                            'last_comment_time': latest.get('dateTime')
                        }
                except PlanfixRateLimitError as e:
                    logger.warning(f"Rate limit encountered while checking comments for task {task_id}: {e.message}. Will retry on next poll cycle.")
                    continue
                except Exception as e:
                    logger.error(f"Error checking comments for task {task_id}: {e}", exc_info=True)
                    continue
        except PlanfixRateLimitError as e:
            logger.warning(f"Rate limit encountered in check_new_comments: {e.message}. Will retry on next poll cycle.")
        except Exception as e:
            logger.error(f"Error in check_new_comments: {e}", exc_info=True)

    async def initialize_tracked_tasks(self):
        """Инициализирует отслеживание существующих активных задач (назначений) и комментариев без рассылки истории.
        Фильтрует только задачи, созданные через бота."""
        try:
            logger.info("Initializing tracked tasks from local assignments...")
            
            # Получаем список всех task_id, созданных через бота
            bot_created_task_ids = await self._get_bot_created_task_ids()
            
            try:
                from database import TaskAssignment
                with self.db_manager.get_db() as db:
                    active_assignments = db.query(TaskAssignment).filter(
                        TaskAssignment.status == "active"
                    ).all()
            except Exception as e:
                active_assignments = []
                logger.error(f"Failed to load active assignments: {e}")

            for a in active_assignments:
                # Фильтруем только задачи, созданные через бота
                if bot_created_task_ids and a.task_id not in bot_created_task_ids:
                    logger.debug(f"Skipping task {a.task_id} - not created by bot")
                    continue
                # Добавляем в tracked_tasks
                self.tracked_tasks.setdefault(a.task_id, {
                    'status_id': None,
                    'last_update': datetime.now()
                })
                # Инициализируем last_comment_id по последнему комментарию, чтобы не слать историю
                try:
                    cr = await self.planfix_client.get_task_comments(
                        a.task_id,
                        fields="id,dateTime",
                        page_size=5
                    )
                    if cr and cr.get('result') == 'success':
                        comments = cr.get('comments', []) or []
                        if comments:
                            # Сортируем по дате (новые первыми)
                            def _k(c):
                                dt = c.get('dateTime', '')
                                if isinstance(dt, dict):
                                    return str(dt.get('value', '')) if 'value' in dt else ''
                                return str(dt) if dt else ''
                            comments.sort(key=_k, reverse=True)
                            latest = comments[0]
                            self.tracked_comments[a.task_id] = {
                                'last_comment_id': latest.get('id'),
                                'last_comment_time': latest.get('dateTime')
                            }
                        else:
                            self.tracked_comments.setdefault(a.task_id, {
                                'last_comment_id': None,
                                'last_comment_time': None
                            })
                except Exception as ce:
                    logger.error(f"Init comments tracking failed for task {a.task_id}: {ce}")

            logger.info(f"Tracked tasks initialized: {len(self.tracked_tasks)} tasks")
            # Инициализируем задачи регистрации как и ра��ьше
            await self._initialize_registration_tasks()
        except Exception as e:
            logger.error(f"Error initializing tracked tasks: {e}", exc_info=True)

    async def _initialize_registration_tasks(self):
        """Инициализирует отслеживание задач регистрации исполнителей."""
        try:
            logger.info("Initializing registration tasks tracking...")
            
            with self.db_manager.get_db() as db:
                # Получаем всех исполнителей с задачами регистрации
                executors = db.query(ExecutorProfile).filter(
                    ExecutorProfile.registration_task_id.isnot(None),
                    ExecutorProfile.profile_status == "ожидает подтверждения"
                ).all()
                
                for executor in executors:
                    task_id = executor.registration_task_id
                    self.registration_tasks[task_id] = {
                        "executor_telegram_id": executor.telegram_id,
                        "status": "pending"
                    }
                    logger.info(f"Added registration task {task_id} for executor {executor.telegram_id}")
                
                logger.info(f"Initialized tracking for {len(self.registration_tasks)} registration tasks")
                
        except Exception as e:
            logger.error(f"Error initializing registration tasks: {e}", exc_info=True)

    async def cleanup_completed_tasks(self):
        """Очищает завершенные задачи из отслеживания."""
        try:
            completed_tasks = []
            
            for task_id, task_info in self.tracked_tasks.items():
                status_id = task_info.get('status_id')
                if is_status(status_id, StatusKey.COMPLETED):
                    completed_tasks.append(task_id)
            
            if completed_tasks:
                logger.info(f"Cleaning up {len(completed_tasks)} completed tasks from tracking")
                for task_id in completed_tasks:
                    self.tracked_tasks.pop(task_id, None)
                    self.tracked_comments.pop(task_id, None)
                    
        except Exception as e:
            logger.error(f"Error cleaning up completed tasks: {e}", exc_info=True)

    async def check_registration_tasks(self):
        """Про��еряет изменения в задачах регистрации исполнителей."""
        try:
            # Подхватываем новые заявки регистрации из БД (созданные после старта сервиса)
            try:
                with self.db_manager.get_db() as db:
                    executors = db.query(ExecutorProfile).filter(
                        ExecutorProfile.registration_task_id.isnot(None),
                        ExecutorProfile.profile_status == "ожидает подтверждения"
                    ).all()
                    for executor in executors:
                        task_id = executor.registration_task_id
                        if task_id and task_id not in self.registration_tasks:
                            self.registration_tasks[task_id] = {
                                "executor_telegram_id": executor.telegram_id,
                                "status": "pending"
                            }
                            logger.info(f"Added registration task {task_id} for executor {executor.telegram_id} (auto-refresh)")
            except Exception as e:
                logger.error(f"Error refreshing registration tasks from DB: {e}", exc_info=True)

            if not self.registration_tasks:
                logger.debug("No registration tasks to check")
                return
            
            logger.debug(f"Checking {len(self.registration_tasks)} registration tasks")
            
            to_remove = []
            for task_id, task_info in list(self.registration_tasks.items()):
                try:
                    # Получаем информацию о задаче
                    task_response = await self.planfix_client.get_task_by_id(
                        task_id,
                        fields="id,status,name"
                    )
                    
                    if not task_response or task_response.get('result') != 'success':
                        logger.warning(f"Failed to get registration task {task_id}")
                        continue
                    
                    task = task_response.get('task', {})
                    status_id = task.get('status', {}).get('id')
                    status_name = task.get('status', {}).get('name', 'Неизвестно')
                    executor_telegram_id = task_info.get('executor_telegram_id')

                    # Если профиль уже не ждёт подтверждения (админ подтвердил в боте) — снимаем с отслеживания
                    try:
                        with self.db_manager.get_db() as db:
                            executor_profile = self.db_manager.get_executor_profile(db, executor_telegram_id)
                        if not executor_profile:
                            logger.warning(f"Executor {executor_telegram_id} not found in DB, removing task {task_id} from tracking")
                            to_remove.append(task_id)
                            continue
                        if executor_profile.profile_status != "ожидает подтверждения":
                            logger.info(f"Executor {executor_telegram_id} status is '{executor_profile.profile_status}', removing task {task_id} from tracking")
                            to_remove.append(task_id)
                            continue
                    except Exception as e:
                        logger.error(f"Error checking executor profile for {executor_telegram_id}: {e}", exc_info=True)
                    
                    # Проверяем, изменился ли статус
                    if status_in(status_id, (StatusKey.COMPLETED, StatusKey.FINISHED)):
                        # Задача регистрации завершена — автоматически активируем исполнителя без участия админа.
                        try:
                            await self._approve_executor_registration(executor_telegram_id, task_id)
                            logger.info(
                                f"Executor {executor_telegram_id} auto-approved by Planfix completion for task {task_id}"
                            )
                            # Снимаем из отслеживания сразу
                            to_remove.append(task_id)
                        except Exception as e:
                            logger.error(
                                f"Error auto-approving executor {executor_telegram_id} by Planfix completion: {e}",
                                exc_info=True
                            )
                        # Уведомления админам не отправляем
                        
                    elif status_in(status_id, (StatusKey.CANCELLED, StatusKey.REJECTED)):
                        # Задача отменена/отклонена - отклоняем регистрацию
                        logger.info(f"Registration task {task_id} cancelled/rejected - rejecting executor {executor_telegram_id}")
                        await self._reject_executor_registration(executor_telegram_id, task_id)
                        # Отложим удаление из отслеживания до окончания итерации
                        to_remove.append(task_id)
                    
                except Exception as e:
                    logger.error(f"Error checking registration task {task_id}: {e}", exc_info=True)
                    continue
                    
        # Удаляем обработанные задачи регистрации после итерации
            if to_remove:
                for _tid in to_remove:
                    self.registration_tasks.pop(_tid, None)
        except Exception as e:
            logger.error(f"Error in check_registration_tasks: {e}", exc_info=True)

    async def _remove_local_assignments(self, task_id: int):
        """Удаляет локальные назначения и связанные записи для указанной задачи."""
        try:
            from database import TaskAssignment
            with self.db_manager.get_db() as db:
                deleted = db.query(TaskAssignment).filter(
                    TaskAssignment.task_id == task_id
                ).delete()
                if deleted:
                    logger.info(f"Removed {deleted} TaskAssignment rows for task {task_id}")
                db.commit()
        except Exception as e:
            logger.error(f"Failed to cleanup TaskAssignment for task {task_id}: {e}", exc_info=True)

    async def _approve_executor_registration(self, executor_telegram_id: int, task_id: int):
        """Подтверждает регистрацию исполнителя."""
        try:
            with self.db_manager.get_db() as db:
                executor = self.db_manager.get_executor_profile(db, executor_telegram_id)
                
                if not executor:
                    logger.error(f"Executor {executor_telegram_id} not found for approval")
                    return
                
                # Получаем информацию о задаче регистрации для извлечения planfix_user_id
                planfix_user_id = None
                try:
                    task_response = await self.planfix_client.get_task_by_id(
                        task_id,
                        fields="id,name,description,customFieldData,comments,assignees"
                    )
                    
                    if task_response and task_response.get('result') == 'success':
                        task = task_response.get('task', {})
                        
                        # ПРИОРИТЕТ 1: Извлекаем planfix_user_id из поля "Исполнители" (assignees)
                        # Администратор просто назначает исполнителя в Planfix
                        assignees = task.get('assignees', {})
                        if isinstance(assignees, dict):
                            users = assignees.get('users', [])
                            if users and isinstance(users, list) and len(users) > 0:
                                # Берём первого назначенного исполнителя
                                first_assignee = users[0]
                                assignee_id = first_assignee.get('id')
                                if assignee_id:
                                    # Нормализуем ID (может быть "user:123" или просто "123")
                                    if isinstance(assignee_id, str) and ':' in assignee_id:
                                        planfix_user_id = assignee_id.split(':')[-1]
                                    else:
                                        planfix_user_id = str(assignee_id)
                                    assignee_name = first_assignee.get('name', 'Unknown')
                                    logger.info(
                                        f"Found planfix_user_id {planfix_user_id} from assignee '{assignee_name}' in task {task_id}"
                                    )
                        
                        # ПРИОРИТЕТ 2: Если нет назначенного исполнителя, ищем в кастомных полях
                        if not planfix_user_id:
                            custom_fields = task.get('customFieldData', [])
                            for field in custom_fields:
                                field_id = field.get('field', {}).get('id')
                                if field_id in (85, 86, 87, 88, 89, 90):
                                    value = field.get('value')
                                    if value and isinstance(value, (int, str)):
                                        planfix_user_id = str(value).strip()
                                        logger.info(f"Found planfix_user_id {planfix_user_id} in custom field {field_id}")
                                        break
                        
                        # ПРИОРИТЕТ 3: Ищем в описании задачи
                        if not planfix_user_id:
                            description = task.get('description', '')
                            import re
                            match = re.search(r'[Pp]lanfix\s+[Uu]ser\s+ID[:\s]+(\d+)', description)
                            if match:
                                planfix_user_id = match.group(1)
                                logger.info(f"Found planfix_user_id {planfix_user_id} in task description")
                        
                        # ПРИОРИТЕТ 4: Ищем в комментариях
                        if not planfix_user_id:
                            comments = task.get('comments', [])
                            if isinstance(comments, list):
                                for comment in comments:
                                    comment_text = comment.get('description', '') if isinstance(comment, dict) else str(comment)
                                    match = re.search(r'[Pp]lanfix\s+[Uu]ser\s+ID[:\s]+(\d+)', comment_text)
                                    if match:
                                        planfix_user_id = match.group(1)
                                        logger.info(f"Found planfix_user_id {planfix_user_id} in task comment")
                                        break
                except Exception as e:
                    logger.warning(f"Error extracting planfix_user_id from task {task_id}: {e}")
                
                # Если planfix_user_id не найден, логируем предупреждение
                if not planfix_user_id:
                    logger.warning(
                        f"planfix_user_id not found for executor {executor_telegram_id} in task {task_id}. "
                        f"Executor will be approved but won't be able to receive tasks until planfix_user_id is set. "
                        f"SOLUTION: Assign the executor to this task in Planfix (field 'Исполнители')."
                    )
                
                # Обновляем статус исполнителя и устанавливаем planfix_user_id если найден
                self.db_manager.update_executor_profile(
                    db,
                    executor_telegram_id,
                    profile_status="активен",
                    confirmation_date=datetime.now(),
                    planfix_user_id=planfix_user_id
                )
                
                # Получаем концепции для уведомления
                concept_names = [FRANCHISE_GROUPS[cid]["name"] for cid in executor.serving_franchise_groups]
                
                # Отправляем уведомление исполнителю
                if self.notification_service:
                    if planfix_user_id:
                        message = (
                            f"✅ Ваша регистрация подтверждена!\n\n"
                            f"Теперь вы будете получать заявки по концепциям:\n"
                            f"🏢 {', '.join(concept_names)}\n\n"
                            f"Используйте меню для работы с заявками."
                        )
                    else:
                        message = (
                            f"✅ Ваша регистрация подтверждена!\n\n"
                            f"Теперь вы будете получать заявки по концепциям:\n"
                            f"🏢 {', '.join(concept_names)}\n\n"
                            f"⚠️ Ваш профиль Planfix ещё не связан. "
                            f"Обратитесь к администратору для завершения настройки."
                        )
                    await self.notification_service._send_notification(executor_telegram_id, message)
                
                logger.info(
                    f"Executor {executor_telegram_id} approved via Planfix task {task_id}. "
                    f"planfix_user_id: {planfix_user_id or 'NOT SET'}"
                )
                
        except Exception as e:
            logger.error(f"Error approving executor {executor_telegram_id}: {e}", exc_info=True)

    async def _reject_executor_registration(self, executor_telegram_id: int, task_id: int):
        """Отклоняет регистрацию исполнителя."""
        try:
            with self.db_manager.get_db() as db:
                executor = self.db_manager.get_executor_profile(db, executor_telegram_id)
                
                if not executor:
                    logger.error(f"Executor {executor_telegram_id} not found for rejection")
                    return
                
                # Обновляем статус исполнителя
                self.db_manager.update_executor_profile(
                    db,
                    executor_telegram_id,
                    profile_status="отклонен"
                )
                
                # Отправляем уведомление исполнителю
                if self.notification_service:
                    message = (
                        f"❌ Ваша регистрация отклонена.\n\n"
                        f"Обратитесь к администратору для выяснения причин."
                    )
                    await self.notification_service._send_notification(executor_telegram_id, message)
                
                logger.info(f"Executor {executor_telegram_id} rejected via Planfix task {task_id}")
                
        except Exception as e:
            logger.error(f"Error rejecting executor {executor_telegram_id}: {e}", exc_info=True)

    async def run(self, bot=None):
        """
        Запускает бесконечный цикл опроса Planfix.
        
        Args:
            bot: Экземпляр бота для отправки уведомлений (опционально)
        """
        logger.info(f"🚀 Starting Planfix polling service (interval: {self.poll_interval_seconds}s)")
        
        # Инициализируем сервис уведомлений если передан бот
        if bot:
            from notifications import NotificationService
            self.notification_service = NotificationService(bot)
            logger.info("✅ Notification service initialized")
            
            # Инициализируем отслеживание существующих задач
            await self.initialize_tracked_tasks()
        
        while True:
            try:
                logger.debug("Polling Planfix for updates...")
                
                # Проверяем новые задачи
                await self.check_new_tasks()
                
                # Проверяем обновления задач
                await self.check_task_updates()
                
                # Проверяем новые комментарии
                await self.check_new_comments()
                
                # Очищаем завершенные задачи из отслеживания
                await self.cleanup_completed_tasks()
                
                # Проверяем задачи регистрации исполнителей
                await self.check_registration_tasks()
                
                # Обновляем время последней проверки
                self.last_check_time = datetime.now()
                
            except Exception as e:
                logger.error(f"❌ Planfix polling error: {e}", exc_info=True)
                
            finally:
                # Ждем до следующей проверки
                await asyncio.sleep(self.poll_interval_seconds)
