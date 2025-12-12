"""
Система уведомлений для пользователей и исполнителей

"""

import logging
import json
import io
import mimetypes
from typing import Optional, List
from aiogram import Bot
import aiogram.types
from aiogram.types import BufferedInputFile
from db_manager import DBManager
from database import UserProfile, ExecutorProfile, TaskAssignment, BotLog
from planfix_client import planfix_client
from config import (
    FRANCHISE_GROUPS,
    TELEGRAM_ADMIN_IDS,
    PLANFIX_BASE_URL,
    PLANFIX_IT_TEMPLATES,
    PLANFIX_SE_TEMPLATES,
    PLANFIX_IT_TAG,
    PLANFIX_SE_TAG,
)
from services.status_registry import (
    StatusKey,
    is_status,
    status_labels,
)

from keyboards import get_executor_confirmation_keyboard

logger = logging.getLogger(__name__)


def _normalize_int(value):
    try:
        if isinstance(value, str) and ':' in value:
            value = value.split(':')[-1]
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_restaurant_ids(data) -> set[int]:
    ids: set[int] = set()
    for item in data or []:
        if isinstance(item, dict):
            val = _normalize_int(item.get("id"))
        else:
            val = _normalize_int(item)
        if val is not None:
            ids.add(val)
    return ids


class NotificationService:
    """Сервис для отправки уведомлений пользователям и исполнителям."""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.db_manager = DBManager()
        # Используем глобальный экземпляр клиента
        self.planfix_client = planfix_client
    
    async def notify_task_status_changed(self, task_id: int, old_status_id: int, new_status_id: int):
        """
        Уведомление об изменении статуса задачи.
        
        Args:
            task_id: ID задачи в Planfix
            old_status_id: Старый статус (None при первом обнаружении)
            new_status_id: Новый статус
        """
        try:
            # Пропускаем уведомление если это первое обнаружение задачи (old_status_id == None)
            # Это предотвращает отправку уведомлений о статусе при инициализации отслеживания
            if old_status_id is None:
                logger.debug(f"Skipping status change notification for task {task_id} (first detection)")
                return
            
            # Получаем информацию о ��адаче
            task_response = await self.planfix_client.get_task_by_id(
                task_id,
                fields="id,name,status,counterparty,assignees"
            )
            
            if not task_response or task_response.get('result') != 'success':
                logger.error(f"Failed to get task {task_id} for notification")
                return
            
            task = task_response.get('task', {})
            task_name = task.get('name', 'Без названия')
            status_name = task.get('status', {}).get('name', 'Неизвестно')
            counterparty_id = task.get('counterparty', {}).get('id')
            
            # Находим заявителя по counterparty_id
            if counterparty_id:
                with self.db_manager.get_db() as db:
                    user = db.query(UserProfile).filter(
                        UserProfile.restaurant_contact_id == counterparty_id
                    ).first()
                    
                    if user:
                        message = self._format_status_change_message(
                            task_id, task_name, status_name, old_status_id, new_status_id
                        )
                        await self._send_notification(user.telegram_id, message)
            
            # Уведомляем исполнителей
            assignees = task.get('assignees', {}).get('users', [])
            for assignee in assignees:
                assignee_id = assignee.get('id', '').replace('user:', '')
                if assignee_id:
                    await self._notify_executor_by_planfix_id(
                        assignee_id, task_id, task_name, f"Статус изменён на: {status_name}"
                    )
                    
        except Exception as e:
            logger.error(f"Error notifying status change for task {task_id}: {e}", exc_info=True)
    
    async def notify_new_task(self, task_id: int, project_id: int):
        """
        Уведомление о новой задаче для исполнителей.
        
        Args:
            task_id: ID задачи
            project_id: ID проекта (для определения концепции, может быть 0 или None если не найден)
        """
        try:
            # Получаем информацию о задаче (включая теги для определения направления)
            task_response = await self.planfix_client.get_task_by_id(
                task_id,
                fields="id,name,description,counterparty.id,counterparty.group.id,project,template.id,tags"
            )
            
            if not task_response or task_response.get('result') != 'success':
                logger.warning(f"Could not get task {task_id} for notification")
                return
            
            task = task_response.get('task', {})
            task_name = task.get('name', 'Без названия')
            counterparty = task.get('counterparty', {}) or {}
            counterparty_name = counterparty.get('name', 'Неизвестно')
            counterparty_id = _normalize_int(counterparty.get('id'))
            # Получаем группу контакта ресторана (концепцию)
            counterparty_group = counterparty.get('group', {}) or {}
            counterparty_group_id = _normalize_int(counterparty_group.get('id'))

            template_raw = (task.get('template') or {}).get('id')
            template_id = _normalize_int(template_raw)
            
            # Определяем направление задачи по шаблону или по тегам
            task_direction = None
            if template_id in PLANFIX_IT_TEMPLATES:
                task_direction = "it"
            elif template_id in PLANFIX_SE_TEMPLATES:
                task_direction = "se"
            else:
                # Пробуем определить по тегам
                tags = task.get('tags', []) or []
                tag_names = []
                for tag in tags:
                    if isinstance(tag, dict):
                        tag_name = tag.get('name', '')
                    elif isinstance(tag, str):
                        tag_name = tag
                    else:
                        tag_name = str(tag)
                    if tag_name:
                        tag_names.append(tag_name.lower())
                
                if PLANFIX_IT_TAG.lower() in tag_names:
                    task_direction = "it"
                elif PLANFIX_SE_TAG.lower() in tag_names:
                    task_direction = "se"
            
            # Определяем концепцию по группе контакта ресторана (приоритет)
            franchise_group_id = None
            
            # Способ 1: Из counterparty_group_id (если доступен)
            if counterparty_group_id and counterparty_group_id in FRANCHISE_GROUPS:
                franchise_group_id = counterparty_group_id
                logger.debug(f"Determined franchise_group_id {franchise_group_id} from counterparty_group_id for task {task_id}")
            
            # Способ 2: Получаем группу контакта напрямую из Planfix
            if not franchise_group_id and counterparty_id:
                try:
                    contact_response = await self.planfix_client.get_contact_by_id(
                        counterparty_id,
                        fields="id,group.id"
                    )
                    if contact_response and contact_response.get('result') == 'success':
                        contact = contact_response.get('contact', {}) or {}
                        contact_group = contact.get('group', {}) or {}
                        contact_group_id = _normalize_int(contact_group.get('id'))
                        if contact_group_id and contact_group_id in FRANCHISE_GROUPS:
                            franchise_group_id = contact_group_id
                            logger.debug(f"Determined franchise_group_id {franchise_group_id} from contact {counterparty_id} group for task {task_id}")
                except Exception as contact_err:
                    logger.debug(f"Could not get contact group for counterparty {counterparty_id}: {contact_err}")
            
            # Способ 3: Определяем по project_id
            if not franchise_group_id and project_id and project_id > 0:
                for group_id, group_data in FRANCHISE_GROUPS.items():
                    if group_data.get('project_id') == project_id:
                        franchise_group_id = group_id
                        logger.debug(f"Determined franchise_group_id {franchise_group_id} from project_id {project_id} for task {task_id}")
                        break
            
            # Способ 4: Ищем по тегам и направлению - находим всех исполнителей с подходящим направлением
            # и берем их franchise_groups
            if not franchise_group_id and task_direction:
                try:
                    with self.db_manager.get_db() as db:
                        from database import ExecutorProfile
                        executors_with_direction = db.query(ExecutorProfile).filter(
                            ExecutorProfile.profile_status == "активен",
                            ExecutorProfile.service_direction == task_direction
                        ).all()
                        
                        if executors_with_direction:
                            # Берем первую найденную группу из исполнителей с подходящим направлением
                            for executor in executors_with_direction:
                                if executor.serving_franchise_groups:
                                    for group_id in executor.serving_franchise_groups:
                                        if group_id in FRANCHISE_GROUPS:
                                            franchise_group_id = group_id
                                            logger.debug(f"Determined franchise_group_id {franchise_group_id} from executor direction {task_direction} for task {task_id}")
                                            break
                                    if franchise_group_id:
                                        break
                except Exception as direction_err:
                    logger.debug(f"Could not determine franchise_group_id from direction: {direction_err}")
            
            if not franchise_group_id:
                logger.warning(f"Could not determine franchise group for task {task_id} (project_id={project_id}, counterparty_group_id={counterparty_group_id}, task_direction={task_direction})")
                return
            
            # Находим всех исполнителей этой концепции
            with self.db_manager.get_db() as db:
                executors = db.query(ExecutorProfile).filter(
                    ExecutorProfile.profile_status == "активен"
                ).all()
                
                for executor in executors:
                    if franchise_group_id not in (executor.serving_franchise_groups or []):
                        continue
                    executor_direction = (executor.service_direction or "").lower()
                    if task_direction and executor_direction and executor_direction != task_direction:
                        continue
                    executor_restaurants = _extract_restaurant_ids(executor.serving_restaurants)
                    if counterparty_id and executor_restaurants and counterparty_id not in executor_restaurants:
                        continue

                    from keyboards import get_task_actions_keyboard
                    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                    
                    message = (
                        f"🆕 Новая заявка #{task_id}\n\n"
                        f"📝 {task_name}\n"
                        f"🏪 Ресторан: {counterparty_name}\n"
                        f"📊 Статус: Новая\n\n"
                        f"Примите задачу в работу, если она вам подходит."
                    )
                    
                    # Создаем клавиатуру с кнопкой "Принять в работу"
                    keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(
                                text="✅ Принять в работу",
                                callback_data=f"accept:{task_id}"
                            )]
                        ]
                    )
                    
                    await self._send_notification(executor.telegram_id, message, reply_markup=keyboard)
                        
        except Exception as e:
            logger.error(f"Error notifying new task {task_id}: {e}", exc_info=True)
    
    async def notify_new_comment(self, task_id: int, comment_author: str, comment_text: str, recipients: str = "both", comment_id: int = None):
        """
        Уведомление о новом комментарии. recipients: "user" | "executors" | "both".
        Админам уведомления о комментариях НЕ отправляются.
        """
        try:
            # Получаем полные данные задачи включая кастомные поля для поиска контрагента
            task_response = await self.planfix_client.get_task_by_id(
                task_id,
                fields="id,name,counterparty,customFieldData,files"
            )
            if not task_response or task_response.get('result') != 'success':
                logger.warning(f"Failed to get task {task_id} for comment notification")
                return
            task = task_response.get('task', {})
            task_name = task.get('name', 'Без названия')
            counterparty_id = task.get('counterparty', {}).get('id')

            # Нормализация counterparty_id -> int
            counterparty_num = None
            if counterparty_id:
                if isinstance(counterparty_id, str) and ':' in counterparty_id:
                    try:
                        counterparty_num = int(counterparty_id.split(':')[1])
                    except Exception:
                        counterparty_num = None
                else:
                    try:
                        counterparty_num = int(counterparty_id)
                    except Exception:
                        counterparty_num = None
            
            # Если counterparty_id не найден, пытаемся получить его из кастомных полей
            if not counterparty_num:
                custom_fields = task.get('customFieldData', []) or []
                for cf in custom_fields:
                    field_id = cf.get('field', {}).get('id')
                    # Поле 82 - это обычно "Контакт" (Contact)
                    if field_id == 82:
                        contact_value = cf.get('value')
                        if isinstance(contact_value, dict):
                            contact_id = contact_value.get('id')
                            if contact_id:
                                if isinstance(contact_id, str) and ':' in contact_id:
                                    try:
                                        counterparty_num = int(contact_id.split(':')[1])
                                    except Exception:
                                        pass
                                else:
                                    try:
                                        counterparty_num = int(contact_id)
                                    except Exception:
                                        pass
                                if counterparty_num:
                                    break

            send_to_user = recipients in ("both", "user")
            send_to_execs = recipients in ("both", "executors")

            # Текст уведомления
            message = (
                f"💬 Новый комментарий по заявке #{task_id}\n\n"
                f"📝 {task_name}\n"
                f"👤 От: {comment_author}\n\n"
                f"{comment_text[:200]}{'...' if len(comment_text) > 200 else ''}"
            )
            # Добавляем ссылки на вложения, если есть
            try:
                files_list = task.get('files') or []
                if files_list:
                    attach_lines = []
                    for f in files_list[:5]:
                        fid_raw = f.get('id')
                        name = f.get('name') or f"file_{fid_raw}"
                        try:
                            fid = int(str(fid_raw).split(':')[-1])
                            url = f"{PLANFIX_BASE_URL.replace('/rest','')}/?action=getfile&uniqueid={fid}"
                            attach_lines.append(f"• {name}: {url}")
                        except Exception:
                            continue
                    if attach_lines:
                        message += "\n\n📎 Вложения:\n" + "\n".join(attach_lines)
            except Exception:
                pass

            # Получаем файлы из конкретного комментария (если передан comment_id)
            comment_files = []
            if comment_id:
                try:
                    logger.debug(f"Fetching files from comment {comment_id} for task {task_id}")
                    cr = await self.planfix_client.get_task_comments(task_id, fields="id,dateTime,files", offset=0, page_size=100)
                    comments = (cr.get('comments') or []) if cr and cr.get('result') == 'success' else []
                    logger.debug(f"Found {len(comments)} comments for task {task_id}")
                    
                    for cm in comments:
                        cm_id = cm.get('id')
                        if str(cm_id) == str(comment_id):
                            comment_files = cm.get('files', [])
                            logger.info(f"Found {len(comment_files)} files in comment {comment_id}: {[f.get('name', f.get('id')) for f in comment_files]}")
                            break
                    
                    if not comment_files:
                        logger.warning(f"No files found in comment {comment_id} for task {task_id}")
                except Exception as e:
                    logger.warning(f"Failed to get files from comment {comment_id}: {e}", exc_info=True)
            
            # Скачиваем медиафайлы из комментария (в память, не на диск)
            # ВАЖНО: Файлы хранятся в памяти только во время отправки, затем удаляются
            media_files = []
            if comment_files:
                logger.info(f"Downloading {len(comment_files)} files from comment {comment_id}")
                for f in comment_files:
                    fid_raw = f.get('id')
                    name = f.get('name') or f"file_{fid_raw}"
                    try:
                        fid = int(str(fid_raw).split(':')[-1])
                        logger.debug(f"Downloading file {fid} ({name}) from Planfix...")
                        # Скачиваем файл из Planfix в память (не на диск)
                        file_data = await self.planfix_client.download_file(fid)
                        if file_data:
                            # Ограничение размера файла (50 МБ) для безопасности
                            max_size = 50 * 1024 * 1024  # 50 МБ
                            if len(file_data) > max_size:
                                logger.warning(f"File {fid} ({name}) is too large ({len(file_data)} bytes), skipping")
                                continue
                            
                            media_files.append({
                                'file_id': fid,
                                'name': name,
                                'data': file_data
                            })
                            logger.info(f"✅ Downloaded file {fid} ({name}), size: {len(file_data)} bytes (in memory)")
                        else:
                            logger.warning(f"Failed to download file {fid} ({name}): file_data is None")
                    except Exception as e:
                        logger.error(f"Failed to download file {fid_raw} from comment: {e}", exc_info=True)
                
                if media_files:
                    logger.info(f"Successfully downloaded {len(media_files)} files for sending")
                else:
                    logger.warning(f"No files were successfully downloaded from comment {comment_id}")
            else:
                logger.debug(f"No comment_files to download (comment_id={comment_id})")

            notified_any = False

            # 1) Клиент: сначала по контакту из кастомного поля CUSTOM_FIELD_CONTACT_ID, затем по restaurant_contact_id, затем фолбэки
            if send_to_user:
                user_notified = False
                
                # ПРИОРИТЕТ 1: Ищем заявителя по контакту из кастомного поля CUSTOM_FIELD_CONTACT_ID
                try:
                    from config import CUSTOM_FIELD_CONTACT_ID
                    custom_fields = task.get('customFieldData', []) or []
                    user_contact_id = None
                    
                    for cf in custom_fields:
                        field_id = cf.get('field', {}).get('id')
                        if field_id == CUSTOM_FIELD_CONTACT_ID:
                            contact_value = cf.get('value')
                            if isinstance(contact_value, dict):
                                contact_id_raw = contact_value.get('id')
                                if contact_id_raw:
                                    if isinstance(contact_id_raw, str) and ':' in contact_id_raw:
                                        try:
                                            user_contact_id = int(contact_id_raw.split(':')[-1])
                                        except Exception:
                                            pass
                                    else:
                                        try:
                                            user_contact_id = int(contact_id_raw)
                                        except Exception:
                                            pass
                                    if user_contact_id:
                                        break
                    
                    if user_contact_id:
                        logger.debug(f"Found user contact {user_contact_id} from custom field CUSTOM_FIELD_CONTACT_ID for task {task_id}")
                        with self.db_manager.get_db() as db:
                            # Ищем пользователя по planfix_contact_id
                            user = db.query(UserProfile).filter(
                                UserProfile.planfix_contact_id == str(user_contact_id)
                            ).first()
                            # Исключаем админов из уведомлений о комментариях
                            if user and user.telegram_id not in TELEGRAM_ADMIN_IDS:
                                await self._send_notification(user.telegram_id, message, media_files=media_files)
                                notified_any = True
                                user_notified = True
                                logger.info(f"✅ Notified user {user.telegram_id} about comment in task {task_id} (found by planfix_contact_id={user_contact_id})")
                            elif user:
                                logger.debug(f"User {user.telegram_id} is admin, skipping notification for task {task_id}")
                            else:
                                logger.warning(f"No user found with planfix_contact_id={user_contact_id} for task {task_id}")
                except Exception as e:
                    logger.error(f"Error searching user by CUSTOM_FIELD_CONTACT_ID for task {task_id}: {e}", exc_info=True)
                
                # ПРИОРИТЕТ 2: Фолбэк - ищем по restaurant_contact_id (counterparty)
                if not user_notified and counterparty_num:
                    try:
                        with self.db_manager.get_db() as db:
                            # Логируем поиск
                            logger.debug(f"Fallback: Searching for user with restaurant_contact_id={counterparty_num} for task {task_id}")
                            user = db.query(UserProfile).filter(
                                UserProfile.restaurant_contact_id == counterparty_num
                            ).first()
                            # Исключаем админов из уведомлений о комментариях
                            if user and user.telegram_id not in TELEGRAM_ADMIN_IDS:
                                await self._send_notification(user.telegram_id, message, media_files=media_files)
                                notified_any = True
                                user_notified = True
                                logger.info(f"✅ Notified user {user.telegram_id} about comment in task {task_id} (found by restaurant_contact_id={counterparty_num})")
                            elif user:
                                logger.debug(f"User {user.telegram_id} is admin, skipping notification for task {task_id}")
                            else:
                                logger.warning(f"No user found with restaurant_contact_id={counterparty_num} for task {task_id}. Will try fallback methods.")
                    except Exception as e:
                        logger.error(f"Error notifying user for task {task_id}: {e}", exc_info=True)
                elif not user_notified:
                    logger.warning(f"counterparty_id is None or invalid for task {task_id}, cannot search by restaurant_contact_id")
                
                # Диагностика: логируем всех пользователей в БД для отладки
                if not user_notified:
                    try:
                        with self.db_manager.get_db() as db:
                            all_users = db.query(UserProfile).all()
                            logger.debug(f"Total users in DB: {len(all_users)}")
                            for u in all_users[:10]:  # Логируем первых 10
                                logger.debug(f"  User: tg_id={u.telegram_id}, restaurant_id={u.restaurant_contact_id}, phone={u.phone_number}")
                    except Exception as e:
                        logger.error(f"Error logging users for diagnostics: {e}")
                
                # Фолбэк: ищем пользователя по телефону из кастомных полей
                if not user_notified:
                    try:
                        tr = await self.planfix_client.get_task_by_id(
                            task_id,
                            fields="id,customFieldData"
                        )
                        task2 = tr.get('task', {}) if tr and tr.get('result') == 'success' else {}
                        phone_value = None
                        for cf in task2.get('customFieldData', []) or []:
                            if cf.get('field', {}).get('id') in (84, 88):  # Телефон (84: Номер, 88: Мобильный)
                                phone_value = cf.get('value')
                                break
                        if phone_value:
                            with self.db_manager.get_db() as db:
                                user = db.query(UserProfile).filter(
                                    UserProfile.phone_number == phone_value
                                ).first()
                                # Исключаем админов из уведомлений о комментариях
                                if user and user.telegram_id not in TELEGRAM_ADMIN_IDS:
                                    await self._send_notification(user.telegram_id, message, media_files=media_files)
                                    notified_any = True
                                    user_notified = True
                                    logger.info(f"Notified user {user.telegram_id} by phone {phone_value} about comment in task {task_id}")
                    except Exception as e:
                        logger.error(f"Fallback phone notify error for task {task_id}: {e}", exc_info=True)

                # Доп. фолбэк по BotLog: task_id -> user_telegram_id
                if send_to_user and not user_notified:
                    try:
                        with self.db_manager.get_db() as db:
                            logs = db.query(BotLog).filter(BotLog.action == 'create_task').order_by(BotLog.timestamp.desc()).limit(500).all()
                            logger.warning(f"Searching BotLog for task {task_id}, found {len(logs)} create_task logs")
                            for log in logs:
                                try:
                                    details = log.details or {}
                                    # Обработка случая, когда details может быть строкой (JSON)
                                    if isinstance(details, str):
                                        try:
                                            details = json.loads(details)
                                        except Exception:
                                            details = {}
                                    log_task_id = details.get('task_id', -1)
                                    logger.warning(f"  BotLog entry: task_id={log_task_id}, details type={type(details)}, details={details}")
                                    if int(log_task_id) == int(task_id):
                                        tg = details.get('user_telegram_id')
                                        logger.warning(f"  Found matching BotLog entry! user_telegram_id={tg}")
                                        # Исключаем админов из уведомлений о комментариях
                                        if tg and int(tg) not in TELEGRAM_ADMIN_IDS:
                                            await self._send_notification(int(tg), message, media_files=media_files)
                                            notified_any = True
                                            user_notified = True
                                            logger.info(f"Notified user {tg} by BotLog for task {task_id}")
                                            break
                                except Exception as e:
                                    logger.warning(f"  Error processing BotLog entry: {e}")
                                    continue
                    except Exception as e:
                        logger.error(f"Fallback BotLog notify error for task {task_id}: {e}", exc_info=True)

            # 2) Исполнители: из локальных назначений (TaskAssignment) и из assignees в задаче
            if send_to_execs:
                executors_notified = set()
                # Сначала ищем в TaskAssignment
                try:
                    with self.db_manager.get_db() as db:
                        accepted = db.query(TaskAssignment).filter(
                            TaskAssignment.task_id == task_id,
                            TaskAssignment.status == "active"
                        ).all()
                        for a in accepted:
                            try:
                                await self._send_notification(a.executor_telegram_id, message, media_files=media_files)
                                notified_any = True
                                executors_notified.add(a.executor_telegram_id)
                                logger.info(f"✅ Notified executor {a.executor_telegram_id} about comment in task {task_id} (from TaskAssignment)")
                            except Exception as se:
                                logger.error(f"Error notifying executor tg:{a.executor_telegram_id} for task {task_id}: {se}")
                except Exception as e:
                    logger.error(f"Error loading local assignments for task {task_id}: {e}", exc_info=True)
                
                # Фолбэк: если не нашли в TaskAssignment, ищем по assignees в задаче из Planfix
                if not executors_notified:
                    try:
                        logger.debug(f"No active TaskAssignment found for task {task_id}, trying to find executors via assignees in task")
                        task_with_assignees = await self.planfix_client.get_task_by_id(
                            task_id,
                            fields="id,assignees"
                        )
                        if task_with_assignees and task_with_assignees.get('result') == 'success':
                            task_data = task_with_assignees.get('task', {})
                            assignees = task_data.get('assignees', {}).get('users', [])
                            
                            if assignees:
                                logger.info(f"Found {len(assignees)} assignees in task {task_id} from Planfix")
                                # Нормализуем assignees (может быть список или один объект)
                                if not isinstance(assignees, list):
                                    assignees = [assignees]
                                
                                with self.db_manager.get_db() as db:
                                    for assignee in assignees:
                                        try:
                                            # Получаем ID пользователя/контакта из assignee
                                            assignee_id = None
                                            if isinstance(assignee, dict):
                                                assignee_id = assignee.get('id')
                                            elif isinstance(assignee, str):
                                                assignee_id = assignee
                                            
                                            if not assignee_id:
                                                continue
                                            
                                            # Нормализуем ID (может быть строка с "user:123" или просто число)
                                            if isinstance(assignee_id, str):
                                                if ':' in assignee_id:
                                                    assignee_id = assignee_id.split(':')[-1]
                                                try:
                                                    assignee_id = int(assignee_id)
                                                except ValueError:
                                                    continue
                                            
                                            # Ищем исполнителя по planfix_user_id или planfix_contact_id
                                            executor = db.query(ExecutorProfile).filter(
                                                (ExecutorProfile.planfix_user_id == str(assignee_id)) |
                                                (ExecutorProfile.planfix_contact_id == str(assignee_id))
                                            ).first()
                                            
                                            if executor and executor.telegram_id not in executors_notified:
                                                try:
                                                    await self._send_notification(executor.telegram_id, message, media_files=media_files)
                                                    notified_any = True
                                                    executors_notified.add(executor.telegram_id)
                                                    logger.info(f"✅ Notified executor {executor.telegram_id} about comment in task {task_id} (found via assignees, planfix_id={assignee_id})")
                                                except Exception as se:
                                                    logger.error(f"Error notifying executor tg:{executor.telegram_id} for task {task_id}: {se}")
                                        except Exception as assignee_err:
                                            logger.warning(f"Error processing assignee {assignee} for task {task_id}: {assignee_err}")
                            else:
                                logger.debug(f"No assignees found in task {task_id} from Planfix")
                    except Exception as e:
                        logger.error(f"Error finding executors via assignees for task {task_id}: {e}", exc_info=True)

            if not notified_any:
                logger.warning(f"No notifications sent for comment in task {task_id} (recipients={recipients})")

        except Exception as e:
            logger.error(f"Error notifying comment for task {task_id}: {e}", exc_info=True)
    
    async def notify_task_assigned(self, task_id: int, executor_planfix_id: str):
        """
        Уведомление исполнителя о назначении задачи.
        
        Args:
            task_id: ID задачи
            executor_planfix_id: ID исполнителя в Planfix
        """
        try:
            task_response = await self.planfix_client.get_task_by_id(
                task_id,
                fields="id,name,description,counterparty"
            )
            
            if not task_response or task_response.get('result') != 'success':
                return
            
            task = task_response.get('task', {})
            task_name = task.get('name', 'Без названия')
            counterparty_name = task.get('counterparty', {}).get('name', 'Неизвестно')
            
            message = (
                f"📌 Вам назначена задача #{task_id}\n\n"
                f"📝 {task_name}\n"
                f"🏪 Ресторан: {counterparty_name}\n"
                f"📊 Статус: В работе"
            )
            
            await self._notify_executor_by_planfix_id(executor_planfix_id, task_id, task_name, message)
            
        except Exception as e:
            logger.error(f"Error notifying assignment for task {task_id}: {e}", exc_info=True)
    
    async def notify_task_completed(self, task_id: int):
        """
        Уведомление о завершении задачи.
        
        Args:
            task_id: ID задачи
        """
        try:
            task_response = await self.planfix_client.get_task_by_id(
                task_id,
                fields="id,name,counterparty"
            )
            
            if not task_response or task_response.get('result') != 'success':
                return
            
            task = task_response.get('task', {})
            task_name = task.get('name', 'Без названия')
            counterparty_id = task.get('counterparty', {}).get('id')
            
            if counterparty_id:
                with self.db_manager.get_db() as db:
                    user = db.query(UserProfile).filter(
                        UserProfile.restaurant_contact_id == counterparty_id
                    ).first()
                    
                    if user:
                        message = (
                            f"✅ Заявка #{task_id} выполнена!\n\n"
                            f"📝 {task_name}\n"
                            f"📊 Статус: Выполнена\n\n"
                            f"Проблема решена. Если у вас остались вопросы, "
                            f"вы можете добавить комментарий к заявке."
                        )
                        await self._send_notification(user.telegram_id, message)
                        
        except Exception as e:
            logger.error(f"Error notifying completion for task {task_id}: {e}", exc_info=True)
    
    async def notify_task_cancelled(self, task_id: int, cancelled_by: str):
        """
        Уведомление об отмене задачи.
        
        Args:
            task_id: ID задачи
            cancelled_by: Кто отменил
        """
        try:
            task_response = await self.planfix_client.get_task_by_id(
                task_id,
                fields="id,name,assignees"
            )
            
            if not task_response or task_response.get('result') != 'success':
                return
            
            task = task_response.get('task', {})
            task_name = task.get('name', 'Без названия')
            
            # Уведомляем исполнителей
            assignees = task.get('assignees', {}).get('users', [])
            for assignee in assignees:
                assignee_id = assignee.get('id', '').replace('user:', '')
                if assignee_id:
                    message = (
                        f"❌ Заявка #{task_id} отменена\n\n"
                        f"📝 {task_name}\n"
                        f"👤 Отменил: {cancelled_by}\n"
                        f"📊 Статус: Отменена"
                    )
                    await self._notify_executor_by_planfix_id(assignee_id, task_id, task_name, message)
                    
        except Exception as e:
            logger.error(f"Error notifying cancellation for task {task_id}: {e}", exc_info=True)

    async def notify_admin_executor_approval_request(self, executor_profile, task_id: int):
        """
        Уведомляет администраторов о необходимости подтвердить исполнителя в боте
        и ввести Planfix User ID. Прикладывает inline-кнопки подтверждения/отклонения.
        """
        try:
            concept_names = [
                FRANCHISE_GROUPS[cid]["name"]
                for cid in (executor_profile.serving_franchise_groups or [])
                if cid in FRANCHISE_GROUPS
            ]
            message = (
                f"🆕 Задача регистрации исполнителя завершена в Planfix\n\n"
                f"📋 Задача #{task_id}\n"
                f"👤 ФИО: {executor_profile.full_name}\n"
                f"📱 Телефон: {executor_profile.phone_number}\n"
                f"🏢 Концепции: {', '.join(concept_names) if concept_names else '—'}\n\n"
                f"✅ Подтвердите регистрацию через бота и введите Planfix User ID."
            )
            for admin_id in TELEGRAM_ADMIN_IDS:
                try:
                    await self.bot.send_message(
                        admin_id,
                        message,
                        reply_markup=get_executor_confirmation_keyboard(executor_profile.telegram_id)
                    )
                except Exception as e:
                    logger.error(f"Failed to notify admin {admin_id} about executor approval: {e}")
        except Exception as e:
            logger.error("Error while preparing admin approval notification: %s", e, exc_info=True)
    
    def _format_status_change_message(
        self, task_id: int, task_name: str, status_name: str, 
        old_status_id: int, new_status_id: int
    ) -> str:
        """Форматирование сообщения об изменении статуса."""
        emoji_map = status_labels(
            (
                (StatusKey.NEW, "🆕"),
                (StatusKey.IN_PROGRESS, "🔄"),
                (StatusKey.INFO_SENT, "📤"),
                (StatusKey.REPLY_RECEIVED, "📥"),
                (StatusKey.COMPLETED, "✅"),
                (StatusKey.CANCELLED, "❌"),
            )
        )
        
        emoji = emoji_map.get(new_status_id, "📊")
        
        message = (
            f"{emoji} Статус заявки #{task_id} изменён\n\n"
            f"📝 {task_name}\n"
            f"📊 Новый статус: {status_name}\n\n"
        )
        
        # Добавляем подсказки в зависимости от статуса
        if is_status(new_status_id, StatusKey.IN_PROGRESS):
            message += "Ваша заявка принята в работу. Исполнитель свяжется с вами при необходимости."
        elif is_status(new_status_id, StatusKey.INFO_SENT):
            message += "⚠️ Исполнителю требуется дополнительная информация. Проверьте комментарии к заявке."
        elif is_status(new_status_id, StatusKey.COMPLETED):
            message += "Проблема решена! Спасибо за обращение."
        elif is_status(new_status_id, StatusKey.CANCELLED):
            message += "Заявка отменена."
        
        return message
    
    async def _notify_executor_by_planfix_id(
        self, planfix_user_id: str, task_id: int, task_name: str, message: str
    ):
        """Уведомление исполнителя по его Planfix ID."""
        try:
            with self.db_manager.get_db() as db:
                executor = db.query(ExecutorProfile).filter(
                    ExecutorProfile.planfix_user_id == planfix_user_id
                ).first()
                
                if executor:
                    await self._send_notification(executor.telegram_id, message)
                    
        except Exception as e:
            logger.error(f"Error notifying executor {planfix_user_id}: {e}", exc_info=True)
    
    async def _send_notification(self, telegram_id: int, message: str, media_files: list = None, reply_markup=None):
        """Отправка уведомления пользо��ателю."""
        logger.debug(f"Attempting to send notification to user {telegram_id}")
        if media_files:
            # Отправляем медиафайлы вместе с текстом
            await self._send_notification_with_media(telegram_id, message, media_files, reply_markup=reply_markup)
        else:
            # Отправляем только текст
            try:
                await self.bot.send_message(telegram_id, message, reply_markup=reply_markup)
                logger.info(f"✅ Notification sent to user {telegram_id}")
            except Exception as e:
                logger.error(f"❌ Failed to send notification to {telegram_id}: {e}", exc_info=True)
    
    async def _send_notification_with_media(self, telegram_id: int, message: str, media_files: list, reply_markup=None):
        """
        Отправляет уведомление с медиафайлами.
        ВАЖНО: Файлы работают только в памяти (io.BytesIO), не сохраняются на диск.
        После отправки файлы автоматически удаляются из памяти.
        """
        try:
            logger.info(f"Sending notification with {len(media_files)} media files to user {telegram_id}")
            # Определяем тип файла и отправляем соответствующим методом
            photos = []
            documents = []
            
            for file_info in media_files:
                file_data = file_info.get('data')
                file_name = file_info.get('name', 'file')
                
                if not file_data:
                    logger.warning(f"File {file_name} has no data, skipping")
                    continue
                
                # Определяем MIME-тип по расширению
                mime_type, _ = mimetypes.guess_type(file_name)
                logger.debug(f"File {file_name}: mime_type={mime_type}, size={len(file_data)} bytes")
                
                # Определяем, является ли файл изображением
                if mime_type and mime_type.startswith('image/'):
                    photos.append((file_data, file_name))
                    logger.debug(f"Added {file_name} as photo")
                else:
                    # Для всех остальных файлов отправляем как документ
                    documents.append((file_data, file_name, mime_type))
                    logger.debug(f"Added {file_name} as document")
            
            logger.info(f"Prepared {len(photos)} photos and {len(documents)} documents for sending")
            
            # Отправляем фото (если есть)
            # Используем BufferedInputFile для работы в памяти, файлы не сохраняются на диск
            if photos:
                if len(photos) == 1:
                    # Одно фото - отправляем с подписью
                    photo_data, photo_name = photos[0]
                    try:
                        # Создаем BufferedInputFile из bytes (не сохраняется на диск)
                        photo_file = BufferedInputFile(photo_data, filename=photo_name)
                        await self.bot.send_photo(
                            telegram_id,
                            photo=photo_file,
                            caption=message,
                            parse_mode=None,
                            reply_markup=reply_markup
                        )
                        logger.info(f"✅ Notification with photo sent to user {telegram_id}")
                    finally:
                        # Явно удаляем из памяти
                        del photo_data
                else:
                    # Несколько фото - отправляем медиагруппой
                    media_group = []
                    try:
                        for i, (photo_data, photo_name) in enumerate(photos):
                            photo_file = BufferedInputFile(photo_data, filename=photo_name)
                            media_group.append(
                                aiogram.types.InputMediaPhoto(
                                    media=photo_file,
                                    caption=message if i == 0 else None
                                )
                            )
                        await self.bot.send_media_group(telegram_id, media=media_group)
                        logger.info(f"✅ Notification with {len(photos)} photos sent to user {telegram_id}")
                    finally:
                        # Освобождаем память
                        for photo_data, _ in photos:
                            del photo_data
                
                # Отправляем документы отдельно (если есть)
                for doc_data, doc_name, doc_mime in documents:
                    try:
                        doc_file = BufferedInputFile(doc_data, filename=doc_name)
                        await self.bot.send_document(
                            telegram_id,
                            document=doc_file,
                            caption=f"📎 {doc_name}" if not photos else None
                        )
                    finally:
                        del doc_data
            elif documents:
                # Только документы - первый с подписью, остальные без
                for i, (doc_data, doc_name, doc_mime) in enumerate(documents):
                    try:
                        doc_file = BufferedInputFile(doc_data, filename=doc_name)
                        await self.bot.send_document(
                            telegram_id,
                            document=doc_file,
                            caption=message if i == 0 else f"📎 {doc_name}"
                        )
                    finally:
                        del doc_data
                logger.info(f"✅ Notification with {len(documents)} documents sent to user {telegram_id}")
            else:
                # Если не удалось скачать файлы, отправляем только текст
                await self.bot.send_message(telegram_id, message, reply_markup=reply_markup)
            
            # Явно очищаем список медиафайлов из памяти
            for file_info in media_files:
                if 'data' in file_info:
                    del file_info['data']
            media_files.clear()
                
        except Exception as e:
            logger.error(f"❌ Failed to send notification with media to {telegram_id}: {e}", exc_info=True)
            # Fallback: отправляем только текст
            try:
                await self.bot.send_message(telegram_id, message, reply_markup=reply_markup)
            except Exception:
                pass
            finally:
                # В любом случае очищаем память
                for file_info in media_files:
                    if 'data' in file_info:
                        del file_info['data']
                media_files.clear()
