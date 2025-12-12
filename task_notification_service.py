"""
Сервис для отправки уведомлений о новых заявках исполнителям.
Использует те же фильтры, что и функция show_new_tasks в executor_handlers.py
"""

import logging
from typing import List, Set, Dict
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db_manager import DBManager
from database import ExecutorProfile
from planfix_client import planfix_client
from config import (
    PLANFIX_IT_TEMPLATES,
    PLANFIX_SE_TEMPLATES,
    PLANFIX_IT_TAG,
    PLANFIX_SE_TAG,
    SUPPORT_CONTACT_GROUP_ID,
    SUPPORT_CONTACT_TEMPLATE_ID,
)

logger = logging.getLogger(__name__)


def _normalize_pf_id(value) -> int | None:
    """Нормализует ID из Planfix (может быть строкой вида "task:123" или числом)."""
    try:
        if isinstance(value, str) and ':' in value:
            value = value.split(':')[-1]
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_restaurant_ids(data) -> List[int]:
    """Извлекает список ID ресторанов из данных исполнителя."""
    ids = []
    if not data:
        return ids
    
    items = data if isinstance(data, list) else [data]
    for item in items:
        if isinstance(item, int):
            ids.append(item)
        elif isinstance(item, str):
            try:
                ids.append(int(item))
            except (TypeError, ValueError):
                continue
        elif isinstance(item, dict):
            val = item.get("id")
            try:
                if isinstance(val, int):
                    ids.append(val)
                elif isinstance(val, str):
                    ids.append(int(val))
            except (TypeError, ValueError):
                continue
    return ids


def _get_allowed_template_ids(executor: ExecutorProfile) -> Set[int]:
    """Возвращает множество разрешенных ID шаблонов для исполнителя."""
    direction = (executor.service_direction or "").strip().lower()
    allowed = set()
    
    if not direction or direction in ("it", "ит", "it отдел", "it-служба", "it служба"):
        allowed.update(PLANFIX_IT_TEMPLATES.keys())
    
    if not direction or direction in ("se", "сэ", "служба эксплуатации", "эксплуатация", "отдел эксплуатации"):
        allowed.update(PLANFIX_SE_TEMPLATES.keys())
    
    return allowed


def _get_allowed_tags(executor: ExecutorProfile) -> Set[str]:
    """Возвращает допустимые теги задач для исполнителя на основе направления."""
    if not executor:
        return {PLANFIX_IT_TAG, PLANFIX_SE_TAG}
    
    direction = (executor.service_direction or "").strip().lower()
    tags: Set[str] = set()
    
    if not direction or direction in ("se", "сэ", "служба эксплуатации", "эксплуатация", "отдел эксплуатации"):
        if PLANFIX_SE_TAG:
            tags.add(PLANFIX_SE_TAG)
    
    if not direction or direction in ("it", "ит", "it отдел", "it-служба", "it служба"):
        if PLANFIX_IT_TAG:
            tags.add(PLANFIX_IT_TAG)
    
    if not tags:
        if PLANFIX_SE_TAG:
            tags.add(PLANFIX_SE_TAG)
        if PLANFIX_IT_TAG:
            tags.add(PLANFIX_IT_TAG)
    
    return tags


def _extract_task_tags(task: dict) -> Set[str]:
    """Извлекает множество тегов задачи в нижнем регистре.
    Проверяет как поле 'tags', так и 'dataTags' (для совместимости с разными версиями API).
    """
    names: Set[str] = set()
    
    # Проверяем поле 'tags'
    tags_field = task.get('tags')
    if isinstance(tags_field, list):
        for tag in tags_field:
            if isinstance(tag, str):
                name = tag.strip()
            elif isinstance(tag, dict):
                name = (
                    tag.get('name')
                    or tag.get('value')
                    or tag.get('title')
                    or ""
                ).strip()
            else:
                name = ""
            if name:
                names.add(name.lower())
    elif isinstance(tags_field, str):
        name = tags_field.strip()
        if name:
            names.add(name.lower())
    
    # Проверяем поле 'dataTags' (альтернативный формат)
    data_tags_field = task.get('dataTags')
    if isinstance(data_tags_field, list):
        for data_tag_entry in data_tags_field:
            if isinstance(data_tag_entry, dict):
                data_tag = data_tag_entry.get('dataTag', {})
                if isinstance(data_tag, dict):
                    tag_name = (
                        data_tag.get('name')
                        or data_tag.get('value')
                        or ""
                    ).strip()
                    if tag_name:
                        names.add(tag_name.lower())
                elif isinstance(data_tag, str):
                    names.add(data_tag.lower())
    
    return names


class TaskNotificationService:
    """Сервис для отправки уведомлений о новых заявках исполнителям."""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.db_manager = DBManager()
    
    async def notify_executors_about_new_task(self, task_id: int):
        """
        Находит всех подходящих исполнителей для задачи и отправляет им уведомления.
        Использует те же фильтры, что и show_new_tasks в executor_handlers.py
        
        Args:
            task_id: ID задачи в Planfix
        """
        try:
            logger.info(f"📨 Starting notification process for task {task_id}")
            # Получаем информацию о задаче
            # ВАЖНО: task_id может быть как generalId, так и внутренним id
            # Пробуем сначала как generalId, если не получится - как внутренний id
            task_response = None
            try:
                task_response = await planfix_client.get_task_by_id(
                    task_id,
                    fields="id,name,description,status,template,counterparty,tags,dataTags,project"
                )
            except Exception as api_err:
                logger.warning(f"Failed to get task {task_id} by generalId: {api_err}, trying to find in BotLog")
                # Пробуем найти generalId в BotLog
                try:
                    from database import BotLog
                    with self.db_manager.get_db() as db:
                        bot_logs = db.query(BotLog).filter(
                            BotLog.action == "create_task",
                            BotLog.success == True
                        ).order_by(BotLog.id.desc()).limit(50).all()
                        
                        for log in bot_logs:
                            if not log.details:
                                continue
                            
                            # Проверяем все возможные ID
                            log_task_id = log.details.get('task_id')
                            log_internal_id = log.details.get('task_id_internal')
                            
                            # Нормализуем для сравнения
                            log_id_int = None
                            if log_internal_id:
                                try:
                                    log_id_int = int(log_internal_id)
                                except (ValueError, TypeError):
                                    pass
                            
                            if not log_id_int and log_task_id:
                                try:
                                    log_id_int = int(log_task_id)
                                except (ValueError, TypeError):
                                    pass
                            
                            if log_id_int == task_id:
                                general_id = log.details.get('task_id_general') or log.details.get('task_id')
                                if general_id:
                                    try:
                                        general_id_int = int(general_id)
                                        logger.info(f"Found generalId {general_id_int} for task {task_id} in BotLog, retrying API call")
                                        task_response = await planfix_client.get_task_by_id(
                                            general_id_int,
                                            fields="id,name,description,status,template,counterparty,tags,dataTags,project"
                                        )
                                        break
                                    except Exception:
                                        pass
                except Exception as log_err:
                    logger.warning(f"Error searching BotLog for task {task_id}: {log_err}")
            
            if not task_response or task_response.get('result') != 'success':
                logger.warning(f"❌ Could not get task {task_id} for executor notification (response: {task_response})")
                return
            
            task = task_response.get('task', {})
            task_name = task.get('name', 'Без названия')
            
            # Извлекаем данные задачи
            template_id = _normalize_pf_id((task.get('template') or {}).get('id'))
            counterparty_id = _normalize_pf_id((task.get('counterparty') or {}).get('id'))
            task_tags = _extract_task_tags(task)
            
            # Определяем направление задачи по шаблону или по тегам
            task_direction = None
            if template_id in PLANFIX_IT_TEMPLATES:
                task_direction = "it"
            elif template_id in PLANFIX_SE_TEMPLATES:
                task_direction = "se"
            else:
                # Пробуем определить по тегам
                if task_tags:
                    if PLANFIX_IT_TAG and PLANFIX_IT_TAG.lower() in task_tags:
                        task_direction = "it"
                    elif PLANFIX_SE_TAG and PLANFIX_SE_TAG.lower() in task_tags:
                        task_direction = "se"
            
            logger.info(
                f"Notifying executors about task {task_id}: "
                f"template_id={template_id}, counterparty_id={counterparty_id}, "
                f"tags={task_tags}, task_direction={task_direction}"
            )
            
            # Получаем имя ресторана
            counterparty_name = "Неизвестно"
            if counterparty_id:
                try:
                    # Сначала проверяем кэш
                    from shared_cache import cache as shared_cache
                    cached_name = shared_cache.get(f"cp_name:{task_id}")
                    if cached_name:
                        counterparty_name = cached_name
                    else:
                        # Пытаемся получить из BotLog (как в executor_handlers)
                        from database import BotLog
                        with self.db_manager.get_db() as db:
                            # Получаем все записи о создании задач и проверяем в Python
                            bot_logs = db.query(BotLog).filter(
                                BotLog.action == "create_task"
                            ).order_by(BotLog.id.desc()).all()
                            
                            bot_log = None
                            for log in bot_logs:
                                if log.details:
                                    try:
                                        log_task_id = log.details.get('task_id')
                                        if log_task_id is not None:
                                            log_task_id_int = int(str(log_task_id).split(':')[-1])
                                            if log_task_id_int == task_id:
                                                bot_log = log
                                                break
                                    except (ValueError, TypeError, AttributeError):
                                        continue
                            
                            if bot_log and bot_log.details:
                                user_telegram_id = bot_log.details.get('user_telegram_id')
                                if user_telegram_id:
                                    from database import UserProfile
                                    user = db.query(UserProfile).filter(
                                        UserProfile.telegram_id == user_telegram_id
                                    ).first()
                                    if user and user.restaurant_contact_id:
                                        # Используем restaurant_contact_id для получения имени
                                        try:
                                            contact_resp = await planfix_client.get_contact_by_id(
                                                int(user.restaurant_contact_id),
                                                fields="id,name"
                                            )
                                            if contact_resp and contact_resp.get('result') == 'success':
                                                contact = contact_resp.get('contact', {}) or {}
                                                counterparty_name = contact.get('name', 'Неизвестно')
                                                # Кэшируем результат
                                                shared_cache.set(f"cp_name:{task_id}", counterparty_name, ttl_seconds=24*3600)
                                        except Exception:
                                            pass
                        
                        # Если не получили из BotLog, пробуем напрямую из counterparty_id
                        if counterparty_name == "Неизвестно":
                            try:
                                contact_resp = await planfix_client.get_contact_by_id(
                                    counterparty_id,
                                    fields="id,name"
                                )
                                if contact_resp and contact_resp.get('result') == 'success':
                                    contact = contact_resp.get('contact', {}) or {}
                                    counterparty_name = contact.get('name', 'Неизвестно')
                                    # Кэшируем результат
                                    shared_cache.set(f"cp_name:{task_id}", counterparty_name, ttl_seconds=24*3600)
                            except Exception:
                                pass
                except Exception as name_err:
                    logger.debug(f"Could not get counterparty name for task {task_id}: {name_err}")
            
            # Получаем всех активных исполнителей
            with self.db_manager.get_db() as db:
                executors = db.query(ExecutorProfile).filter(
                    ExecutorProfile.profile_status == "активен"
                ).all()
            
            # Собираем всех подходящих исполнителей для автоматического назначения
            matching_executors = []
            assignee_contact_ids = []
            
            for executor in executors:
                # Применяем те же фильтры, что и в show_new_tasks
                
                # Фильтр 0: Направление задачи должно совпадать с направлением исполнителя (если оба заданы)
                executor_direction = (executor.service_direction or "").strip().lower()
                if task_direction and executor_direction:
                    # Нормализуем направление исполнителя
                    executor_dir_normalized = None
                    if executor_direction in ("it", "ит", "it отдел", "it-служба", "it служба"):
                        executor_dir_normalized = "it"
                    elif executor_direction in ("se", "сэ", "служба эксплуатации", "эксплуатация", "отдел эксплуатации"):
                        executor_dir_normalized = "se"
                    
                    if executor_dir_normalized and executor_dir_normalized != task_direction:
                        logger.debug(
                            f"Executor {executor.telegram_id} filtered out: "
                            f"task direction {task_direction} != executor direction {executor_dir_normalized}"
                        )
                        continue
                
                # Фильтр 1: Шаблон задачи должен быть в списке разрешенных шаблонов исполнителя
                allowed_templates = _get_allowed_template_ids(executor)
                if allowed_templates:
                    if template_id is None or template_id not in allowed_templates:
                        logger.debug(
                            f"Executor {executor.telegram_id} filtered out: "
                            f"template {template_id} not in {allowed_templates}"
                        )
                        continue
                
                # Фильтр 2: Ресторан (counterparty) должен быть в списке обслуживаемых ресторанов
                allowed_restaurant_ids = set(_extract_restaurant_ids(executor.serving_restaurants))
                if allowed_restaurant_ids:
                    if counterparty_id is None or counterparty_id not in allowed_restaurant_ids:
                        logger.debug(
                            f"Executor {executor.telegram_id} filtered out: "
                            f"counterparty {counterparty_id} not in {allowed_restaurant_ids}"
                        )
                        continue
                
                # Фильтр 3: Теги задачи должны пересекаться с разрешенными тегами исполнителя
                # ИСКЛЮЧЕНИЕ: если шаблон правильный, но тега нет - отправляем уведомление (теги в шаблоне)
                allowed_tags = _get_allowed_tags(executor)
                allowed_tag_names = {tag.lower() for tag in allowed_tags if isinstance(tag, str)}
                if allowed_tag_names:
                    if task_tags:
                        # У задачи есть теги - проверяем соответствие
                        if not (task_tags & allowed_tag_names):
                            # У задачи есть теги, но они не совпадают с разрешенными
                            logger.debug(
                                f"Executor {executor.telegram_id} filtered out: "
                                f"task tags {task_tags} don't match allowed tags {allowed_tag_names}"
                            )
                            continue
                        else:
                            logger.debug(
                                f"Executor {executor.telegram_id} passed tag filter: "
                                f"task tags {task_tags} match allowed tags {allowed_tag_names}"
                            )
                    else:
                        # У задачи нет тегов - проверяем, соответствует ли шаблон
                        # Если шаблон правильный, отправляем уведомление (теги в шаблоне задачи)
                        if template_id in allowed_templates:
                            logger.debug(
                                f"Executor {executor.telegram_id} passed tag filter: "
                                f"no tags but template_id={template_id} matches allowed_templates "
                                f"(tags are in template)"
                            )
                        else:
                            # Шаблон не соответствует, и тегов нет - отфильтровываем
                            logger.debug(
                                f"Executor {executor.telegram_id} filtered out: "
                                f"task has no tags and template_id={template_id} not in allowed_templates={allowed_templates}"
                            )
                            continue
                
                # Все фильтры пройдены - добавляем исполнителя в список для назначения
                matching_executors.append(executor)
                
                # Получаем или создаем planfix_contact_id для исполнителя
                planfix_contact_id = None
                if executor.planfix_contact_id:
                    try:
                        if isinstance(executor.planfix_contact_id, str):
                            if ':' in executor.planfix_contact_id:
                                planfix_contact_id = int(executor.planfix_contact_id.split(':')[-1])
                            else:
                                planfix_contact_id = int(executor.planfix_contact_id)
                        else:
                            planfix_contact_id = int(executor.planfix_contact_id)
                        logger.debug(f"Using existing Planfix contact {planfix_contact_id} for executor {executor.telegram_id}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid planfix_contact_id for executor {executor.telegram_id}: {e}")
                
                # Если контакта нет, создаем его
                if not planfix_contact_id:
                    try:
                        logger.info(f"Creating Planfix contact for executor {executor.telegram_id} (contact not found)")
                        # Разделяем ФИО на части
                        name_parts = executor.full_name.strip().split()
                        if len(name_parts) >= 2:
                            name = " ".join(name_parts[1:])
                            lastname = name_parts[0]
                        else:
                            name = executor.full_name
                            lastname = executor.full_name
                        
                        # Создаем контакт в группе "Поддержка"
                        contact_response = None
                        try:
                            contact_response = await planfix_client.create_contact(
                                name=name,
                                lastname=lastname,
                                phone=executor.phone_number,
                                email=executor.email,
                                group_id=SUPPORT_CONTACT_GROUP_ID,
                                template_id=SUPPORT_CONTACT_TEMPLATE_ID
                            )
                        except Exception as e:
                            logger.error(f"Failed to create contact in support group for executor {executor.telegram_id}: {e}")
                            contact_response = None
                        
                        if contact_response and contact_response.get('result') == 'success':
                            contact_id = contact_response.get('id') or contact_response.get('contact', {}).get('id')
                            if contact_id:
                                planfix_contact_id = int(str(contact_id).split(':')[-1]) if isinstance(contact_id, str) and ':' in contact_id else int(contact_id)
                                
                                # Сохраняем contact_id в профиль исполнителя
                                from services.db_service import db_manager
                                await db_manager.update_executor_profile(
                                    executor.telegram_id,
                                    planfix_contact_id=str(planfix_contact_id),
                                    planfix_user_id=str(planfix_contact_id)
                                )
                                logger.info(f"Created and saved Planfix contact {planfix_contact_id} for executor {executor.telegram_id}")
                        else:
                            logger.warning(f"Failed to create Planfix contact for executor {executor.telegram_id}: {contact_response}")
                    except Exception as e:
                        logger.error(f"Error creating Planfix contact for executor {executor.telegram_id}: {e}", exc_info=True)
                
                # Добавляем contact_id в список для назначения
                if planfix_contact_id:
                    assignee_contact_ids.append(planfix_contact_id)
                    logger.debug(f"Added executor {executor.telegram_id} (contact_id={planfix_contact_id}) to assignment list")
            
            # Автоматически назначаем всех подходящих исполнителей в Planfix
            if assignee_contact_ids:
                try:
                    logger.info(f"Auto-assigning {len(assignee_contact_ids)} executor(s) to task {task_id}")
                    update_response = await planfix_client.update_task(
                        task_id,
                        assignee_contacts=assignee_contact_ids
                    )
                    
                    if update_response and update_response.get('result') == 'success':
                        logger.info(f"✅ Successfully assigned {len(assignee_contact_ids)} executor(s) to task {task_id}")
                    else:
                        logger.warning(f"Failed to assign executors to task {task_id}: {update_response}")
                except Exception as assign_err:
                    logger.error(f"Error assigning executors to task {task_id}: {assign_err}", exc_info=True)
            
            # Отправляем уведомления всем подходящим исполнителям (без кнопки "Принять")
            notified_count = 0
            for executor in matching_executors:
                try:
                    message = (
                        f"🆕 Новая заявка #{task_id}\n\n"
                        f"📝 {task_name}\n"
                        f"🏪 Ресторан: {counterparty_name}\n"
                        f"📊 Статус: Новая\n\n"
                        f"Вы автоматически назначены исполнителем. Начните работу над задачей."
                    )
                    
                    await self.bot.send_message(
                        executor.telegram_id,
                        message
                    )
                    
                    notified_count += 1
                    logger.info(f"✅ Notification sent to executor {executor.telegram_id} for task {task_id}")
                    
                except Exception as send_err:
                    logger.error(
                        f"Failed to send notification to executor {executor.telegram_id} "
                        f"for task {task_id}: {send_err}"
                    )
            
            logger.info(
                f"✅ Auto-assigned and notified {notified_count} executor(s) about new task {task_id} "
                f"(total executors checked: {len(executors)}, matching: {len(matching_executors)})"
            )
            
        except Exception as e:
            logger.error(f"Error notifying executors about new task {task_id}: {e}", exc_info=True)

