"""
Обработчики команд для исполнителей (техников/ИТ-специалистов)
Версия: 1.0

"""

import logging
import asyncio
import time
import re
import json
from typing import Dict, List, Set
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.filters.state import StateFilter
from aiogram.types import Message, CallbackQuery, ContentType, InlineKeyboardButton, InlineKeyboardMarkup, BufferedInputFile, InputMediaPhoto, InputMediaDocument
from aiogram.fsm.context import FSMContext

from states import (
    ExecutorRegistration,
    ExecutorTaskManagement,
    AdminExecutorApproval,
    ExecutorProfileEdit,
)
from keyboards import (
    get_phone_number_keyboard,
    create_dynamic_keyboard,
    get_executor_main_menu_keyboard,
    get_task_actions_keyboard,
    get_skip_or_done_keyboard,
    get_executor_confirmation_keyboard,
    get_executor_profile_edit_keyboard,
    get_executor_direction_keyboard,
)
from services.db_service import db_manager
from planfix_client import planfix_client
from counterparty_helper import (
    normalize_counterparty_id,
    extract_counterparty_from_task,
    extract_contact_info,
    format_counterparty_display
)
from config import (
    PLANFIX_TASK_PROCESS_ID,
    FRANCHISE_GROUPS,
    CUSTOM_FIELD_CONTACT_ID,
    CUSTOM_FIELD_RESTAURANT_ID,
    TELEGRAM_ADMIN_IDS,
    get_template_info,
    PLANFIX_IT_TEMPLATES,
    PLANFIX_SE_TEMPLATES,
    PLANFIX_BASE_URL,
    PLANFIX_IT_TAG,
    PLANFIX_SE_TAG,
)
from services.status_registry import (
    StatusKey,
    collect_status_ids,
    ensure_status_registry_loaded,
    is_status,
    require_status_id,
    resolve_status_id,
    status_in,
    status_labels,
)

logger = logging.getLogger(__name__)
router = Router()

# Простой кэш для отслеживания последних проверенных комментариев для исполнителей
# Формат: {task_id: {executor_id: last_comment_id}}
_executor_last_checked_comments = {}

async def _check_comments_for_executor(task_id: int, executor_id: int, bot):
    """Проверяет новые комментарии для задачи и отправляет уведомления исполнителю."""
    try:
        # Получаем комментарии
        comments_response = await planfix_client.get_task_comments(
            task_id,
            fields="id,description,owner,dateTime",
            page_size=10
        )
        
        if not comments_response or comments_response.get('result') != 'success':
            return
        
        comments = comments_response.get('comments', [])
        if not comments:
            return
        
        # Получаем последний проверенный комментарий для этого исполнителя
        last_checked = _executor_last_checked_comments.get(task_id, {}).get(executor_id)
        
        # Сортируем комментарии (новые первыми)
        def get_sort_key(comment):
            dt = comment.get('dateTime', '')
            if isinstance(dt, dict):
                return str(dt.get('value', '')) if 'value' in dt else ''
            return str(dt) if dt else ''
        
        comments.sort(key=get_sort_key, reverse=True)
        
        # Находим новые комментарии
        new_comments = []
        for c in comments:
            cid = c.get('id')
            if isinstance(cid, str) and ':' in cid:
                try:
                    cid = int(cid.split(':')[-1])
                except ValueError:
                    continue
            elif not isinstance(cid, int):
                try:
                    cid = int(cid)
                except (ValueError, TypeError):
                    continue
            
            if last_checked is None or cid > last_checked:
                new_comments.append(c)
            else:
                break
        
        # Отправляем уведомления о новых комментариях
        if new_comments:
            from notifications import NotificationService
            notification_service = NotificationService(bot)
            
            for c in reversed(new_comments):  # отправляем в хронологическом порядке
                comment_id = c.get('id')
                comment_text = c.get('description', '')
                comment_author = (c.get('owner') or {}).get('name', 'Неизвестно')
                
                # Пропускаем комментарии от ботов
                if 'робот' in comment_author.lower() or 'bot' in comment_author.lower():
                    continue
                
                # Отправляем уведомление только исполнителям
                await notification_service.notify_new_comment(
                    task_id=task_id,
                    comment_author=comment_author,
                    comment_text=comment_text,
                    comment_id=comment_id,
                    recipients="executors"
                )
            
            # Обновляем последний проверенный комментарий
            latest_id = new_comments[0].get('id')
            if isinstance(latest_id, str) and ':' in latest_id:
                latest_id = int(latest_id.split(':')[-1])
            elif not isinstance(latest_id, int):
                latest_id = int(latest_id)
            
            if task_id not in _executor_last_checked_comments:
                _executor_last_checked_comments[task_id] = {}
            _executor_last_checked_comments[task_id][executor_id] = latest_id
    except Exception as e:
        logger.error(f"Error checking comments for task {task_id} (executor {executor_id}): {e}", exc_info=True)
# Простой in-memory TTL кэш (точечная вставка)
class TTLCache:
    def __init__(self):
        self._store = {}
    def get(self, key):
        item = self._store.get(key)
        if not item:
            return None
        value, exp = item
        if exp is not None and exp < time.time():
            try:
                del self._store[key]
            except Exception:
                pass
            return None
        return value
    def set(self, key, value, ttl_seconds: int = 60):
        exp = (time.time() + ttl_seconds) if ttl_seconds else None
        self._store[key] = (value, exp)

cache = TTLCache()

# Защита от множественных одновременных вызовов
_show_new_tasks_locks = {}  # {user_id: asyncio.Lock}

DIRECTION_LABELS = {
    "it": "ИТ служба",
    "se": "Служба эксплуатации",
}


def _format_direction(direction: str | None) -> str:
    if not direction:
        return "Не указано"
    return DIRECTION_LABELS.get(direction, "Не указано")


def _extract_restaurant_ids(data) -> List[int]:
    """Извлекает ID ресторанов из различных форматов данных."""
    ids: List[int] = []
    for item in data or []:
        if isinstance(item, int):
            ids.append(item)
        elif isinstance(item, str):
            # Если это строка, пытаемся преобразовать в число
            try:
                ids.append(int(item))
            except (TypeError, ValueError):
                continue
        elif isinstance(item, dict):
            val = item.get("id")
            try:
                # Обрабатываем как число, так и строку
                if isinstance(val, int):
                    ids.append(val)
                elif isinstance(val, str):
                    ids.append(int(val))
            except (TypeError, ValueError):
                continue
    return ids


def _normalize_pf_id(value) -> int | None:
    try:
        if isinstance(value, str) and ':' in value:
            value = value.split(':')[-1]
        return int(value)
    except (TypeError, ValueError):
        return None


def _get_allowed_tags(executor) -> Set[str]:
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
    """Извлекает множество тегов задачи в нижнем регистре."""
    tags_field = task.get('tags')
    names: Set[str] = set()
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
    return names


async def _load_restaurant_map(concept_ids: List[int]) -> Dict[int, str]:
    """Загружает карту ресторанов из групп концепций, исключая контакты из группы 'Поддержка'."""
    from config import SUPPORT_CONTACT_GROUP_ID
    restaurants_map: Dict[int, str] = {}
    support_group_id = SUPPORT_CONTACT_GROUP_ID
    
    # Множество контактов, которые находятся в группе "Поддержка" (для исключения)
    support_contact_ids: Set[int] = set()
    
    # Сначала получаем все контакты из группы "Поддержка", чтобы исключить их
    if support_group_id:
        try:
            support_contacts_response = await planfix_client.get_contact_list_by_group(
                support_group_id, fields="id", page_size=1000
            )
            if support_contacts_response and support_contacts_response.get('result') == 'success':
                for c in support_contacts_response.get('contacts', []):
                    try:
                        cid = int(c.get('id'))
                        support_contact_ids.add(cid)
                    except Exception:
                        continue
                logger.debug(f"Found {len(support_contact_ids)} contacts in support group {support_group_id}")
        except Exception as e:
            logger.warning(f"Failed to load support group contacts for filtering: {e}")
    
    # Теперь загружаем контакты из групп концепций, исключая те, что в группе "Поддержка"
    for group_id in concept_ids:
        try:
            # Запрашиваем контакты из группы концепции
            contacts_response = await planfix_client.get_contact_list_by_group(
                group_id, fields="id,name", page_size=100
            )
            if contacts_response and contacts_response.get('result') == 'success':
                for c in contacts_response.get('contacts', []):
                    try:
                        cid = int(c.get('id'))
                    except Exception:
                        continue
                    
                    # Пропускаем контакты, которые находятся в группе "Поддержка"
                    if cid in support_contact_ids:
                        logger.debug(f"Skipping contact {cid} (in support group {support_group_id})")
                        continue
                    
                    name = (c.get('name') or f"Контакт {cid}").strip()
                    restaurants_map[cid] = name
            else:
                logger.warning(f"Failed to load contacts for group {group_id}")
        except Exception as e:
            logger.error(f"Error loading contacts for group {group_id}: {e}")
    return restaurants_map


def _format_restaurant_list(restaurants) -> str:
    names = []
    for item in restaurants or []:
        if isinstance(item, dict):
            name = item.get("name")
            if not name:
                rid = item.get("id")
                name = f"Ресторан #{rid}"
        else:
            name = f"Ресторан #{item}"
        if name:
            names.append(name)
    if not names:
        return "Не выбраны"
    if len(names) == 1:
        return names[0]
    return "\n".join(f"• {name}" for name in names)



# Помощник: надёжно получить имя контрагента задачи
async def resolve_counterparty_name(task: dict) -> str:
    """
    Оставлена только стратегия 2.7: восстановление названия контрагента через BotLog -> UserProfile -> restaurant_contact_id.
    Все остальные стратегии отключены по требованию.
    """
    try:
        task_id = task.get('id', 'unknown')
        logger.info(f"[Task #{task_id}] ===== START Resolving counterparty name (Strategy 2.7 only) =====")
        
        # Стратегия 2.7: восстановление через BotLog -> user -> restaurant_contact_id
        try:
            logger.info(f"[Task #{task_id}] Strategy 2.7: Attempting to resolve via BotLog mapping...")
            from database import BotLog, UserProfile
            with db_manager.get_db() as db:
                logs = db.query(BotLog).filter(BotLog.action == 'create_task').order_by(BotLog.timestamp.desc()).limit(500).all()
                matched_tg = None
                for log in logs:
                    try:
                        details = log.details or {}
                        if isinstance(details, str):
                            import json
                            try:
                                details = json.loads(details)
                            except Exception:
                                details = {}
                        log_task_id = details.get('task_id')
                        # Приводим к int для надёжного сравнения
                        if log_task_id is not None and int(str(log_task_id).split(':')[-1]) == int(task_id):
                            matched_tg = details.get('user_telegram_id') or log.telegram_id
                            break
                    except Exception:
                        continue
                if matched_tg:
                    user = db.query(UserProfile).filter(UserProfile.telegram_id == int(matched_tg)).first()
                    if user and user.restaurant_contact_id:
                        try:
                            cid = int(str(user.restaurant_contact_id).split(':')[-1])
                        except Exception:
                            cid = None
                        if cid:
                            resp = await planfix_client.get_contact_by_id(cid, fields="id,name,midName,lastName,isCompany")
                            if resp and resp.get('result') == 'success':
                                contact = resp.get('contact') or {}
                                contact_info = extract_contact_info(contact)
                                if contact_info.get('name') and contact_info['name'] != "Неизвестно":
                                    logger.info(f"[Task #{task_id}] ✅ Strategy 2.7 SUCCESS: Found name from user mapping: {contact_info['name']}")
                                    return contact_info['name']
                logger.info(f"[Task #{task_id}] ❌ Strategy 2.7 FAILED: No mapping found in BotLog/DB")
        except Exception as e:
            logger.error(f"[Task #{task_id}] ❌ Strategy 2.7 OUTER EXCEPTION: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[Task #{task.get('id', 'unknown')}] ❌ OUTER EXCEPTION: {e}", exc_info=True)
    
    # Если стратегия 2.7 не сработала
    logger.warning(f"[Task #{task.get('id', 'unknown')}] Strategy 2.7 did not resolve name. Returning 'Не указан'")
    return "Не указан"

async def resolve_project_name(task: dict) -> str:
    try:
        proj = task.get('project') or {}
        name = proj.get('name')
        if isinstance(name, str) and name.strip():
            return name.strip()
        raw_id = proj.get('id')
        pid = None
        if raw_id is not None:
            try:
                if isinstance(raw_id, str) and ':' in raw_id:
                    pid = int(str(raw_id).split(':')[-1])
                else:
                    pid = int(raw_id)
            except Exception:
                pid = None
        if pid:
            try:
                resp = await planfix_client.get_project_by_id(pid, fields="id,name")
                if resp and resp.get('result') == 'success':
                    project = resp.get('project') or {}
                    pname = project.get('name')
                    if isinstance(pname, str) and pname.strip():
                        return pname.strip()
            except Exception:
                pass
        if pid:
            return f"Проект #{pid}"
        
        # Доп. попытка: перезагрузить задачу с полем project и вернуть имя
        try:
            await asyncio.sleep(0.8)
            trp = await planfix_client.get_task_by_id(task.get('id'), fields="id,project.id,project.name")
            if trp and trp.get('result') == 'success':
                t3 = trp.get('task') or {}
                pj = t3.get('project') or {}
                pname = pj.get('name')
                if isinstance(pname, str) and pname.strip():
                    return pname.strip()
                pid2_raw = pj.get('id')
                pid2 = None
                if pid2_raw is not None:
                    try:
                        if isinstance(pid2_raw, str) and ':' in pid2_raw:
                            pid2 = int(str(pid2_raw).split(':')[-1])
                        else:
                            pid2 = int(pid2_raw)
                    except Exception:
                        pid2 = None
                if pid2:
                    try:
                        resp = await planfix_client.get_project_by_id(pid2, fields="id,name")
                        if resp and resp.get('result') == 'success':
                            project = resp.get('project') or {}
                            pname = project.get('name')
                            if isinstance(pname, str) and pname.strip():
                                return pname.strip()
                    except Exception:
                        pass
                    return f"Проект #{pid2}"
        except Exception:
            pass
        
        # Фолбэк через BotLog: восстановить project_id по task_id из лога создания
        try:
            from database import BotLog
            with db_manager.get_db() as db:
                logs = db.query(BotLog).filter(BotLog.action == 'create_task').order_by(BotLog.timestamp.desc()).limit(500).all()
                t_id = task.get('id')
                found_pid = None
                for log in logs:
                    try:
                        details = log.details or {}
                        if isinstance(details, str):
                            import json
                            try:
                                details = json.loads(details)
                            except Exception:
                                details = {}
                        log_tid = details.get('task_id')
                        if log_tid is not None and int(str(log_tid).split(':')[-1]) == int(t_id):
                            found_pid = details.get('project_id')
                            break
                    except Exception:
                        continue
            if found_pid:
                try:
                    proj_resp = await planfix_client.get_project_by_id(int(found_pid), fields="id,name")
                    if proj_resp and proj_resp.get('result') == 'success':
                        p = proj_resp.get('project') or {}
                        pname = p.get('name')
                        if isinstance(pname, str) and pname.strip():
                            return pname.strip()
                except Exception:
                    return f"Проект #{found_pid}"
        except Exception:
            pass
        
        # Фолбэк: определяем проект по шаблону задачи
        try:
            tpl = task.get('template') or {}
            tpl_raw = tpl.get('id') if isinstance(tpl, dict) else None
            tpl_id = None
            if not tpl_raw:
                tr = await planfix_client.get_task_by_id(task.get('id'), fields="id,template.id")
                if tr and tr.get('result') == 'success':
                    t2 = tr.get('task') or {}
                    tpl_raw = ((t2.get('template') or {}).get('id')) if isinstance(t2, dict) else None
            if tpl_raw is not None:
                if isinstance(tpl_raw, str) and ':' in tpl_raw:
                    try:
                        tpl_id = int(str(tpl_raw).split(':')[-1])
                    except Exception:
                        tpl_id = None
                else:
                    try:
                        tpl_id = int(tpl_raw)
                    except Exception:
                        tpl_id = None
            if tpl_id:
                tpl_info = PLANFIX_IT_TEMPLATES.get(tpl_id) or PLANFIX_SE_TEMPLATES.get(tpl_id)
                cfg_pid = tpl_info.get('project_id') if tpl_info else None
                if cfg_pid:
                    try:
                        resp = await planfix_client.get_project_by_id(int(cfg_pid), fields="id,name")
                        if resp and resp.get('result') == 'success':
                            p = resp.get('project') or {}
                            pname = p.get('name')
                            if isinstance(pname, str) and pname.strip():
                                return pname.strip()
                    except Exception:
                        return f"Проект #{cfg_pid}"
        except Exception:
            pass
    except Exception:
        pass
    return "Не указан"

# ============================================================================
# РЕГИСТРАЦИЯ ИСПОЛНИТЕЛЯ
# ============================================================================

@router.message(Command("register_executor"))
async def cmd_register_executor(message: Message, state: FSMContext):
    """Команда для начала регистрации исполнителя."""
    executor = await db_manager.get_executor_profile(message.from_user.id)
    
    if executor:
        if executor.profile_status == "активен":
            await message.answer(
                f"✅ Вы уже зарегистрированы как исполнитель.\n\n"
                f"Статус: {executor.profile_status}",
                reply_markup=get_executor_main_menu_keyboard()
            )
        elif executor.profile_status == "ожидает подтверждения":
            await message.answer(
                "⏳ Ваша заявка на регистрацию уже отправлена администратору.\n\n"
                "Ожидайте подтверждения."
            )
        else:
            await message.answer(
                f"❌ Ваша регистрация была отклонена.\n\n"
                f"Статус: {executor.profile_status}\n\n"
                "Обратитесь к администратору для уточнения."
            )
        return
    
    await message.answer(
        "👷 Регистрация исполнителя техподдержки\n\n"
        "Введите ваше ФИО:"
    )
    await state.set_state(ExecutorRegistration.waiting_for_full_name)


@router.message(ExecutorRegistration.waiting_for_full_name)
async def executor_process_full_name(message: Message, state: FSMContext):
    """Обработка ввода ФИО исполнителя."""
    full_name = message.text.strip()
    
    if len(full_name) < 3:
        await message.answer("❌ ФИО слишком короткое. Введите полное ФИО:")
        return
    
    await state.update_data(full_name=full_name)
    await message.answer(
        "📱 Отлично! Теперь поделитесь вашим номером телефона.\n\n"
        "Вы можете нажать кнопку ниже или ввести номер вручную:",
        reply_markup=get_phone_number_keyboard()
    )
    await state.set_state(ExecutorRegistration.waiting_for_phone_number)


@router.message(ExecutorRegistration.waiting_for_phone_number, F.contact)
async def executor_process_phone_contact(message: Message, state: FSMContext):
    """Обработка номера телефона через кнопку."""
    phone_number = message.contact.phone_number
    await state.update_data(phone_number=phone_number)
    await executor_ask_position(message, state)


@router.message(ExecutorRegistration.waiting_for_phone_number, F.text)
async def executor_process_phone_text(message: Message, state: FSMContext):
    """Обработка номера телефона введенного вручную."""
    import re
    phone_text = message.text.strip()
    
    normalized = re.sub(r"[^0-9+]", "", phone_text)
    if not normalized or len(re.sub(r"\D", "", normalized)) < 10:
        await message.answer(
            "❌ Некорректный номер телефона.\n\n"
            "Введите номер в формате +79991234567 или используйте кнопку:",
            reply_markup=get_phone_number_keyboard()
        )
        return
    
    await state.update_data(phone_number=normalized)
    await executor_ask_position(message, state)


async def executor_ask_position(message: Message, state: FSMContext):
    """Запрос должности исполнителя."""
    await message.answer(
        "💼 Укажите вашу должность/роль:\n\n"
        "Например: 'ИТ-инженер', 'Техник СЭ', 'Электрик'\n\n"
        "Или нажмите 'Пропустить', если не хотите указывать:",
        reply_markup=get_skip_or_done_keyboard()
    )
    await state.set_state(ExecutorRegistration.waiting_for_position)


@router.message(ExecutorRegistration.waiting_for_position)
async def executor_process_position(message: Message, state: FSMContext):
    """Обработка должности исполнителя."""
    raw_text = (message.text or "").strip()
    position = raw_text if raw_text and raw_text.lower() != "пропустить" else None
    await state.update_data(position_role=position)
    await executor_ask_direction(message, state)


@router.callback_query(ExecutorRegistration.waiting_for_position, F.data == "skip_file")
async def executor_skip_position(callback_query: CallbackQuery, state: FSMContext):
    """Пропуск указания должности."""
    await state.update_data(position_role=None)
    await callback_query.answer()
    await executor_ask_direction(callback_query.message, state)


async def executor_ask_direction(target_message: Message, state: FSMContext):
    """Запрашивает направление работы исполнителя."""
    await target_message.answer(
        "🧭 Выберите направление, в котором вы будете работать:",
        reply_markup=get_executor_direction_keyboard(prefix="reg_dir")
    )
    await state.set_state(ExecutorRegistration.waiting_for_direction)


@router.callback_query(ExecutorRegistration.waiting_for_direction, F.data.startswith("reg_dir:"))
async def executor_process_direction(callback_query: CallbackQuery, state: FSMContext):
    """Сохранение выбранного направления."""
    direction = callback_query.data.split(":")[1]
    if direction not in ("it", "se"):
        await callback_query.answer("Недопустимое направление", show_alert=True)
        return
    await state.update_data(service_direction=direction)
    await callback_query.answer(f"Вы выбрали {DIRECTION_LABELS[direction]}")
    await executor_show_concepts(callback_query.message, state)


@router.message(ExecutorRegistration.waiting_for_direction)
async def executor_direction_text(message: Message):
    await message.answer("Пожалуйста, выберите направление, используя кнопки ниже.")


async def executor_show_concepts(message: Message, state: FSMContext):
    """Показ списка концепций для выбора."""
    # Создаем клавиатуру с концепциями
    keyboard_items = [
        (str(group_id), group_data["name"])
        for group_id, group_data in FRANCHISE_GROUPS.items()
    ]
    keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=False)
    
    await message.answer(
        "🏢 Выберите концепции, за которые вы отвечаете:\n\n"
        "Вы можете выбрать несколько концепций.\n"
        "После выбора всех нужных концепций нажмите 'Готово'.",
        reply_markup=keyboard
    )
    await state.update_data(selected_concepts=[])
    await state.set_state(ExecutorRegistration.waiting_for_concepts)


@router.callback_query(ExecutorRegistration.waiting_for_concepts)
async def executor_process_concept(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора концепции."""
    # Проверяем, не нажата ли кнопка "Готово"
    if callback_query.data == "done":
        await executor_show_restaurants(callback_query, state)
        return
    
    concept_id = int(callback_query.data)
    user_data = await state.get_data()
    selected_concepts = user_data.get('selected_concepts', [])
    
    if concept_id in selected_concepts:
        selected_concepts.remove(concept_id)
        action = "убрана"
    else:
        selected_concepts.append(concept_id)
        action = "добавлена"
    
    await state.update_data(selected_concepts=selected_concepts)
    
    concept_name = FRANCHISE_GROUPS[concept_id]["name"]
    
    # Обновляем сообщение с текущим выбором
    selected_names = [FRANCHISE_GROUPS[cid]["name"] for cid in selected_concepts]
    
    keyboard_items = [
        (str(group_id), f"{'✅ ' if group_id in selected_concepts else ''}{group_data['name']}")
        for group_id, group_data in FRANCHISE_GROUPS.items()
    ]
    
    # Добавляем кнопку "Готово" если выбрана хотя бы одна концепция
    if selected_concepts:
        keyboard_items.append(("done", "✅ Готово"))
    
    keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=False)
    
    await callback_query.message.edit_text(
        f"🏢 Концепция '{concept_name}' {action}.\n\n"
        f"Выбрано концепций: {len(selected_concepts)}\n"
        f"{'📋 ' + ', '.join(selected_names) if selected_names else ''}\n\n"
        "Выберите ещё или нажмите 'Готово':",
        reply_markup=keyboard
    )
    await callback_query.answer(f"Концепция {action}")



async def executor_show_restaurants(callback_query: CallbackQuery, state: FSMContext):
    """Показ списка ресторанов по выбранным концепциям."""
    user_data = await state.get_data()
    selected_concepts = user_data.get('selected_concepts', [])
    if not selected_concepts:
        await callback_query.answer("❌ Сначала выберите концепции", show_alert=True)
        return
    try:
        restaurants_map = await _load_restaurant_map(selected_concepts)
        if not restaurants_map:
            await callback_query.answer("❌ Не удалось загрузить рестораны", show_alert=True)
            return

        keyboard_items = [(str(cid), name) for cid, name in sorted(restaurants_map.items(), key=lambda x: x[1])]
        keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=False)

        await state.update_data(available_restaurants=restaurants_map, selected_restaurants=[])

        await callback_query.message.edit_text(
            "🏪 Выберите рестораны, за которые вы отвечаете:\n\n"
            "Можно выбрать несколько. После выбора нажмите 'Готово'.",
            reply_markup=keyboard
        )
        await state.set_state(ExecutorRegistration.waiting_for_restaurants)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error preparing restaurants list: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка загрузки ресторанов", show_alert=True)

@router.callback_query(ExecutorRegistration.waiting_for_restaurants)
async def executor_process_restaurant(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора ресторана."""
    user_data = await state.get_data()
    if callback_query.data == "done":
        await executor_finalize_registration(callback_query, state)
        return
    try:
        restaurant_id = int(callback_query.data)
    except ValueError:
        await callback_query.answer()
        return

    selected_restaurants = user_data.get('selected_restaurants', [])
    available_restaurants = user_data.get('available_restaurants', {}) or {}

    if restaurant_id in selected_restaurants:
        selected_restaurants.remove(restaurant_id)
        action = "убран"
    else:
        selected_restaurants.append(restaurant_id)
        action = "добавлен"

    await state.update_data(selected_restaurants=selected_restaurants)

    keyboard_items = []
    for cid, name in sorted(available_restaurants.items(), key=lambda x: x[1]):
        prefix = "✅ " if cid in selected_restaurants else ""
        keyboard_items.append((str(cid), f"{prefix}{name}"))
    if selected_restaurants:
        keyboard_items.append(("done", "✅ Готово"))
    keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=False)

    selected_names = []
    for rid in selected_restaurants:
        nm = available_restaurants.get(rid) or available_restaurants.get(str(rid))
        if nm:
            selected_names.append(nm)

    await callback_query.message.edit_text(
        f"🏪 Ресторан {action}.\n\n"
        f"Выбрано ресторанов: {len(selected_restaurants)}\n"
        f"{'📋 ' + ', '.join(selected_names) if selected_names else ''}\n\n"
        "Выберите ещё или нажмите 'Готово':",
        reply_markup=keyboard
    )
    await callback_query.answer(f"Ресторан {action}")

# Финализация после выбора ресторанов
async def executor_finalize_registration(callback_query: CallbackQuery, state: FSMContext):
    """Завершение регистрации исполнителя."""
    user_data = await state.get_data()
    selected_concepts = user_data.get('selected_concepts', [])
    selected_restaurants = user_data.get('selected_restaurants', [])
    direction = user_data.get('service_direction')
    
    if not selected_concepts:
        await callback_query.answer("❌ Выберите хотя бы одну концепцию!", show_alert=True)
        return
    if not direction:
        await callback_query.answer("❌ Выберите направление работы.", show_alert=True)
        return
    if not selected_restaurants:
        await callback_query.answer("❌ Выберите хотя бы один ресторан!", show_alert=True)
        return
    
    try:
        avail = (user_data.get('available_restaurants') or {})
        serving_restaurants_payload = []
        restaurant_names = []
        for rid in selected_restaurants:
            name = avail.get(rid) or avail.get(str(rid)) or f"Ресторан #{rid}"
            serving_restaurants_payload.append({"id": rid, "name": name})
            restaurant_names.append(name)

        # Создаем контакт исполнителя в Planfix
        planfix_contact_id = None
        planfix_user_id = None  # Инициализируем переменную
        try:
            # Создаем контакт исполнителя в группе "Поддержка" с template_id
            from config import SUPPORT_CONTACT_GROUP_ID, SUPPORT_CONTACT_TEMPLATE_ID
            
            # Передаем полное имя, чтобы метод create_contact сам правильно разделил ФИО
            # Это избежит конфликтов с логикой разделения внутри метода
            logger.info(f"Creating Planfix contact for executor {callback_query.from_user.id} with name: {user_data['full_name']}")
            # Получаем Telegram username если есть
            telegram_username = callback_query.from_user.username
            telegram_id = str(callback_query.from_user.id) if callback_query.from_user.id else None
            
            logger.info(f"Telegram data for contact: username={telegram_username}, telegram_id={telegram_id}")
            
            contact_response = await planfix_client.create_contact(
                name=user_data['full_name'],  # Передаем полное имя, метод сам разделит
                phone=user_data['phone_number'],
                email=user_data.get('email'),
                group_id=SUPPORT_CONTACT_GROUP_ID,  # Группа "Поддержка"
                template_id=SUPPORT_CONTACT_TEMPLATE_ID,  # Template ID 1
                position=user_data.get('position_role'),  # Должность исполнителя
                telegram=telegram_username,  # Telegram username (если есть) - будет преобразован в https://t.me/username
                telegram_id=telegram_id  # Telegram ID
            )
            
            if contact_response and contact_response.get('result') == 'success':
                contact_id = contact_response.get('id') or contact_response.get('contact', {}).get('id')
                if contact_id:
                    # Нормализуем ID контакта
                    if isinstance(contact_id, str) and ':' in contact_id:
                        planfix_contact_id = contact_id.split(':')[-1]
                    else:
                        planfix_contact_id = str(contact_id)
                    logger.info(f"Created Planfix contact {planfix_contact_id} for executor {callback_query.from_user.id}")
                    
                    # Используем ID контакта как planfix_user_id
                    planfix_user_id = planfix_contact_id
                    logger.info(f"Using contact_id {planfix_contact_id} as planfix_user_id")
            else:
                logger.warning(f"Failed to create Planfix contact for executor {callback_query.from_user.id}: {contact_response}")
        except Exception as e:
            logger.error(f"Error creating Planfix contact for executor {callback_query.from_user.id}: {e}", exc_info=True)
            # Продолжаем регистрацию даже если не удалось создать контакт
        
        # Проверяем, существует ли уже профиль исполнителя
        existing_executor = await db_manager.get_executor_profile(callback_query.from_user.id)
        
        if existing_executor:
            # Обновляем существующий профиль
            logger.info(f"Updating existing executor profile for {callback_query.from_user.id}")
            executor = await db_manager.update_executor_profile(
                callback_query.from_user.id,
                full_name=user_data['full_name'],
                phone_number=user_data['phone_number'],
                serving_franchise_groups=selected_concepts,
                position_role=user_data.get('position_role'),
                profile_status="ожидает подтверждения",
                serving_restaurants=serving_restaurants_payload,
                service_direction=direction,
                planfix_user_id=planfix_user_id if planfix_user_id else None  # Сохраняем ID контакта как planfix_user_id
            )
        else:
            # Создаем новый профиль
            executor = await db_manager.create_executor_profile(
                telegram_id=callback_query.from_user.id,
                full_name=user_data['full_name'],
                phone_number=user_data['phone_number'],
                serving_franchise_groups=selected_concepts,
                position_role=user_data.get('position_role'),
                profile_status="ожидает подтверждения",
                serving_restaurants=serving_restaurants_payload,
                service_direction=direction,
                planfix_user_id=planfix_user_id if planfix_user_id else None  # Сохраняем ID контакта как planfix_user_id
            )
        
        # Создаем задачу в Planfix для подтверждения регистрации (по ТЗ)
        concept_names = [FRANCHISE_GROUPS[cid]["name"] for cid in selected_concepts]
        
        try:
            # Формируем описание задачи (используем \n вместо реальных переводов строк)
            task_description = (
                f"🆕 Заявка на регистрацию исполнителя техподдержки\n\n"
                f"👤 ФИО: {user_data['full_name']}\n"
                f"📱 Телефон: {user_data['phone_number']}\n"
                f"💼 Должность: {user_data.get('position_role', 'Не указана')}\n"
                f"🏢 Концепции: {', '.join(concept_names)}\n"
                f"🧭 Направление: {DIRECTION_LABELS.get(direction, direction)}\n"
                f"🏪 Рестораны: {', '.join(restaurant_names) if restaurant_names else 'Не выбраны'}\n"
                f"🆔 Telegram ID: {callback_query.from_user.id}\n\n"
                f"Действия администратора:\n"
                f"✅ Завершить задачу - подтвердить регистрацию\n"
                f"❌ Отменить задачу - отклонить регистрацию\n\n"
                f"После выполнения действия бот автоматически обновит статус исполнителя."
            )
            
            # Получаем начальный статус из процесса для быстрого получения подтверждения
            initial_status_id = None
            if PLANFIX_TASK_PROCESS_ID:
                try:
                    # Убеждаемся, что status registry загружен
                    await ensure_status_registry_loaded()
                    # Получаем ID статуса "Новая" из процесса
                    from services.status_registry import get_status_id
                    initial_status_id = get_status_id(StatusKey.NEW, required=False)
                    if initial_status_id:
                        logger.info(f"Using initial status {initial_status_id} (NEW) from process {PLANFIX_TASK_PROCESS_ID}")
                    else:
                        logger.warning(f"Could not get NEW status ID from process {PLANFIX_TASK_PROCESS_ID}, creating task without explicit status")
                except Exception as e:
                    logger.warning(f"Error getting initial status from process: {e}, creating task without explicit status")
            
            # Создаем задачу в Planfix с процессом и начальным статусом
            # Согласно swagger.json, processId - это просто число, а не объект
            create_task_kwargs = {
                "name": f"Регистрация исполнителя: {user_data['full_name']}",
                "description": task_description,
                "template_id": None,  # Используем стандартный шаблон
                "project_id": None,   # Используем проект по умолчанию
                "counterparty_id": None,  # Без контрагента
                "custom_field_data": None,
                "assignee_users": [2],
                "files": None,
            }
            
            # Добавляем process_id если он настроен
            if PLANFIX_TASK_PROCESS_ID:
                create_task_kwargs["process_id"] = PLANFIX_TASK_PROCESS_ID
                logger.info(f"Creating task with process_id={PLANFIX_TASK_PROCESS_ID}")
            
            # Добавляем status_id только если он получен
            if initial_status_id:
                create_task_kwargs["status_id"] = initial_status_id
                logger.info(f"Creating task with status_id={initial_status_id}")
            else:
                logger.info("Creating task without explicit status_id (Planfix will set default)")
            
            task_response = await planfix_client.create_task(**create_task_kwargs)
            
            if task_response and task_response.get('result') == 'success':
                # В Planfix есть два типа ID: id (внутренний) и generalId (общий)
                # create_task возвращает generalId, но в webhook приходит id
                # Нужно получить оба ID, чтобы правильно искать задачу
                task_data = task_response.get('task', {}) if 'task' in task_response else task_response
                general_id = task_data.get('id') or task_response.get('id')  # create_task возвращает generalId в поле id
                
                # Получаем внутренний id задачи, запросив её по generalId
                task_id = None
                if general_id:
                    try:
                        # Запрашиваем задачу по generalId, чтобы получить внутренний id
                        # Нужно запросить и id, и generalId, чтобы различить их
                        task_info = await planfix_client.get_task_by_id(
                            general_id,
                            fields="id,generalId"
                        )
                        if task_info and task_info.get('result') == 'success':
                            task_info_data = task_info.get('task', {})
                            # API может вернуть generalId в поле id, если запрашиваем по generalId
                            # Поэтому проверяем: если id == generalId, значит это generalId, а не внутренний id
                            returned_id = task_info_data.get('id')
                            returned_general_id = task_info_data.get('generalId')
                            
                            # Если id совпадает с generalId, значит API вернул generalId в поле id
                            # В этом случае нужно использовать id из webhook или запросить по-другому
                            if returned_id and str(returned_id) == str(general_id):
                                logger.warning(f"API returned generalId ({returned_id}) in id field for generalId={general_id}. Will use webhook id when available.")
                                # Не используем этот id, т.к. это generalId, а не внутренний id
                                task_id = None
                            else:
                                task_id = returned_id
                                logger.info(f"Got task id={task_id} (generalId={returned_general_id}) for generalId={general_id}")
                    except Exception as e:
                        logger.warning(f"Failed to get task id for generalId={general_id}: {e}")
                
                # Используем task_id если получили, иначе general_id (для обратной совместимости)
                # В webhook приходит id, поэтому предпочитаем его
                registration_task_id = task_id if task_id else general_id
                
                logger.info(f"Created Planfix task generalId={general_id}, id={task_id}, saving registration_task_id={registration_task_id} for executor registration {callback_query.from_user.id}")
                
                # Сохраняем ID задачи в профиле исполнителя (обновляем существующий профиль)
                # Сохраняем внутренний id, так как в webhook приходит именно он
                await db_manager.update_executor_profile(
                    callback_query.from_user.id,
                    registration_task_id=registration_task_id
                )
                logger.info(f"Saved registration_task_id={registration_task_id} to executor profile {callback_query.from_user.id}")
                
                # Добавляем задачу в отслеживание polling сервиса
                # Это нужно для автоматического отслеживания изменений статуса
                logger.info(f"Registration task {registration_task_id} added to polling tracking for executor {callback_query.from_user.id}")
                
                # Отправляем ув��домление администраторам
                admin_message = (
                    f"🆕 Создана задача в Planfix для подтверждения регистрации исполнителя:\n\n"
                    f"📋 Задача #{general_id if general_id else registration_task_id}\n"
                    f"👤 ФИО: {user_data['full_name']}\n"
                    f"🏢 Концепции: {', '.join(concept_names)}\n"
                    f"🏪 Рестораны: {', '.join(restaurant_names) if restaurant_names else 'Не выбраны'}\n\n"
                    f"Перейдите в Planfix для подтверждения или отмены регистрации."
                )
                
                try:
                    for admin_id in TELEGRAM_ADMIN_IDS:
                        await callback_query.bot.send_message(admin_id, admin_message)
                except Exception as send_e:
                    logger.error(f"Failed to send success notification to admins: {send_e}", exc_info=True)
                        
            else:
                logger.error(f"Failed to create Planfix task for executor registration. Response: {task_response}")
                # Fallback - отправляем уведомление через Telegram
                admin_message = (
                    f"🆕 Новая заявка на регистрацию исполнителя:\n\n"
                    f"👤 ФИО: {user_data['full_name']}\n"
                    f"📱 Телефон: {user_data['phone_number']}\n"
                    f"💼 Должность: {user_data.get('position_role', 'Не указана')}\n"
                    f"🏢 Концепции: {', '.join(concept_names)}\n"
                    f"🏪 Рестораны: {', '.join(restaurant_names) if restaurant_names else 'Не выбраны'}\n"
                    f"🆔 Telegram ID: {callback_query.from_user.id}\n\n"
                    f"⚠️ Не удалось создать задачу в Planfix. Используйте команды:\n"
                    f"/approve_executor {callback_query.from_user.id}\n"
                    f"/reject_executor {callback_query.from_user.id}"
                )
                
                for admin_id in TELEGRAM_ADMIN_IDS:
                    try:
                        await callback_query.bot.send_message(
                            admin_id,
                            admin_message,
                            reply_markup=get_executor_confirmation_keyboard(callback_query.from_user.id)
                        )
                    except Exception as e:
                        logger.error(f"Failed to send notification to admin {admin_id}: {e}")
                        
        except Exception as e:
            logger.error(f"Error creating Planfix task for executor registration: {e}", exc_info=True)
            # Fallback - отправляем уведомление через Telegram
            admin_message = (
                f"🆕 Новая заявка на регистрацию исполнителя:\n\n"
                f"👤 ФИО: {user_data['full_name']}\n"
                f"📱 Телефон: {user_data['phone_number']}\n"
                f"💼 Должность: {user_data.get('position_role', 'Не указана')}\n"
                f"🏢 Концепции: {', '.join(concept_names)}\n"
                f"🏪 Рестораны: {', '.join(restaurant_names) if restaurant_names else 'Не выбраны'}\n"
                f"🆔 Telegram ID: {callback_query.from_user.id}\n\n"
                f"⚠️ Ошибка создания задачи в Planfix. Используйте команды:\n"
                f"/approve_executor {callback_query.from_user.id}\n"
                f"/reject_executor {callback_query.from_user.id}"
            )
            
            for admin_id in TELEGRAM_ADMIN_IDS:
                try:
                    await callback_query.bot.send_message(
                        admin_id,
                        admin_message,
                        reply_markup=get_executor_confirmation_keyboard(callback_query.from_user.id)
                    )
                except Exception as e:
                    logger.error(f"Failed to send notification to admin {admin_id}: {e}")
        
        await state.clear()
        await callback_query.message.edit_text(
            "✅ Заявка на регистрацию отправлена!\n\n"
            f"👤 {user_data['full_name']}\n"
            f"📱 {user_data['phone_number']}\n"
            f"🏢 Концепции: {', '.join(concept_names)}\n"
            f"🏪 Рестораны: {', '.join(restaurant_names) if restaurant_names else 'Не выбраны'}\n\n"
            "⏳ Ожидайте подтверждения от администратора.\n"
            "Вы получите уведомление, когда ваша заявка будет рассмотрена."
        )
        
        logger.info(f"Executor registration request created for user {callback_query.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error during executor registration: {e}", exc_info=True)
        await callback_query.message.edit_text(
            "❌ Произошла ошибка при регистрации. Попробуйте позже."
        )
        await state.clear()


# ============================================================================
# ПОДТВЕРЖДЕНИЕ/ОТКЛОНЕНИЕ РЕГИСТРАЦИИ (АДМИН)
# ============================================================================

@router.message(Command("approve_executor"))
async def cmd_approve_executor(message: Message, state: FSMContext):
    """Команда для подтверждения регистрации исполнителя (только для админов)."""
    if message.from_user.id not in TELEGRAM_ADMIN_IDS:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return
    
    try:
        executor_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("❌ Использование: /approve_executor <telegram_id>")
        return
    
    executor = await db_manager.get_executor_profile(executor_id)
    
    if not executor:
        await message.answer(f"❌ Исполнитель с ID {executor_id} не найден.")
        return
    
    if executor.profile_status == "активен":
        await message.answer("ℹ️ Исполнитель уже подтверждён.")
        return
    
    # Запрашиваем Planfix User ID
    concept_names = [FRANCHISE_GROUPS[cid]["name"] for cid in executor.serving_franchise_groups]
    await message.answer(
        f"👤 Подтверждение исполнителя:\n\n"
        f"ФИО: {executor.full_name}\n"
        f"Телефон: {executor.phone_number}\n"
        f"Концепции: {', '.join(concept_names)}\n\n"
        f"📝 Введите Planfix User ID для этого исполнителя:\n\n"
        f"💡 Найти User ID можно в Planfix в профиле пользователя."
    )
    await state.update_data(executor_id_to_approve=executor_id)
    await state.set_state(AdminExecutorApproval.waiting_for_planfix_user_id)


@router.message(AdminExecutorApproval.waiting_for_planfix_user_id)
async def process_planfix_user_id(message: Message, state: FSMContext):
    """Обработка ввода Planfix User ID администратором."""
    if message.from_user.id not in TELEGRAM_ADMIN_IDS:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        await state.clear()
        return
    
    planfix_user_id = message.text.strip()
    
    # Проверяем, что введено число
    if not planfix_user_id.isdigit():
        await message.answer(
            "❌ Planfix User ID должен быть числом.\n\n"
            "Попробуйте ещё раз или используйте /cancel для отмены."
        )
        return
    
    user_data = await state.get_data()
    executor_id = user_data.get('executor_id_to_approve')
    
    executor = await db_manager.get_executor_profile(executor_id)
    
    if not executor:
        await message.answer(f"❌ Исполнитель с ID {executor_id} не найден.")
        await state.clear()
        return
    
    await db_manager.update_executor_profile(
        executor_id,
        profile_status="активен",
        planfix_user_id=planfix_user_id
    )
    
    # Уведомляем исполнителя
    concept_names = [FRANCHISE_GROUPS[cid]["name"] for cid in executor.serving_franchise_groups]
    try:
        await message.bot.send_message(
            executor_id,
            f"✅ Ваша регистрация подтверждена!\n\n"
            f"Теперь вы будете получать заявки по концепциям:\n"
            f"🏢 {', '.join(concept_names)}\n\n"
            f"Ваш профиль связан с учётной записью Planfix.\n"
            f"Используйте меню для работы с заявками.",
            reply_markup=get_executor_main_menu_keyboard()
        )
        await message.answer(
            f"✅ Исполнитель {executor.full_name} (ID: {executor_id}) подтверждён!\n\n"
            f"Концепции: {', '.join(concept_names)}\n"
            f"Planfix User ID: {planfix_user_id}"
        )
    except Exception as e:
        logger.error(f"Failed to notify executor {executor_id}: {e}")
        await message.answer(
            f"✅ Исполнитель подтверждён, но не удалось отправить уведомление.\n\n"
            f"Planfix User ID: {planfix_user_id}"
        )
    
    await state.clear()
    logger.info(f"Executor {executor_id} approved with Planfix User ID {planfix_user_id}")


@router.message(Command("set_executor_planfix_id"))
async def cmd_set_executor_planfix_id(message: Message, state: FSMContext):
    """Команда для установки Planfix Contact ID существующему исполнителю (только для админов)."""
    if message.from_user.id not in TELEGRAM_ADMIN_IDS:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "❌ Использование: /set_executor_planfix_id <telegram_id>\n\n"
                "Пример: /set_executor_planfix_id 466085358"
            )
            return
        
        executor_id = int(parts[1])
    except (IndexError, ValueError):
        await message.answer("❌ Использование: /set_executor_planfix_id <telegram_id>")
        return
    
    executor = await db_manager.get_executor_profile(executor_id)
    
    if not executor:
        await message.answer(f"❌ Исполнитель с ID {executor_id} не найден.")
        return
    
    await message.answer(
        f"👤 Исполнитель: {executor.full_name}\n"
        f"📱 Telegram ID: {executor_id}\n"
        f"📋 Текущий Planfix Contact ID: {executor.planfix_user_id or 'не установлен'}\n"
        f"📋 Planfix Contact ID (из профиля): {executor.planfix_contact_id or 'не установлен'}\n\n"
        f"📝 Введите Planfix Contact ID (ID контакта в Planfix):\n\n"
        f"💡 Это должен быть ID контакта, созданного при регистрации исполнителя в Planfix."
    )
    await state.update_data(executor_id_to_update=executor_id)
    await state.set_state(AdminExecutorApproval.waiting_for_planfix_contact_id)


@router.message(AdminExecutorApproval.waiting_for_planfix_contact_id)
async def process_planfix_contact_id(message: Message, state: FSMContext):
    """Обработка ввода Planfix Contact ID администратором."""
    if message.from_user.id not in TELEGRAM_ADMIN_IDS:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        await state.clear()
        return
    
    planfix_contact_id = message.text.strip()
    
    # Проверяем, что введено число
    if not planfix_contact_id.isdigit():
        await message.answer(
            "❌ Planfix Contact ID должен быть числом.\n\n"
            "Попробуйте ещё раз или используйте /cancel для отмены."
        )
        return
    
    user_data = await state.get_data()
    executor_id = user_data.get('executor_id_to_update')
    
    executor = await db_manager.get_executor_profile(executor_id)
    
    if not executor:
        await message.answer(f"❌ Исполнитель с ID {executor_id} не найден.")
        await state.clear()
        return
    
    # Обновляем planfix_user_id (используем ID контакта)
    await db_manager.update_executor_profile(
        executor_id,
        planfix_user_id=planfix_contact_id,
        planfix_contact_id=planfix_contact_id
    )
    
    await message.answer(
        f"✅ Planfix Contact ID обновлён для исполнителя {executor.full_name} (ID: {executor_id})\n\n"
        f"📋 Planfix Contact ID: {planfix_contact_id}\n\n"
        f"Теперь исполнитель сможет использовать все функции бота."
    )
    
    # Уведомляем исполнителя
    try:
        await message.bot.send_message(
            executor_id,
            f"✅ Ваш профиль обновлён!\n\n"
            f"Теперь ваш профиль связан с контактом Planfix (ID: {planfix_contact_id}).\n"
            f"Вы можете использовать все функции бота."
        )
    except Exception as e:
        logger.error(f"Failed to notify executor {executor_id}: {e}")
    
    await state.clear()
    logger.info(f"Updated planfix_user_id for executor {executor_id} to {planfix_contact_id}")


@router.message(Command("reject_executor"))
async def cmd_reject_executor(message: Message):
    """Команда для отклонения регистрации исполнителя (только для админов)."""
    if message.from_user.id not in TELEGRAM_ADMIN_IDS:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return
    
    try:
        executor_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("❌ Использование: /reject_executor <telegram_id>")
        return
    
    executor = await db_manager.get_executor_profile(executor_id)
    
    if not executor:
        await message.answer(f"❌ Исполнитель с ID {executor_id} не найден.")
        return
    
    await db_manager.update_executor_profile(executor_id, profile_status="отклонён")
    
    
    try:
        await message.bot.send_message(
            executor_id,
            "❌ Ваша заявка на регистрацию отклонена.\n\n"
            "Обратитесь к администратору для уточнения причин."
        )
        await message.answer(f"✅ Исполнитель {executor.full_name} (ID: {executor_id}) отклонён.")
    except Exception as e:
        logger.error(f"Failed to notify executor {executor_id}: {e}")
        await message.answer(f"✅ Исполнитель отклонён, но не удалось отправить уведомление.")


@router.callback_query(F.data.startswith("confirm_executor:"))
async def callback_confirm_executor(callback_query: CallbackQuery, state: FSMContext):
    """Подтверждение через inline кнопку."""
    if callback_query.from_user.id not in TELEGRAM_ADMIN_IDS:
        await callback_query.answer("❌ У вас нет прав", show_alert=True)
        return
    
    executor_id = int(callback_query.data.split(":")[1])
    
    executor = await db_manager.get_executor_profile(executor_id)
    
    if not executor:
        await callback_query.answer("❌ Исполнитель не найден", show_alert=True)
        return
    
    if executor.profile_status == "активен":
        await callback_query.answer("ℹ️ Исполнитель уже подтверждён", show_alert=True)
        return
    
    # Запрашиваем Planfix User ID
    concept_names = [FRANCHISE_GROUPS[cid]["name"] for cid in executor.serving_franchise_groups]
    
    await callback_query.message.answer(
        f"👤 Подтверждение исполнителя:\n\n"
        f"ФИО: {executor.full_name}\n"
        f"Телефон: {executor.phone_number}\n"
        f"Концепции: {', '.join(concept_names)}\n\n"
        f"📝 Введите Planfix User ID для этого исполнителя:\n\n"
        f"💡 Найти User ID можно в Planfix в профиле пользователя."
    )
    
    await state.update_data(executor_id_to_approve=executor_id)
    await state.set_state(AdminExecutorApproval.waiting_for_planfix_user_id)
    await callback_query.answer("📝 Введите Planfix User ID")


@router.callback_query(F.data.startswith("reject_executor:"))
async def callback_reject_executor(callback_query: CallbackQuery):
    """Отклонение через inline кнопку."""
    if callback_query.from_user.id not in TELEGRAM_ADMIN_IDS:
        await callback_query.answer("❌ У вас нет прав", show_alert=True)
        return
    
    executor_id = int(callback_query.data.split(":")[1])
    
    executor = await db_manager.get_executor_profile(executor_id)
    
    if not executor:
        await callback_query.answer("❌ Исполнитель не найден", show_alert=True)
        return
    
    await db_manager.update_executor_profile(executor_id, profile_status="отклонён")
    
    try:
        await callback_query.bot.send_message(
            executor_id,
            "❌ Ваша заявка на регистрацию отклонена.\n\n"
            "Обратитесь к администратору."
        )
    except Exception as e:
        logger.error(f"Failed to notify executor: {e}")
    
    await callback_query.message.edit_text(
        f"❌ Исполнитель {executor.full_name} отклонён.\n"
        f"{callback_query.message.text}"
    )
    await callback_query.answer("❌ Отклонено")


# ============================================================================
# ПРОСМОТР НОВЫХ ЗАЯВОК
# ============================================================================

@router.message(F.text == "🆕 Новые заявки")
async def show_new_tasks(message: Message, state: FSMContext):
    """Показать список новых заявок для исполнителя."""
    logger.info(f"Handler 'show_new_tasks' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    user_id = message.from_user.id
    
    # Проверяем, что пользователь является исполнителем
    executor = await db_manager.get_executor_profile(user_id)
    if not executor or executor.profile_status != "активен":
        logger.warning(f"User {user_id} tried to access executor menu but is not an active executor")
        await message.answer(
            "❌ Вы не зарегистрированы как исполнитель или ваша заявка не подтверждена.\n\n"
            "Используйте /register_executor для регистрации."
        )
        return
    
    # Защита от множественных одновременных вызовов
    if user_id not in _show_new_tasks_locks:
        _show_new_tasks_locks[user_id] = asyncio.Lock()
    
    lock = _show_new_tasks_locks[user_id]
    
    # Проверяем, не выполняется ли уже запрос для этого пользователя
    if lock.locked():
        logger.warning(f"User {user_id} requested new tasks while previous request is still processing")
        await message.answer(
            "⏳ Загрузка заявок уже выполняется. Пожалуйста, подождите..."
        )
        return
    
    # Проверяем кэш (защита от слишком частых запросов)
    cache_key = f"new_tasks_request:{user_id}"
    last_request_time = cache.get(f"{cache_key}:time")
    if last_request_time and time.time() - last_request_time < 5:  # Минимум 5 секунд между запросами
        cached_result = cache.get(f"{cache_key}:result")
        if cached_result:
            logger.info(f"Returning cached result for user {user_id}")
            await message.answer(cached_result["text"], reply_markup=cached_result.get("kb"))
            return
    
    async with lock:
        try:

            await ensure_status_registry_loaded()
            
            allowed_templates = _get_allowed_template_ids(executor)
            allowed_restaurant_ids = set(_extract_restaurant_ids(executor.serving_restaurants))
            allowed_tags = _get_allowed_tags(executor)
            allowed_tag_names = {tag.lower() for tag in allowed_tags if isinstance(tag, str)}
            
            logger.info(
                f"🔍 Executor {executor.telegram_id} filters: "
                f"direction={executor.service_direction}, "
                f"allowed_templates={allowed_templates} (count: {len(allowed_templates)}), "
                f"allowed_restaurant_ids={allowed_restaurant_ids} (count: {len(allowed_restaurant_ids)}), "
                f"allowed_tag_names={allowed_tag_names} (count: {len(allowed_tag_names)}), "
                f"serving_restaurants={executor.serving_restaurants}"
            )

            # Показываем только задачи со статусом "Новая"
            working_status_ids = collect_status_ids(
                (StatusKey.NEW,),
                required=False,
            )
            if not working_status_ids:
                try:
                    working_status_ids = [require_status_id(StatusKey.NEW)]
                except Exception:
                    working_status_ids = []

            logger.info(
                f"📊 Executor {executor.telegram_id} will query tasks with status_ids: {working_status_ids} (count: {len(working_status_ids) if working_status_ids else 0})"
            )

            all_new_tasks = []
            seen_task_ids = set()

            # ОПТИМИЗАЦИЯ: Загружаем все task_id из BotLog один раз для быстрой проверки
            bot_task_ids_set = set()
            try:
                with db_manager.get_db() as db:
                    from database import BotLog
                    bot_logs = db.query(BotLog).filter(
                        BotLog.action == "create_task",
                        BotLog.success == True
                    ).order_by(BotLog.id.desc()).limit(1000).all()
                    
                    for log in bot_logs:
                        if log.details:
                            try:
                                task_id_candidates = [
                                    log.details.get('task_id'),
                                    log.details.get('task_id_internal'),
                                    log.details.get('task_id_general'),
                                ]
                                
                                for log_task_id in task_id_candidates:
                                    if log_task_id is None:
                                        continue
                                    
                                    try:
                                        if isinstance(log_task_id, int):
                                            bot_task_ids_set.add(log_task_id)
                                        elif isinstance(log_task_id, str):
                                            if ':' in log_task_id:
                                                bot_task_ids_set.add(int(log_task_id.split(':')[-1]))
                                            else:
                                                bot_task_ids_set.add(int(log_task_id))
                                    except (ValueError, TypeError):
                                        continue
                            except (ValueError, TypeError, AttributeError):
                                continue
                    
                    logger.info(f"Loaded {len(bot_task_ids_set)} bot task IDs from BotLog for fast lookup")
            except Exception as log_err:
                logger.warning(f"Error loading bot task IDs from BotLog: {log_err}")

            # Вычисляем дату 7 дней назад для фильтрации
            from datetime import datetime, timedelta
            seven_days_ago = datetime.now() - timedelta(days=7)

            def _parse_planfix_datetime(raw_value):
                """Парсит значение dateTime из Planfix в datetime (без таймзоны)."""
                if not raw_value:
                    return None
                value = None
                if isinstance(raw_value, dict):
                    value = raw_value.get("datetime") or raw_value.get("value") or raw_value.get("date")
                else:
                    value = str(raw_value)

                if not value:
                    return None

                normalized = value.replace("Z", "+00:00")
                try:
                    parsed = datetime.fromisoformat(normalized)
                    return parsed.replace(tzinfo=None)
                except ValueError:
                    pass

                try:
                    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
                        return datetime.strptime(value[:10], "%Y-%m-%d")
                except Exception:
                    pass

                try:
                    if len(value) >= 10 and value[2] == "-" and value[5] == "-":
                        return datetime.strptime(value[:10], "%d-%m-%Y")
                except Exception:
                    pass

                logger.debug(f"Unable to parse Planfix date value: {raw_value}")
                return None
            
            status_ids_to_query = working_status_ids if working_status_ids else []
            
            if not status_ids_to_query:
                logger.warning("No working status IDs found, will query tasks without status filter")
                status_ids_to_query = [None]

            page_size = 50
            max_pages_per_status = 1  # Оптимизация: запрашиваем только первую страницу для быстрой загрузки
            max_total_tasks = 50  # Максимальное количество задач для обработки

            for status_id in status_ids_to_query:
                offset = 0
                page_index = 0
                
                # Если уже набрали достаточно задач, прекращаем запросы
                if len(all_new_tasks) >= max_total_tasks:
                    logger.info(f"Reached max_total_tasks limit ({max_total_tasks}), stopping pagination")
                    break

                while page_index < max_pages_per_status:
                    # Проверяем лимит задач
                    if len(all_new_tasks) >= max_total_tasks:
                        break
                    filters = []

                    if status_id is not None:
                        filters.append(
                            {"type": 10, "operator": "equal", "value": status_id},  # type 10 = Task status (not type 3 = Task auditor)
                        )
                    
                    # Оптимизация: добавляем фильтр по дате на уровне API (только последние 7 дней)
                    # Формат даты для Planfix: "DD-MM-YYYY"
                    date_from = (seven_days_ago.strftime("%d-%m-%Y"))
                    filters.append({
                        "type": 13,  # type 13 = Start date filter
                        "operator": "gt",  # greater than (больше чем)
                        "value": {
                            "dateType": "otherDate",
                            "dateValue": date_from
                        }
                    })

                    logger.info(
                        f"Querying tasks with status_id={status_id}, offset={offset}, page_size={page_size}, date_from={date_from}"
                    )

                    # Проверяем кэш запроса к API
                    api_cache_key = f"api_tasks:{status_id}:{offset}:{page_size}:{date_from}"
                    cached_api_response = cache.get(api_cache_key)
                    if cached_api_response:
                        logger.debug(f"Using cached API response for {api_cache_key}")
                        tasks_response = cached_api_response
                    else:
                        tasks_response = await planfix_client.get_task_list(
                            filters=filters,
                            fields="id,name,description,status,template,counterparty,dateTime,tags,project",
                            page_size=page_size,
                            offset=offset,
                            result_order=[{"field": "dateTime", "direction": "Desc"}]
                        )
                        # Кэшируем результат API запроса на 30 секунд
                        if tasks_response and tasks_response.get('result') == 'success':
                            cache.set(api_cache_key, tasks_response, ttl_seconds=30)

                    # Детальное логирование ответа для диагностики
                    if tasks_response:
                        logger.debug(f"Tasks response for status {status_id}: result={tasks_response.get('result')}, keys={list(tasks_response.keys())}")
                        if tasks_response.get('result') == 'success':
                            logger.debug(f"Response structure: tasks key exists={('tasks' in tasks_response)}, tasks value type={type(tasks_response.get('tasks'))}")
                    else:
                        logger.warning(f"Empty tasks_response for status {status_id}")

                    if not tasks_response or tasks_response.get('result') != 'success':
                        logger.warning(f"Failed to load task list for status {status_id}: {tasks_response}")
                        break

                    tasks = tasks_response.get('tasks', []) or []
                    logger.info(
                        f"📥 Executor {executor.telegram_id} fetched {len(tasks)} tasks for status {status_id} (page {page_index + 1}), "
                        f"total tasks so far: {len(all_new_tasks)}"
                    )
                    
                    # Дополнительная диагностика если задач нет, но ответ успешный
                    if not tasks and tasks_response.get('result') == 'success':
                        logger.warning(f"API returned success but no tasks. Full response keys: {list(tasks_response.keys())}, response sample: {str(tasks_response)[:500]}")

                    if tasks:
                        task_ids = [t.get('id') for t in tasks]
                        logger.info(
                            f"All tasks fetched for status {status_id}, page {page_index + 1}: "
                            f"{task_ids[:20]}..." if len(task_ids) > 20 else
                            f"All tasks fetched for status {status_id}, page {page_index + 1}: {task_ids}"
                        )
                    else:
                        logger.debug(f"No tasks found for status {status_id} on page {page_index + 1}")
                        break

                    oldest_task_date_in_page = None

                    for task in tasks:
                        task_id = task.get('id')
                        if not task_id or task_id in seen_task_ids:
                            continue

                        template_id = _normalize_pf_id((task.get('template') or {}).get('id'))
                        counterparty_id = _normalize_pf_id((task.get('counterparty') or {}).get('id'))
                        task_status = task.get('status', {})
                        task_status_id = task_status.get('id') if isinstance(task_status, dict) else None
                        task_status_name = task_status.get('name') if isinstance(task_status, dict) else None
                        
                        # Нормализуем status_id
                        if isinstance(task_status_id, str) and ':' in str(task_status_id):
                            try:
                                task_status_id = int(str(task_status_id).split(':')[-1])
                            except Exception:
                                pass
                        elif isinstance(task_status_id, int):
                            pass  # Уже число
                        else:
                            task_status_id = None

                        # Фильтруем по статусу
                        if status_id is not None:
                            if task_status_id != status_id:
                                continue
                        elif working_status_ids:
                            if task_status_id not in working_status_ids:
                                continue
                        
                        # Дополнительная фильтрация по дате (последние 7 дней)
                        task_date = _parse_planfix_datetime(task.get('dateTime'))
                        if task_date:
                            if oldest_task_date_in_page is None or task_date < oldest_task_date_in_page:
                                oldest_task_date_in_page = task_date
                            if task_date < seven_days_ago:
                                logger.debug(f"Task {task_id} filtered out: date {task_date} is older than 7 days")
                                continue
                        else:
                            logger.debug(f"Task {task_id} has no parseable dateTime, skipping date filter")

                        task_tag_names = _extract_task_tags(task)
                        task_name = task.get('name', '')[:50]
                        task_desc = (task.get('description') or '')[:100]
                        
                        # БЫСТРАЯ ПРОВЕРКА: Проверяем, создана ли задача через бота (используем предзагруженный set)
                        is_bot_task_verified = task_id in bot_task_ids_set
                        
                        # Логируем только задачи, которые прошли проверку статуса и даты
                        logger.info(
                            f"Task {task_id} passed filters: template_id={template_id}, counterparty_id={counterparty_id}, "
                            f"status_id={task_status_id}, status_name={task_status_name}, "
                            f"tags={list(task_tag_names)}, name={task_name}, "
                            f"desc_preview={task_desc}, is_bot_task_verified={is_bot_task_verified}"
                        )

                        # Фильтр по шаблонам (только если у исполнителя есть ограничения)
                        # ВАЖНО: Фильтр применяется ВСЕГДА, даже для задач из BotLog
                        if allowed_templates:
                            if template_id is None or template_id not in allowed_templates:
                                logger.info(
                                    f"Task {task_id} filtered out by template filter: "
                                    f"template_id={template_id} not in allowed_templates={allowed_templates} "
                                    f"(is_bot_task={is_bot_task_verified})"
                                )
                                continue
                        else:
                            # Если allowed_templates пусто, значит исполнитель может видеть все шаблоны
                            logger.debug(f"Task {task_id} passed template filter (no restrictions)")

                        # Фильтр по ресторанам (только если у исполнителя есть ограничения)
                        if allowed_restaurant_ids:
                            if counterparty_id is None or counterparty_id not in allowed_restaurant_ids:
                                logger.info(
                                    f"Task {task_id} filtered out by restaurant filter: "
                                    f"counterparty_id={counterparty_id} not in allowed_restaurant_ids={allowed_restaurant_ids} "
                                    f"(executor has {len(allowed_restaurant_ids)} restaurants)"
                                )
                                continue
                        else:
                            # Если allowed_restaurant_ids пусто, значит исполнитель может видеть все рестораны
                            logger.debug(f"Task {task_id} passed restaurant filter (no restrictions)")

                        seen_task_ids.add(task_id)
                        
                        # Фильтр по тегам: если у исполнителя есть ограничения, задача ДОЛЖНА иметь соответствующий тег
                        if allowed_tag_names:
                            # Если у исполнителя есть ограничения по тегам, задача ОБЯЗАТЕЛЬНО должна иметь один из разрешенных тегов
                            if not task_tag_names:
                                # У задачи нет тегов, но у исполнителя есть ограничения - отфильтровываем
                                logger.info(
                                    f"Task {task_id} filtered out by tag filter: "
                                    f"task has no tags, but executor requires tags: {allowed_tag_names}"
                                )
                                continue
                            elif not (task_tag_names & allowed_tag_names):
                                # У задачи есть теги, но они не совпадают с разрешенными
                                logger.info(
                                    f"Task {task_id} filtered out by tag filter: "
                                    f"task_tags={task_tag_names} don't intersect with allowed_tags={allowed_tag_names}"
                                )
                                continue
                            else:
                                logger.debug(f"Task {task_id} passed tag filter: task_tags={task_tag_names} match allowed_tags={allowed_tag_names}")
                        else:
                            # Если у исполнителя нет ограничений по тегам - пропускаем фильтр
                            logger.debug(f"Task {task_id} passed tag filter (executor has no tag restrictions)")

                        logger.info(f"Task {task_id} passed all filters, adding to list")
                        all_new_tasks.append(task)

                    offset += len(tasks)
                    page_index += 1

                    if len(tasks) < page_size:
                        logger.info(
                            f"Reached last page for status {status_id}: fetched {len(tasks)} tasks (< {page_size})"
                        )
                        break

                    if oldest_task_date_in_page and oldest_task_date_in_page < seven_days_ago:
                        logger.info(
                            f"Stopping pagination for status {status_id}: "
                            f"oldest task date {oldest_task_date_in_page} is older than 7 days"
                        )
                        break
                
            # Фильтрация: показываем только заявки, созданные через бота
            # Используем BotLog для более надежной проверки
            def _is_bot_task(t):
                task_id = t.get('id')
                if not task_id:
                    return False
                
                # Нормализуем task_id
                try:
                    if isinstance(task_id, str) and ':' in task_id:
                        task_id = int(task_id.split(':')[-1])
                    else:
                        task_id = int(task_id)
                except (ValueError, TypeError):
                    logger.warning(f"Task {task_id} has invalid id format")
                    return False
                
                # ПРИОРИТЕТ 1: Проверяем BotLog (наиболее надежный способ)
                try:
                    with db_manager.get_db() as db:
                        from database import BotLog
                        # Ищем задачу в BotLog по task_id (может быть сохранен как id или generalId)
                        bot_logs = db.query(BotLog).filter(
                            BotLog.action == "create_task",
                            BotLog.success == True
                        ).order_by(BotLog.id.desc()).limit(500).all()
                        
                        for log in bot_logs:
                            if log.details:
                                try:
                                    # Проверяем все возможные поля с ID задачи
                                    task_id_candidates = [
                                        log.details.get('task_id'),
                                        log.details.get('task_id_internal'),
                                        log.details.get('task_id_general'),
                                    ]
                                    
                                    for log_task_id in task_id_candidates:
                                        if log_task_id is None:
                                            continue
                                        
                                        # Нормализуем ID из лога
                                        log_task_id_int = None
                                        if isinstance(log_task_id, int):
                                            log_task_id_int = log_task_id
                                        elif isinstance(log_task_id, str):
                                            if ':' in log_task_id:
                                                log_task_id_int = int(log_task_id.split(':')[-1])
                                            else:
                                                log_task_id_int = int(log_task_id)
                                        
                                        # Сравниваем с task_id из задачи
                                        if log_task_id_int == task_id:
                                            logger.debug(f"Task {task_id} found in BotLog (matched {log_task_id}) - confirmed as bot task")
                                            return True
                                except (ValueError, TypeError, AttributeError):
                                    continue
                except Exception as log_err:
                    logger.debug(f"Error checking BotLog for task {task_id}: {log_err}")
                
                # ПРИОРИТЕТ 2: Проверяем текст в названии и описании (fallback)
                desc = t.get('description') or ''
                name = t.get('name') or ''
                desc_lower = desc.lower()
                name_lower = name.lower()
                is_bot = (
                    "создано через telegram бот" in desc_lower or 
                    "telegram бот" in desc_lower or 
                    "запрос через бот" in name_lower or
                    "запрос через telegram бот" in name_lower
                )
                
                if not is_bot:
                    logger.warning(
                        f"Task {task_id} filtered out: not a bot task "
                        f"(name='{name[:50]}', desc has 'telegram бот'={('telegram бот' in desc_lower)}, "
                        f"desc has 'создано через telegram бот'={('создано через telegram бот' in desc_lower)}, "
                        f"name has 'запрос через бот'={('запрос через бот' in name_lower)}, "
                        f"not found in BotLog)"
                    )
                else:
                    logger.debug(f"Task {task_id} confirmed as bot task by text markers")
                
                return is_bot
            
            # Фильтрация по BotLog: показываем только заявки, созданные через бота
            before_bot_filter = len(all_new_tasks)
            filtered_tasks = []
            for t in all_new_tasks:
                if _is_bot_task(t):
                    filtered_tasks.append(t)
                else:
                    logger.info(f"Task {t.get('id')} filtered out by _is_bot_task check")
            
            all_new_tasks = filtered_tasks
            logger.info(
                f"Executor {executor.telegram_id} final filtered tasks: {len(all_new_tasks)} "
                f"(before bot filter: {before_bot_filter}, filtered out: {before_bot_filter - len(all_new_tasks)})"
            )
            
            # Дополнительная диагностика: если задач нет, но были до фильтра бота
            if not all_new_tasks and before_bot_filter > 0:
                logger.warning(
                    f"Executor {executor.telegram_id}: {before_bot_filter} tasks passed all filters "
                    f"but were filtered out by _is_bot_task. This may indicate that tasks are not marked as bot tasks."
                )
            
            # Дедупликация и ортировка по дате
            try:
                unique = {}
                for t in all_new_tasks:
                    unique[t['id']] = t
                all_new_tasks = list(unique.values())
                all_new_tasks.sort(key=lambda x: x.get('dateTime', ''), reverse=True)
            except Exception:
                pass

            if not all_new_tasks:
                # Детальная диагностика: проверяем, были ли задачи до фильтров
                logger.warning(
                    f"Executor {executor.telegram_id}: No tasks shown. "
                    f"Filters applied: direction={executor.service_direction}, "
                    f"allowed_templates={allowed_templates}, "
                    f"allowed_restaurant_ids={allowed_restaurant_ids}, "
                    f"allowed_tag_names={allowed_tag_names}, "
                    f"working_status_ids={working_status_ids}"
                )
                
                # Проверяем, есть ли задачи в BotLog для этого исполнителя
                try:
                    with db_manager.get_db() as db:
                        from database import BotLog
                        recent_bot_tasks = db.query(BotLog).filter(
                            BotLog.action == "create_task",
                            BotLog.success == True
                        ).order_by(BotLog.id.desc()).limit(10).all()
                        
                        if recent_bot_tasks:
                            logger.info(
                                f"Found {len(recent_bot_tasks)} recent bot tasks in BotLog. "
                                f"First task_id: {recent_bot_tasks[0].details.get('task_id') if recent_bot_tasks[0].details else 'N/A'}"
                            )
                except Exception as diag_err:
                    logger.debug(f"Error in diagnostics: {diag_err}")
                
                await message.answer(
                    "📋 <b>Новых заявок нет.</b>\n\n"
                    "Все заявки по вашим концепциям обработаны.",
                    parse_mode="HTML"
                )
                return

            # Формируем список заявок
            lines = [f"🆕 <b>Новые заявки ({len(all_new_tasks)}):</b>\n"]
            
            for task in all_new_tasks[:10]:  # Показываем первые 10
                task_id = task['id']
                task_name = task.get('name', 'Без названия')[:50]
                # КЭШ: контрагент с фоновой подгрузкой (точечное ускорение)
                _cp_key = f"cp_name:{task_id}"
                counterparty = cache.get(_cp_key) or "Определяется…"
                if counterparty == "Определяется…":
                    async def _bg_resolve_cp(tid, tdata):
                        try:
                            name = await resolve_counterparty_name(tdata)
                            cache.set(f"cp_name:{tid}", name, ttl_seconds=300)
                        except Exception:
                            pass
                    asyncio.create_task(_bg_resolve_cp(task_id, task))

                # Определяем и нормализуем статус
                raw_status = task.get('status', {}) or {}
                raw_status_id = raw_status.get('id')
                status_name = raw_status.get('name')
                status_id = None
                if isinstance(raw_status_id, int):
                    status_id = raw_status_id
                elif isinstance(raw_status_id, str):
                    try:
                        status_id = int(str(raw_status_id).split(':')[-1])
                    except Exception:
                        status_id = None

                # В списке новых заявок по умолчанию показываем «Новая», т.к. уже отфильтровано
                status_display_name = status_name or status_labels(
                    (
                        (StatusKey.NEW, "Новая"),
                        (StatusKey.REPLY_RECEIVED, "Получен ответ"),
                        (StatusKey.TIMEOUT, "Истек срок ответа"),
                        (StatusKey.IN_PROGRESS, "В работе"),
                        (StatusKey.INFO_SENT, "Отправлена информация"),
                        (StatusKey.COMPLETED, "Выполненная"),
                        (StatusKey.POSTPONED, "Отложенная"),
                    )
                ).get(status_id, "Новая")

                            
                lines.append(
                    f"📋 <b>#{task_id}</b> – {status_display_name}\n"
                    f"🏪 <b>Ресторан:</b> {counterparty}\n"
                    f"📝 <b>Описание:</b> {task_name}\n"
                    f"────────────────────"
                )
            
            if len(all_new_tasks) > 10:
                lines.append(f"\n💡 <i>... и ещё {len(all_new_tasks) - 10} заявок</i>")
            
            # Вместо ручного ввода показываем кнопки с номерами заявок
            from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
            task_ids = [t.get('id') for t in all_new_tasks][:10]
            rows = []
            row = []
            for tid in task_ids:
                row.append(KeyboardButton(text=f"#{tid}"))
                if len(row) == 3:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)
            kb = ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)
            lines.append("\n👇 <b>Выберите заявку кнопкой ниже:</b>")
            _final_text = "\n".join(lines)
            # КЭШ: сохраняем сформированный вывод на короткий TTL
            cache.set(f"new_tasks:{user_id}", {"text": _final_text, "kb": kb}, ttl_seconds=30)
            # Сохраняем результат для защиты от частых запросов
            cache.set(f"new_tasks_request:{user_id}:result", {"text": _final_text, "kb": kb}, ttl_seconds=10)
            cache.set(f"new_tasks_request:{user_id}:time", time.time(), ttl_seconds=10)
            
            await message.answer(_final_text, reply_markup=kb, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Error loading new tasks for user {user_id}: {e}", exc_info=True)
            await message.answer("❌ Ошибка при загрузке заявок. Попробуйте позже.")


# ============================================================================
# ПРОСМОТР МОИХ ЗАДАЧ
# ============================================================================

@router.message(F.text == "📋 Мои задачи")
async def show_my_tasks(message: Message, state: FSMContext):
    """Показать задачи, назначенные на исполнителя."""
    logger.info(f"Handler 'show_my_tasks' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    executor = await db_manager.get_executor_profile(message.from_user.id)
    
    if not executor or executor.profile_status != "активен":
        logger.warning(f"User {message.from_user.id} tried to access executor menu but is not an active executor")
        await message.answer("❌ Вы не зарегистрированы как исполнитель.")
        return
    
    if not executor.planfix_user_id:
        await message.answer(
            "⚠️ Ваш профиль не связан с учётной записью Planfix.\n\n"
            "Обратитесь к администратору для настройки."
        )
        return
    
    try:
        from database import TaskAssignment
        # Получаем принятые задачи исполнителя из БД (только активные)
        with db_manager.get_db() as dbs:
            assignments = dbs.query(TaskAssignment).filter(
                TaskAssignment.executor_telegram_id == executor.telegram_id,
                TaskAssignment.status == "active"
            ).all()

        if not assignments:
            await message.answer(
                "📋 У вас нет принятых задач.\n\n"
                "Вы ещё не взяли ни одной заявки в работу."
            )
            return

        tasks = []
        for a in assignments:
            try:
                tr = await planfix_client.get_task_by_id(
                    a.task_id,
                    fields="id,name,status,statusId,project.id,project.name,counterparty.id,counterparty.name,dateTime"
                )
                if tr and tr.get('result') == 'success':
                    t = tr.get('task', {})
                    # Фильтруем финальные статусы (выполнена/завершена/отменена/отклонена)
                    raw_status_id = t.get('status', {}).get('id')
                    sid = None
                    if isinstance(raw_status_id, int):
                        sid = raw_status_id
                    elif isinstance(raw_status_id, str):
                        try:
                            sid = int(str(raw_status_id).split(':')[-1])
                        except Exception:
                            sid = None
                    if sid is None:
                        alt_status_id = t.get('statusId') or t.get('status_id')
                        if isinstance(alt_status_id, int):
                            sid = alt_status_id
                        elif isinstance(alt_status_id, str):
                            try:
                                sid = int(str(alt_status_id).split(':')[-1])
                            except Exception:
                                sid = None
                    final_status_ids = set(
                        collect_status_ids(
                            (
                                StatusKey.COMPLETED,
                                StatusKey.FINISHED,
                                StatusKey.CANCELLED,
                                StatusKey.REJECTED,
                            ),
                            required=False,
                        )
                    )
                    # Хеуристика по имени статуса (если ID недоступен/некорректен)
                    sname_text = ((t.get('status', {}) or {}).get('name') or '').strip().lower()
                    is_final_by_name = any(k in sname_text for k in ["выполн", "заверш", "отмен", "отклон"])
                    if sid in final_status_ids or is_final_by_name:
                        # Деакт��вируем локальное назначение, чтобы больше не показывать в списке
                        try:
                            from database import TaskAssignment
                            with db_manager.get_db() as dbx:
                                rec = dbx.query(TaskAssignment).filter(
                                    TaskAssignment.task_id == a.task_id,
                                    TaskAssignment.executor_telegram_id == executor.telegram_id,
                                    TaskAssignment.status == "active"
                                ).first()
                                if rec:
                                    rec.status = "inactive"
                                    dbx.commit()
                        except Exception:
                            pass
                        continue
                    tasks.append(t)
            except Exception:
                continue

        logger.info(f"Found {len(tasks)} accepted tasks for executor {message.from_user.id}")

        if not tasks:
            await message.answer(
                "📋 У вас нет активных принятых задач.\n\n"
                "Все принятые задачи завершены."
            )
            return

        lines = [f"📋 Мои задачи ({len(tasks)}):\n"]

        for task in tasks[:15]:
            task_id = task['id']
            task_name = task.get('name', 'Без названия')[:50]
            
            # Проверяем новые комментарии для каждой задачи
            try:
                await _check_comments_for_executor(task_id, executor.telegram_id, message.bot)
            except Exception as e:
                logger.error(f"Error checking comments for task {task_id}: {e}")

            # Нормализуем статус, убираем «Неизвестно»
            raw_status = task.get('status', {}) or {}
            raw_status_id = raw_status.get('id')
            status_name = raw_status.get('name')
            status_id = None
            if isinstance(raw_status_id, int):
                status_id = raw_status_id
            elif isinstance(raw_status_id, str):
                try:
                    status_id = int(str(raw_status_id).split(':')[-1])
                except Exception:
                    status_id = None
            # Fallback: некоторые аккаунты возвращают statusId/status_id вместо status.id
            if status_id is None:
                alt_status_id = task.get('statusId') or task.get('status_id')
                if isinstance(alt_status_id, int):
                    status_id = alt_status_id
                elif isinstance(alt_status_id, str):
                    try:
                        status_id = int(str(alt_status_id).split(':')[-1])
                    except Exception:
                        status_id = None

            status_map = status_labels(
                (
                    (StatusKey.NEW, "Новая"),
                    (StatusKey.IN_PROGRESS, "В работе"),
                    (StatusKey.INFO_SENT, "Отправлена информация"),
                    (StatusKey.COMPLETED, "Выполненная"),
                    (StatusKey.POSTPONED, "Отложенная"),
                    (StatusKey.FINISHED, "Завершенная"),
                    (StatusKey.CANCELLED, "Отменена"),
                    (StatusKey.REJECTED, "Отклонена"),
                )
            )
            # Приоритет как у заказчика: используем ��мя статуса из API, если оно есть
            snt = (status_name or '').strip()
            snt_lower = snt.lower()
            if snt:
                if any(k in snt_lower for k in ["отлож", "paused"]):
                    status_display_name = "Отложенная"
                elif any(k in snt_lower for k in ["информац", "info"]):
                    status_display_name = "Отправлена информация"
                elif any(k in snt_lower for k in ["выполн", "заверш"]):
                    status_display_name = "Выполненная"
                elif "отмен" in snt_lower:
                    status_display_name = "Отменена"
                elif "отклон" in snt_lower:
                    status_display_name = "Отклонена"
                else:
                    status_display_name = snt
            else:
                status_display_name = status_map.get(status_id) or "Неизвестно"

            # КЭШ: контрагент с фоновой подгрузкой (точечное ускорение)
            _cp_key = f"cp_name:{task_id}"
            counterparty = cache.get(_cp_key) or "Определяется…"
            if counterparty == "Определяется…":
                async def _bg_resolve_cp_my(tid, tdata):
                    try:
                        name = await resolve_counterparty_name(tdata)
                        cache.set(f"cp_name:{tid}", name, ttl_seconds=300)
                    except Exception:
                        pass
                asyncio.create_task(_bg_resolve_cp_my(task_id, task))

            # Эмодзи для статусов
            status_emoji = status_labels(
                (
                    (StatusKey.IN_PROGRESS, "🔄"),
                    (StatusKey.POSTPONED, "⏸"),
                    (StatusKey.INFO_SENT, "📤"),
                    (StatusKey.COMPLETED, "✅"),
                    (StatusKey.FINISHED, "✅"),
                    (StatusKey.CANCELLED, "❌"),
                    (StatusKey.REJECTED, "❌"),
                )
            ).get(status_id)
            if not status_emoji:
                emoji_by_name = {
                    "в работе": "🔄",
                    "отложенная": "⏸",
                    "отправлена информация": "📤",
                    "выполненная": "✅",
                    "завершенная": "✅",
                    "отменена": "❌",
                    "отклонена": "❌",
                    "получен ответ": "📥",
                    "неизвестно": "📌",
                }
                status_emoji = emoji_by_name.get((status_display_name or '').lower(), "📌")

            lines.append(
                f"{status_emoji} <b>#{task_id}</b> – {status_display_name}\n"
                f"🏪 <b>Ресторан:</b> {counterparty}\n"
                f"📝 <b>Описание:</b> {task_name}\n"
                f"────────────────────"
            )

        if len(tasks) > 15:
            lines.append(f"\n💡 <i>... и ещё {len(tasks) - 15} задач</i>")

        # Вместо ручного ввода показываем кнопки с номерами задач.
        # Используем временную reply-клавиатуру: при нажатии отправится сообщение с номером задачи,
        # которое уже обрабатывается текущим хендлером показа деталей по номеру.
        from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
        task_ids = [t.get('id') for t in tasks][:15]
        rows = []
        row = []
        for tid in task_ids:
            row.append(KeyboardButton(text=f"#{tid}"))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        kb = ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)
        # КЭШ: сохраняем список «Мои задачи» кратковременно
        header_text = f"📋 <b>Мои задачи ({len(tasks)}):</b>\n\n"
        footer_text = "\n👇 <b>Выберите задачу кнопкой ниже:</b>"
        _final_text = header_text + "\n".join(lines) + footer_text
        cache.set(f"my_tasks:{message.from_user.id}", {"text": _final_text, "kb": kb}, ttl_seconds=45)
        await message.answer(
            _final_text,
            reply_markup=kb,
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Error loading executor tasks: {e}", exc_info=True)
        await message.answer("❌ Ошибка при загрузке задач.")


# ============================================================================
# ПРОСМОТР ДЕТАЛЕЙ ЗАДАЧИ И УПРАВЛЕНИЕ
# ============================================================================

@router.message(
    F.text.regexp(r'^#?\d+$'),
    ~StateFilter(ExecutorTaskManagement.entering_comment)
)
async def show_task_details(message: Message, state: FSMContext):
    """Показать детали задачи по номеру."""
    # Проверяем другие состояния пользователя, которые могут конфликтовать
    current_state = await state.get_state()
    logger.debug(f"show_task_details: current_state={current_state}, text={message.text}")
    if current_state:
        # Если пользователь находится в состоянии ввода комментария или описания, не обрабатываем это как номер задачи
        from states import CommentFlow, TicketCreation
        if (current_state == CommentFlow.waiting_for_text or
            current_state == CommentFlow.waiting_for_task_id or
            current_state == TicketCreation.entering_description):
            logger.debug(f"show_task_details: skipping because state is {current_state}, letting other handler handle it")
            return  # Пусть это обработает соответствующий обработчик состояния
    
    def _local_pf_id_to_int(raw_id):
        try:
            if isinstance(raw_id, int):
                return raw_id
            if isinstance(raw_id, str):
                part = raw_id.split(':')[-1]
                return int(part)
        except Exception:
            return None
        return None
    executor = await db_manager.get_executor_profile(message.from_user.id)
    is_executor = bool(executor and executor.profile_status == "активен")
    user_profile = None
    if not is_executor:
        user_profile = await db_manager.get_user_profile(message.from_user.id)
        if not user_profile:
            return  # ни исполнитель, ни пользователь
    
    task_id = int(message.text.strip().lstrip('#'))
    
    try:
        # Получаем информацию о задаче
        task_response = await planfix_client.get_task_by_id(
            task_id,
            fields="id,name,description,status,statusId,project.id,project.name,template.id,counterparty.id,counterparty.name,assignees,customFieldData,files,dateTime"
        )
        
        if not task_response or task_response.get('result') != 'success':
            await message.answer(f"❌ Задача #{task_id} не найдена.")
            return
        
        task = task_response.get('task', {})
        
        assignees_users = task.get('assignees', {}).get('users', [])
        is_assigned_to_executor = False
        allowed_by_local_assignment = False
        task_matches_executor = False
        if is_executor:
            is_assigned_to_executor = any(
                _local_pf_id_to_int(a.get('id')) == int(executor.planfix_user_id)
                for a in assignees_users
            ) if executor.planfix_user_id else False
            task_matches_executor = _task_matches_executor(task, executor)
            try:
                from database import TaskAssignment
                with db_manager.get_db() as db_sess:
                    allowed_by_local_assignment = db_sess.query(TaskAssignment).filter(
                        TaskAssignment.task_id == task_id,
                        TaskAssignment.executor_telegram_id == executor.telegram_id,
                        TaskAssignment.status == "active"
                    ).first() is not None
            except Exception:
                allowed_by_local_assignment = False

            # Определяем, является ли задача "Новой"
            raw_status = task.get('status', {})
            task_status_id = None
            if isinstance(raw_status, dict):
                raw_status_id = raw_status.get('id')
                if isinstance(raw_status_id, int):
                    task_status_id = raw_status_id
                elif isinstance(raw_status_id, str):
                    try:
                        task_status_id = int(str(raw_status_id).split(':')[-1])
                    except Exception:
                        pass
            
            is_new_status = is_status(task_status_id, StatusKey.NEW) if task_status_id else False
            
            # Проверяем, создана ли задача через бота
            task_name_value = task.get('name') or ''
            description_value = task.get('description') or ''
            is_bot_task = (
                task_name_value.lower().startswith('запрос через бот') or 
                'Создано через Telegram бот' in description_value or
                'telegram бот' in description_value.lower()
            )
            
            # Если задача уже принята в работу или назначена - разрешаем доступ
            # Если задача в статусе "Новая" и создана через бота - разрешаем доступ 
            # (она показывается в "Новые заявки" только после прохождения фильтров)
            # Иначе проверяем соответствие фильтрам
            if not allowed_by_local_assignment and not is_assigned_to_executor:
                # Для новых задач, созданных через бота - разрешаем доступ без дополнительных проверок
                # так как они уже прошли фильтрацию в "Новые заявки"
                if not is_new_status or not is_bot_task:
                    # Для остальных задач - строгая проверка фильтров
                    if not task_matches_executor:
                        logger.warning(
                            f"Executor {executor.telegram_id} tried to access task {task_id} "
                            f"that doesn't match filters: template_id={_normalize_pf_id((task.get('template') or {}).get('id'))}, "
                            f"counterparty_id={_normalize_pf_id((task.get('counterparty') or {}).get('id'))}, "
                            f"executor_templates={_get_allowed_template_ids(executor)}, "
                            f"executor_restaurants={set(_extract_restaurant_ids(executor.serving_restaurants))}, "
                            f"status_id={task_status_id}, is_new={is_new_status}, is_bot_task={is_bot_task}"
                        )
                        await message.answer("❌ Эта задача не относится к вашим ресторанам или направлению.")
                        return
                # Для is_new_status and is_bot_task - доступ разрешён, продолжаем
        else:
            counterparty_id = _normalize_pf_id((task.get('counterparty') or {}).get('id'))
            try:
                user_restaurant_id = int(user_profile.restaurant_contact_id)
            except Exception:
                user_restaurant_id = None
            if not counterparty_id or counterparty_id != user_restaurant_id:
                await message.answer("❌ Эта задача не относится к вашему ресторану.")
                return
        
        # Нормализуем статус задачи (поддерживаем разные форматы из API)
        status_id = None
        status_name = None
        raw_status = task.get('status')
        try:
            if isinstance(raw_status, dict):
                raw_status_id = raw_status.get('id')
                status_name = raw_status.get('name')
                if isinstance(raw_status_id, int):
                    status_id = raw_status_id
                elif isinstance(raw_status_id, str):
                    try:
                        status_id = int(raw_status_id.split(':')[-1])
                    except Exception:
                        status_id = None
            elif isinstance(raw_status, (int, str)):
                if isinstance(raw_status, int):
                    status_id = raw_status
                else:
                    try:
                        status_id = int(str(raw_status).split(':')[-1])
                    except Exception:
                        status_id = None
            if status_id is None:
                alt_status_id = task.get('statusId') or task.get('status_id')
                if isinstance(alt_status_id, int):
                    status_id = alt_status_id
                elif isinstance(alt_status_id, str):
                    try:
                        status_id = int(alt_status_id.split(':')[-1])
                    except Exception:
                        status_id = None
        except Exception:
            status_id = None
            status_name = None

        if not status_name and status_id is not None:
            status_name = status_labels(
                (
                    (StatusKey.NEW, "Новая"),
                    (StatusKey.IN_PROGRESS, "В работе"),
                    (StatusKey.INFO_SENT, "Отправлена информация"),
                    (StatusKey.COMPLETED, "Выполненная"),
                    (StatusKey.POSTPONED, "Отложенная"),
                )
            ).get(status_id)

        # Fallback: если статус не определён, но задача создана через бота, считаем её «Новая»
        try:
            task_name_value = task.get('name') or ''
            description_value = task.get('description') or ''
            is_bot_task_marker = task_name_value.lower().startswith('запрос через бот') or ('Создано через Telegram бот' in description_value)
            if status_id is None and not status_name and is_bot_task_marker:
                status_id = resolve_status_id(StatusKey.NEW, required=False)
                status_name = "Новая"
        except Exception:
            pass

        # Формируем детальную информацию
        task_name = task.get('name', 'Без названия')
        description = task.get('description', 'Нет описания')
        # КЭШ: контрагент для деталей без ожидания
        _cp_key = f"cp_name:{task_id}"
        counterparty = cache.get(_cp_key) or "Определяется…"
        if counterparty == "Определяется…":
            async def _bg_resolve_cp_details(tid, tdata):
                try:
                    name = await resolve_counterparty_name(tdata)
                    cache.set(f"cp_name:{tid}", name, ttl_seconds=300)
                except Exception:
                    pass
            asyncio.create_task(_bg_resolve_cp_details(task_id, task))
        project_name = await resolve_project_name(task)
        
        # Извлекаем кастомные поля
        custom_fields = task.get('customFieldData', [])
        phone = "Не указан"
        contact_name = "Не указан"
        
        for field in custom_fields:
            field_id = field.get('field', {}).get('id')
            if field_id == 84:  # Номер телефона
                phone = field.get('value', 'Не указан')
            elif field_id == 82:  # Контакт
                try:
                    val = field.get('value')
                    if isinstance(val, dict):
                        # Если имя есть в значении поля — используем его
                        nm = (val.get('name') or '').strip()
                        if nm:
                            contact_name = nm
                        else:
                            # Пытаемся получить id контакта и запросить полное имя через API
                            cid_raw = val.get('id')
                            cid = None
                            if cid_raw:
                                if isinstance(cid_raw, str) and ':' in cid_raw:
                                    try:
                                        cid = int(cid_raw.split(':')[-1])
                                    except Exception:
                                        cid = None
                                else:
                                    try:
                                        cid = int(cid_raw)
                                    except Exception:
                                        cid = None
                            if cid:
                                try:
                                    resp = await planfix_client.get_contact_by_id(cid, fields="id,name,midName,lastName,isCompany")
                                    if resp and resp.get('result') == 'success':
                                        contact = resp.get('contact') or {}
                                        info = extract_contact_info(contact)
                                        if info.get('name') and info['name'] != "Неизвестно":
                                            contact_name = info['name']
                                except Exception:
                                    pass
                    else:
                        # Значение может быть строкой с ID контакта
                        cid = None
                        if isinstance(val, str):
                            try:
                                cid = int(val.split(':')[-1]) if ':' in val else int(val)
                            except Exception:
                                cid = None
                        if cid:
                            try:
                                resp = await planfix_client.get_contact_by_id(cid, fields="id,name,midName,lastName,isCompany")
                                if resp and resp.get('result') == 'success':
                                    contact = resp.get('contact') or {}
                                    info = extract_contact_info(contact)
                                    if info.get('name') and info['name'] != "Неизвестно":
                                        contact_name = info['name']
                            except Exception:
                                pass
                except Exception:
                    pass
        # Fallback: попробуем извлечь из описания, если customFieldData пусты
        try:
            if (not phone) or (phone == "Не указан"):
                import re
                m = re.search(r"Телефон:\s*([+\d][\d\s\-()]+)", description)
                if m:
                    phone = m.group(1).strip()
            if (not contact_name) or (contact_name == "Не указан"):
                import re
                m2 = re.search(r"Заявитель:\s*([^\n\r]*?)(?=\s*(Телефон:|Описани|Создано|$))", description, flags=re.IGNORECASE)
                if m2:
                    contact_name = m2.group(1).strip()
        except Exception:
            pass
        
        # Проверяем, назначена ли задача
        assignees = task.get('assignees', {}).get('users', [])
        is_assigned = any(
            _local_pf_id_to_int(a.get('id')) == int(executor.planfix_user_id)
            for a in assignees
        ) if is_executor and executor.planfix_user_id else False

        has_any_assignee = bool(assignees)

        accepted_by_executor = False
        if is_executor:
            try:
                from database import TaskAssignment
                with db_manager.get_db() as db_sess:
                    accepted_by_executor = db_sess.query(TaskAssignment).filter(
                        TaskAssignment.task_id == task_id,
                        TaskAssignment.executor_telegram_id == executor.telegram_id,
                        TaskAssignment.status == "active"
                    ).first() is not None
            except Exception:
                accepted_by_executor = False

        # Логика отображения кнопок: кнопку «Принять в работу» показываем строго при статусе «Новая»,
        # независимо от назначений в Planfix. Остальные действия доступны только после принятия.
        is_new = is_status(status_id, StatusKey.NEW)
        is_waiting = is_status(status_id, StatusKey.INFO_SENT)
        status_name_text = (status_name or "").strip().lower()
        is_paused = is_status(status_id, StatusKey.POSTPONED) or (
            "отлож" in status_name_text or "paused" in status_name_text
        )

        # Отображаемое имя статуса
        status_display_name = status_name or status_labels(
            (
                (StatusKey.NEW, "Новая"),
                (StatusKey.IN_PROGRESS, "В работе"),
                (StatusKey.INFO_SENT, "Отправлена информация"),
                (StatusKey.COMPLETED, "Выполненная"),
                (StatusKey.POSTPONED, "Отложенная"),
                (StatusKey.FINISHED, "Завершенная"),
                (StatusKey.CANCELLED, "Отменена"),
                (StatusKey.REJECTED, "Отклонена"),
            )
        ).get(status_id, "В работе")
        # Хеуристика: если имя статуса указывает на паузу — принудительно отображаем «Отложенная»
        _sn = (status_name or "").strip().lower()
        if "отлож" in _sn or "paused" in _sn:
            status_display_name = "Отложенная"
        
        message_text = (
            f"📋 Задача #{task_id}\n\n"
            f"📝 {task_name}\n\n"
            f"📊 Статус: {status_display_name}\n"
            f"🏢 Проект: {project_name}\n"
            f"🏪 Ресторан: {counterparty}\n"
            f"👤 Заявитель: {contact_name}\n"
            f"📱 Телефон: {phone}\n\n"
            f"📄 Описание:\n{description[:500]}"
        )
        
        # Сохраняем ID задачи в состояние
        reply_kb = None
        if is_executor:
            await state.update_data(current_task_id=task_id)
            await state.set_state(ExecutorTaskManagement.viewing_task)
            if accepted_by_executor:
                reply_kb = get_task_actions_keyboard(task_id, is_new=False, is_waiting=is_waiting, is_paused=is_paused)
            elif is_new:
                reply_kb = get_task_actions_keyboard(task_id, is_new=True, is_waiting=is_waiting, is_paused=is_paused)
            else:
                message_text += "\n\n🔒 Действия недоступны до принятия задачи через бот.\nСтатус задачи не 'Новая', поэтому кнопка принятия не отображается."
        else:
            await state.clear()

        # Файлы будут отправлены как медиа ниже, не добавляем ссылки

        await message.answer(
            message_text,
            reply_markup=reply_kb
        )
        
        # Отправляем файлы как медиа (как у заявителей) - работа в памяти, без сохранения на диск
        try:
            files = (task.get('files') or []) if isinstance(task, dict) else []
            # Стру��тура может быть {"files": [{"id":.., "name":..}]} или сразу список
            if isinstance(task, dict) and not files:
                files = ((task.get('task') or {}).get('files')) or []
            
            import mimetypes
            
            # Собираем все файлы: из задачи + из комментариев
            all_files = []
            
            # Файлы из задачи
            for f in files[:10]:  # Максимум 10 файлов из задачи
                fid_raw = f.get('id')
                name = f.get('name') if isinstance(f, dict) else f"file_{fid_raw}"
                if fid_raw:
                    all_files.append((fid_raw, name, 'task'))
            
            # Файлы из комментариев (только последние 5 комментариев)
            try:
                cr = await planfix_client.get_task_comments(task_id, fields="id,dateTime,files", offset=0, page_size=5)
                comments = (cr.get('comments') or []) if cr and cr.get('result') == 'success' else []
                for cm in reversed(comments):
                    for f in (cm.get('files') or []):
                        if len(all_files) >= 15:  # Максимум 15 файлов всего
                            break
                        fid_raw = f.get('id')
                        name = f.get('name') or f"file_{fid_raw}"
                        if fid_raw and (fid_raw, name, 'comment') not in all_files:
                            all_files.append((fid_raw, name, 'comment'))
                    if len(all_files) >= 15:
                        break
            except Exception as e:
                logger.debug(f"Error loading comments files for task {task_id}: {e}")
            
            if not all_files:
                return
            
            # Загружаем файлы в память
            photos = []
            documents = []
            
            logger.info(f"Loading {len(all_files)} files for task {task_id} as media")
            for fid_raw, name, source in all_files[:15]:
                try:
                    fid = int(str(fid_raw).split(':')[-1])
                    logger.debug(f"Downloading file {fid} ({name}) from {source}...")
                    # Скачиваем файл из Planfix в память (не на диск)
                    file_data = await planfix_client.download_file(fid)
                    if file_data:
                        # Ограничение размера файла (50 МБ) для безопасности
                        max_size = 50 * 1024 * 1024  # 50 МБ
                        if len(file_data) > max_size:
                            logger.warning(f"File {fid} ({name}) is too large ({len(file_data)} bytes), skipping")
                            continue
                        
                        # Определяем MIME-тип по расширению
                        mime_type, _ = mimetypes.guess_type(name)
                        logger.debug(f"File {name}: mime_type={mime_type}, size={len(file_data)} bytes")
                        
                        # Определяем, является ли файл изображением
                        if mime_type and mime_type.startswith('image/'):
                            photos.append((file_data, name))
                            logger.debug(f"Added {name} as photo")
                        else:
                            # Для всех остальных файлов отправляем как документ
                            documents.append((file_data, name, mime_type))
                            logger.debug(f"Added {name} as document")
                    else:
                        logger.warning(f"Failed to download file {fid} ({name}): file_data is None")
                except Exception as e:
                    logger.error(f"Failed to download file {fid_raw} ({name}): {e}", exc_info=True)
            
            # Отправляем медиафайлы
            if photos:
                if len(photos) == 1:
                    # Одно фото - отправляем с подписью
                    photo_data, photo_name = photos[0]
                    try:
                        photo_file = BufferedInputFile(photo_data, filename=photo_name)
                        await message.answer_photo(
                            photo=photo_file,
                            caption=f"📎 {photo_name}"
                        )
                        logger.info(f"✅ Sent photo {photo_name} for task {task_id}")
                    finally:
                        del photo_data
                else:
                    # Несколько фото - отправляем медиагруппой
                    media_group = []
                    try:
                        for i, (photo_data, photo_name) in enumerate(photos):
                            photo_file = BufferedInputFile(photo_data, filename=photo_name)
                            media_group.append(
                                InputMediaPhoto(
                                    media=photo_file,
                                    caption=f"📎 {photo_name}" if i == 0 else None
                                )
                            )
                        await message.answer_media_group(media=media_group)
                        logger.info(f"✅ Sent {len(photos)} photos for task {task_id}")
                    finally:
                        # Освобождаем память
                        for photo_data, _ in photos:
                            del photo_data
                
                # Отправляем документы отдельно (если есть)
                for doc_data, doc_name, doc_mime in documents:
                    try:
                        doc_file = BufferedInputFile(doc_data, filename=doc_name)
                        await message.answer_document(
                            document=doc_file,
                            caption=f"📎 {doc_name}"
                        )
                    finally:
                        del doc_data
            elif documents:
                # Только документы - первый с подписью, остальные без
                for i, (doc_data, doc_name, doc_mime) in enumerate(documents):
                    try:
                        doc_file = BufferedInputFile(doc_data, filename=doc_name)
                        await message.answer_document(
                            document=doc_file,
                            caption=f"📎 {doc_name}" if i == 0 else None
                        )
                    finally:
                        del doc_data
                logger.info(f"✅ Sent {len(documents)} documents for task {task_id}")
        except Exception as e:
            logger.error(f"Error while sending task attachments for #{task_id}: {e}", exc_info=True)
        
    except Exception as e:
        logger.error(f"Error loading task details: {e}", exc_info=True)
        await message.answer("❌ Ошибка при загрузке задачи.")


# ============================================================================
# ДЕЙСТВИЯ С ЗАДАЧАМИ
# ============================================================================

@router.callback_query(F.data.startswith("accept:"))
async def accept_task(callback_query: CallbackQuery, state: FSMContext):
    """Принять задачу в работу."""
    executor = await db_manager.get_executor_profile(callback_query.from_user.id)
    
    if not executor:
        await callback_query.answer("❌ Профиль не настроен", show_alert=True)
        return
    
    task_id = int(callback_query.data.split(":")[1])
    
    try:
        # Проверяем, есть ли у исполнителя contact_id в Planfix
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
                logger.info(f"Using existing Planfix contact {planfix_contact_id} for executor {executor.telegram_id}")
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
                
                # Пробуем найти существующий контакт по телефону
                # Если контакт уже существует, используем его
                try:
                    # Ищем контакт по телефону через API (если есть такой метод)
                    # Пока просто пробуем создать, если не получится - будет ошибка
                    pass
                except Exception:
                    pass
                
                # Создаем контакт в группе "Поддержка" с template_id=1
                from config import SUPPORT_CONTACT_GROUP_ID, SUPPORT_CONTACT_TEMPLATE_ID
                
                contact_response = None
                try:
                    contact_response = await planfix_client.create_contact(
                        name=name,
                        lastname=lastname,
                        phone=executor.phone_number,
                        email=executor.email,
                        group_id=SUPPORT_CONTACT_GROUP_ID,  # Группа "Поддержка"
                        template_id=SUPPORT_CONTACT_TEMPLATE_ID  # Template ID 1
                    )
                except Exception as e:
                    logger.error(f"Failed to create contact in support group: {e}")
                    contact_response = None
                
                if contact_response and contact_response.get('result') == 'success':
                    contact_id = contact_response.get('id') or contact_response.get('contact', {}).get('id')
                    if contact_id:
                        planfix_contact_id = int(str(contact_id).split(':')[-1]) if isinstance(contact_id, str) and ':' in contact_id else int(contact_id)
                        
                        # Используем ID контакта как planfix_user_id
                        planfix_user_id = str(planfix_contact_id)
                        
                        # Сохраняем contact_id и planfix_user_id в профиль исполнителя
                        await db_manager.update_executor_profile(
                            executor.telegram_id,
                            planfix_contact_id=str(planfix_contact_id),
                            planfix_user_id=planfix_user_id
                        )
                        logger.info(f"Created and saved Planfix contact {planfix_contact_id} for executor {executor.telegram_id} (planfix_user_id: {planfix_user_id})")
                else:
                    logger.warning(f"Failed to create Planfix contact for executor {executor.telegram_id}: {contact_response}")
                    await callback_query.answer("❌ Не удалось создать контакт в Planfix", show_alert=True)
                    return
            except Exception as e:
                logger.error(f"Error creating Planfix contact for executor {executor.telegram_id}: {e}", exc_info=True)
                await callback_query.answer("❌ Ошибка при создании контакта", show_alert=True)
                return
        
        logger.info(f"Accepting task {task_id} by executor {executor.telegram_id} (planfix_contact_id={planfix_contact_id})")
        
        # Обновляем задачу: меняем статус и назначаем исполнителя как контакт
        # Согласно swagger.json, в assignees.users можно добавлять и user:ID, и contact:ID
        update_response = await planfix_client.update_task(
            task_id,
            status_id=require_status_id(StatusKey.IN_PROGRESS),
            assignee_contacts=[planfix_contact_id]  # Добавляем исполнителя как контакт
        )
        
        if update_response and update_response.get('result') == 'success':
            # Проверяем, что исполнитель действительно назначен
            try:
                await asyncio.sleep(0.3)  # Небольшая задержка для обработки Planfix
                task_check = await planfix_client.get_task_by_id(
                    task_id,
                    fields="id,assignees"
                )
                if task_check and task_check.get('result') == 'success':
                    task_obj = task_check.get('task', {}) or {}
                    assignees = task_obj.get('assignees', {}) or {}
                    assigned_users = assignees.get('users', []) or []
                    
                    # Проверяем, есть ли наш исполнитель в списке назначенных (как контакт)
                    executor_found = False
                    for user in assigned_users:
                        user_id_raw = user.get('id', '')
                        if isinstance(user_id_raw, str):
                            # Проверяем, является ли это контактом (contact:ID)
                            if user_id_raw.startswith('contact:'):
                                contact_id = int(user_id_raw.split(':')[-1])
                                if contact_id == planfix_contact_id:
                                    executor_found = True
                                    break
                            elif ':' in user_id_raw:
                                # Это может быть user:ID
                                user_id = int(user_id_raw.split(':')[-1])
                                # Не проверяем user_id, так как мы используем контакты
                        elif isinstance(user_id_raw, (int, float)):
                            # Если это просто число, проверяем как contact_id
                            if int(user_id_raw) == planfix_contact_id:
                                executor_found = True
                                break
                    
                    if not executor_found:
                        logger.warning(f"⚠️ Executor contact {planfix_contact_id} not found in assignees after update. Retrying assignment...")
                        # Пробуем назначить исполнителя отдельным запросом
                        try:
                            retry_response = await planfix_client.update_task(
                                task_id,
                                assignee_contacts=[planfix_contact_id]
                            )
                            if retry_response and retry_response.get('result') == 'success':
                                logger.info(f"✅ Executor contact {planfix_contact_id} successfully assigned to task {task_id} on retry")
                            else:
                                logger.error(f"Failed to assign executor contact {planfix_contact_id} to task {task_id} on retry: {retry_response}")
                        except Exception as retry_err:
                            logger.error(f"Error retrying executor assignment for task {task_id}: {retry_err}")
                    else:
                        logger.info(f"✅ Verified: Executor contact {planfix_contact_id} is assigned to task {task_id}")
            except Exception as verify_err:
                logger.warning(f"Could not verify executor assignment for task {task_id}: {verify_err}")
                # Продолжаем работу, даже если проверка не удалась
            # Сохраняем назначение в базу данных
            from database import TaskAssignment
            with db_manager.get_db() as db:
                # Проверяем, нет ли уже активного назначения
                existing = db.query(TaskAssignment).filter(
                    TaskAssignment.task_id == task_id,
                    TaskAssignment.status == "active"
                ).first()
                
                if not existing:
                    assignment = TaskAssignment(
                        task_id=task_id,
                        executor_telegram_id=executor.telegram_id,
                        planfix_user_id=str(planfix_contact_id),  # Сохраняем contact_id в planfix_user_id для совместимости
                        status="active"
                    )
                    db.add(assignment)
                    db.commit()
                    logger.info(f"Task assignment created: task {task_id} -> executor {executor.telegram_id}")
            
            await callback_query.message.edit_text(
                f"✅ <b>Вы приняли задачу #{task_id} в работу!</b>\n\n"
                f"📊 <b>Статус:</b> В работе\n\n"
                f"💡 Не забудьте связаться с заявителем при необходимости.",
                reply_markup=get_task_actions_keyboard(task_id, is_new=False, is_waiting=False, is_paused=False),
                parse_mode="HTML"
            )
            await callback_query.answer("✅ Задача принята")
            
            # Возвращаем главное меню исполнителя, чтобы оно не пропало
            await callback_query.bot.send_message(
                callback_query.from_user.id,
                "📋 Используйте меню для работы с заявками:",
                reply_markup=get_executor_main_menu_keyboard()
            )
            
            # Добавляем комментарий и уведомляем заявителя
            comment_text = f"Задача принята в работу исполнителем {executor.full_name}"
            await planfix_client.add_comment_to_task(
                task_id,
                description=comment_text
            )
            # Уведомление клиенту о принятии задачи в работу
            try:
                from notifications import NotificationService
                notification_service = NotificationService(callback_query.bot)
                await notification_service.notify_new_comment(task_id, executor.full_name, comment_text, recipients="user")
            except Exception as notify_err:
                logger.error(f"Failed to notify counterparty about task acceptance #{task_id}: {notify_err}")
            
            logger.info(f"Task {task_id} accepted by executor {executor.telegram_id}")
        else:
            await callback_query.answer("❌ Не удалось принять задачу", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error accepting task: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при принятии задачи", show_alert=True)


@router.callback_query(F.data.startswith("resume:"))
async def resume_task(callback_query: CallbackQuery):
    """Возобновить задачу."""
    executor = await db_manager.get_executor_profile(callback_query.from_user.id)
    if not executor or not executor.planfix_user_id:
        await callback_query.answer("❌ Сначала настройте профиль исполнителя", show_alert=True)
        return

    task_id = int(callback_query.data.split(":")[1])

    # Блокируем действие до явного принятия в работу (по записи TaskAssignment)
    try:
        from database import TaskAssignment
        with db_manager.get_db() as db:
            accepted = db.query(TaskAssignment).filter(
                TaskAssignment.task_id == task_id,
                TaskAssignment.executor_telegram_id == executor.telegram_id,
                TaskAssignment.status == "active"
            ).first()
        if not accepted:
            await callback_query.answer("❌ Сначала примите задачу в работу", show_alert=True)
            return
    except Exception as e:
        logger.error(f"Error checking acceptance for resume: {e}")
        await callback_query.answer("❌ Ошибка проверки принятия", show_alert=True)
        return

    try:
        update_response = await planfix_client.update_task(
            task_id,
            status_id=require_status_id(StatusKey.IN_PROGRESS)
        )
        
        if update_response and update_response.get('result') == 'success':
            await planfix_client.add_comment_to_task(
                task_id,
                description=f"Работа по задаче возобновлена ({executor.full_name})"
            )
            
            await callback_query.message.edit_text(
                f"▶️ Задача #{task_id} возобновлена!\n\n"
                f"Статус изменён на 'В работе'.",
                reply_markup=get_task_actions_keyboard(task_id, is_new=False, is_waiting=False, is_paused=False)
            )
            await callback_query.answer("✅ Задача возобновлена")
            
            # Возвращаем главное меню исполнителя, чтобы оно не пропало
            await callback_query.bot.send_message(
                callback_query.from_user.id,
                "📋 Используйте меню для работы с заявками:",
                reply_markup=get_executor_main_menu_keyboard()
            )
        else:
            await callback_query.answer("❌ Ошибка", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error resuming task: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка", show_alert=True)


@router.callback_query(F.data.startswith("close:"))
async def close_task(callback_query: CallbackQuery, state: FSMContext):
    """Закрыть задачу."""
    executor = await db_manager.get_executor_profile(callback_query.from_user.id)
    if not executor or not executor.planfix_user_id:
        await callback_query.answer("❌ Сначала настройте профиль исполнителя", show_alert=True)
        return

    task_id = int(callback_query.data.split(":")[1])

    # Комментарий можно оставлять без принятия задачи в работу
    # Просто проверяем, что задача существует и доступна исполнителю
    try:
        task_response = await planfix_client.get_task_by_id(task_id, fields="id")
        if not task_response or task_response.get('result') != 'success':
            await callback_query.answer("❌ Задача не найдена", show_alert=True)
            return
    except Exception as e:
        logger.error(f"Error checking task {task_id} for comment: {e}")
        await callback_query.answer("❌ Ошибка при проверке задачи", show_alert=True)
        return

    await callback_query.message.edit_text(
        f"✅ Завершение задачи #{task_id}\n\n"
        f"Опишите выполненные работы:"
    )
    await state.update_data(current_task_id=task_id, action="close")
    await state.set_state(ExecutorTaskManagement.entering_comment)
    await callback_query.answer()


@router.callback_query(F.data.startswith("comment:"))
async def add_comment(callback_query: CallbackQuery, state: FSMContext):
    """Добавить комментарий к задаче."""
    executor = await db_manager.get_executor_profile(callback_query.from_user.id)
    if not executor or not executor.planfix_user_id:
        await callback_query.answer("❌ Сначала настройте профиль исполнителя", show_alert=True)
        return

    task_id = int(callback_query.data.split(":")[1])

    # Блокируем действие до явного ��ринятия в работу (по записи TaskAssignment)
    # Комментарий можно оставлять без принятия задачи в работу
    # Просто проверяем, что задача существует и доступна исполнителю
    try:
        task_response = await planfix_client.get_task_by_id(task_id, fields="id")
        if not task_response or task_response.get('result') != 'success':
            await callback_query.answer("❌ Задача не найдена", show_alert=True)
            return
    except Exception as e:
        logger.error(f"Error checking task {task_id} for comment: {e}")
        await callback_query.answer("❌ Ошибка при проверке задачи", show_alert=True)
        return

    await callback_query.message.edit_text(
        f"💬 Комментарий к задаче #{task_id}\n\n"
        f"Введите текст комментария (можно приложить фото):"
    )
    await state.update_data(current_task_id=task_id, action="comment", comment_files=[])
    await state.set_state(ExecutorTaskManagement.entering_comment)
    await callback_query.answer()


# ============================================================================
# ОБРАБОТКА КОММЕНТАРИЕВ И ДЕЙСТВИЙ
# ============================================================================

# ============================================================================
# ОБРАБОТКА ПРИКРЕПЛЕНИЯ ФАЙЛОВ К КОММЕНТАРИЯМ
# ============================================================================

@router.message(ExecutorTaskManagement.attaching_file, F.content_type == ContentType.PHOTO)
async def process_executor_comment_photo(message: Message, state: FSMContext):
    """Обработка фото для комментария исполнителя."""
    user_data = await state.get_data()
    task_id = user_data.get('current_task_id')
    
    executor = await db_manager.get_executor_profile(message.from_user.id)
    
    try:
        # Скачиваем фото
        file_id = message.photo[-1].file_id
        tg_file = await message.bot.get_file(file_id)
        file_bytes = await message.bot.download_file(tg_file.file_path)
        
        # Загружаем фото в Planfix
        upload_response = await planfix_client.upload_file(file_bytes, filename="photo.jpg")
        planfix_file_id = None
        
        if upload_response and upload_response.get('result') == 'success':
            planfix_file_id = upload_response.get('id')
            # Нормализуем file_id (убираем префикс "file:" и конвертируем в int)
            if planfix_file_id is not None:
                if isinstance(planfix_file_id, str):
                    if ':' in planfix_file_id:
                        try:
                            planfix_file_id = int(planfix_file_id.split(':')[-1])
                        except (ValueError, TypeError):
                            logger.warning(f"Could not parse file_id: {planfix_file_id}")
                            planfix_file_id = None
                    else:
                        try:
                            planfix_file_id = int(planfix_file_id)
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert file_id to int: {planfix_file_id}")
                            planfix_file_id = None
                elif not isinstance(planfix_file_id, int):
                    try:
                        planfix_file_id = int(planfix_file_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert file_id to int: {planfix_file_id}")
                        planfix_file_id = None
        
        if planfix_file_id:
            # Сохраняем ID файла в состоянии
            comment_files = user_data.get('comment_files', [])
            comment_files.append(planfix_file_id)
            await state.update_data(comment_files=comment_files)
            
            files_count = len(comment_files)
            await message.answer(
                f"📷 Фото прикреплено ({files_count} шт.). Можете добавить ещё фото или нажмите 'Пропустить' для отправки комментария.",
                reply_markup=get_skip_or_done_keyboard()
            )
        else:
            await message.answer(
                "⚠️ Ошибка при загрузке фото. Попробуйте ещё раз или нажмите 'Пропустить'.",
                reply_markup=get_skip_or_done_keyboard()
            )
            
    except Exception as e:
        logger.error(f"Error uploading photo for executor comment: {e}", exc_info=True)
        await message.answer(
            "⚠️ Ошибка при загрузке фото. Попробуйте ещё раз или нажмите 'Пропустить'.",
            reply_markup=get_skip_or_done_keyboard()
        )


@router.message(ExecutorTaskManagement.attaching_file, F.text.casefold() == "готово")
async def executor_comment_finalize_no_file(message: Message, state: FSMContext):
    """Отправка комментария без дополнительных файлов."""
    await _finalize_executor_comment(message, state, skip_files=False)


@router.callback_query(ExecutorTaskManagement.attaching_file, F.data == "skip_file")
async def executor_comment_skip_file(callback_query: CallbackQuery, state: FSMContext):
    """Пропуск прикрепления файла к комментарию."""
    await callback_query.answer()
    await _finalize_executor_comment(callback_query, state, skip_files=True)


async def _finalize_executor_comment(message_or_callback, state: FSMContext, skip_files: bool = False):
    """Финализация комментария исполнителя - отправка в Planfix."""
    from aiogram.types import CallbackQuery, Message
    
    user_data = await state.get_data()
    task_id = user_data.get('current_task_id')
    action = user_data.get('action')
    comment_text = user_data.get('comment_text', '')
    comment_files = user_data.get('comment_files', [])
    
    # Определяем тип объекта и получаем необходимые атрибуты
    is_callback = isinstance(message_or_callback, CallbackQuery)
    
    if is_callback:
        # Для CallbackQuery используем его атрибуты
        user_id = message_or_callback.from_user.id
        bot = message_or_callback.bot
        # Для отправки сообщений используем message.answer()
        answer_func = lambda text, **kwargs: message_or_callback.message.answer(text, **kwargs)
    else:
        # Для Message используем его атрибуты
        user_id = message_or_callback.from_user.id
        bot = message_or_callback.bot
        answer_func = message_or_callback.answer
    
    executor = await db_manager.get_executor_profile(user_id)
    if not executor:
        await answer_func("❌ Ошибка: профиль исполнителя не найден.")
        await state.clear()
        return
    
    if not executor.planfix_user_id:
        await answer_func("❌ Ошибка: не настроен Planfix пользователь в профиле исполнителя.")
        await state.clear()
        return
    
    try:
        from notifications import NotificationService
        notification_service = NotificationService(bot)
        
        if action == "close":
            # Получаем ID статуса "Выполнена" с безопасной обработкой
            completed_status_id = resolve_status_id(StatusKey.COMPLETED, required=False)
            if not completed_status_id:
                logger.warning(f"Status 'completed' not found, trying to update task without status change")
            else:
                try:
                    await planfix_client.update_task(task_id, status_id=completed_status_id)
                except Exception as e:
                    logger.error(f"Error updating task {task_id} status to completed: {e}", exc_info=True)
                    await answer_func("⚠️ Задача не была обновлена, но комментарий будет добавлен.")
            
            full_comment = f"✅ Задача выполнена.\n\nВыполненные работы:\n{comment_text}\n\n({executor.full_name})"
            files = comment_files if comment_files else None
            
            try:
                await planfix_client.add_comment_to_task(
                    task_id,
                    description=full_comment,
                    files=files
                )
            except Exception as e:
                logger.error(f"Error adding comment to task {task_id} (close action): {e}", exc_info=True)
                await answer_func("❌ Ошибка при добавлении комментария. Попробуйте позже.")
                await state.clear()
                return
            
            # Получаем ID только что созданного комментария для отправки медиа
            comment_id = None
            if comment_files:  # Только если есть файлы, иначе не нужно
                try:
                    import asyncio
                    await asyncio.sleep(0.5)  # Небольшая задержка для синхронизации
                    comments_response = await planfix_client.get_task_comments(
                        task_id,
                        fields="id,dateTime",
                        offset=0,
                        page_size=10
                    )
                    if comments_response and comments_response.get('result') == 'success':
                        comments = comments_response.get('comments', [])
                        if comments:
                            # Функция для получения сортировочного ключа из dateTime
                            def get_date_key(c):
                                dt = c.get('dateTime', '')
                                if isinstance(dt, dict):
                                    return str(dt.get('value', '')) if 'value' in dt else ''
                                return str(dt) if dt else ''
                            
                            # Сортируем по дате (новые первыми) и берем первый
                            comments_sorted = sorted(comments, key=get_date_key, reverse=True)
                            latest_comment = comments_sorted[0]
                            comment_id = latest_comment.get('id')
                            logger.info(f"Found latest comment ID {comment_id} for task {task_id}")
                except Exception as e:
                    logger.warning(f"Failed to get comment ID for task {task_id}: {e}", exc_info=True)
            
            # Отправляем уведомление заявителю
            try:
                await notification_service.notify_new_comment(task_id, executor.full_name, full_comment, recipients="user", comment_id=comment_id)
            except Exception as e:
                logger.error(f"Error sending notification for task {task_id}: {e}", exc_info=True)
            
            # Деактивируем локальное назначение
            try:
                from database import TaskAssignment
                with db_manager.get_db() as dbx:
                    executor_id = executor.telegram_id
                    rec = dbx.query(TaskAssignment).filter(
                        TaskAssignment.task_id == task_id,
                        TaskAssignment.executor_telegram_id == executor_id,
                        TaskAssignment.status == "active"
                    ).first()
                    if rec:
                        rec.status = "inactive"
                        dbx.commit()
            except Exception as e:
                logger.error(f"Error deactivating task assignment for task {task_id}: {e}", exc_info=True)
            
            files_msg = f" (прикреплено фото: {len(comment_files)})" if comment_files else ""
            await answer_func(
                f"✅ Задача #{task_id} завершена!{files_msg}\n\n"
                f"Выполненные работы:\n{comment_text}",
                reply_markup=get_executor_main_menu_keyboard()
            )
        else:  # comment
            full_comment = f"{comment_text}\n\n({executor.full_name})"
            files = comment_files if comment_files else None
            
            try:
                await planfix_client.add_comment_to_task(
                    task_id,
                    description=full_comment,
                    files=files
                )
            except Exception as e:
                logger.error(f"Error adding comment to task {task_id}: {e}", exc_info=True)
                await answer_func("❌ Ошибка при добавлении комментария. Попробуйте позже.")
                await state.clear()
                return
            
            # Получаем ID только что созданного комментария для отправки медиа
            comment_id = None
            if comment_files:  # Только если есть файлы, иначе не нужно
                try:
                    import asyncio
                    await asyncio.sleep(0.5)  # Небольшая задержка для синхронизации
                    comments_response = await planfix_client.get_task_comments(
                        task_id,
                        fields="id,dateTime",
                        offset=0,
                        page_size=10
                    )
                    if comments_response and comments_response.get('result') == 'success':
                        comments = comments_response.get('comments', [])
                        if comments:
                            # Функция для получения сортировочного ключа из dateTime
                            def get_date_key(c):
                                dt = c.get('dateTime', '')
                                if isinstance(dt, dict):
                                    return str(dt.get('value', '')) if 'value' in dt else ''
                                return str(dt) if dt else ''
                            
                            # Сортируем по дате (новые первыми) и берем первый
                            comments_sorted = sorted(comments, key=get_date_key, reverse=True)
                            latest_comment = comments_sorted[0]
                            comment_id = latest_comment.get('id')
                            logger.info(f"Found latest comment ID {comment_id} for task {task_id}")
                except Exception as e:
                    logger.warning(f"Failed to get comment ID for task {task_id}: {e}", exc_info=True)
            
            # Отправляем уведомление заявителю
            try:
                await notification_service.notify_new_comment(task_id, executor.full_name, full_comment, recipients="user", comment_id=comment_id)
            except Exception as e:
                logger.error(f"Error sending notification for task {task_id}: {e}", exc_info=True)
            
            files_msg = f" (прикреплено фото: {len(comment_files)})" if comment_files else ""
            await answer_func(
                f"✅ Комментарий добавлен к задаче #{task_id}.{files_msg}",
                reply_markup=get_executor_main_menu_keyboard()
            )
        
        await state.clear()
        logger.info(f"Executor {executor.telegram_id} performed action '{action}' on task {task_id} with {len(comment_files)} files")
        
    except Exception as e:
        logger.error(f"Error finalizing executor comment: {e}", exc_info=True)
        await answer_func("❌ Ошибка при выполнении действия. Попробуйте позже.")
        await state.clear()


@router.message(ExecutorTaskManagement.entering_comment)
async def process_executor_comment(message: Message, state: FSMContext):
    """Обработка комментария/действия исполнителя."""
    logger.info(f"process_executor_comment called for user {message.from_user.id}, text: {message.text}")
    user_data = await state.get_data()
    task_id = user_data.get('current_task_id')
    action = user_data.get('action')
    comment_files = user_data.get('comment_files', [])
    
    logger.info(f"Task ID: {task_id}, Action: {action}, Comment files: {len(comment_files)}")
    
    # Проверка наличия критических данных
    if not task_id:
        logger.error(f"No task_id in state for user {message.from_user.id}")
        await message.answer("❌ Ошибка: не указана задача. Попробуйте начать заново.")
        await state.clear()
        return
    
    # Если пришёл текст, используем его
    comment_text = message.text.strip() if message.text else ""
    
    executor = await db_manager.get_executor_profile(message.from_user.id)
    if not executor:
        logger.error(f"No executor profile for user {message.from_user.id}")
        await message.answer("❌ Ошибка: профиль исполнителя не найден.")
        await state.clear()
        return
    
    try:
        # Импортируем NotificationService для отправки уведомлений
        from notifications import NotificationService
        notification_service = NotificationService(message.bot)
        
        if action == "close":
            if not comment_text:
                await message.answer("❌ Необходимо указать выполненные работы. Введите текст.")
                return
            
            # Сохраняем текст комментария и переходим к прикреплению файла
            await state.update_data(comment_text=comment_text, comment_files=[])
            await message.answer(
                "📷 Прикрепите фото (если нужно) или нажмите 'Пропустить':",
                reply_markup=get_skip_or_done_keyboard()
            )
            await state.set_state(ExecutorTaskManagement.attaching_file)
            return  # Не очищаем состояние, переходим к прикреплению файла
            
        else:  # comment
            # Если нет текста - просим ввести текст
            if not comment_text:
                await message.answer("❌ Введите текст комментария.")
                return
            
            # Сохраняем текст комментария и переходим к прикреплению файла
            await state.update_data(comment_text=comment_text, comment_files=[])
            await message.answer(
                "📷 Прикрепите фото (если нужно) или нажмите 'Пропустить':",
                reply_markup=get_skip_or_done_keyboard()
            )
            await state.set_state(ExecutorTaskManagement.attaching_file)
            return  # Не очищаем состояние, переходим к прикреплению файла
        
    except Exception as e:
        logger.error(f"Error processing executor action: {e}", exc_info=True)
        await message.answer("❌ Ошибка при выполнении действия. Попробуйте позже.")
        await state.clear()


@router.message(F.text == "👤 Профиль исполнителя")
async def show_executor_profile(message: Message, state: FSMContext):
    """Показать профиль исполнителя."""
    logger.info(f"Handler 'show_executor_profile' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    executor = await db_manager.get_executor_profile(message.from_user.id)
    
    if not executor:
        logger.warning(f"User {message.from_user.id} tried to access executor profile but is not an executor")
        await message.answer("❌ Профиль исполнителя не найден.")
        return
    
    concept_names = []
    for cid in executor.serving_franchise_groups or []:
        info = FRANCHISE_GROUPS.get(cid)
        concept_names.append(info["name"] if info else f"ID {cid}")
    if not concept_names:
        concept_names = ["Не выбраны"]
    
    restaurants_text = _format_restaurant_list(executor.serving_restaurants)
    
    status_emoji = {
        "активен": "✅",
        "ожидает подтверждения": "⏳",
        "отклонён": "❌"
    }
    
    status_icon = status_emoji.get(executor.profile_status, "❓")
    
    profile_text = (
        f"👷 Профиль исполнителя:\n\n"
        f"👤 ФИО: {executor.full_name}\n"
        f"📱 Телефон: {executor.phone_number}\n"
        f"💼 Должность: {executor.position_role or 'Не указана'}\n"
        f"🏢 Концепции: {', '.join(concept_names)}\n"
        f"🧭 Направление: {_format_direction(executor.service_direction)}\n"
        f"🏪 Рестораны:\n{restaurants_text}\n"
        f"{status_icon} Статус: {executor.profile_status}\n"
        f"📅 Дата регистрации: {executor.registration_date.strftime('%d.%m.%Y')}\n"
    )
    
    # Если исполнитель активен, показываем кнопки редактирования
    status_normalized = (executor.profile_status or "").strip().lower()
    
    if status_normalized == "активен":
        profile_text += "\n\nВыберите, что хотите изменить:"
        await message.answer(
            profile_text,
            reply_markup=get_executor_profile_edit_keyboard()
        )
    else:
        # Если не активен, просто показываем профиль без возможности редактирования
        if status_normalized == "ожидает подтверждения":
            profile_text += "\n\n⏳ Ваш профиль ожидает подтверждения администратором."
        await message.answer(profile_text, reply_markup=get_executor_main_menu_keyboard())

async def _ensure_executor_profile(user_id: int, target_message: Message, require_active: bool = True):
    """Проверяет наличие профиля исполнителя и его статус."""
    executor = await db_manager.get_executor_profile(user_id)
    if not executor:
        await target_message.answer("❌ Профиль исполнителя не найден. Пройдите регистрацию: /start")
        return None
    
    if require_active:
        # Нормализуем статус для сравнения (убираем пробелы, приводим к нижнему регистру)
        status_normalized = (executor.profile_status or "").strip().lower()
        if status_normalized != "активен":
            status_msg = {
                "ожидает подтверждения": "⏳ Ваш профиль ожидает подтверждения администратором.",
                "отклонён": "❌ Ваша регистрация была отклонена. Обратитесь к администратору.",
                "отклонен": "❌ Ваша регистрация была отклонена. Обратитесь к администратору.",
            }.get(status_normalized, f"❌ Ваш профиль не активен. Текущий статус: '{executor.profile_status}'")
            await target_message.answer(f"❌ Редактирование профиля недоступно.\n\n{status_msg}")
            return None
    
    return executor


def _build_concepts_keyboard(selected_ids: list[int]) -> InlineKeyboardMarkup:
    selected_ids = selected_ids or []
    buttons = []
    for cid, data in sorted(FRANCHISE_GROUPS.items(), key=lambda item: item[1]["name"]):
        prefix = "✅ " if cid in selected_ids else "⬜️ "
        buttons.append([
            InlineKeyboardButton(
                text=f"{prefix}{data['name']}",
                callback_data=f"exec_toggle_concept:{cid}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="✅ Готово", callback_data="exec_concepts_done"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="exec_cancel_edit"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_restaurants_keyboard(restaurants: Dict[int, str], selected_ids: list[int]) -> InlineKeyboardMarkup:
    selected_ids = selected_ids or []
    buttons = []
    for cid, name in sorted(restaurants.items(), key=lambda item: item[1]):
        prefix = "✅ " if cid in selected_ids else "⬜️ "
        buttons.append([
            InlineKeyboardButton(
                text=f"{prefix}{name}",
                callback_data=f"exec_toggle_restaurant:{cid}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="✅ Готово", callback_data="exec_restaurants_done"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="exec_cancel_edit"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _get_allowed_template_ids(executor) -> Set[int]:
    direction = (executor.service_direction or "").lower()
    templates: Set[int] = set()
    if not direction or direction in ("se", "сэ", "служба эксплуатации"):
        templates.update(PLANFIX_SE_TEMPLATES.keys())
    if not direction or direction in ("it", "ит", "it служба"):
        templates.update(PLANFIX_IT_TEMPLATES.keys())
    if not templates:
        templates.update(PLANFIX_SE_TEMPLATES.keys())
        templates.update(PLANFIX_IT_TEMPLATES.keys())
    return templates


def _task_matches_executor(task: dict, executor) -> bool:
    template_id = _normalize_pf_id((task.get('template') or {}).get('id'))
    allowed_templates = _get_allowed_template_ids(executor)
    if allowed_templates:
        if template_id is None or template_id not in allowed_templates:
            return False

    allowed_restaurants = set(_extract_restaurant_ids(executor.serving_restaurants))
    if allowed_restaurants:
        counterparty_id = _normalize_pf_id((task.get('counterparty') or {}).get('id'))
        if counterparty_id is None or counterparty_id not in allowed_restaurants:
            return False

    return True


@router.callback_query(F.data == "exec_edit_name")
async def exec_edit_name_start(callback_query: CallbackQuery, state: FSMContext):
    executor = await _ensure_executor_profile(callback_query.from_user.id, callback_query.message, require_active=True)
    if not executor:
        await callback_query.answer("❌ Редактирование недоступно", show_alert=True)
        return
    await callback_query.message.edit_text("👤 Введите новое ФИО исполнителя:")
    await state.set_state(ExecutorProfileEdit.editing_full_name)
    await callback_query.answer()


@router.message(ExecutorProfileEdit.editing_full_name, F.text)
async def exec_edit_name_process(message: Message, state: FSMContext):
    # Проверяем, что исполнитель активен
    executor = await db_manager.get_executor_profile(message.from_user.id)
    status_normalized = (executor.profile_status or "").strip().lower() if executor else ""
    if not executor or status_normalized != "активен":
        await message.answer("❌ Редактирование недоступно. Ваш профиль не активен.")
        await state.clear()
        return
    
    full_name = (message.text or "").strip()
    if len(full_name) < 3:
        await message.answer("❌ ФИО слишком короткое. Попробуйте ещё раз:")
        return
    if len(full_name) > 255:
        await message.answer("❌ ФИО слишком длинное. Максимум 255 символов.")
        return

    try:
        await db_manager.update_executor_profile(message.from_user.id, full_name=full_name)
        await state.clear()
        await message.answer(
            f"✅ ФИО обновлено.\nНовое значение: {full_name}",
            reply_markup=get_executor_main_menu_keyboard()
        )
        logger.info(f"Executor {message.from_user.id} updated full name")
    except Exception as e:
        logger.error(f"Error updating executor full name: {e}", exc_info=True)
        await message.answer("❌ Не удалось обновить ФИО. Попробуйте позже.")
        await state.clear()


@router.callback_query(F.data == "exec_edit_phone")
async def exec_edit_phone_start(callback_query: CallbackQuery, state: FSMContext):
    executor = await _ensure_executor_profile(callback_query.from_user.id, callback_query.message, require_active=True)
    if not executor:
        await callback_query.answer("❌ Редактирование недоступно", show_alert=True)
        return

    await callback_query.message.edit_text("📱 Введите новый номер телефона или поделитесь контактом:")
    await callback_query.message.answer(
        "Отправьте номер в формате +79991234567 или нажмите кнопку ниже:",
        reply_markup=get_phone_number_keyboard()
    )
    await state.set_state(ExecutorProfileEdit.editing_phone)
    await callback_query.answer()


@router.message(ExecutorProfileEdit.editing_phone, F.contact)
async def exec_edit_phone_contact(message: Message, state: FSMContext):
    phone_number = message.contact.phone_number
    await _update_executor_phone(message, state, phone_number)


@router.message(ExecutorProfileEdit.editing_phone, F.text)
async def exec_edit_phone_text(message: Message, state: FSMContext):
    phone_text = (message.text or "").strip()
    normalized = re.sub(r"[^0-9+]", "", phone_text)
    if not normalized or len(re.sub(r"\D", "", normalized)) < 10:
        await message.answer("❌ Некорректный номер телефона. Введите его заново в формате +79991234567.")
        return
    await _update_executor_phone(message, state, normalized)


async def _update_executor_phone(message: Message, state: FSMContext, phone: str):
    # Проверяем, что исполнитель активен
    executor = await db_manager.get_executor_profile(message.from_user.id)
    status_normalized = (executor.profile_status or "").strip().lower() if executor else ""
    if not executor or status_normalized != "активен":
        await message.answer("❌ Редактирование недоступно. Ваш профиль не активен.")
        await state.clear()
        return
    
    try:
        await db_manager.update_executor_profile(message.from_user.id, phone_number=phone)
        await state.clear()
        await message.answer(
            f"✅ Телефон обновлён.\nНовый номер: {phone}",
            reply_markup=get_executor_main_menu_keyboard()
        )
        logger.info(f"Executor {message.from_user.id} updated phone to {phone}")
    except Exception as e:
        logger.error(f"Error updating executor phone: {e}", exc_info=True)
        await message.answer("❌ Не удалось обновить номер. Попробуйте позже.")
        await state.clear()


@router.callback_query(F.data == "exec_edit_position")
async def exec_edit_position_start(callback_query: CallbackQuery, state: FSMContext):
    executor = await _ensure_executor_profile(callback_query.from_user.id, callback_query.message, require_active=True)
    if not executor:
        await callback_query.answer("❌ Редактирование недоступно", show_alert=True)
        return
    await callback_query.message.edit_text("💼 Введите новую должность (до 100 символов):")
    await state.set_state(ExecutorProfileEdit.editing_position)
    await callback_query.answer()


@router.message(ExecutorProfileEdit.editing_position, F.text)
async def exec_edit_position_process(message: Message, state: FSMContext):
    # Проверяем, что исполнитель активен
    executor = await db_manager.get_executor_profile(message.from_user.id)
    status_normalized = (executor.profile_status or "").strip().lower() if executor else ""
    if not executor or status_normalized != "активен":
        await message.answer("❌ Редактирование недоступно. Ваш профиль не активен.")
        await state.clear()
        return
    
    position = (message.text or "").strip()
    if not position:
        await message.answer("❌ Должность не может быть пустой. Укажите текст.")
        return
    if len(position) > 100:
        await message.answer("❌ Слишком длинное значение. Максимум 100 символов.")
        return
    try:
        await db_manager.update_executor_profile(message.from_user.id, position_role=position)
        await state.clear()
        await message.answer(
            f"✅ Должность обновлена.\nНовое значение: {position}",
            reply_markup=get_executor_main_menu_keyboard()
        )
        logger.info(f"Executor {message.from_user.id} updated position")
    except Exception as e:
        logger.error(f"Error updating executor position: {e}", exc_info=True)
        await message.answer("❌ Не удалось обновить должность.")
        await state.clear()


@router.callback_query(F.data == "exec_edit_concepts")
async def exec_edit_concepts_start(callback_query: CallbackQuery, state: FSMContext):
    executor = await _ensure_executor_profile(callback_query.from_user.id, callback_query.message, require_active=True)
    if not executor:
        await callback_query.answer("❌ Редактирование недоступно", show_alert=True)
        return
    current = executor.serving_franchise_groups or []
    await state.update_data(concept_selection=current)
    await callback_query.message.edit_text(
        "🏢 Выберите концепции, в которых вы работаете.\n"
        "Нажимайте на кнопки, чтобы отметить/снять выбор. Минимум одна концепция.",
        reply_markup=_build_concepts_keyboard(current)
    )
    await state.set_state(ExecutorProfileEdit.editing_concepts)
    await callback_query.answer()


@router.callback_query(ExecutorProfileEdit.editing_concepts, F.data.startswith("exec_toggle_concept:"))
async def exec_toggle_concept(callback_query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = list(data.get("concept_selection") or [])
    cid = int(callback_query.data.split(":")[1])
    if cid not in FRANCHISE_GROUPS:
        await callback_query.answer("Недопустимая концепция", show_alert=True)
        return
    if cid in selected:
        selected.remove(cid)
    else:
        selected.append(cid)
    await state.update_data(concept_selection=selected)
    await callback_query.message.edit_text(
        "🏢 Выберите концепции, в которых вы работаете.\n"
        "Нажимайте на кнопки, чтобы отметить/снять выбор. Минимум одна концепция.",
        reply_markup=_build_concepts_keyboard(selected)
    )
    await callback_query.answer()


@router.callback_query(ExecutorProfileEdit.editing_concepts, F.data == "exec_concepts_done")
async def exec_concepts_done(callback_query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    concept_ids = data.get("concept_selection") or []
    if not concept_ids:
        await callback_query.answer("Выберите минимум одну концепцию.", show_alert=True)
        return
    try:
        await db_manager.update_executor_profile(
            callback_query.from_user.id,
            serving_franchise_groups=concept_ids
        )
        await state.clear()
        concept_names = [FRANCHISE_GROUPS[cid]['name'] for cid in concept_ids if cid in FRANCHISE_GROUPS]
        await callback_query.message.edit_text(
            "✅ Концепции обновлены:\n" + "\n".join(f"- {name}" for name in concept_names)
        )
        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_executor_main_menu_keyboard()
        )
        logger.info(f"Executor {callback_query.from_user.id} updated franchise groups to {concept_ids}")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error updating executor concepts: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Не удалось обновить список концепций.")
        await state.clear()
        await callback_query.answer()


@router.callback_query(F.data == "exec_edit_restaurants")
async def exec_edit_restaurants_start(callback_query: CallbackQuery, state: FSMContext):
    executor = await _ensure_executor_profile(callback_query.from_user.id, callback_query.message, require_active=True)
    if not executor:
        await callback_query.answer("❌ Редактирование недоступно", show_alert=True)
        return
    if not executor.serving_franchise_groups:
        await callback_query.message.edit_text("❌ Сначала укажите концепции, затем выберите рестораны.")
        await callback_query.answer()
        return
    restaurants_map = await _load_restaurant_map(executor.serving_franchise_groups)
    if not restaurants_map:
        await callback_query.message.edit_text("❌ Не удалось загрузить список ресторанов.")
        await callback_query.answer()
        return
    selected_ids = _extract_restaurant_ids(executor.serving_restaurants)
    await state.update_data(
        exec_available_restaurants=restaurants_map,
        exec_restaurant_selection=selected_ids
    )
    await callback_query.message.edit_text(
        "🏪 Выберите рестораны, которые вы обслуживаете.\n"
        "Нажимайте на кнопки, чтобы отметить/снять выбор. Минимум один ресторан.",
        reply_markup=_build_restaurants_keyboard(restaurants_map, selected_ids)
    )
    await state.set_state(ExecutorProfileEdit.editing_restaurants)
    await callback_query.answer()


@router.callback_query(ExecutorProfileEdit.editing_restaurants, F.data.startswith("exec_toggle_restaurant:"))
async def exec_toggle_restaurant(callback_query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = list(data.get("exec_restaurant_selection") or [])
    restaurants_map = data.get("exec_available_restaurants") or {}
    try:
        rid = int(callback_query.data.split(":")[1])
    except Exception:
        await callback_query.answer("Некорректный выбор", show_alert=True)
        return
    if rid not in restaurants_map:
        await callback_query.answer("Ресторан не найден", show_alert=True)
        return
    if rid in selected:
        selected.remove(rid)
    else:
        selected.append(rid)
    await state.update_data(exec_restaurant_selection=selected)
    await callback_query.message.edit_text(
        "🏪 Выберите рестораны, которые вы обслуживаете.\n"
        "Нажимайте на кнопки, чтобы отметить/снять выбор. Минимум один ресторан.",
        reply_markup=_build_restaurants_keyboard(restaurants_map, selected)
    )
    await callback_query.answer()


@router.callback_query(ExecutorProfileEdit.editing_restaurants, F.data == "exec_restaurants_done")
async def exec_restaurants_done(callback_query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get("exec_restaurant_selection") or []
    restaurants_map = data.get("exec_available_restaurants") or {}
    if not selected:
        await callback_query.answer("Выберите минимум один ресторан.", show_alert=True)
        return
    payload = []
    display_names = []
    for rid in selected:
        name = restaurants_map.get(rid)
        if not name:
            name = restaurants_map.get(str(rid), f"Ресторан #{rid}")
        payload.append({"id": rid, "name": name})
        display_names.append(name)
    try:
        await db_manager.update_executor_profile(
            callback_query.from_user.id,
            serving_restaurants=payload
        )
        await state.clear()
        await callback_query.message.edit_text(
            "✅ Рестораны обновлены:\n" + "\n".join(f"- {name}" for name in display_names)
        )
        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_executor_main_menu_keyboard()
        )
        logger.info(f"Executor {callback_query.from_user.id} updated restaurants to {selected}")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error updating executor restaurants: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Не удалось обновить список ресторанов.")
        await state.clear()
        await callback_query.answer()


@router.callback_query(F.data == "exec_edit_direction")
async def exec_edit_direction_start(callback_query: CallbackQuery, state: FSMContext):
    executor = await _ensure_executor_profile(callback_query.from_user.id, callback_query.message, require_active=True)
    if not executor:
        await callback_query.answer("❌ Редактирование недоступно", show_alert=True)
        return
    await callback_query.message.edit_text(
        "🧭 Выберите направление, в котором вы работаете:",
        reply_markup=get_executor_direction_keyboard(include_cancel=True)
    )
    await state.set_state(ExecutorProfileEdit.editing_direction)
    await callback_query.answer()


@router.callback_query(ExecutorProfileEdit.editing_direction, F.data.startswith("exec_dir:"))
async def exec_edit_direction_process(callback_query: CallbackQuery, state: FSMContext):
    direction = callback_query.data.split(":")[1]
    if direction not in ("it", "se"):
        await callback_query.answer("Недопустимое направление", show_alert=True)
        return
    try:
        await db_manager.update_executor_profile(
            callback_query.from_user.id,
            service_direction=direction
        )
        await state.clear()
        await callback_query.message.edit_text(
            f"✅ Направление обновлено: {DIRECTION_LABELS.get(direction, direction)}"
        )
        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_executor_main_menu_keyboard()
        )
        logger.info(f"Executor {callback_query.from_user.id} updated direction to {direction}")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error updating executor direction: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Не удалось обновить направление.")
        await state.clear()
        await callback_query.answer()


@router.callback_query(F.data == "exec_cancel_edit")
async def exec_cancel_edit(callback_query: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback_query.message.edit_text("❌ Редактирование отменено.")
    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=get_executor_main_menu_keyboard()
    )
    await callback_query.answer()
