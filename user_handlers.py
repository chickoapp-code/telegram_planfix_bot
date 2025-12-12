"""
Обработчики команд пользователей
Версия: 3.0 
"""

import logging
import re
import json
import asyncio
from datetime import datetime
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, ContentType
from aiogram.fsm.context import FSMContext

from states import RoleSelection, UserRegistration, ExecutorRegistration, TicketCreation, StatusInquiry, CommentFlow, ProfileEdit, TaskCancellation
from keyboards import (
    get_phone_number_keyboard, 
    create_dynamic_keyboard, 
    get_main_menu_keyboard, 
    get_skip_or_done_keyboard, 
    get_task_actions_keyboard,
    get_profile_edit_keyboard,
    get_cancel_keyboard,
    get_confirmation_keyboard,
    get_role_selection_keyboard,
    get_executor_main_menu_keyboard,
    create_tasks_keyboard,
    get_task_action_keyboard
)
from services.db_service import db_manager
from services.status_registry import StatusKey, require_status_id, ensure_status_registry_loaded
from planfix_client import planfix_client
from config import (
    PLANFIX_TASK_PROCESS_ID,
    CUSTOM_FIELD_RESTAURANT_ID,
    CUSTOM_FIELD_CONTACT_ID,
    CUSTOM_FIELD_PHONE_ID,
    CUSTOM_FIELD_TYPE_ID,
    CUSTOM_FIELD_MOBILE_PHONE_ID,
    DIRECTORY_RESTAURANTS_ID,
    get_available_templates,
    get_template_info,
    get_template_direction,
    get_direction_tag,
    FRANCHISE_GROUPS,
    get_contacts_by_group,
)

logger = logging.getLogger(__name__)
router = Router()

# Простой кэш для отслеживания последних проверенных комментариев
# Формат: {task_id: {user_id: last_comment_id}}
_last_checked_comments = {}

async def _check_comments_for_task(task_id: int, user_id: int, bot):
    """Проверяет новые комментарии для задачи и отправляет уведомления пользователю."""
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
        
        # Получаем последний проверенный комментарий для этого пользователя
        last_checked = _last_checked_comments.get(task_id, {}).get(user_id)
        
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
                
                # Отправляем уведомление только пользователю (заявителю)
                await notification_service.notify_new_comment(
                    task_id=task_id,
                    comment_author=comment_author,
                    comment_text=comment_text,
                    comment_id=comment_id,
                    recipients="user"
                )
            
            # Обновляем последний проверенный комментарий
            latest_id = new_comments[0].get('id')
            if isinstance(latest_id, str) and ':' in latest_id:
                latest_id = int(latest_id.split(':')[-1])
            elif not isinstance(latest_id, int):
                latest_id = int(latest_id)
            
            if task_id not in _last_checked_comments:
                _last_checked_comments[task_id] = {}
            _last_checked_comments[task_id][user_id] = latest_id
    except Exception as e:
        logger.error(f"Error checking comments for task {task_id}: {e}", exc_info=True)

async def get_user_tasks(user_id: int, limit: int = 10, only_active: bool = False):
    """Получает список заявок пользователя, только созданных через бота.
    
    ОПТИМИЗАЦИЯ: Использует TaskCache вместо API запросов для ускорения работы.
    
    Args:
        user_id: ID пользователя в Telegram
        limit: Максимальное количество заявок
        only_active: Если True, возвращает только активные заявки (не завершенные)
    """
    # #region agent log
    import time
    perf_start = time.time()
    log_path = r"b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log"
    import json as json_module
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json_module.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"CACHE_GET_TASKS","location":"user_handlers.py:142","message":"get_user_tasks starting (using cache)","data":{"user_id":user_id,"limit":limit,"only_active":only_active},"timestamp":int(time.time()*1000)})+"\n")
    except: pass
    # #endregion
    
    try:
        # Проверяем, что пользователь существует
        user = await db_manager.get_user_profile(user_id)
        if not user:
            return None

        # ОПТИМИЗАЦИЯ: Получаем задачи из TaskCache вместо API запросов
        cached_tasks = await db_manager.run(
            db_manager.get_user_tasks_from_cache,
            user_id,
            limit * 2  # Берем больше для фильтрации
        )
        
        logger.info(f"Found {len(cached_tasks)} tasks in cache for user {user_id}")
        
        if not cached_tasks:
            logger.info(f"No tasks found in cache for user {user_id}")
            return []

        # Преобразуем TaskCache в формат, совместимый с API ответом
        tasks = []
        for cached_task in cached_tasks:
            task_dict = {
                'id': cached_task.task_id,
                'name': cached_task.name or 'Без названия',
                'status': {
                    'id': cached_task.status_id,
                    'name': cached_task.status_name or 'Неизвестно'
                },
                'counterparty': {
                    'id': cached_task.counterparty_id
                } if cached_task.counterparty_id else {},
                'dateOfLastUpdate': cached_task.date_of_last_update.isoformat() if cached_task.date_of_last_update else None
            }
            tasks.append(task_dict)
        
        # Фильтруем только активные заявки если требуется
        if only_active:
            try:
                from planfix_client import planfix_client
                final_status_ids = await planfix_client.get_terminal_status_ids(PLANFIX_TASK_PROCESS_ID)
                terminal_status_names = {
                    'завершенная', 'завершенное', 'завершена', 'завершено',
                    'completed', 'done', 'finished',
                    'отмененная', 'отмененное', 'отменена', 'отменено', 'отмена',
                    'canceled', 'cancelled',
                    'отклоненная', 'отклоненное', 'отклонена', 'отклонено',
                    'rejected'
                }
                
                def normalize_status_id(sid):
                    if isinstance(sid, str) and ':' in sid:
                        try:
                            return int(sid.split(':')[1])
                        except ValueError:
                            return None
                    try:
                        return int(sid) if sid is not None else None
                    except (TypeError, ValueError):
                        return None
                
                active_tasks = []
                for t in tasks:
                    status_id = normalize_status_id(t.get('status', {}).get('id'))
                    status_name = t.get('status', {}).get('name', 'Неизвестно')
                    status_name_lower = status_name.lower().strip() if status_name else ''
                    
                    is_terminal = False
                    if status_id is not None and status_id in final_status_ids:
                        is_terminal = True
                    elif status_name_lower in terminal_status_names:
                        is_terminal = True
                    else:
                        for terminal_keyword in ['отмен', 'завершен', 'cancel', 'completed', 'finished', 'rejected', 'отклонен']:
                            if terminal_keyword in status_name_lower:
                                is_terminal = True
                                break
                    
                    if not is_terminal:
                        active_tasks.append(t)
                
                tasks = active_tasks
                logger.info(f"Filtered active tasks: {len(active_tasks)} active out of {len(cached_tasks)} total")
            except Exception as e:
                logger.error(f"Error filtering active tasks: {e}", exc_info=True)
        
        # Сортируем по dateOfLastUpdate (новые сверху)
        def get_sort_key(task):
            date_val = task.get('dateOfLastUpdate', '')
            if isinstance(date_val, dict):
                return date_val.get('value', '') or date_val.get('timestamp', '') or ''
            return date_val or ''
        
        tasks.sort(key=get_sort_key, reverse=True)
        tasks = tasks[:limit]  # Применяем лимит после сортировки
        
        # #region agent log
        perf_duration = (time.time() - perf_start) * 1000
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json_module.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"CACHE_GET_TASKS","location":"user_handlers.py:274","message":"get_user_tasks completed (using cache)","data":{"user_id":user_id,"task_count":len(tasks),"duration_ms":perf_duration},"timestamp":int(time.time()*1000)})+"\n")
        except: pass
        # #endregion
        
        return tasks
        
        # Обновляем статусы в tracked_tasks для ускорения синхронизации
        # (статусы уже актуальные, так как получаем их напрямую из Planfix)
        
        # Фильтруем только активные заявки если требуется
        if only_active:
            try:
                final_status_ids = await planfix_client.get_terminal_status_ids(PLANFIX_TASK_PROCESS_ID)
                # Расширенный список терминальных статусов (включая все варианты написания)
                terminal_status_names = {
                    'завершенная', 'завершенное', 'завершена', 'завершено',
                    'completed', 'done', 'finished',
                    'отмененная', 'отмененное', 'отменена', 'отменено', 'отмена',
                    'canceled', 'cancelled',
                    'отклоненная', 'отклоненное', 'отклонена', 'отклонено',
                    'rejected'
                }
                
                def normalize_status_id(sid):
                    if isinstance(sid, str) and ':' in sid:
                        try:
                            return int(sid.split(':')[1])
                        except ValueError:
                            return None
                    try:
                        return int(sid) if sid is not None else None
                    except (TypeError, ValueError):
                        return None
                
                active_tasks = []
                for t in tasks:
                    status_id = normalize_status_id(t.get('status', {}).get('id'))
                    status_name = t.get('status', {}).get('name', 'Неизвестно')
                    status_name_lower = status_name.lower().strip() if status_name else ''
                    
                    # Проверяем, является ли статус терминальным
                    is_terminal = False
                    
                    # Проверка по ID статуса
                    if status_id is not None and status_id in final_status_ids:
                        is_terminal = True
                        logger.debug(f"Task {t.get('id')} filtered: status ID {status_id} is in final_status_ids")
                    
                    # Проверка по имени статуса (точное совпадение)
                    if not is_terminal and status_name_lower in terminal_status_names:
                        is_terminal = True
                        logger.debug(f"Task {t.get('id')} filtered: status name '{status_name}' matches terminal status")
                    
                    # Проверка по корню слова (для надежности)
                    if not is_terminal:
                        for terminal_keyword in ['отмен', 'завершен', 'cancel', 'completed', 'finished', 'rejected', 'отклонен']:
                            if terminal_keyword in status_name_lower:
                                is_terminal = True
                                logger.debug(f"Task {t.get('id')} filtered: status name '{status_name}' contains terminal keyword '{terminal_keyword}'")
                                break
                    
                    # Добавляем только если статус НЕ терминальный
                    if not is_terminal:
                        active_tasks.append(t)
                    else:
                        logger.debug(f"Task {t.get('id')} ({status_name}, ID: {status_id}) excluded from active tasks")
                
                tasks = active_tasks
                logger.info(f"Filtered active tasks: {len(active_tasks)} active out of {len(task_results)} total")
            except Exception as e:
                logger.error(f"Error filtering active tasks: {e}", exc_info=True)
                # Если ошибка при фильтрации, возвращаем все задачи
        
        # Сортируем по dateOfLastUpdate (новые сверху)
        # dateOfLastUpdate может быть строкой или словарём, поэтому обрабатываем оба случая
        def get_sort_key(task):
            date_val = task.get('dateOfLastUpdate', '')
            if isinstance(date_val, dict):
                # Если это словарь, пытаемся получить значение из него
                return date_val.get('value', '') or date_val.get('timestamp', '') or ''
            return date_val or ''
        
        tasks.sort(key=get_sort_key, reverse=True)

        return tasks

    except Exception as e:
        logger.error(f"Error getting user tasks: {e}", exc_info=True)
        return None


# ============================================================================
# РЕГИСТРАЦИЯ ПОЛЬЗОВАТЕЛЯ
# ============================================================================

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    """Команда /start - начало работы с ботом."""
    # Проверяем, зарегистрирован ли пользователь как сотрудник
    user = await db_manager.get_user_profile(message.from_user.id)
    executor = await db_manager.get_executor_profile(message.from_user.id)
    
    if user:
        # Пользователь зарегистрирован как сотрудник ресторана
        await message.answer(
            f"👋 <b>Добро пожаловать, {user.full_name}!</b>\n\n"
            f"Вы можете создавать заявки в техподдержку.",
            reply_markup=get_main_menu_keyboard(),
            parse_mode="HTML"
        )
    elif executor:
        # Пользователь зарегистрирован как исполнитель
        if executor.profile_status == "активен":
            await message.answer(
                f"👋 <b>Добро пожаловать, {executor.full_name}!</b>\n\n"
                f"Вы можете просматривать и обрабатывать заявки.",
                reply_markup=get_executor_main_menu_keyboard(),
                parse_mode="HTML"
            )
        elif executor.profile_status == "ожидает подтверждения":
            await message.answer(
                f"👋 <b>Здравствуйте, {executor.full_name}!</b>\n\n"
                f"⏳ <b>Ваша регистрация ожидает подтверждения администратором.</b>\n\n"
                f"🔔 Вы получите уведомление, когда ваш профиль будет активирован.",
                parse_mode="HTML"
            )
        else:
            await message.answer(
                f"👋 <b>Здравствуйте, {executor.full_name}!</b>\n\n"
                f"❌ <b>Ваша регистрация была отклонена.</b>\n\n"
                f"Для получения дополнительной информации обратитесь к администратору.",
                parse_mode="HTML"
            )
    else:
        # Пользователь не зарегистрирован - предлагаем выбрать роль
        await message.answer(
            "👋 <b>Добро пожаловать в службу поддержки!</b>\n\n"
            "Для начала работы выберите вашу роль:",
            reply_markup=get_role_selection_keyboard(),
            parse_mode="HTML"
        )
        await state.set_state(RoleSelection.choosing_role)


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Команда для отмены текущего действия."""
    current_state = await state.get_state()
    
    if current_state is None:
        await message.answer("❌ Нет активных действий для отмены.")
        return
    
    await state.clear()
    
    # Проверяем, зарегистрирован ли пользователь
    user = await db_manager.get_user_profile(message.from_user.id)
    
    if user:
        await message.answer(
            "❌ Действие отменено.",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await message.answer(
            "❌ Регистрация отменена.\n\n"
            "Для начала работы используйте команду /start"
        )


@router.message(F.text == "🔄 Перезапустить бот")
async def restart_bot(message: Message, state: FSMContext):
    """Обработчик кнопки 'Перезапустить бот' - очищает состояние и перезапускает бота."""
    # Очищаем состояние FSM
    await state.clear()
    
    # Вызываем ту же логику, что и /start
    user = await db_manager.get_user_profile(message.from_user.id)
    executor = await db_manager.get_executor_profile(message.from_user.id)
    
    if user:
        # Пользователь зарегистрирован как сотрудник ресторана
        await message.answer(
            f"🔄 <b>Бот перезапущен!</b>\n\n"
            f"👋 <b>Добро пожаловать, {user.full_name}!</b>\n\n"
            f"Вы можете создавать заявки в техподдержку.",
            reply_markup=get_main_menu_keyboard(),
            parse_mode="HTML"
        )
    elif executor:
        # Пользователь зарегистрирован как исполнитель
        if executor.profile_status == "активен":
            await message.answer(
                f"🔄 <b>Бот перезапущен!</b>\n\n"
                f"👋 <b>Добро пожаловать, {executor.full_name}!</b>\n\n"
                f"Вы можете просматривать и обрабатывать заявки.",
                reply_markup=get_executor_main_menu_keyboard(),
                parse_mode="HTML"
            )
        elif executor.profile_status == "ожидает подтверждения":
            await message.answer(
                f"🔄 <b>Бот перезапущен!</b>\n\n"
                f"👋 <b>Здравствуйте, {executor.full_name}!</b>\n\n"
                f"⏳ <b>Ваша регистрация ожидает подтверждения администратором.</b>\n\n"
                f"🔔 Вы получите уведомление, когда ваш профиль будет активирован.",
                parse_mode="HTML"
            )
        else:
            await message.answer(
                f"🔄 <b>Бот перезапущен!</b>\n\n"
                f"👋 <b>Здравствуйте, {executor.full_name}!</b>\n\n"
                f"❌ <b>Ваша регистрация была отклонена.</b>\n\n"
                f"Для получения дополнительной информации обратитесь к администратору.",
                parse_mode="HTML"
            )
    else:
        # Пользователь не зарегистрирован - предлагаем выбрать роль
        await message.answer(
            "🔄 <b>Бот перезапущен!</b>\n\n"
            "👋 <b>Добро пожаловать в службу поддержки!</b>\n\n"
            "Для начала работы выберите вашу роль:",
            reply_markup=get_role_selection_keyboard(),
            parse_mode="HTML"
        )
        await state.set_state(RoleSelection.choosing_role)


@router.callback_query(RoleSelection.choosing_role, F.data == "role_user")
async def role_user_selected(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора роли 'Сотрудник ресторана'."""
    await callback_query.answer()
    await callback_query.message.edit_text(
        "👤 <b>Регистрация сотрудника ресторана</b>\n\n"
        "📝 Пожалуйста, введите ваше <b>ФИО</b>:\n\n"
        "💡 Для отмены используйте команду /cancel",
        parse_mode="HTML"
    )
    await state.set_state(UserRegistration.waiting_for_full_name)


@router.callback_query(RoleSelection.choosing_role, F.data == "role_executor")
async def role_executor_selected(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора роли 'Исполнитель техподдержки'."""
    await callback_query.answer()
    await callback_query.message.edit_text(
        "👷 <b>Регистрация исполнителя техподдержки</b>\n\n"
        "📝 Пожалуйста, введите ваше <b>ФИО</b>:\n\n"
        "💡 Для отмены используйте команду /cancel",
        parse_mode="HTML"
    )
    await state.set_state(ExecutorRegistration.waiting_for_full_name)


@router.message(UserRegistration.waiting_for_full_name)
async def process_full_name(message: Message, state: FSMContext):
    """Обработка ввода ФИО."""
    full_name = message.text.strip()
    
    # Проверка на команду отмены
    if full_name.lower() in ['/cancel', 'отмена']:
        await cmd_cancel(message, state)
        return
    
    if len(full_name) < 3:
        await message.answer(
            "❌ ФИО слишком короткое. Пожалуйста, введите полное ФИО:\n\n"
            "💡 Для отмены регистрации используйте команду /cancel"
        )
        return
    
    await state.update_data(full_name=full_name)
    await message.answer(
        "📱 <b>Отлично!</b> Теперь поделитесь вашим номером телефона.\n\n"
        "Вы можете:\n"
        "• Нажать кнопку ниже для автоматической отправки\n"
        "• Ввести номер вручную (например, +79991234567)\n\n"
        "💡 Для отмены используйте команду /cancel",
        reply_markup=get_phone_number_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(UserRegistration.waiting_for_phone_number)


@router.message(UserRegistration.waiting_for_phone_number, F.contact)
async def process_phone_contact(message: Message, state: FSMContext):
    """Обработка номера телефона через кнопку."""
    phone_number = message.contact.phone_number
    await state.update_data(phone_number=phone_number)
    await show_franchise_selection(message, state)


@router.message(UserRegistration.waiting_for_phone_number, F.text)
async def process_phone_text(message: Message, state: FSMContext):
    """Обработка номера телефона введенного вручную."""
    phone_text = message.text.strip()
    
    # Валидация номера телефона
    normalized = re.sub(r"[^0-9+]", "", phone_text)
    if not normalized or len(re.sub(r"\D", "", normalized)) < 10:
        await message.answer(
            "❌ Некорректный номер телефона.\n\n"
            "Пожалуйста, введите номер в формате +79991234567 или используйте кнопку ниже:",
            reply_markup=get_phone_number_keyboard()
        )
        return
    
    await state.update_data(phone_number=normalized)
    await show_franchise_selection(message, state)


async def show_franchise_selection(message: Message, state: FSMContext):
    """Показывает выбор франчайзи (группы контактов)."""
    try:
        franchise_groups = [
            {"id": gid, "name": data["name"]}
            for gid, data in FRANCHISE_GROUPS.items()
        ]
        if not franchise_groups:
            logger.error("FRANCHISE_GROUPS is empty")
            await message.answer("❌ Не найдены группы франчайзи. Обратитесь к администратору.")
            await state.clear()
            return

        franchise_groups.sort(key=lambda item: item["name"])

        keyboard_items = [(str(group["id"]), group["name"]) for group in franchise_groups]
        keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=True)

        await message.answer(
            "🏢 Выберите вашу концепцию:",
            reply_markup=keyboard
        )
        await state.set_state(UserRegistration.waiting_for_franchise)
        
    except Exception as e:
        logger.error(f"Error loading franchise groups: {e}", exc_info=True)
        await message.answer(
            "❌ Произошла ошибка при загрузке списка концепций. Попробуйте позже."
        )
        await state.clear()


@router.callback_query(UserRegistration.waiting_for_franchise)
async def process_franchise(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора франчайзи."""
    # Проверяем, не нажата ли кнопка отмены
    if callback_query.data == "cancel_registration":
        await state.clear()
        await callback_query.message.edit_text("❌ Регистрация отменена.")
        await callback_query.answer()
        return
    
    franchise_group_id = int(callback_query.data)
    await state.update_data(franchise_group_id=franchise_group_id)
    await callback_query.answer()
    
    try:
        # Получаем контакты из Planfix через API
        contacts = await get_contacts_by_group(planfix_client, franchise_group_id)
        if not contacts:
            logger.warning(f"No contacts found for franchise group {franchise_group_id}")
            
            # Получаем название франчайзи для более информативного сообщения
            franchise_name = FRANCHISE_GROUPS.get(franchise_group_id, {}).get('name', 'выбранной концепции')
            
            await callback_query.message.edit_text(
                f"❌ К сожалению, для концепции \"{franchise_name}\" пока не добавлены рестораны в систему.\n\n"
                f"Пожалуйста, обратитесь к администратору для добавления вашего ресторана или выберите другую концепцию."
            )
            
            # Возвращаем пользователя к выбору франчайзи
            await show_franchise_selection(callback_query.message, state)
            return
        
        # Создаем клавиатуру с ресторанами
        keyboard_items = [
            (str(contact_id), name)
            for contact_id, name in sorted(contacts.items(), key=lambda item: item[1])
        ]
        keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=True)
        
        await callback_query.message.edit_text(
            "🏪 <b>Выберите ваш ресторан:</b>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.set_state(UserRegistration.waiting_for_restaurant)
        
    except Exception as e:
        logger.error(f"Error loading restaurants: {e}", exc_info=True)
        await callback_query.message.answer(
            "❌ Произошла ошибка при загрузке списка ресторанов. Попробуйте позже."
        )
        await state.clear()


@router.callback_query(UserRegistration.waiting_for_restaurant)
async def process_restaurant(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора ресторана и завершение регистрации."""
    # Проверяем, не нажата ли кнопка отмены
    if callback_query.data == "cancel_registration":
        await state.clear()
        await callback_query.message.edit_text("❌ Регистрация отменена.")
        await callback_query.answer()
        return
    
    restaurant_contact_id = int(callback_query.data)
    user_data = await state.get_data()
    
    try:
        # Создаем контакт заявителя в Planfix в группе концепции
        planfix_contact_id = None
        try:
            # Разделяем ФИО на имя и фамилию
            name_parts = user_data['full_name'].strip().split()
            if len(name_parts) >= 2:
                lastname = name_parts[0]
                name = " ".join(name_parts[1:])
            else:
                name = user_data['full_name']
                lastname = user_data['full_name']
            
            # Создаем контакт в группе "Поддержка" с template_id=1
            from config import SUPPORT_CONTACT_GROUP_ID, SUPPORT_CONTACT_TEMPLATE_ID
            
            contact_response = await planfix_client.create_contact(
                name=name,
                lastname=lastname,
                phone=user_data['phone_number'],
                email=user_data.get('email'),
                group_id=SUPPORT_CONTACT_GROUP_ID,  # Группа "Поддержка"
                template_id=SUPPORT_CONTACT_TEMPLATE_ID  # Template ID 1
            )
            
            if contact_response and contact_response.get('result') == 'success':
                contact_id = contact_response.get('id') or contact_response.get('contact', {}).get('id')
                if contact_id:
                    # Нормализуем ID контакта
                    if isinstance(contact_id, str) and ':' in contact_id:
                        planfix_contact_id = contact_id.split(':')[-1]
                    else:
                        planfix_contact_id = str(contact_id)
                    logger.info(f"Created Planfix contact {planfix_contact_id} for user {callback_query.from_user.id}")
            else:
                logger.warning(f"Failed to create Planfix contact for user {callback_query.from_user.id}: {contact_response}")
        except Exception as e:
            logger.error(f"Error creating Planfix contact for user {callback_query.from_user.id}: {e}", exc_info=True)
            # Продолжаем регистрацию даже если не удалось создать контакт
        
        # Сохраняем профиль в БД
        await db_manager.create_user_profile(
            telegram_id=callback_query.from_user.id,
            full_name=user_data['full_name'],
            phone_number=user_data['phone_number'],
            franchise_group_id=user_data['franchise_group_id'],
            restaurant_contact_id=restaurant_contact_id,
            restaurant_directory_key=None,  # Будет заполнено при первом создании задачи
            planfix_contact_id=planfix_contact_id,  # ID контакта в Planfix
        )
        await state.clear()
        await callback_query.message.edit_text(
            "✅ <b>Регистрация завершена!</b>\n\n"
            f"👤 <b>ФИО:</b> {user_data['full_name']}\n"
            f"📱 <b>Телефон:</b> {user_data['phone_number']}\n\n"
            "🎉 Теперь вы можете создавать заявки в техподдержку.",
            parse_mode="HTML"
        )
        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_main_menu_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Error during registration: {e}", exc_info=True)
        await callback_query.message.answer(
            "❌ Произошла ошибка при регистрации. Попробуйте позже или обратитесь к администратору."
        )
        await state.clear()


@router.callback_query(F.data == "cancel_registration")
async def cancel_registration(callback_query: CallbackQuery, state: FSMContext):
    """Отмена регистрации."""
    await state.clear()
    await callback_query.message.edit_text("❌ Регистрация отменена.")
    await callback_query.answer()


# ============================================================================
# СОЗДАНИЕ ЗАЯВКИ
# ============================================================================

@router.message(F.text == "📝 Создать заявку")
async def start_create_ticket(message: Message, state: FSMContext):
    """Начало создания заявки."""
    logger.info(f"Handler 'start_create_ticket' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    # Проверяем регистрацию
    user = await db_manager.get_user_profile(message.from_user.id)
    
    if not user:
        await message.answer(
            "❌ Сначала пройдите регистрацию.\n\n"
            "Используйте команду /start"
        )
        return
    
    try:
        # Получаем доступные шаблоны для пользователя
        templates = get_available_templates(
            user.franchise_group_id,
            user.restaurant_contact_id
        )
        
        if not templates:
            await message.answer(
                "❌ Для вашего ресторана нет доступных шаблонов заявок.\n\n"
                "Обратитесь к администратору."
            )
            return
        
        # Создаем клавиатуру с шаблонами (используем уникальные full_name для избежания дубликатов)
        keyboard_items = [(str(t['id']), t['full_name']) for t in templates]
        keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=True)
        
        await state.update_data(available_templates=templates)
        await message.answer(
            "📋 <b>Выберите тип запроса:</b>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.set_state(TicketCreation.choosing_template)
        
    except Exception as e:
        logger.error(f"Error starting ticket creation: {e}", exc_info=True)
        await message.answer(
            "❌ Произошла ошибка. Попробуйте позже."
        )


@router.callback_query(TicketCreation.choosing_template)
async def choose_template(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора шаблона."""
    template_id = int(callback_query.data)
    await state.update_data(template_id=template_id)
    await callback_query.answer()
    
    # Получаем информацию о шаблоне
    template_info = get_template_info(template_id)
    template_name = template_info.get('name', 'Заявка') if template_info else 'Заявка'
    
    await callback_query.message.edit_text(
        f"📝 Шаблон: {template_name}\n\n"
        "Опишите проблему подробно:"
    )
    await state.set_state(TicketCreation.entering_description)


@router.message(TicketCreation.entering_description, F.content_type.in_({ContentType.PHOTO, ContentType.VIDEO, ContentType.VIDEO_NOTE}))
async def enter_description_with_media(message: Message, state: FSMContext):
    """Обработка описания проблемы с фото/видео в одном сообщении."""
    # Получаем текст из подписи к медиа или из предыдущего сообщения
    description = message.caption or ""
    description = description.strip()
    
    # Определяем тип медиа и получаем file_id
    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = "photo"
        default_filename = "photo.jpg"
    elif message.video:
        file_id = message.video.file_id
        media_type = "video"
        # Используем оригинальное имя файла, если есть, иначе дефолтное
        default_filename = message.video.file_name or f"video_{file_id}.mp4"
    elif message.video_note:
        file_id = message.video_note.file_id
        media_type = "video_note"
        default_filename = "video_note.mp4"
    else:
        await message.answer("❌ Не удалось определить тип медиа файла.")
        return
    
    # Если описания нет в подписи, просим ввести его отдельно
    if not description or len(description) < 10:
        # Сохраняем file_id медиа для последующей обработки
        await state.update_data(has_media=True, media_file_id=file_id, media_type=media_type)
        media_name = "Фото" if media_type == "photo" else "Видео"
        await message.answer(
            f"📷 <b>{media_name} получено!</b>\n\n"
            "Теперь опишите проблему подробно (минимум 10 символов):"
        )
        # Остаемся в том же состоянии, чтобы получить описание
        return
    
    # Если описание есть в подписи, обрабатываем медиа и создаем заявку
    try:
        tg_file = await message.bot.get_file(file_id)
        file_bytes = await message.bot.download_file(tg_file.file_path)
        
        # Загружаем в Planfix
        upload_response = await planfix_client.upload_file(file_bytes, filename=default_filename)
        
        if upload_response and upload_response.get('result') == 'success':
            planfix_file_id = upload_response.get('id')
            # Нормализуем ID файла: убираем префикс если есть (например "file:4450782" -> 4450782)
            if isinstance(planfix_file_id, str) and ':' in planfix_file_id:
                try:
                    planfix_file_id = int(planfix_file_id.split(':')[-1])
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse file_id: {planfix_file_id}")
                    planfix_file_id = None
            elif isinstance(planfix_file_id, (int, float)):
                planfix_file_id = int(planfix_file_id)
            else:
                logger.warning(f"Unexpected file_id type: {type(planfix_file_id)}, value: {planfix_file_id}")
                planfix_file_id = None
            
            if planfix_file_id:
                await state.update_data(description=description, files=[planfix_file_id])
                media_name = "фото" if media_type == "photo" else "видео"
                logger.info(f"Uploaded {media_name} {planfix_file_id} to Planfix with description")
            else:
                await state.update_data(description=description)
                await message.answer("⚠️ Не удалось загрузить медиа файл, но заявка будет создана без него.")
        else:
            logger.warning("Failed to upload file to Planfix")
            await state.update_data(description=description)
            await message.answer("⚠️ Не удалось загрузить медиа файл, но заявка будет создана без него.")
        
        # Создаем заявку сразу
        await finalize_create_task(message, state, message.from_user.id)
        
    except Exception as e:
        logger.error(f"Error uploading media: {e}", exc_info=True)
        await state.update_data(description=description)
        await message.answer("⚠️ Ошибка при загрузке медиа файла, но заявка будет создана без него.")
        await finalize_create_task(message, state, message.from_user.id)


@router.message(TicketCreation.entering_description)
async def enter_description(message: Message, state: FSMContext):
    """Обработка описания проблемы (только текст)."""
    # Проверяем, есть ли уже медиа в состоянии (если пользователь отправил медиа без подписи)
    state_data = await state.get_data()
    if state_data.get('has_media') and state_data.get('media_file_id'):
        # Пользователь отправил медиа без подписи, а теперь отправляет описание
        description = message.text.strip()
        
        if len(description) < 10:
            await message.answer(
                "❌ Описание слишком короткое.\n\n"
                "Пожалуйста, опишите проблему подробнее (минимум 10 символов):"
            )
            return
        
        # Обрабатываем медиа из предыдущего сообщения
        media_file_id = state_data['media_file_id']
        media_type = state_data.get('media_type', 'photo')
        
        # Определяем имя файла по умолчанию
        if media_type == "photo":
            default_filename = "photo.jpg"
        elif media_type == "video":
            default_filename = f"video_{media_file_id}.mp4"
        elif media_type == "video_note":
            default_filename = "video_note.mp4"
        else:
            default_filename = "file"
        
        try:
            tg_file = await message.bot.get_file(media_file_id)
            file_bytes = await message.bot.download_file(tg_file.file_path)
            
            # Загружаем в Planfix
            upload_response = await planfix_client.upload_file(file_bytes, filename=default_filename)
            
            if upload_response and upload_response.get('result') == 'success':
                planfix_file_id = upload_response.get('id')
                # Нормализуем ID файла
                if isinstance(planfix_file_id, str) and ':' in planfix_file_id:
                    try:
                        planfix_file_id = int(planfix_file_id.split(':')[-1])
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse file_id: {planfix_file_id}")
                        planfix_file_id = None
                elif isinstance(planfix_file_id, (int, float)):
                    planfix_file_id = int(planfix_file_id)
                else:
                    planfix_file_id = None
                
                if planfix_file_id:
                    await state.update_data(description=description, files=[planfix_file_id], has_media=None, media_file_id=None, media_type=None)
                    media_name = "фото" if media_type == "photo" else "видео"
                    logger.info(f"Uploaded {media_name} {planfix_file_id} to Planfix with description")
                else:
                    await state.update_data(description=description, has_media=None, media_file_id=None, media_type=None)
                    await message.answer("⚠️ Не удалось загрузить медиа файл, но заявка будет создана без него.")
            else:
                await state.update_data(description=description, has_media=None, media_file_id=None, media_type=None)
                await message.answer("⚠️ Не удалось загрузить медиа файл, но заявка будет создана без него.")
            
            # Создаем заявку сразу
            await finalize_create_task(message, state, message.from_user.id)
            return
            
        except Exception as e:
            logger.error(f"Error uploading media: {e}", exc_info=True)
            await state.update_data(description=description, has_media=None, media_file_id=None, media_type=None)
            await message.answer("⚠️ Ошибка при загрузке медиа файла, но заявка будет создана без него.")
            await finalize_create_task(message, state, message.from_user.id)
            return
    
    # Обычный случай: только текст без фото
    description = message.text.strip()
    
    if len(description) < 10:
        await message.answer(
            "❌ Описание слишком короткое.\n\n"
            "Пожалуйста, опишите проблему подробнее (минимум 10 символов):"
        )
        return
    
    await state.update_data(description=description)
    await message.answer(
        "📷 <b>Прикрепите фото или видео проблемы</b> (если есть)\n\n"
        "Или нажмите кнопку <b>«Пропустить»</b> для продолжения без файлов:",
        reply_markup=get_skip_or_done_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(TicketCreation.attaching_photo)


@router.callback_query(TicketCreation.attaching_photo, F.data == "skip_file")
async def skip_file(callback_query: CallbackQuery, state: FSMContext):
    """Пропуск прикрепления фото."""
    await callback_query.answer()
    await finalize_create_task(callback_query.message, state, callback_query.from_user.id)


@router.message(TicketCreation.attaching_photo, F.content_type.in_({ContentType.PHOTO, ContentType.VIDEO, ContentType.VIDEO_NOTE}))
async def receive_media(message: Message, state: FSMContext):
    """Обработка прикрепленного фото/видео."""
    try:
        # Определяем тип медиа и получаем file_id
        if message.photo:
            file_id = message.photo[-1].file_id
            media_type = "photo"
            default_filename = "photo.jpg"
        elif message.video:
            file_id = message.video.file_id
            media_type = "video"
            default_filename = message.video.file_name or f"video_{file_id}.mp4"
        elif message.video_note:
            file_id = message.video_note.file_id
            media_type = "video_note"
            default_filename = "video_note.mp4"
        else:
            await message.answer("❌ Не удалось определить тип медиа файла.")
            return
        
        tg_file = await message.bot.get_file(file_id)
        file_bytes = await message.bot.download_file(tg_file.file_path)
        
        # Загружаем в Planfix
        upload_response = await planfix_client.upload_file(file_bytes, filename=default_filename)
        
        if upload_response and upload_response.get('result') == 'success':
            planfix_file_id = upload_response.get('id')
            # Нормализуем ID файла: убираем префикс если есть (например "file:4450782" -> 4450782)
            if isinstance(planfix_file_id, str) and ':' in planfix_file_id:
                try:
                    planfix_file_id = int(planfix_file_id.split(':')[-1])
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse file_id: {planfix_file_id}")
            elif isinstance(planfix_file_id, (int, float)):
                planfix_file_id = int(planfix_file_id)
            else:
                logger.warning(f"Unexpected file_id type: {type(planfix_file_id)}, value: {planfix_file_id}")
                planfix_file_id = None
            
            if planfix_file_id:
                await state.update_data(files=[planfix_file_id])
                media_name = "фото" if media_type == "photo" else "видео"
                logger.info(f"Uploaded {media_name} {planfix_file_id} to Planfix")
        else:
            logger.warning("Failed to upload file to Planfix")
            await message.answer("⚠️ Не удалось загрузить медиа файл, но заявка будет создана без него.")
        
        await finalize_create_task(message, state, message.from_user.id)
        
    except Exception as e:
        logger.error(f"Error uploading media: {e}", exc_info=True)
        await message.answer("⚠️ Ошибка при загрузке медиа файла, но заявка будет создана без него.")
        await finalize_create_task(message, state, message.from_user.id)


async def finalize_create_task(message: Message, state: FSMContext, user_id: int):
    """Финализация создания задачи в Planfix."""
    # Защита от дублирования: проверяем, не создается ли уже задача
    task_creation_key = f"task_creation:{user_id}"
    if hasattr(finalize_create_task, '_in_progress'):
        if finalize_create_task._in_progress.get(user_id, False):
            logger.warning(f"Task creation already in progress for user {user_id}, skipping duplicate call")
            return
    else:
        finalize_create_task._in_progress = {}
    
    finalize_create_task._in_progress[user_id] = True
    
    # #region agent log
    import json, os, time
    log_path = r"b:\telegram_planfix_bot\telegram_planfix_bot\.cursor\debug.log"
    perf_start = time.time()
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"PERF1","location":"user_handlers.py:1023","message":"finalize_create_task started","data":{"user_id":user_id},"timestamp":int(time.time()*1000)})+"\n")
    except: pass
    # #endregion
    try:
        user_data = await state.get_data()
        template_id = user_data.get('template_id')
        description = user_data.get('description')
        files = user_data.get('files', [])
        
        # Получаем профиль пользователя
        perf_step = time.time()
        user = await db_manager.get_user_profile(user_id)
        # #region agent log
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"PERF1","location":"user_handlers.py:1030","message":"get_user_profile completed","data":{"user_id":user_id,"duration_ms":(time.time()-perf_step)*1000},"timestamp":int(time.time()*1000)})+"\n")
        except: pass
        # #endregion
        
        if not user:
            await message.answer("❌ Профиль не найден. Пройдите регистрацию: /start")
            await state.clear()
            return
        
        try:
            # Получаем информацию о шаблоне
            template_info = get_template_info(template_id)
            if not template_info:
                await message.answer("❌ Шаблон не найден. Попробуйте снова.")
                await state.clear()
                return
            
            # Получаем restaurant_directory_key если его нет
            restaurant_directory_key = user.restaurant_directory_key
            if not restaurant_directory_key:
                if DIRECTORY_RESTAURANTS_ID:
                    # Пытаемся найти ключ в локальном кеше справочника, если он синхронизирован
                    try:
                        entries = await db_manager.get_directory_entries_by_directory_id(DIRECTORY_RESTAURANTS_ID)
                        for entry in entries:
                            if entry.key == str(user.restaurant_contact_id):
                                restaurant_directory_key = entry.key
                                logger.info(
                                    "Found directory key %s for restaurant %s",
                                    restaurant_directory_key,
                                    user.restaurant_contact_id,
                                )
                                break
                    except Exception as e:
                        logger.error(f"Error getting directory key from directory {DIRECTORY_RESTAURANTS_ID}: {e}")

            # Фолбэк: используем ID контакта ресторана как ключ (подходит для справочника с ключами=ID)
            if not restaurant_directory_key:
                restaurant_directory_key = str(user.restaurant_contact_id)
                logger.warning(
                    "Using contact_id %s as directory key (directory lookup unavailable)",
                    user.restaurant_contact_id,
                )
            
            # Формируем название задачи
            task_name = f"Запрос через бот: {description[:50]}..."
            
            # Формируем описание
            task_description = f"""Заявитель: {user.full_name}
Телефон: {user.phone_number}

Описание проблемы:
{description}

Создано через Telegram бот"""
            
            # Формируем кастомные поля
            # ВАЖНО: Для разных типов полей нужны разные форматы значений:
            # - Directory entry (type 9): передаём ключ записи справочника
            # - Contact (type 10): передаём ID контакта (число или строка)
            # - Text (type 2): передаём строку
            # - Phone (type 1): передаём строку с номером
            type_field_value = f"Запрос через Telegram бот #{user_id}-{int(datetime.utcnow().timestamp())}"

            # Формируем кастомные поля согласно swagger.json
            # Directory entry (type 9): {"id": 2} или {"id": "5"}
            # Contact (type 10): {"id": "contact:5"} или {"id": 5}
            # Пробуем преобразовать ключ справочника в число, если возможно
            try:
                directory_key_value = int(restaurant_directory_key) if restaurant_directory_key else None
            except (ValueError, TypeError):
                directory_key_value = restaurant_directory_key
            
            # Формируем кастомные поля
            custom_field_data = [
                {
                    "field": {"id": CUSTOM_FIELD_RESTAURANT_ID},
                    "value": {"id": directory_key_value}  # Объект с ключом записи справочника (Directory entry type 9)
                },
                {
                    "field": {"id": CUSTOM_FIELD_CONTACT_ID},
                    "value": {"id": int(user.restaurant_contact_id)}  # Объект с ID контакта ресторана (Contact type 10)
                },
                {
                    "field": {"id": CUSTOM_FIELD_PHONE_ID},
                    "value": user.phone_number  # Строка
                },
                {
                    "field": {"id": CUSTOM_FIELD_MOBILE_PHONE_ID},
                    "value": user.phone_number  # Строка
                },
                {
                    "field": {"id": CUSTOM_FIELD_TYPE_ID},
                    "value": type_field_value  # Значение должно быть уникальным
                }
            ]
            
            # Заменяем контакт ресторана на контакт заявителя в поле "Контакт"
            # Если контакт заявителя не создан в Planfix, создаем его
            user_contact_id = None
            if user.planfix_contact_id:
                try:
                    user_contact_id = int(user.planfix_contact_id)
                    logger.info(f"Using existing Planfix contact {user_contact_id} for user {user_id}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid planfix_contact_id for user {user_id}: {e}")
            
            # Если контакт не создан, пытаемся создать его
            if not user_contact_id:
                perf_step = time.time()
                try:
                    logger.info(f"Creating Planfix contact for user {user_id} (contact not found)")
                    # Разделяем ФИО на имя и фамилию
                    name_parts = user.full_name.strip().split()
                    if len(name_parts) >= 2:
                        lastname = name_parts[0]
                        name = " ".join(name_parts[1:])
                    else:
                        name = user.full_name
                        lastname = user.full_name
                    
                    # Создаем контакт в группе "Поддержка" с template_id=1
                    from config import SUPPORT_CONTACT_GROUP_ID, SUPPORT_CONTACT_TEMPLATE_ID
                    
                    contact_response = await planfix_client.create_contact(
                        name=name,
                        lastname=lastname,
                        phone=user.phone_number,
                        email=user.email,
                        group_id=SUPPORT_CONTACT_GROUP_ID,  # Группа "Поддержка"
                        template_id=SUPPORT_CONTACT_TEMPLATE_ID  # Template ID 1
                    )
                    
                    if contact_response and contact_response.get('result') == 'success':
                        contact_id = contact_response.get('id') or contact_response.get('contact', {}).get('id')
                        if contact_id:
                            # Нормализуем ID контакта
                            if isinstance(contact_id, str) and ':' in contact_id:
                                user_contact_id = int(contact_id.split(':')[-1])
                            else:
                                user_contact_id = int(contact_id)
                            
                            # Сохраняем ID контакта в профиль пользователя
                            from db_manager import DBManager
                            sync_db_manager = DBManager()
                            with sync_db_manager.get_db() as db:
                                sync_db_manager.update_user_profile(
                                    db=db,
                                    telegram_id=user_id,
                                    planfix_contact_id=str(user_contact_id)
                                )
                            logger.info(f"Created and saved Planfix contact {user_contact_id} for user {user_id}")
                    else:
                        logger.warning(f"Failed to create Planfix contact for user {user_id}: {contact_response}")
                    # #region agent log
                    try:
                        with open(log_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"PERF2","location":"user_handlers.py:1177","message":"create_contact completed","data":{"user_id":user_id,"duration_ms":(time.time()-perf_step)*1000},"timestamp":int(time.time()*1000)})+"\n")
                    except: pass
                    # #endregion
                except Exception as e:
                    logger.error(f"Error creating Planfix contact for user {user_id}: {e}", exc_info=True)
                    # #region agent log
                    try:
                        with open(log_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"PERF2","location":"user_handlers.py:1181","message":"create_contact failed","data":{"user_id":user_id,"error":str(e),"duration_ms":(time.time()-perf_step)*1000},"timestamp":int(time.time()*1000)})+"\n")
                    except: pass
                    # #endregion
            
            # Заменяем значение поля CUSTOM_FIELD_CONTACT_ID на контакт заявителя
            if user_contact_id:
                for field in custom_field_data:
                    if field.get('field', {}).get('id') == CUSTOM_FIELD_CONTACT_ID:
                        field['value'] = {"id": user_contact_id}  # Контакт заявителя вместо ресторана
                        logger.info(f"Replaced restaurant contact with user contact {user_contact_id} in task custom fields")
                        break
            else:
                logger.warning(f"Could not set user contact in task custom fields for user {user_id} (contact_id is None)")
            
            # Валидация и нормализация custom_field_data перед отправкой
            # Проверяем, что все значения имеют правильный формат
            validated_custom_fields = []
            for field_item in custom_field_data:
                field_id = field_item.get('field', {}).get('id')
                field_value = field_item.get('value')
                
                if field_id is None:
                    logger.warning(f"Skipping custom field with missing field.id: {field_item}")
                    continue
                
                # Валидация значения в зависимости от типа поля
                # Поля Directory entry (type 9) и Contact (type 10) требуют {"id": value}
                # Поля Phone (type 1) и Text (type 2) требуют строку
                if field_id in [CUSTOM_FIELD_RESTAURANT_ID, CUSTOM_FIELD_CONTACT_ID]:
                    # Directory entry или Contact - должны быть {"id": value}
                    if isinstance(field_value, dict) and "id" in field_value:
                        # Проверяем, что id не None
                        if field_value.get("id") is not None:
                            validated_custom_fields.append(field_item)
                        else:
                            logger.warning(f"Skipping field {field_id} - id is None: {field_value}")
                    else:
                        logger.warning(f"Invalid value format for field {field_id} (expected {{'id': value}}): {field_value}")
                elif field_id in [CUSTOM_FIELD_PHONE_ID, CUSTOM_FIELD_MOBILE_PHONE_ID, CUSTOM_FIELD_TYPE_ID]:
                    # Phone или Text - должны быть строкой
                    if isinstance(field_value, str) and field_value.strip():
                        validated_custom_fields.append(field_item)
                    else:
                        logger.warning(f"Invalid value format for field {field_id} (expected non-empty string): {field_value}")
                else:
                    # Неизвестный тип поля - добавляем как есть
                    validated_custom_fields.append(field_item)
            
            # Используем валидированные поля
            custom_field_data = validated_custom_fields
            
            # Создаем задачу
            logger.info(f"Creating task with template {template_id} for user {user_id}")
            
            # ВАЖНО: counterparty_id должен быть ID контакта, который является контрагентом (заказчиком)
            # В нашем случае это restaurant_contact_id - контакт ресторана, который создал заявку
            template_direction = get_template_direction(template_id)
            task_tag = get_direction_tag(template_direction)
            
            # Теги уже прописаны в шаблоне задачи в Planfix, поэтому не добавляем их через API
            # Определяем тег только для логики бота (для фильтрации и уведомлений)
            if task_tag:
                logger.info(f"Task template {template_id} has direction: {template_direction}, expected tag in Planfix: {task_tag} (not adding via API - tags are in template)")
            else:
                logger.warning(f"No tag determined for template {template_id} (direction: {template_direction})")
            
            # ОПТИМИЗАЦИЯ: Создаем задачу с минимальными обязательными полями, затем обновляем остальные
            # Это быстрее, чем множественные попытки с разными вариантами
            create_response = None
            
            # Формируем обязательное поле (мобильный телефон) для шаблона
            required_fields_only = [
                {
                    "field": {"id": CUSTOM_FIELD_MOBILE_PHONE_ID},
                    "value": user.phone_number
                }
            ]
            
            try:
                # Создаем задачу с обязательными полями (быстрее и надежнее)
                # ВАЖНО: Теги нельзя устанавливать при создании задачи (нет в TaskCreateRequest),
                # поэтому создаем задачу без тегов, затем обновим её
                perf_step = time.time()
                create_response = await planfix_client.create_task(
                    name=task_name,
                    description=task_description,
                    template_id=template_id,
                    counterparty_id=int(user.restaurant_contact_id),
                    custom_field_data=required_fields_only,
                    files=None,  # Файлы добавим после создания
                    tags=None  # Теги добавим после создания через update_task
                )
            except Exception as e:
                logger.error(f"Failed to create task: {e}", exc_info=True)
                raise
            
            if create_response and create_response.get('result') == 'success':
                # #region agent log
                try:
                    with open(log_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"PERF3","location":"user_handlers.py:1304","message":"create_task completed","data":{"user_id":user_id,"duration_ms":(time.time()-perf_step)*1000},"timestamp":int(time.time()*1000)})+"\n")
                except: pass
                # #endregion
                # create_task возвращает generalId в поле id
                task_id_general = create_response.get('id') or create_response.get('task', {}).get('id')
                logger.info(f"Task created successfully, generalId: {task_id_general}")
                
                # ОПТИМИЗАЦИЯ: Используем generalId напрямую, не делаем лишний запрос для internal_id
                # Planfix API работает с generalId для большинства операций
                task_id = task_id_general
                task_id_internal = None  # Не используем internal_id, экономим 1-2 секунды
                notification_task_id = task_id_general
                logger.info(f"Using task_id: {task_id} (generalId, skipping internal_id lookup for performance)")
                
                # ОПТИМИЗАЦИЯ: Сначала пытаемся получить project_id из шаблона или franchise_group (без API вызова)
                project_id = None
                if template_id:
                    try:
                        template_info = get_template_info(template_id)
                        if template_info and 'project_id' in template_info:
                            project_id = template_info.get('project_id')
                            if project_id:
                                logger.info(f"✅ Found project_id {project_id} from template {template_id}")
                    except Exception:
                        pass
                
                if not project_id and user.franchise_group_id:
                    from config import FRANCHISE_GROUPS
                    if user.franchise_group_id in FRANCHISE_GROUPS:
                        project_id = FRANCHISE_GROUPS[user.franchise_group_id].get('project_id')
                        if project_id:
                            logger.info(f"✅ Found project_id {project_id} from franchise_group {user.franchise_group_id}")
                
                # ОПТИМИЗАЦИЯ: Обновляем все остальные поля одним запросом (быстрее, чем множественные попытки)
                # Формируем кастомные поля без обязательного поля 88 (мобильный телефон), которое уже установлено
                remaining_custom_fields = [
                    field for field in custom_field_data 
                    if field.get("field", {}).get("id") != CUSTOM_FIELD_MOBILE_PHONE_ID
                ]
                
                # Обновляем все поля одним запросом
                # ВАЖНО: Теги не добавляем - они уже прописаны в шаблоне задачи в Planfix
                update_kwargs = {}
                if remaining_custom_fields:
                    update_kwargs["custom_field_data"] = remaining_custom_fields
                if files:
                    update_kwargs["files"] = files
                
                # ОПТИМИЗАЦИЯ: Параллельно обновляем задачу и получаем project_id из API (если не найден выше)
                perf_step = time.time()
                tasks_to_run = []
                if update_kwargs:
                    tasks_to_run.append(planfix_client.update_task(task_id, **update_kwargs))
                if not project_id:
                    tasks_to_run.append(planfix_client.get_task_by_id(
                        task_id,
                        fields="id,project.id,project.name"
                    ))
                
                if tasks_to_run:
                    results = await asyncio.gather(*tasks_to_run, return_exceptions=True)
                    if update_kwargs and len(results) > 0 and not isinstance(results[0], Exception):
                        logger.info(f"✅ All remaining fields updated for task {task_id} (tags are in template, not added via API)")
                        # #region agent log
                        try:
                            with open(log_path, "a", encoding="utf-8") as f:
                                f.write(json.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"PERF5","location":"user_handlers.py:1340","message":"update_task for remaining fields completed","data":{"user_id":user_id,"duration_ms":(time.time()-perf_step)*1000},"timestamp":int(time.time()*1000)})+"\n")
                        except: pass
                        # #endregion
                    elif update_kwargs and len(results) > 0 and isinstance(results[0], Exception):
                        logger.warning(f"Failed to update remaining fields for task {task_id}: {results[0]}")
                    
                    # Получаем project_id из результата API вызова
                    if not project_id and len(tasks_to_run) > (1 if update_kwargs else 0):
                        task_info_idx = 1 if update_kwargs else 0
                        if len(results) > task_info_idx and not isinstance(results[task_info_idx], Exception):
                            task_info = results[task_info_idx]
                            if task_info and task_info.get('result') == 'success':
                                task_obj = task_info.get('task', {})
                                project = task_obj.get('project', {}) or {}
                                project_id_raw = project.get('id')
                                if project_id_raw:
                                    if isinstance(project_id_raw, str) and ':' in project_id_raw:
                                        project_id = int(project_id_raw.split(':')[-1])
                                    else:
                                        project_id = int(project_id_raw)
                                    logger.info(f"✅ Found project_id {project_id} from task {task_id} project field")
                
                # ОПТИМИЗАЦИЯ: Убрана проверка задачи после создания (экономит 1-2 секунды)
                # Если нужна проверка, можно включить опционально через флаг
                
                # Отправляем уведомление подходящим исполнителям о новой заявке
                perf_step = time.time()
                try:
                    
                    # Если project_id все еще не найден, используем альтернативный способ:
                    # Получаем полную информацию о задаче и извлекаем project из любого места
                    if not project_id:
                        try:
                            await asyncio.sleep(0.5)  # Еще одна небольшая задержка
                            task_info_full = await planfix_client.get_task_by_id(
                                task_id,
                                fields="id,project,process"
                            )
                            if task_info_full and task_info_full.get('result') == 'success':
                                task_obj_full = task_info_full.get('task', {})
                                # Пробуем разные способы извлечения project_id
                                project_full = task_obj_full.get('project')
                                if project_full:
                                    if isinstance(project_full, dict):
                                        project_id_raw_full = project_full.get('id')
                                    elif isinstance(project_full, (int, str)):
                                        project_id_raw_full = project_full
                                    else:
                                        project_id_raw_full = None
                                    
                                    if project_id_raw_full:
                                        if isinstance(project_id_raw_full, str) and ':' in project_id_raw_full:
                                            project_id = int(project_id_raw_full.split(':')[-1])
                                        else:
                                            project_id = int(project_id_raw_full)
                                        logger.info(f"✅ Found project_id {project_id} from task {task_id} full info")
                        except Exception as full_err:
                            logger.debug(f"Could not get project_id from full task info for task {task_id}: {full_err}")
                    
                    # Отправляем уведомление исполнителям о новой заявке
                    # Используем новый сервис, который применяет те же фильтры, что и show_new_tasks
                    from task_notification_service import TaskNotificationService
                    task_notification_service = TaskNotificationService(message.bot)
                    # ВАЖНО: Используем generalId для уведомлений, так как API может не работать с внутренним ID
                    logger.info(f"📤 Starting notification for task {notification_task_id} (generalId) to executors (internal_id={task_id})")
                    try:
                        # Вызываем уведомление синхронно (await), чтобы гарантировать выполнение
                        # Это важно для автоматического назначения исполнителей
                        await task_notification_service.notify_executors_about_new_task(notification_task_id)
                        logger.info(f"✅ Notification completed for new task {task_id} (using task_notification_service)")
                    except Exception as notify_err:
                        logger.error(f"❌ Failed to notify executors for task {task_id}: {notify_err}", exc_info=True)
                        # Не прерываем создание задачи из-за ошибки уведомления
                except Exception as notify_err:
                    logger.error(f"❌ Failed to initialize notification service for task {task_id}: {notify_err}", exc_info=True)
                    # Не прерываем создание задачи из-за ошибки уведомления
                
                # Предзаполняем кэш именем ресторана для задачи (cp_name:<task_id>)
                try:
                    from shared_cache import cache as shared_cache
                    # Пытаемся получить имя ресторана из списка групп (быстро) или напрямую из контакта
                    restaurant_name = None
                    try:
                        contacts_response = await planfix_client.get_contact_list_by_group(
                            user.franchise_group_id,
                            fields="id,name",
                            page_size=100
                        )
                        if contacts_response and contacts_response.get('result') == 'success':
                            for c in contacts_response.get('contacts', []) or []:
                                try:
                                    if int(c.get('id')) == int(user.restaurant_contact_id):
                                        nm = (c.get('name') or '').strip()
                                        if nm:
                                            restaurant_name = nm
                                        break
                                except Exception:
                                    continue
                    except Exception:
                        pass
                    if not restaurant_name:
                        try:
                            resp = await planfix_client.get_contact_by_id(int(user.restaurant_contact_id), fields="id,name,midName,lastName,isCompany")
                            if resp and resp.get('result') == 'success':
                                from counterparty_helper import extract_contact_info
                                info = extract_contact_info(resp.get('contact') or {})
                                nm = (info.get('name') or '').strip()
                                if nm:
                                    restaurant_name = nm
                        except Exception:
                            pass
                    if restaurant_name:
                        shared_cache.set(f"cp_name:{task_id}", restaurant_name, ttl_seconds=24*3600)
                        logger.info(f"Pre-populated cache for task #{task_id} with restaurant '{restaurant_name}'")
                except Exception as cache_err:
                    logger.debug(f"Failed to pre-populate cp_name cache for task {task_id}: {cache_err}")
                
                # ВАЖНО: Planfix игнорирует customFieldData при создании через шаблон
                # Поэтому обновляем задачу ПОСЛЕ создания, чтобы установить кастомные поля
                try:
                    logger.info(f"🔄 Updating custom fields for task {task_id}")
                    logger.debug(f"Custom field data for update: {json.dumps(custom_field_data, ensure_ascii=False, indent=2)}")
                    
                    update_response = await planfix_client.update_task(
                        task_id,
                        custom_field_data=custom_field_data
                    )
                    
                    if update_response and update_response.get('result') == 'success':
                        logger.info(f"✅ Custom fields updated successfully for task {task_id}")
                        logger.debug(f"Update response: {json.dumps(update_response, ensure_ascii=False, indent=2)}")
                    else:
                        logger.warning(f"❌ Failed to update custom fields for task {task_id}")
                        logger.warning(f"Update response: {json.dumps(update_response, ensure_ascii=False, indent=2) if update_response else 'No response'}")
                except Exception as update_err:
                    logger.error(f"❌ Error updating custom fields for task {task_id}: {update_err}", exc_info=True)
                
                # Добавляем файлы после создания задачи (отдельным запросом)
                if files:
                    try:
                        logger.info(f"📎 Adding {len(files)} file(s) to task {task_id}")
                        # Нормализуем ID файлов перед отправкой
                        normalized_files = []
                        for f_id in files:
                            if f_id is None:
                                continue
                            if isinstance(f_id, str) and ':' in f_id:
                                try:
                                    f_id = int(f_id.split(':')[-1])
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not parse file_id: {f_id}")
                                    continue
                            elif not isinstance(f_id, int):
                                try:
                                    f_id = int(f_id)
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert file_id to int: {f_id}")
                                    continue
                            normalized_files.append(f_id)
                        
                        if normalized_files:
                            # Пробуем добавить файлы через update_task
                            try:
                                await planfix_client.update_task(task_id, files=normalized_files)
                                logger.info(f"✅ Files added to task {task_id}: {normalized_files}")
                            except Exception as file_update_err:
                                logger.warning(f"Failed to add files via update_task: {file_update_err}")
                                # Фоллбэк: добавляем файлы через комментарий
                                try:
                                    logger.info(f"Trying to add files via comment for task {task_id}")
                                    for file_id in normalized_files:
                                        await planfix_client.add_comment_to_task(
                                            task_id,
                                            description=f"Файл из Telegram бота",
                                            files=[file_id]
                                        )
                                    logger.info(f"✅ Files added via comment to task {task_id}")
                                except Exception as comment_err:
                                    logger.error(f"Failed to add files via comment: {comment_err}", exc_info=True)
                    except Exception as files_err:
                        logger.error(f"❌ Error adding files to task {task_id}: {files_err}", exc_info=True)
                
                # Сохраняем привязку task_id -> telegram_id для последующих уведомлений
                # Сохраняем оба ID для совместимости с разными форматами
                try:
                    bot_log_details = {
                        "task_id": int(task_id_general),  # Основной ID - всегда generalId
                        "task_id_general": int(task_id_general),  # Всегда сохраняем generalId явно
                        "user_telegram_id": int(user_id),
                    }
                    # Сохраняем internal ID если он есть и отличается от generalId
                    if task_id_internal and task_id_internal != task_id_general:
                        bot_log_details["task_id_internal"] = int(task_id_internal)
                        logger.info(f"✅ Saved both IDs in BotLog: internal={task_id_internal}, general={task_id_general}")
                    else:
                        logger.info(f"✅ Saved task_id_general in BotLog: {task_id_general}")
                    
                    await db_manager.create_bot_log(
                        telegram_id=user_id,
                        action="create_task",
                        details=bot_log_details,
                    )
                    
                    # ОПТИМИЗАЦИЯ: Сохраняем задачу в TaskCache для быстрого доступа
                    try:
                        # Получаем информацию о статусе из созданной задачи
                        task_info_for_cache = await planfix_client.get_task_by_id(
                            task_id_general,
                            fields="id,name,status,project,counterparty,template"
                        )
                        if task_info_for_cache and task_info_for_cache.get('result') == 'success':
                            task_obj_cache = task_info_for_cache.get('task', {})
                            status_obj_cache = task_obj_cache.get('status', {})
                            status_id_cache = None
                            status_name_cache = None
                            if isinstance(status_obj_cache, dict):
                                # Нормализуем status_id
                                status_id_raw = status_obj_cache.get('id')
                                if status_id_raw:
                                    if isinstance(status_id_raw, str) and ':' in status_id_raw:
                                        status_id_raw = status_id_raw.split(':')[-1]
                                    try:
                                        status_id_cache = int(status_id_raw) if str(status_id_raw).isdigit() else None
                                    except:
                                        pass
                                status_name_cache = status_obj_cache.get('name')
                            
                            counterparty_id_cache = None
                            counterparty_cache = task_obj_cache.get('counterparty', {})
                            if isinstance(counterparty_cache, dict):
                                counterparty_id_cache = counterparty_cache.get('id')
                                if isinstance(counterparty_id_cache, str) and ':' in counterparty_id_cache:
                                    counterparty_id_cache = int(counterparty_id_cache.split(':')[-1])
                                elif isinstance(counterparty_id_cache, (int, str)) and str(counterparty_id_cache).isdigit():
                                    counterparty_id_cache = int(counterparty_id_cache)
                            
                            project_id_cache = None
                            project_cache = task_obj_cache.get('project', {})
                            if isinstance(project_cache, dict):
                                project_id_cache = project_cache.get('id')
                                if isinstance(project_id_cache, str) and ':' in project_id_cache:
                                    project_id_cache = int(project_id_cache.split(':')[-1])
                                elif isinstance(project_id_cache, (int, str)) and str(project_id_cache).isdigit():
                                    project_id_cache = int(project_id_cache)
                            
                            template_id_cache = None
                            template_cache = task_obj_cache.get('template', {})
                            if isinstance(template_cache, dict):
                                template_id_cache = template_cache.get('id')
                                if isinstance(template_id_cache, (int, str)) and str(template_id_cache).isdigit():
                                    template_id_cache = int(template_id_cache)
                            
                            await db_manager.run(
                                db_manager.create_or_update_task_cache,
                                task_id=task_id_general,
                                task_id_internal=task_id_internal,
                                name=task_obj_cache.get('name', ''),
                                status_id=status_id_cache,
                                status_name=status_name_cache,
                                counterparty_id=counterparty_id_cache,
                                project_id=project_id_cache,
                                template_id=template_id_cache,
                                user_telegram_id=user_id,
                                created_by_bot=True,
                                date_of_last_update=datetime.now()
                            )
                            logger.debug(f"✅ Saved task {task_id_general} to TaskCache")
                    except Exception as cache_err:
                        logger.warning(f"Failed to save task {task_id_general} to TaskCache: {cache_err}")
                    
                    # ОПТИМИЗАЦИЯ: Сохраняем задачу в TaskCache для быстрого доступа
                    try:
                        # Получаем информацию о статусе из созданной задачи
                        task_info_for_cache = await planfix_client.get_task_by_id(
                            task_id_general,
                            fields="id,name,status,project,counterparty,template"
                        )
                        if task_info_for_cache and task_info_for_cache.get('result') == 'success':
                            task_obj_cache = task_info_for_cache.get('task', {})
                            status_obj_cache = task_obj_cache.get('status', {})
                            status_id_cache = None
                            status_name_cache = None
                            if isinstance(status_obj_cache, dict):
                                # Нормализуем status_id
                                status_id_raw = status_obj_cache.get('id')
                                if status_id_raw:
                                    if isinstance(status_id_raw, str) and ':' in status_id_raw:
                                        status_id_raw = status_id_raw.split(':')[-1]
                                    try:
                                        status_id_cache = int(status_id_raw) if str(status_id_raw).isdigit() else None
                                    except:
                                        status_id_cache = None
                                else:
                                    status_id_cache = None
                                status_name_cache = status_obj_cache.get('name')
                            
                            counterparty_id_cache = None
                            counterparty_cache = task_obj_cache.get('counterparty', {})
                            if isinstance(counterparty_cache, dict):
                                counterparty_id_cache = counterparty_cache.get('id')
                                if isinstance(counterparty_id_cache, str) and ':' in counterparty_id_cache:
                                    counterparty_id_cache = int(counterparty_id_cache.split(':')[-1])
                                elif isinstance(counterparty_id_cache, (int, str)) and str(counterparty_id_cache).isdigit():
                                    counterparty_id_cache = int(counterparty_id_cache)
                            
                            project_id_cache = None
                            project_cache = task_obj_cache.get('project', {})
                            if isinstance(project_cache, dict):
                                project_id_cache = project_cache.get('id')
                                if isinstance(project_id_cache, str) and ':' in project_id_cache:
                                    project_id_cache = int(project_id_cache.split(':')[-1])
                                elif isinstance(project_id_cache, (int, str)) and str(project_id_cache).isdigit():
                                    project_id_cache = int(project_id_cache)
                            
                            template_id_cache = None
                            template_cache = task_obj_cache.get('template', {})
                            if isinstance(template_cache, dict):
                                template_id_cache = template_cache.get('id')
                                if isinstance(template_id_cache, (int, str)) and str(template_id_cache).isdigit():
                                    template_id_cache = int(template_id_cache)
                            
                            await db_manager.run(
                                db_manager.create_or_update_task_cache,
                                task_id=task_id_general,
                                task_id_internal=task_id_internal,
                                name=task_obj_cache.get('name', ''),
                                status_id=status_id_cache,
                                status_name=status_name_cache,
                                counterparty_id=counterparty_id_cache,
                                project_id=project_id_cache,
                                template_id=template_id_cache,
                                user_telegram_id=user_id,
                                created_by_bot=True,
                                date_of_last_update=datetime.now()
                            )
                            logger.debug(f"✅ Saved task {task_id_general} to TaskCache")
                    except Exception as cache_err:
                        logger.warning(f"Failed to save task {task_id_general} to TaskCache: {cache_err}")
                except Exception as log_err:
                    logger.warning(f"Failed to write BotLog for task {task_id}: {log_err}")
                
                await message.answer(
                    f"✅ <b>Заявка успешно создана!</b>\n\n"
                    f"📋 <b>Номер заявки:</b> #{task_id}\n"
                    f"📝 <b>Тип:</b> {template_info.get('name', 'Заявка')}\n"
                    f"📊 <b>Статус:</b> В работе\n\n"
                    "Исполнители назначены автоматически и уже приступают к задаче.",
                    reply_markup=get_main_menu_keyboard(),
                    parse_mode="HTML"
                )
                logger.info(f"Created task {task_id} for user {user_id}")
            else:
                error_msg = create_response.get('error', 'Неизвестная ошибка') if create_response else 'Нет ответа от сервера'
                logger.error(f"Failed to create task: {error_msg}")
                await message.answer(
                    f"❌ Не удалось создать заявку.\n\n"
                    f"Ошибка: {error_msg}\n\n"
                    "Попробуйте позже или обратитесь к администратору."
                )
        except Exception as e:
            logger.error(f"Error creating task: {e}", exc_info=True)
            await message.answer(
                "❌ Произошла ошибка при создании заявки. Попробуйте позже."
            )
    finally:
        # Снимаем флаг создания задачи
        if hasattr(finalize_create_task, '_in_progress'):
            finalize_create_task._in_progress[user_id] = False
        # #region agent log
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"perf","hypothesisId":"PERF_TOTAL","location":"user_handlers.py:1621","message":"finalize_create_task completed","data":{"user_id":user_id,"total_duration_ms":(time.time()-perf_start)*1000},"timestamp":int(time.time()*1000)})+"\n")
        except: pass
        # #endregion
    
    await state.clear()


# ============================================================================
# ПРОСМОТР ЗАЯВОК
# ============================================================================

@router.message(F.text == "📋 Мои заявки")
async def list_my_tickets(message: Message, state: FSMContext):
    """Список заявок пользователя."""
    logger.info(f"Handler 'list_my_tickets' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    user = await db_manager.get_user_profile(message.from_user.id)
    
    if not user:
        await message.answer("❌ Сначала пройдите регистрацию: /start")
        return
    
    try:
        tasks = await get_user_tasks(message.from_user.id, limit=20)
        if tasks is None:
            logger.error("get_user_tasks returned None")
            await message.answer("❌ Не удалось загрузить список заявок.")
            return

        logger.info(f"Found {len(tasks)} tasks for user {message.from_user.id}")

        # Получаем ID статусов "Новая" и "В работе"
        await ensure_status_registry_loaded()
        new_status_id = require_status_id(StatusKey.NEW)
        in_progress_status_id = require_status_id(StatusKey.IN_PROGRESS)
        allowed_status_ids = {new_status_id, in_progress_status_id}
        logger.debug(f"Allowed status IDs for 'Мои заявки': {allowed_status_ids} (NEW={new_status_id}, IN_PROGRESS={in_progress_status_id})")

        def normalize_status_id(sid):
            if isinstance(sid, str) and ':' in sid:
                try:
                    return int(sid.split(':')[1])
                except ValueError:
                    return None
            try:
                return int(sid) if sid is not None else None
            except (TypeError, ValueError):
                return None

        # Фильтруем только заявки со статусами "Новая" и "В работе"
        # Также проверяем по названию статуса на случай если ID не совпадает
        allowed_status_names = {
            'новая', 'new', 'новое', 'новый',
            'в работе', 'в работе', 'in progress', 'in_progress', 'выполняется',
            'работа', 'working', 'active', 'активная', 'активное'
        }
        
        active_tasks = []
        for t in tasks:
            status_id = normalize_status_id(t.get('status', {}).get('id'))
            status_name = t.get('status', {}).get('name', 'Неизвестно')
            status_name_lower = (status_name.lower().strip() if status_name else '')
            
            # Логируем для отладки
            logger.info(f"Task #{t.get('id', 'unknown')}: status_id={status_id}, status_name='{status_name}', is_allowed_by_id={status_id in allowed_status_ids if status_id else False}, is_allowed_by_name={status_name_lower in allowed_status_names}")
            
            # Проверяем соответствие по ID
            is_allowed_by_id = status_id is not None and status_id in allowed_status_ids
            
            # Проверяем соответствие по названию (более гибкая проверка)
            is_allowed_by_name = False
            if status_name_lower:
                # Проверяем точное совпадение
                if status_name_lower in allowed_status_names:
                    is_allowed_by_name = True
                else:
                    # Проверяем частичное совпадение (содержит ключевые слова)
                    for allowed_name in allowed_status_names:
                        if allowed_name in status_name_lower or status_name_lower in allowed_name:
                            is_allowed_by_name = True
                            break
            
            # Добавляем если соответствует по ID или по названию
            if is_allowed_by_id or is_allowed_by_name:
                active_tasks.append(t)
                logger.debug(f"Task #{t.get('id')} added to active tasks (status: {status_name})")
            else:
                logger.debug(f"Task #{t.get('id')} filtered out (status: {status_name}, id: {status_id})")

        if not active_tasks:
            await message.answer(
                "📋 У вас нет активных заявок.\n\n"
                "Создайте новую заявку, нажав кнопку 'Создать заявку'."
            )
            return

        lines = ["📋 Ваши активные заявки:\n"]
        for t in active_tasks:
            status_name = t.get('status', {}).get('name', 'Неизвестно')
            task_name = t.get('name', 'Без названия')
            lines.append(f"#{t['id']} – {status_name}\n{task_name}\n")
            
            # Проверяем новые комментарии для каждой задачи
            try:
                await _check_comments_for_task(t['id'], message.from_user.id, message.bot)
            except Exception as e:
                logger.error(f"Error checking comments for task {t['id']}: {e}")

        await message.answer("\n".join(lines))
        
    except Exception as e:
        logger.error(f"Error listing tickets: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при загрузке заявок.")


# ============================================================================
# УТОЧНЕНИЕ СТАТУСА
# ============================================================================

@router.message(F.text == "🔍 Уточнить статус")
async def ask_status_task_id(message: Message, state: FSMContext):
    """Запрос номера заявки для уточнения статуса."""
    logger.info(f"Handler 'ask_status_task_id' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    # Проверяем регистрацию
    user = await db_manager.get_user_profile(message.from_user.id)
    
    if not user:
        await message.answer("❌ Сначала пройдите регистрацию: /start")
        return
    
    # Получаем список активных заявок пользователя
    tasks = await get_user_tasks(message.from_user.id, limit=50, only_active=True)
    
    if not tasks:
        await message.answer(
            "📋 У вас пока нет активных заявок.\n\n"
            "Создайте первую заявку, нажав кнопку 'Создать заявку'."
        )
        return
    
    # Создаем клавиатуру с заявками
    keyboard = create_tasks_keyboard(tasks, action_type="status")
    
    await message.answer(
        "🔍 Выберите заявку для уточнения статуса:",
        reply_markup=keyboard
    )
    await state.set_state(StatusInquiry.choosing_from_list)


@router.callback_query(StatusInquiry.choosing_from_list, F.data.startswith("status_task:"))
async def handle_status_task_selection(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора заявки для уточнения статуса."""
    task_id = int(callback_query.data.split(":")[1])
    
    try:
        # Получаем информацию о задаче
        task_response = await planfix_client.get_task_by_id(
            task_id,
            fields="id,status,name,description"
        )
        
        if task_response and task_response.get('result') == 'success':
            task = task_response.get('task', {})
            status_name = task.get('status', {}).get('name', 'Неизвестно')
            task_name = task.get('name', 'Без названия')
            
            # Добавляем комментарий с запросом статуса
            try:
                await planfix_client.add_comment_to_task(
                    task_id,
                    description="Уточните, пожалуйста, на каком этапе моя задача"
                )
            except Exception as e:
                logger.warning(f"Failed to add comment to task {task_id}: {e}")

            # Уведомляем исполнителей о запросе на уточнение
            try:
                user = await db_manager.get_user_profile(callback_query.from_user.id)
                author_name = user.full_name if user else "Заявитель"
                from notifications import NotificationService
                notification_service = NotificationService(callback_query.bot)
                await notification_service.notify_new_comment(task_id, author_name, "Уточните, пожалуйста, на каком этапе моя задача", recipients="executors")
            except Exception as notify_err:
                logger.error(f"Failed to notify executors about status inquiry for task {task_id}: {notify_err}")
            
            await callback_query.message.edit_text(
                f"📋 Заявка #{task_id}\n\n"
                f"📝 {task_name}\n"
                f"📊 Статус: {status_name}\n\n"
                "Запрос на уточнение отправлен исполнителю."
            )
            await callback_query.message.answer(
                "Выберите действие:",
                reply_markup=get_main_menu_keyboard()
            )
        else:
            await callback_query.message.edit_text(
                f"❌ Не удалось получить информацию о заявке #{task_id}.\n\n"
                "Проверьте номер и попробуйте снова."
            )
        
    except Exception as e:
        logger.error(f"Error getting task status: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Произошла ошибка при получении статуса.")
    
    await state.clear()
    await callback_query.answer()


@router.callback_query(StatusInquiry.choosing_from_list, F.data == "manual_input")
async def handle_manual_status_input(callback_query: CallbackQuery, state: FSMContext):
    """Переключение на ручной ввод номера заявки для уточнения статуса."""
    await callback_query.message.edit_text("🔍 Введите номер заявки (например, 12345):")
    await state.set_state(StatusInquiry.waiting_for_task_id)
    await callback_query.answer()


@router.callback_query(StatusInquiry.choosing_from_list, F.data == "cancel_action")
async def cancel_status_inquiry(callback_query: CallbackQuery, state: FSMContext):
    """Отмена уточнения статуса."""
    await state.clear()
    await callback_query.message.edit_text("❌ Уточнение статуса отменено.")
    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=get_main_menu_keyboard()
    )
    await callback_query.answer()


@router.message(StatusInquiry.waiting_for_task_id)
async def do_status_inquiry(message: Message, state: FSMContext):
    """Получение статуса заявки."""
    task_id_text = message.text.strip().lstrip("#")
    
    if not task_id_text.isdigit():
        await message.answer("❌ Некорректный номер. Введите число, например: 12345")
        return
    
    task_id = int(task_id_text)
    
    try:
        # Добавляем комментарий с запросом статуса
        try:
            await planfix_client.add_comment_to_task(
                task_id,
                description="Уточните, пожалуйста, на каком этапе м��я задача"
            )
        except Exception as e:
            logger.warning(f"Failed to add comment to task {task_id}: {e}")

        # Уведомляем исполнителей о запросе на уточнение
        try:
            user = await db_manager.get_user_profile(message.from_user.id)
            author_name = user.full_name if user else "Заявитель"
            from notifications import NotificationService
            notification_service = NotificationService(message.bot)
            await notification_service.notify_new_comment(task_id, author_name, "Уточните, пожалуйста, на каком этапе моя задача", recipients="executors")
        except Exception as notify_err:
            logger.error(f"Failed to notify executors about status inquiry for task {task_id}: {notify_err}")
        
        # Получаем информацию о задаче
        task_response = await planfix_client.get_task_by_id(
            task_id,
            fields="id,status,name,description"
        )
        
        if task_response and task_response.get('result') == 'success':
            task = task_response.get('task', {})
            status_name = task.get('status', {}).get('name', 'Неизвестно')
            task_name = task.get('name', 'Без названия')
            
            await message.answer(
                f"📋 Заявка #{task_id}\n\n"
                f"📝 {task_name}\n"
                f"📊 Статус: {status_name}\n\n"
                "Запрос на уточнение отправлен исполнителю."
            )
        else:
            await message.answer(
                f"❌ Не удалось получить информацию о заявке #{task_id}.\n\n"
                "Проверьте номер и попробуйте снова."
            )
        
    except Exception as e:
        logger.error(f"Error getting task status: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при получении статуса.")
    
    await state.clear()


# ============================================================================
# КОММЕНТАРИИ
# ============================================================================

@router.message(F.text == "💬 Написать комментарий")
async def comment_start(message: Message, state: FSMContext):
    """Начало добавления комментария."""
    logger.info(f"Handler 'comment_start' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    # Проверяем регистрацию
    user = await db_manager.get_user_profile(message.from_user.id)
    
    if not user:
        await message.answer("❌ Сначала пройдите регистрацию: /start")
        return
    
    # Получаем список активных заявок пользователя
    tasks = await get_user_tasks(message.from_user.id, limit=10, only_active=True)
    
    if not tasks:
        await message.answer(
            "📋 У вас пока нет активных заявок.\n\n"
            "Создайте первую заявку, нажав кнопку 'Создать заявку'."
        )
        return
    
    # Создаем клавиатуру с заявками
    keyboard = create_tasks_keyboard(tasks, action_type="comment")
    
    await message.answer(
        "💬 Выберите заявку для добавления комментария:",
        reply_markup=keyboard
    )
    await state.set_state(CommentFlow.choosing_from_list)


@router.callback_query(CommentFlow.choosing_from_list, F.data.startswith("comment_task:"))
async def handle_comment_task_selection(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора заявки для добавления комментария."""
    task_id = int(callback_query.data.split(":")[1])
    
    await state.update_data(task_id=task_id)
    await callback_query.message.edit_text("📝 Введите текст комментария:")
    await state.set_state(CommentFlow.waiting_for_text)
    await callback_query.answer()


@router.callback_query(CommentFlow.choosing_from_list, F.data == "manual_input")
async def handle_manual_comment_input(callback_query: CallbackQuery, state: FSMContext):
    """Переключение на ручной ввод номера заявки для комментария."""
    await callback_query.message.edit_text("💬 Введите номер заявки, в которую хотите добавить комментарий:")
    await state.set_state(CommentFlow.waiting_for_task_id)
    await callback_query.answer()


@router.callback_query(CommentFlow.choosing_from_list, F.data == "cancel_action")
async def cancel_comment_flow(callback_query: CallbackQuery, state: FSMContext):
    """Отмена добавления комментария."""
    await state.clear()
    await callback_query.message.edit_text("❌ Добавление комментария отменено.")
    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=get_main_menu_keyboard()
    )
    await callback_query.answer()


@router.message(CommentFlow.waiting_for_task_id)
async def comment_task_id(message: Message, state: FSMContext):
    """Обработка номера заявки для комментария."""
    task_id_text = message.text.strip().lstrip("#")
    
    if not task_id_text.isdigit():
        await message.answer("❌ Некорректный номер. Введите число, например: 12345")
        return
    
    await state.update_data(task_id=int(task_id_text))
    await message.answer("📝 Введите текст комментария:")
    await state.set_state(CommentFlow.waiting_for_text)


@router.message(CommentFlow.waiting_for_text)
async def comment_text(message: Message, state: FSMContext):
    """Обработка текста комментария."""
    await state.update_data(comment_text=message.text)
    await message.answer(
        "📷 Прикрепите фото или видео (если нужно) или напишите 'Готово':",
        reply_markup=get_skip_or_done_keyboard()
    )
    await state.set_state(CommentFlow.waiting_for_file)


@router.message(CommentFlow.waiting_for_file, F.text.casefold() == "готово")
async def comment_finalize_no_file(message: Message, state: FSMContext):
    """Отправка комментария без файла."""
    data = await state.get_data()
    await submit_comment(message, data.get("task_id"), data.get("comment_text"), None)
    await state.clear()


@router.callback_query(CommentFlow.waiting_for_file, F.data == "skip_file")
async def comment_skip_file(callback_query: CallbackQuery, state: FSMContext):
    """Пропуск прикрепления файла к комментарию."""
    data = await state.get_data()
    await callback_query.answer()
    await submit_comment(callback_query.message, data.get("task_id"), data.get("comment_text"), None)
    await state.clear()


@router.message(CommentFlow.waiting_for_file, F.content_type.in_({ContentType.PHOTO, ContentType.VIDEO, ContentType.VIDEO_NOTE}))
async def comment_with_media(message: Message, state: FSMContext):
    """Отправка комментария с фото/видео."""
    data = await state.get_data()
    
    try:
        # Определяем тип медиа и получаем file_id
        if message.photo:
            file_id = message.photo[-1].file_id
            media_type = "photo"
            default_filename = "photo.jpg"
        elif message.video:
            file_id = message.video.file_id
            media_type = "video"
            default_filename = message.video.file_name or f"video_{file_id}.mp4"
        elif message.video_note:
            file_id = message.video_note.file_id
            media_type = "video_note"
            default_filename = "video_note.mp4"
        else:
            await message.answer("❌ Не удалось определить тип медиа файла.")
            await state.clear()
            return
        
        tg_file = await message.bot.get_file(file_id)
        file_bytes = await message.bot.download_file(tg_file.file_path)
        
        upload_response = await planfix_client.upload_file(file_bytes, filename=default_filename)
        planfix_file_id = upload_response.get('id') if upload_response and upload_response.get('result') == 'success' else None
        
        await submit_comment(message, data.get("task_id"), data.get("comment_text"), planfix_file_id)
        
    except Exception as e:
        logger.error(f"Error uploading media for comment: {e}", exc_info=True)
        await message.answer("⚠️ Ошибка при загрузке медиа файла, комментарий будет отправлен без него.")
        await submit_comment(message, data.get("task_id"), data.get("comment_text"), None)
    
    await state.clear()


async def submit_comment(message: Message, task_id: int, text: str, file_id: int | None):
    """Отправка комментария в Planfix."""
    try:
        # Получаем информацию о пользователе
        user = await db_manager.get_user_profile(message.from_user.id)

        author_name = user.full_name if user else "Пользователь"
        
        files = [file_id] if file_id else None
        response = await planfix_client.add_comment_to_task(
            task_id,
            description=text,
            files=files
        )
        
        if response and response.get('result') == 'success':
            # Отправляем уведомление исполнителям
            logger.info(f"Comment added successfully to task {task_id} by user {author_name}, sending notifications...")
            from notifications import NotificationService
            notification_service = NotificationService(message.bot)
            await notification_service.notify_new_comment(task_id, author_name, text, recipients="executors")
            logger.info(f"Notification service called for task {task_id}")
            
            await message.answer(
                f"✅ Комментарий добавлен к заявке #{task_id}.",
                reply_markup=get_main_menu_keyboard()
            )
        else:
            await message.answer("❌ Не удалось добавить комментарий. Попробуйте позже.")
        
    except Exception as e:
        logger.error(f"Error submitting comment: {e}", exc_info=True)
        await message.answer("❌ Ошибка при добавлении комментария.")


# ============================================================================
# ПРОФИЛЬ
# ============================================================================

@router.message(F.text == "👤 Профиль")
async def show_profile(message: Message, state: FSMContext):
    """Показать профиль пользователя."""
    logger.info(f"Handler 'show_profile' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    user = await db_manager.get_user_profile(message.from_user.id)
    
    if not user:
        await message.answer("❌ Профиль не найден. Пройдите регистрацию: /start")
        return
    
    await message.answer(
        f"👤 Ваш профиль:\n\n"
        f"ФИО: {user.full_name}\n"
        f"📱 Телефон: {user.phone_number}\n"
        f"🏢 Группа франчайзи: {user.franchise_group_id}\n"
        f"🏪 Ресторан ID: {user.restaurant_contact_id}\n"
        f"📅 Дата регистрации: {user.registration_date.strftime('%d.%m.%Y')}\n\n"
        "Выберите, что хотите изменить:",
        reply_markup=get_profile_edit_keyboard()
    )


@router.callback_query(F.data == "edit_name")
async def edit_full_name_start(callback_query: CallbackQuery, state: FSMContext):
    """Начало редактирования ФИО."""
    await callback_query.message.edit_text("👤 Введите новое ФИО:")
    await state.set_state(ProfileEdit.editing_full_name)
    await callback_query.answer()


@router.message(ProfileEdit.editing_full_name, F.text)
async def edit_full_name_process(message: Message, state: FSMContext):
    """Обработка нового ФИО."""
    full_name = (message.text or "").strip()

    if len(full_name) < 3:
        await message.answer("❌ ФИО слишком короткое. Попробуйте ещё раз:")
        return

    if len(full_name) > 255:
        await message.answer("❌ ФИО слишком длинное. Пожалуйста, сократите его до 255 символов.")
        return

    try:
        await db_manager.update_user_profile(message.from_user.id, full_name=full_name)
        await state.clear()
        await message.answer(
            f"✅ ФИО обновлено!\n\nНовое значение: {full_name}",
            reply_markup=get_main_menu_keyboard()
        )
        logger.info(f"User {message.from_user.id} updated full name")
    except Exception as e:
        logger.error(f"Error updating full name: {e}", exc_info=True)
        await message.answer("❌ Не удалось обновить ФИО. Попробуйте позже.")
        await state.clear()


@router.callback_query(F.data == "edit_phone")
async def edit_phone_start(callback_query: CallbackQuery, state: FSMContext):
    """Начало редактирования телефона."""
    await callback_query.message.edit_text(
        "📱 Введите новый номер телефона или нажмите кнопку ниже:"
    )
    await callback_query.message.answer(
        "Поделитесь номером телефона:",
        reply_markup=get_phone_number_keyboard()
    )
    await state.set_state(ProfileEdit.editing_phone)
    await callback_query.answer()


@router.message(ProfileEdit.editing_phone, F.contact)
async def edit_phone_contact(message: Message, state: FSMContext):
    """Обработка нового телефона через кнопку."""
    phone_number = message.contact.phone_number
    await update_user_phone(message, state, phone_number, message.from_user.id)


@router.message(ProfileEdit.editing_phone, F.text)
async def edit_phone_text(message: Message, state: FSMContext):
    """Обработка нового телефона введенного вручную."""
    phone_text = message.text.strip()
    normalized = re.sub(r"[^0-9+]", "", phone_text)
    
    if not normalized or len(re.sub(r"\D", "", normalized)) < 10:
        await message.answer(
            "❌ Некорректный номер телефона.\n\n"
            "Введите номер в формате +79991234567:",
            reply_markup=get_phone_number_keyboard()
        )
        return
    
    await update_user_phone(message, state, normalized, message.from_user.id)


async def update_user_phone(message: Message, state: FSMContext, phone: str, user_id: int):
    """Обновление телефона пользователя."""
    try:
        await db_manager.update_user_profile(user_id, phone_number=phone)

        await state.clear()
        await message.answer(
            f"✅ Телефон успешно обновлён!\n\n"
            f"📱 Новый номер: {phone}",
            reply_markup=get_main_menu_keyboard()
        )
        logger.info(f"User {user_id} updated phone to {phone}")
        
    except Exception as e:
        logger.error(f"Error updating phone: {e}", exc_info=True)
        await message.answer("❌ Ошибка при обновлении телефона.")
        await state.clear()


@router.callback_query(F.data == "edit_franchise")
async def edit_franchise_start(callback_query: CallbackQuery, state: FSMContext):
    """Начало редактирования концепции."""
    try:
        franchise_groups = [
            {"id": gid, "name": data["name"]}
            for gid, data in FRANCHISE_GROUPS.items()
        ]
        if not franchise_groups:
            logger.error("FRANCHISE_GROUPS is empty")
            await callback_query.message.edit_text("❌ Не найдены группы франчайзи.")
            return
        
        # Создаем клавиатуру только с франчайзи
        keyboard_items = [
            (str(group["id"]), group["name"])
            for group in sorted(franchise_groups, key=lambda item: item["name"])
        ]
        keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=True)
        
        await callback_query.message.edit_text(
            "🏢 Выберите новую концепцию:",
            reply_markup=keyboard
        )
        await state.set_state(ProfileEdit.editing_franchise)
        await callback_query.answer()
        
    except Exception as e:
        logger.error(f"Error loading franchises for edit: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Ошибка при загрузке концепций.")


@router.callback_query(ProfileEdit.editing_franchise)
async def edit_franchise_process(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора новой концепции."""
    if callback_query.data == "cancel_registration":
        await callback_query.message.edit_text("❌ Изменение отменено.")
        await state.clear()
        await callback_query.answer()
        return
    
    franchise_group_id = int(callback_query.data)
    await state.update_data(new_franchise_id=franchise_group_id)
    
    try:
        # Получаем контакты из Planfix через API
        contacts = await get_contacts_by_group(planfix_client, franchise_group_id)
        if not contacts:
            await callback_query.message.edit_text("❌ Для выбранной концепции нет ресторанов.")
            await state.clear()
            return

        keyboard_items = [
            (str(contact_id), name)
            for contact_id, name in sorted(contacts.items(), key=lambda item: item[1])
        ]
        keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=True)
        
        await callback_query.message.edit_text(
            "🏪 Выберите новый ресторан:",
            reply_markup=keyboard
        )
        await state.set_state(ProfileEdit.editing_restaurant)
        await callback_query.answer()
        
    except Exception as e:
        logger.error(f"Error loading restaurants for edit: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Ошибка при загрузке ресторанов.")
        await state.clear()


@router.callback_query(F.data == "edit_restaurant")
async def edit_restaurant_start(callback_query: CallbackQuery, state: FSMContext):
    """Начало редактирования ресторана (без смены концепции)."""
    user = await db_manager.get_user_profile(callback_query.from_user.id)
    
    if not user:
        await callback_query.message.edit_text("❌ Профиль не найден.")
        return
    
    try:
        # Получаем контакты из Planfix через API
        contacts = await get_contacts_by_group(planfix_client, user.franchise_group_id)
        if not contacts:
            await callback_query.message.edit_text("❌ Для вашей концепции нет доступных ресторанов.")
            return

        keyboard_items = [
            (str(contact_id), name)
            for contact_id, name in sorted(contacts.items(), key=lambda item: item[1])
        ]
        keyboard = create_dynamic_keyboard(keyboard_items, add_cancel_button=True)
        
        await callback_query.message.edit_text(
            "🏪 Выберите новый ресторан:",
            reply_markup=keyboard
        )
        await state.set_state(ProfileEdit.editing_restaurant)
        await callback_query.answer()
        
    except Exception as e:
        logger.error(f"Error loading restaurants: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Ошибка при загрузке ресторанов.")


@router.callback_query(ProfileEdit.editing_restaurant)
async def edit_restaurant_process(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора нового ресторана."""
    if callback_query.data == "cancel_registration":
        await callback_query.message.edit_text("❌ Изменение отменено.")
        await state.clear()
        await callback_query.answer()
        return
    
    restaurant_contact_id = int(callback_query.data)
    user_data = await state.get_data()
    new_franchise_id = user_data.get('new_franchise_id')
    
    try:
        update_data = {"restaurant_contact_id": restaurant_contact_id}
        if new_franchise_id:
            update_data["franchise_group_id"] = new_franchise_id

        await db_manager.update_user_profile(callback_query.from_user.id, **update_data)

        await state.clear()
        await callback_query.message.edit_text(
            "✅ Профиль успешно обновлён!\n\n"
            "Ваши данные изменены."
        )
        await callback_query.message.answer(
            "Выберите действие:",
            reply_markup=get_main_menu_keyboard()
        )
        logger.info(f"User {callback_query.from_user.id} updated profile")
        await callback_query.answer()
        
    except Exception as e:
        logger.error(f"Error updating profile: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Ошибка при обновлении профиля.")
        await state.clear()


@router.callback_query(F.data == "cancel_edit")
async def cancel_profile_edit(callback_query: CallbackQuery, state: FSMContext):
    """Отмена редактирования профиля."""
    await state.clear()
    await callback_query.message.edit_text("❌ Редактирование отменено.")
    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=get_main_menu_keyboard()
    )
    await callback_query.answer()


# ============================================================================
# ОТМЕНА ЗАЯВКИ
# ============================================================================

@router.message(F.text == "❌ Отменить заявку")
async def cancel_task_start(message: Message, state: FSMContext):
    """Начало отмены заявки."""
    logger.info(f"Handler 'cancel_task_start' called for user {message.from_user.id}, text: '{message.text}'")
    # Очищаем состояние FSM, чтобы кнопки меню работали всегда
    await state.clear()
    
    user = await db_manager.get_user_profile(message.from_user.id)
    
    if not user:
        await message.answer("❌ Сначала пройдите регистрацию: /start")
        return
    
    # Получаем список заявок пользователя и фильтруем только "Новая" и "В работе"
    tasks = await get_user_tasks(message.from_user.id, limit=50)
    
    if not tasks:
        await message.answer(
            "📋 У вас пока нет заявок.\n\n"
            "Создайте первую заявку, нажав кнопку 'Создать заявку'."
        )
        return
    
    # Фильтруем только заявки со статусами "Новая" и "В работе"
    await ensure_status_registry_loaded()
    new_status_id = require_status_id(StatusKey.NEW)
    in_progress_status_id = require_status_id(StatusKey.IN_PROGRESS)
    allowed_status_ids = {new_status_id, in_progress_status_id}
    
    def normalize_status_id(sid):
        if isinstance(sid, str) and ':' in sid:
            try:
                return int(sid.split(':')[1])
            except ValueError:
                return None
        try:
            return int(sid) if sid is not None else None
        except (TypeError, ValueError):
            return None
    
    allowed_status_names = {
        'новая', 'new', 'новое', 'новый',
        'в работе', 'в работе', 'in progress', 'in_progress', 'выполняется',
        'работа', 'working', 'active', 'активная', 'активное'
    }
    
    active_tasks = []
    for t in tasks:
        status_id = normalize_status_id(t.get('status', {}).get('id'))
        status_name = t.get('status', {}).get('name', 'Неизвестно')
        status_name_lower = (status_name.lower().strip() if status_name else '')
        
        # Проверяем соответствие по ID
        is_allowed_by_id = status_id is not None and status_id in allowed_status_ids
        
        # Проверяем соответствие по названию (более гибкая проверка)
        is_allowed_by_name = False
        if status_name_lower:
            # Проверяем точное совпадение
            if status_name_lower in allowed_status_names:
                is_allowed_by_name = True
            else:
                # Проверяем частичное совпадение (содержит ключевые слова)
                for allowed_name in allowed_status_names:
                    if allowed_name in status_name_lower or status_name_lower in allowed_name:
                        is_allowed_by_name = True
                        break
        
        # Добавляем если соответствует по ID или по названию
        if is_allowed_by_id or is_allowed_by_name:
            active_tasks.append(t)
    
    if not active_tasks:
        await message.answer(
            "📋 У вас нет заявок со статусом 'Новая' или 'В работе' для отмены.\n\n"
            "Отменить можно только заявки в этих статусах."
        )
        return
    
    tasks = active_tasks
    
    # Создаем клавиатуру с заявками
    keyboard = create_tasks_keyboard(tasks, action_type="cancel")
    
    await message.answer(
        "❌ Выберите заявку для отмены:",
        reply_markup=keyboard
    )
    await state.set_state(TaskCancellation.choosing_from_list)


@router.callback_query(TaskCancellation.choosing_from_list, F.data.startswith("cancel_task:"))
async def handle_cancel_task_selection(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора заявки для отмены."""
    task_id = int(callback_query.data.split(":")[1])
    
    try:
        # Проверяем существование задачи
        task_response = None
        try:
            task_response = await planfix_client.get_task_by_id(
                task_id,
                fields="id,name,status,counterparty"
            )
        except Exception as e:
            # Если получили 400 Bad Request, возможно это internal ID, а не generalId
            # Пробуем найти generalId в BotLog для этого ID
            if "400" in str(e) or "Bad Request" in str(e):
                logger.warning(f"Got 400 Bad Request for task {task_id}, trying to find generalId in BotLog")
                try:
                    # Ищем в BotLog запись с task_id_internal = task_id или task_id = task_id
                    with db_manager.get_db() as db:
                        from database import BotLog
                        import json as json_module
                        logs = db.query(BotLog).filter(
                            BotLog.action == 'create_task',
                            BotLog.telegram_id == callback_query.from_user.id
                        ).all()
                        for log in logs:
                            if log.details:
                                details = log.details if isinstance(log.details, dict) else json_module.loads(log.details) if isinstance(log.details, str) else {}
                                if details.get('task_id_internal') == task_id or details.get('task_id') == task_id:
                                    general_id = details.get('task_id_general') or details.get('task_id')
                                    if general_id and general_id != task_id:
                                        logger.info(f"Found generalId {general_id} for task {task_id}, retrying")
                                        task_response = await planfix_client.get_task_by_id(
                                            general_id,
                                            fields="id,name,status,counterparty"
                                        )
                                        if task_response and task_response.get('result') == 'success':
                                            task_id = general_id  # Обновляем task_id на generalId для дальнейшего использования
                                            break
                except Exception as retry_err:
                    logger.error(f"Failed to retry with generalId for task {task_id}: {retry_err}")
            
            if not task_response:
                raise e  # Пробрасываем исходную ошибку, если не удалось найти generalId
        
        if not task_response or task_response.get('result') != 'success':
            await callback_query.message.edit_text(f"❌ Заявка #{task_id} не найдена.")
            await state.clear()
            return
        
        task = task_response.get('task', {})
        task_name = task.get('name', 'Без названия')
        status_name = task.get('status', {}).get('name', 'Неизвестно')
        
        # Проверяем, что пользователь - владелец заявки
        user = await db_manager.get_user_profile(callback_query.from_user.id)
        counterparty_id = task.get('counterparty', {}).get('id')

        # Извлекаем числовую часть из counterparty_id (может быть "contact:349" или просто 349)
        counterparty_num = None
        if counterparty_id is not None:
            try:
                if isinstance(counterparty_id, str) and ':' in counterparty_id:
                    counterparty_num = int(counterparty_id.split(':')[-1])
                elif isinstance(counterparty_id, (int, float)):
                    counterparty_num = int(counterparty_id)
                elif isinstance(counterparty_id, str) and counterparty_id.isdigit():
                    counterparty_num = int(counterparty_id)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse counterparty_id {counterparty_id} for task {task_id}: {e}")
                counterparty_num = None

        # Если counterparty_id не найден или не может быть извлечен, проверяем через BotLog
        if counterparty_num is None:
            logger.warning(f"Task {task_id} has no counterparty_id, checking via BotLog...")
            # Пробуем найти задачу через BotLog
            from database import BotLog
            with db_manager.get_db() as db:
                # Получаем все записи о создании задач этим пользователем
                bot_logs = db.query(BotLog).filter(
                    BotLog.action == "create_task",
                    BotLog.telegram_id == callback_query.from_user.id
                ).all()
            
            # Проверяем в Python, так как JSON запросы сложнее
            bot_log_found = False
            for log in bot_logs:
                if log.details:
                    try:
                        log_task_id = log.details.get('task_id')
                        if log_task_id is not None:
                            # Нормализуем task_id для сравнения
                            log_task_id_int = int(str(log_task_id).split(':')[-1])
                            if log_task_id_int == task_id:
                                bot_log_found = True
                                break
                    except (ValueError, TypeError, AttributeError):
                        continue
            
            if not bot_log_found:
                logger.warning(f"User {callback_query.from_user.id} tried to cancel task {task_id} but no BotLog entry found")
                await callback_query.message.edit_text("❌ Вы можете отменять только свои заявки.")
                await state.clear()
                return
            # Если нашли в BotLog, значит это заявка пользователя
            logger.info(f"Task {task_id} ownership verified via BotLog for user {callback_query.from_user.id}")
        elif user.restaurant_contact_id and counterparty_num != int(user.restaurant_contact_id):
            # Сравниваем с restaurant_contact_id пользователя
            logger.warning(
                "User %s tried to cancel task %s. Counterparty: %s (%s), User restaurant: %s",
                callback_query.from_user.id,
                task_id,
                counterparty_id,
                counterparty_num,
                user.restaurant_contact_id,
            )
            await callback_query.message.edit_text("❌ Вы можете отменять только свои заявки.")
            await state.clear()
            return
        
        await state.update_data(task_id=task_id, task_name=task_name)
        await callback_query.message.edit_text(
            f"⚠️ Подтверждение отмены\n\n"
            f"📋 Заявка #{task_id}\n"
            f"📝 {task_name}\n"
            f"📊 Текущий статус: {status_name}\n\n"
            f"Вы уверены, что хотите отменить эту заявку?",
            reply_markup=get_confirmation_keyboard("cancel_task", task_id)
        )
        await state.set_state(TaskCancellation.confirming_cancellation)
        
    except Exception as e:
        logger.error(f"Error checking task for cancellation: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Ошибка при проверке заявки.")
        await state.clear()
    
    await callback_query.answer()


@router.callback_query(TaskCancellation.choosing_from_list, F.data == "manual_input")
async def handle_manual_cancel_input(callback_query: CallbackQuery, state: FSMContext):
    """Переключение на ручной ввод номера заявки для отмены."""
    await callback_query.message.edit_text("❌ Введите номер заявки, которую хотите отменить:")
    await state.set_state(TaskCancellation.waiting_for_task_id)
    await callback_query.answer()


@router.callback_query(TaskCancellation.choosing_from_list, F.data == "cancel_action")
async def cancel_task_cancellation_flow(callback_query: CallbackQuery, state: FSMContext):
    """Отмена процесса отмены заявки."""
    await state.clear()
    await callback_query.message.edit_text("❌ Отмена заявки отменена.")
    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=get_main_menu_keyboard()
    )
    await callback_query.answer()


@router.message(TaskCancellation.waiting_for_task_id)
async def cancel_task_id(message: Message, state: FSMContext):
    """Обработка номера заявки для отмены."""
    task_id_text = message.text.strip().lstrip("#")
    
    if not task_id_text.isdigit():
        await message.answer("❌ Некорректный номер. Введите число, например: 12345")
        return
    
    task_id = int(task_id_text)
    
    try:
        # Проверяем существование задачи
        task_response = await planfix_client.get_task_by_id(
            task_id,
            fields="id,name,status,counterparty"
        )
        
        if not task_response or task_response.get('result') != 'success':
            await message.answer(f"❌ Заявка #{task_id} не найдена.")
            await state.clear()
            return
        
        task = task_response.get('task', {})
        task_name = task.get('name', 'Без названия')
        status_name = task.get('status', {}).get('name', 'Неизвестно')
        
        # Проверяем, что пользователь - владелец заявки
        user = await db_manager.get_user_profile(message.from_user.id)
        counterparty_id = task.get('counterparty', {}).get('id')

        # Извлекаем числовую часть из counterparty_id (может быть "contact:349" или просто 349)
        counterparty_num = None
        if counterparty_id is not None:
            try:
                if isinstance(counterparty_id, str) and ':' in counterparty_id:
                    counterparty_num = int(counterparty_id.split(':')[-1])
                elif isinstance(counterparty_id, (int, float)):
                    counterparty_num = int(counterparty_id)
                elif isinstance(counterparty_id, str) and counterparty_id.isdigit():
                    counterparty_num = int(counterparty_id)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse counterparty_id {counterparty_id} for task {task_id}: {e}")
                counterparty_num = None

        # Если counterparty_id не найден или не может быть извлечен, проверяем через BotLog
        if counterparty_num is None:
            logger.warning(f"Task {task_id} has no counterparty_id, checking via BotLog...")
            # Пробуем найти задачу через BotLog
            from database import BotLog
            with db_manager.get_db() as db:
                # Получаем все записи о создании задач этим пользователем
                bot_logs = db.query(BotLog).filter(
                    BotLog.action == "create_task",
                    BotLog.telegram_id == message.from_user.id
                ).all()
            
            # Проверяем в Python, так как JSON запросы сложнее
            bot_log_found = False
            for log in bot_logs:
                if log.details:
                    try:
                        log_task_id = log.details.get('task_id')
                        if log_task_id is not None:
                            # Нормализуем task_id для сравнения
                            log_task_id_int = int(str(log_task_id).split(':')[-1])
                            if log_task_id_int == task_id:
                                bot_log_found = True
                                break
                    except (ValueError, TypeError, AttributeError):
                        continue
            
            if not bot_log_found:
                logger.warning(f"User {message.from_user.id} tried to cancel task {task_id} but no BotLog entry found")
                await message.answer("❌ Вы можете отменять только свои заявки.")
                await state.clear()
                return
            # Если нашли в BotLog, значит это заявка пользователя
            logger.info(f"Task {task_id} ownership verified via BotLog for user {message.from_user.id}")
        elif user.restaurant_contact_id and counterparty_num != int(user.restaurant_contact_id):
            # Сравниваем с restaurant_contact_id пользователя
            logger.warning(
                "User %s tried to cancel task %s. Counterparty: %s (%s), User restaurant: %s",
                message.from_user.id,
                task_id,
                counterparty_id,
                counterparty_num,
                user.restaurant_contact_id,
            )
            await message.answer("❌ Вы можете отменять только свои заявки.")
            await state.clear()
            return
        
        await state.update_data(task_id=task_id, task_name=task_name)
        await message.answer(
            f"⚠️ Подтверждение отмены\n\n"
            f"📋 Заявка #{task_id}\n"
            f"📝 {task_name}\n"
            f"📊 Текущий статус: {status_name}\n\n"
            f"Вы уверены, что хотите отменить эту заявку?",
            reply_markup=get_confirmation_keyboard("cancel_task", task_id)
        )
        await state.set_state(TaskCancellation.confirming_cancellation)
        
    except Exception as e:
        logger.error(f"Error checking task for cancellation: {e}", exc_info=True)
        await message.answer("❌ Ошибка при проверке заявки.")
        await state.clear()


@router.callback_query(F.data.startswith("confirm_cancel_task:"))
async def confirm_task_cancellation(callback_query: CallbackQuery, state: FSMContext):
    """Подтверждение отмены заявки."""
    task_id = int(callback_query.data.split(":")[1])
    user_data = await state.get_data()
    
    try:
        # Убеждаемся, что реестр статусов загружен
        cancelled_status_id = None
        try:
            await ensure_status_registry_loaded()
            cancelled_status_id = require_status_id(StatusKey.CANCELLED)
            logger.info(f"Found cancelled status {cancelled_status_id} via status registry")
        except Exception as registry_err:
            logger.warning(f"Status registry lookup failed ({registry_err}), falling back to API search")
        
        # Если не нашли через реестр, пробуем найти через API
        if cancelled_status_id is None:
            logger.info("Trying to find cancelled status by system names via API...")
            # Сначала пробуем найти по системным именам
            cancelled_status_id = await planfix_client.find_status_id_by_system_names(
                PLANFIX_TASK_PROCESS_ID,
                {"CANCELED", "CANCELLED"}
            )
            if cancelled_status_id:
                logger.info(f"Found cancelled status {cancelled_status_id} by system names")
        
        # Если не нашли по системным именам, пробуем найти по обычным именам
        if cancelled_status_id is None:
            logger.info("Trying to find cancelled status by names via API...")
            cancelled_status_id = await planfix_client.find_status_id_by_names(
                PLANFIX_TASK_PROCESS_ID,
                {"Отменена", "Отменено", "Отмененная", "Отмененное", "Cancelled", "Canceled", "Отмена"}
            )
            if cancelled_status_id:
                logger.info(f"Found cancelled status {cancelled_status_id} by names")
        
        # Если все еще не нашли, пробуем найти через базу данных
        if cancelled_status_id is None:
            logger.info("Trying to find cancelled status in database...")
            try:
                from database import PlanfixTaskStatus
                with db_manager.get_db() as db:
                    # Ищем статус по имени в базе данных
                    statuses = db.query(PlanfixTaskStatus).all()
                    logger.info(f"Searching in {len(statuses)} statuses from database")
                    for status in statuses:
                        status_name_lower = status.name.lower().strip()
                        logger.debug(f"Checking status {status.id}: '{status.name}' (normalized: '{status_name_lower}')")
                        # Ищем по ключевым словам: отмен, cancel (в любом падеже)
                        if any(keyword in status_name_lower for keyword in ["отмен", "cancel"]):
                            cancelled_status_id = status.id
                            logger.info(f"Found cancelled status {cancelled_status_id} ({status.name}) in database")
                            break
            except Exception as db_err:
                logger.warning(f"Failed to search cancelled status in database: {db_err}", exc_info=True)
        
        # Если все еще не нашли, получаем все статусы из API для отладки
        if cancelled_status_id is None:
            logger.error("Cancelled status not found by any method. Fetching all available statuses for debugging...")
            try:
                statuses_response = await planfix_client.get_process_task_statuses(
                    PLANFIX_TASK_PROCESS_ID,
                    fields="id,name,systemName,isFinal"
                )
                if statuses_response and statuses_response.get('result') == 'success':
                    statuses = statuses_response.get('statuses', [])
                    logger.error(f"Available statuses in process {PLANFIX_TASK_PROCESS_ID}:")
                    for status in statuses:
                        logger.error(f"  - ID: {status.get('id')}, Name: '{status.get('name')}', SystemName: '{status.get('systemName')}', IsFinal: {status.get('isFinal')}")
            except Exception as debug_err:
                logger.error(f"Failed to fetch statuses for debugging: {debug_err}", exc_info=True)
            
            await callback_query.message.edit_text("❌ Не удалось найти статус отмены. Обратитесь к администратору.")
            await state.clear()
            return

        # Получаем информацию о пользователе
        user = await db_manager.get_user_profile(callback_query.from_user.id)
        
        # Обновляем статус задачи на "Отменена"
        update_response = await planfix_client.update_task(
            task_id,
            status_id=cancelled_status_id
        )
        
        if update_response and update_response.get('result') == 'success':
            # Добавляем комментарий об отмене
            await planfix_client.add_comment_to_task(
                task_id,
                description=f"❌ Заявка отменена заявителем: {user.full_name}"
            )
            
            await state.clear()
            await callback_query.message.edit_text(
                f"✅ Заявка #{task_id} успешно отменена!\n\n"
                f"📝 {user_data.get('task_name', 'Без названия')}\n"
                f"📊 Статус: Отменена"
            )
            await callback_query.message.answer(
                "Выберите действие:",
                reply_markup=get_main_menu_keyboard()
            )
            logger.info(f"Task {task_id} cancelled by user {callback_query.from_user.id}")
        else:
            await callback_query.message.edit_text(
                f"❌ Не удалось отменить заявку #{task_id}.\n\n"
                "Возможно, заявка уже выполнена или находится в статусе, "
                "который не позволяет отмену."
            )
            await state.clear()
        
    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {e}", exc_info=True)
        await callback_query.message.edit_text("❌ Ошибка при отмене заявки.")
        await state.clear()
    
    await callback_query.answer()


@router.callback_query(F.data.startswith("cancel_cancel_task:"))
async def abort_task_cancellation(callback_query: CallbackQuery, state: FSMContext):
    """Отмена процесса отмены заявки."""
    await state.clear()
    await callback_query.message.edit_text("❌ Отмена заявки отменена.")
    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=get_main_menu_keyboard()
    )
    await callback_query.answer()

