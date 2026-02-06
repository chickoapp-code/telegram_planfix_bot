"""
Обработчики команд для администраторов
Версия: 1.0
"""

import logging
import asyncio
import json
from typing import List
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext

from states import AdminManagement
from keyboards import (
    get_admin_main_menu_keyboard,
    get_admin_users_menu_keyboard,
    get_admin_executors_menu_keyboard,
    get_admin_profile_actions_keyboard,
    get_admin_edit_user_keyboard,
    get_admin_edit_executor_keyboard,
    get_admin_delete_confirmation_keyboard,
    create_users_list_keyboard,
    create_executors_list_keyboard,
)
from services.db_service import db_manager
from config import TELEGRAM_ADMIN_IDS, FRANCHISE_GROUPS
from database import UserProfile, ExecutorProfile, TaskAssignment, BotLog
from executor_handlers import resolve_counterparty_name
from shared_cache import cache
from services.status_registry import StatusKey, status_labels
from db_manager import DBManager
from db_manager import DBManager


async def _format_user_profile(user_id: int) -> str:
    """Форматирует профиль пользователя для отображения."""
    user = await db_manager.get_user_profile(user_id)
    if not user:
        return "❌ Пользователь не найден."
    
    franchise_name = FRANCHISE_GROUPS.get(user.franchise_group_id, {}).get("name", f"ID: {user.franchise_group_id}")
    
    return (
        f"👤 <b>Профиль пользователя</b>\n\n"
        f"🆔 Telegram ID: <code>{user.telegram_id}</code>\n"
        f"👤 ФИО: {user.full_name or 'не указано'}\n"
        f"📱 Телефон: {user.phone_number or 'не указан'}\n"
        f"📧 Email: {user.email or 'не указан'}\n"
        f"💼 Должность: {user.position or 'не указана'}\n"
        f"🏢 Концепция: {franchise_name}\n"
        f"🏪 ID ресторана: {user.restaurant_contact_id or 'не указан'}\n"
        f"🔗 Planfix Contact ID: {user.planfix_contact_id or 'не указан'}\n"
        f"📅 Дата регистрации: {user.registration_date.strftime('%Y-%m-%d %H:%M:%S') if user.registration_date else 'не указана'}\n"
        f"✅ Активен: {'Да' if user.is_active else 'Нет'}"
    )


async def _format_executor_profile(executor_id: int) -> str:
    """Форматирует профиль исполнителя для отображения."""
    executor = await db_manager.get_executor_profile(executor_id)
    if not executor:
        return "❌ Исполнитель не найден."
    
    from db_manager import DBManager
    sync_db_manager = DBManager()
    
    with sync_db_manager.get_db() as db:
        assignments_count = db.query(TaskAssignment).filter(
            TaskAssignment.executor_telegram_id == executor_id,
            TaskAssignment.status == "active"
        ).count()
    
    concept_names = []
    if executor.serving_franchise_groups:
        for cid in executor.serving_franchise_groups:
            name = FRANCHISE_GROUPS.get(cid, {}).get("name", f"ID: {cid}")
            concept_names.append(name)
    
    restaurants_count = len(executor.serving_restaurants) if executor.serving_restaurants else 0
    
    return (
        f"👷 <b>Профиль исполнителя</b>\n\n"
        f"🆔 Telegram ID: <code>{executor.telegram_id}</code>\n"
        f"👤 ФИО: {executor.full_name or 'не указано'}\n"
        f"📱 Телефон: {executor.phone_number or 'не указан'}\n"
        f"📧 Email: {executor.email or 'не указан'}\n"
        f"💼 Должность: {executor.position_role or 'не указана'}\n"
        f"🧭 Направление: {executor.service_direction or 'не указано'}\n"
        f"🏢 Концепции: {', '.join(concept_names) if concept_names else 'не указаны'}\n"
        f"🏪 Ресторанов: {restaurants_count}\n"
        f"🔗 Planfix Contact ID: {executor.planfix_contact_id or 'не указан'}\n"
        f"🔗 Planfix User ID: {executor.planfix_user_id or 'не указан'}\n"
        f"📋 Статус: {executor.profile_status or 'не указан'}\n"
        f"📅 Дата регистрации: {executor.registration_date.strftime('%Y-%m-%d %H:%M:%S') if executor.registration_date else 'не указана'}\n"
        f"📋 Активных назначений: {assignments_count}"
    )

logger = logging.getLogger(__name__)
router = Router()


def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором."""
    return user_id in TELEGRAM_ADMIN_IDS


# ============================================================================
# КОМАНДЫ
# ============================================================================

@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    """Команда для открытия админ-меню."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для доступа к админ-меню.")
        return
    
    await state.set_state(AdminManagement.main_menu)
    await message.answer(
        "🔐 <b>Админ-панель</b>\n\n"
        "Выберите действие:",
        reply_markup=get_admin_main_menu_keyboard(),
        parse_mode="HTML"
    )


@router.message(Command("admin_tasks"))
async def cmd_admin_tasks(message: Message, state: FSMContext):
    """Команда для просмотра заявок пользователя: /admin_tasks <user_id>"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для доступа к админ-командам.")
        return
    
    try:
        # Парсим аргументы команды
        args = message.text.split()[1:] if len(message.text.split()) > 1 else []
        
        if not args:
            await message.answer(
                "📋 <b>Просмотр заявок пользователя</b>\n\n"
                "Использование: <code>/admin_tasks &lt;user_id&gt;</code>\n\n"
                "Пример: <code>/admin_tasks 123456789</code>\n\n"
                "Или используйте меню: /admin → 👥 Управление пользователями → выберите пользователя → 📋 Заявки пользователя",
                parse_mode="HTML"
            )
            return
        
        user_id = int(args[0])
        user = await db_manager.get_user_profile(user_id)
        
        if not user:
            await message.answer(f"❌ Пользователь с ID {user_id} не найден.")
            return
        
        # Получаем заявки пользователя
        from user_handlers import get_user_tasks
        tasks = await get_user_tasks(user_id, limit=50, only_active=False)
        
        if tasks is None:
            await message.answer("❌ Ошибка при получении заявок пользователя.")
            return
        
        if not tasks:
            user_name = user.full_name or f"ID: {user_id}"
            await message.answer(
                f"📋 <b>Заявки пользователя</b>\n\n"
                f"👤 <b>{user_name}</b> (ID: {user_id})\n\n"
                f"❌ У пользователя нет заявок.",
                parse_mode="HTML"
            )
            return
        
        # Формируем список заявок
        user_name = user.full_name or f"ID: {user_id}"
        lines = [
            f"📋 <b>Заявки пользователя</b>\n",
            f"👤 <b>{user_name}</b> (ID: {user_id})\n",
            f"Всего заявок: {len(tasks)}\n",
            "────────────────────\n"
        ]
        
        # Показываем первые 20 заявок
        for task in tasks[:20]:
            task_id = task.get('id')
            task_name = task.get('name', 'Без названия')[:50]
            status_obj = task.get('status', {})
            status_name = status_obj.get('name', 'Неизвестно') if isinstance(status_obj, dict) else 'Неизвестно'
            
            # Получаем название ресторана (если есть)
            counterparty_id = None
            counterparty_obj = task.get('counterparty', {})
            if isinstance(counterparty_obj, dict):
                counterparty_id = counterparty_obj.get('id')
            
            restaurant_info = ""
            if counterparty_id:
                restaurant_info = f"\n🏪 Ресторан ID: {counterparty_id}"
            
            lines.append(
                f"📋 <b>#{task_id}</b> – {status_name}\n"
                f"📝 {task_name}{restaurant_info}\n"
                f"────────────────────"
            )
        
        if len(tasks) > 20:
            lines.append(f"\n💡 <i>... и ещё {len(tasks) - 20} заявок</i>")
        
        # Создаем клавиатуру с кнопками для просмотра деталей заявок
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        task_buttons = []
        for task in tasks[:10]:  # Показываем кнопки для первых 10 заявок
            task_id = task.get('id')
            task_name = task.get('name', f'Заявка #{task_id}')[:30]
            task_buttons.append([
                InlineKeyboardButton(
                    text=f"#{task_id} - {task_name}",
                    callback_data=f"admin_view_task:{task_id}"
                )
            ])
        
        task_buttons.append([
            InlineKeyboardButton(text="👤 Профиль пользователя", callback_data=f"admin_view_user:{user_id}")
        ])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=task_buttons)
        
        await message.answer(
            "\n".join(lines),
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except ValueError:
        await message.answer("❌ Неверный формат команды. Используйте: <code>/admin_tasks &lt;user_id&gt;</code>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in admin_tasks command: {e}", exc_info=True)
        await message.answer("❌ Ошибка при получении заявок.")


async def _show_admin_executor_tasks_page(message_or_callback, executor_id: int, executor_name: str, tasks: list, page: int = 0, is_callback: bool = False):
    """
    Показывает страницу задач исполнителя для админа с пагинацией.
    
    Args:
        message_or_callback: Message или CallbackQuery объект
        executor_id: ID исполнителя
        executor_name: Имя исполнителя
        tasks: Список всех задач
        page: Номер страницы (начинается с 0)
        is_callback: True если это callback (нужно использовать edit_text), False если message (answer)
    """
    TASKS_PER_PAGE = 5  # Количество задач на странице
    
    total_tasks = len(tasks)
    total_pages = (total_tasks + TASKS_PER_PAGE - 1) // TASKS_PER_PAGE  # Округление вверх
    
    if page < 0:
        page = 0
    if page >= total_pages:
        page = total_pages - 1
    
    start_idx = page * TASKS_PER_PAGE
    end_idx = min(start_idx + TASKS_PER_PAGE, total_tasks)
    page_tasks = tasks[start_idx:end_idx]
    
    # Формируем заголовок
    header = f"📋 <b>Заявки исполнителя</b>\n"
    header += f"👷 <b>{executor_name}</b> (ID: {executor_id})\n"
    header += f"Всего заявок: {total_tasks}\n"
    if total_pages > 1:
        header += f"📄 Страница {page + 1} из {total_pages}\n"
    header += "────────────────────\n"
    
    lines = [header]
    
    sync_db_manager = DBManager()
    
    # Формируем список задач для текущей страницы
    for task in page_tasks:
        task_id = task.get('id')
        task_name = task.get('name', 'Без названия')[:50]
        
        # Получаем название ресторана
        _cp_key = f"cp_name:{task_id}"
        counterparty = cache.get(_cp_key)
        if not counterparty:
            # Пытаемся получить название синхронно
            try:
                counterparty = await resolve_counterparty_name(task)
                # Сохраняем в кэш на 5 минут
                cache.set(_cp_key, counterparty, ttl_seconds=300)
            except Exception as e:
                logger.debug(f"Failed to resolve counterparty name for task {task_id}: {e}")
                # Если не удалось получить название, показываем ID ресторана (если есть)
                counterparty_obj = task.get('counterparty', {})
                if isinstance(counterparty_obj, dict):
                    counterparty_id = counterparty_obj.get('id')
                    if counterparty_id:
                        try:
                            if isinstance(counterparty_id, str) and ':' in counterparty_id:
                                counterparty_id = counterparty_id.split(':')[-1]
                            counterparty = f"Ресторан ID: {counterparty_id}"
                        except Exception:
                            counterparty = "Не указан"
                    else:
                        counterparty = "Не указан"
                else:
                    counterparty = "Не указан"
        
        # Определяем и нормализуем статус
        status_id = None
        status_name = None
        try:
            with sync_db_manager.get_db() as db:
                task_cache = sync_db_manager.get_task_cache(db, task_id)
                if task_cache and task_cache.status_id:
                    status_id = task_cache.status_id
                    status_name = task_cache.status_name
        except Exception:
            pass
        
        # Если статус из кеша недоступен, используем статус из задачи
        if status_id is None:
            raw_status = task.get('status', {}) or {}
            raw_status_id = raw_status.get('id')
            status_name = raw_status.get('name')
            if isinstance(raw_status_id, int):
                status_id = raw_status_id
            elif isinstance(raw_status_id, str):
                try:
                    status_id = int(str(raw_status_id).split(':')[-1])
                except Exception:
                    status_id = None
        
        # Определяем отображаемое имя статуса
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
    
    # Создаем inline-клавиатуру с кнопками выбора задач и навигации
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    buttons = []
    
    # Кнопки выбора задач (по 2 в ряд)
    task_rows = []
    for i in range(0, len(page_tasks), 2):
        row = []
        for task in page_tasks[i:i+2]:
            task_id = task.get('id')
            row.append(InlineKeyboardButton(
                text=f"#{task_id}",
                callback_data=f"admin_view_task:{task_id}"
            ))
        task_rows.append(row)
    
    buttons.extend(task_rows)
    
    # Кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=f"admin_executor_tasks_page:{executor_id}:{page-1}"
        ))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(
            text="Вперед ▶️",
            callback_data=f"admin_executor_tasks_page:{executor_id}:{page+1}"
        ))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Кнопка обновления списка
    buttons.append([InlineKeyboardButton(
        text="🔄 Обновить список",
        callback_data=f"admin_refresh_executor_tasks:{executor_id}"
    )])
    
    # Кнопка возврата к профилю исполнителя
    buttons.append([InlineKeyboardButton(
        text="◀️ Назад к профилю",
        callback_data=f"admin_view_executor:{executor_id}"
    )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    text = "\n".join(lines)
    
    # Отправляем или редактируем сообщение
    if is_callback:
        try:
            await message_or_callback.message.edit_text(
                text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            await message_or_callback.answer()
        except Exception as e:
            logger.debug(f"Error editing message: {e}")
            await message_or_callback.answer()
    else:
        await message_or_callback.answer(text, reply_markup=keyboard, parse_mode="HTML")


@router.message(Command("admin_executor_tasks"))
async def cmd_admin_executor_tasks(message: Message, state: FSMContext):
    """Команда для просмотра заявок исполнителя: /admin_executor_tasks <executor_id>"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для доступа к админ-командам.")
        return
    
    try:
        # Парсим аргументы команды
        args = message.text.split()[1:] if len(message.text.split()) > 1 else []
        
        if not args:
            await message.answer(
                "📋 <b>Просмотр заявок исполнителя</b>\n\n"
                "Использование: <code>/admin_executor_tasks &lt;executor_id&gt;</code>\n\n"
                "Пример: <code>/admin_executor_tasks 123456789</code>\n\n"
                "Или используйте меню: /admin → 👷 Управление исполнителями → выберите исполнителя → 📋 Заявки исполнителя",
                parse_mode="HTML"
            )
            return
        
        executor_id = int(args[0])
        executor = await db_manager.get_executor_profile(executor_id)
        
        if not executor:
            await message.answer(f"❌ Исполнитель с ID {executor_id} не найден.")
            return
        
        # Получаем planfix_user_id или planfix_contact_id исполнителя
        executor_planfix_id = None
        executor_planfix_id_type = None
        
        if executor.planfix_user_id:
            try:
                executor_planfix_id = int(str(executor.planfix_user_id).split(':')[-1])
                executor_planfix_id_type = "user"
            except (ValueError, TypeError):
                pass
        
        if not executor_planfix_id and executor.planfix_contact_id:
            try:
                executor_planfix_id = int(str(executor.planfix_contact_id).split(':')[-1])
                executor_planfix_id_type = "contact"
            except (ValueError, TypeError):
                pass
        
        tasks = []
        
        # Ищем задачи в БД бота (TaskCache) через TaskAssignment
        from db_manager import DBManager
        from database import TaskAssignment, TaskCache
        from services.status_registry import StatusKey, collect_status_ids, require_status_id
        
        sync_db_manager = DBManager()
        
        # Получаем статусы "Новая" и "В работе"
        working_status_ids = collect_status_ids(
            (StatusKey.NEW, StatusKey.IN_PROGRESS),
            required=False,
        )
        if not working_status_ids:
            try:
                working_status_ids = [
                    require_status_id(StatusKey.NEW),
                    require_status_id(StatusKey.IN_PROGRESS)
                ]
                working_status_ids = [sid for sid in working_status_ids if sid is not None]
            except Exception:
                working_status_ids = []
        
        if not working_status_ids:
            await message.answer(
                f"❌ Не удалось получить ID статусов. Проверьте настройки.",
                parse_mode="HTML"
            )
            return
        
        with sync_db_manager.get_db() as db:
            # Получаем все активные назначения для этого исполнителя
            assignments = db.query(TaskAssignment).filter(
                TaskAssignment.executor_telegram_id == executor_id,
                TaskAssignment.status == "active"
            ).all()
            
            logger.info(f"Found {len(assignments)} active task assignments for executor {executor_id}")
            
            if assignments:
                # Получаем task_id из назначений
                task_ids = [assignment.task_id for assignment in assignments]
                
                # Получаем задачи из TaskCache
                cached_tasks = db.query(TaskCache).filter(
                    TaskCache.task_id.in_(task_ids),
                    TaskCache.status_id.in_(working_status_ids)
                ).order_by(TaskCache.date_of_last_update.desc().nullslast()).all()
                
                logger.info(f"Found {len(cached_tasks)} tasks in TaskCache for executor {executor_id} with statuses {working_status_ids}")
                
                # Преобразуем TaskCache в формат, похожий на ответ API
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
                        'dateTime': cached_task.date_of_last_update.isoformat() if cached_task.date_of_last_update else None,
                        'dateOfLastUpdate': cached_task.date_of_last_update.isoformat() if cached_task.date_of_last_update else None
                    }
                    tasks.append(task_dict)
        
        if not tasks:
            executor_name = executor.full_name or f"ID: {executor_id}"
            await message.answer(
                f"📋 <b>Заявки исполнителя</b>\n\n"
                f"👷 <b>{executor_name}</b> (ID: {executor_id})\n\n"
                f"❌ У исполнителя нет назначенных заявок со статусами 'Новая' или 'В работе'.",
                parse_mode="HTML"
            )
            return
        
        # Сохраняем задачи в кэше для пагинации
        executor_name = executor.full_name or f"ID: {executor_id}"
        cache.set(f"admin_executor_tasks:{executor_id}", tasks, ttl_seconds=300)
        
        # Показываем первую страницу
        await _show_admin_executor_tasks_page(message, executor_id, executor_name, tasks, page=0)
    except ValueError:
        await message.answer("❌ Неверный формат команды. Используйте: <code>/admin_executor_tasks &lt;executor_id&gt;</code>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in admin_executor_tasks command: {e}", exc_info=True)
        await message.answer("❌ Ошибка при получении заявок.")


# ============================================================================
# ГЛАВНОЕ МЕНЮ
# ============================================================================

@router.message(AdminManagement.main_menu, F.text == "👥 Управление пользователями")
async def admin_users_menu(message: Message, state: FSMContext):
    """Меню управления пользователями."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав.")
        return
    
    await message.answer(
        "👥 <b>Управление пользователями</b>\n\n"
        "Выберите действие:",
        reply_markup=get_admin_users_menu_keyboard(),
        parse_mode="HTML"
    )


@router.message(AdminManagement.main_menu, F.text == "👷 Управление исполнителями")
async def admin_executors_menu(message: Message, state: FSMContext):
    """Меню управления исполнителями."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав.")
        return
    
    await message.answer(
        "👷 <b>Управление исполнителями</b>\n\n"
        "Выберите действие:",
        reply_markup=get_admin_executors_menu_keyboard(),
        parse_mode="HTML"
    )


@router.message(AdminManagement.main_menu, F.text == "🔄 Синхронизировать задачи из Planfix")
async def admin_sync_tasks(message: Message, state: FSMContext):
    """Синхронизация задач из Planfix в БД бота."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для доступа к админ-командам.")
        return
    
    await message.answer("⏳ Начинаю синхронизацию задач из Planfix...")
    
    try:
        from planfix_client import planfix_client
        from services.status_registry import StatusKey, collect_status_ids, require_status_id
        from datetime import datetime
        from db_manager import DBManager
        from database import TaskCache
        
        sync_db_manager = DBManager()
        
        # Получаем статусы "Новая" и "В работе"
        working_status_ids = collect_status_ids(
            (StatusKey.NEW, StatusKey.IN_PROGRESS),
            required=False,
        )
        if not working_status_ids:
            try:
                working_status_ids = [
                    require_status_id(StatusKey.NEW),
                    require_status_id(StatusKey.IN_PROGRESS)
                ]
                working_status_ids = [sid for sid in working_status_ids if sid is not None]
            except Exception:
                working_status_ids = []
        
        if not working_status_ids:
            await message.answer("❌ Не удалось получить ID статусов. Проверьте настройки.")
            return
        
        total_synced = 0
        total_updated = 0
        total_created = 0
        total_errors = 0
        
        # Получаем все задачи для каждого статуса с пагинацией
        for status_id in working_status_ids:
            if status_id is None:
                continue
            
            logger.info(f"🔄 Syncing tasks with status {status_id}")
            await message.answer(f"🔄 Синхронизация задач со статусом {status_id}...")
            
            filters = [
                {
                    "type": 10,  # Task status
                    "operator": "equal",
                    "value": status_id
                }
            ]
            
            page_size = 100
            offset = 0
            max_pages = 20  # Максимум 20 страниц (2000 задач) для каждого статуса
            
            while offset < max_pages * page_size:
                try:
                    response = await planfix_client.get_task_list(
                        filters=filters,
                        fields="id,name,description,status,project,counterparty,dateTime,dateOfLastUpdate,template,assignees",
                        page_size=page_size,
                        offset=offset
                    )
                    
                    if response and response.get('result') == 'success':
                        tasks_list = response.get('tasks', [])
                        if not tasks_list:
                            break
                        
                        logger.info(f"📥 Processing {len(tasks_list)} tasks with status {status_id} (offset={offset})")
                        
                        with sync_db_manager.get_db() as db:
                            for task in tasks_list:
                                try:
                                    task_id = task.get('id')
                                    if not task_id:
                                        continue
                                    
                                    # Нормализуем task_id
                                    if isinstance(task_id, str) and ':' in task_id:
                                        task_id = int(task_id.split(':')[-1])
                                    else:
                                        task_id = int(task_id)
                                    
                                    # Извлекаем данные задачи
                                    task_name = task.get('name', '')
                                    status_obj = task.get('status', {})
                                    status_id_task = status_obj.get('id') if isinstance(status_obj, dict) else None
                                    status_name = status_obj.get('name') if isinstance(status_obj, dict) else None
                                    
                                    # Нормализуем status_id
                                    if isinstance(status_id_task, str) and ':' in str(status_id_task):
                                        status_id_task = int(str(status_id_task).split(':')[-1])
                                    elif isinstance(status_id_task, int):
                                        pass
                                    else:
                                        status_id_task = None
                                    
                                    counterparty_obj = task.get('counterparty', {})
                                    counterparty_id = counterparty_obj.get('id') if isinstance(counterparty_obj, dict) else None
                                    if counterparty_id:
                                        if isinstance(counterparty_id, str) and ':' in str(counterparty_id):
                                            counterparty_id = int(str(counterparty_id).split(':')[-1])
                                        else:
                                            counterparty_id = int(counterparty_id)
                                    
                                    project_obj = task.get('project', {})
                                    project_id = project_obj.get('id') if isinstance(project_obj, dict) else None
                                    if project_id:
                                        if isinstance(project_id, str) and ':' in str(project_id):
                                            project_id = int(str(project_id).split(':')[-1])
                                        else:
                                            project_id = int(project_id)
                                    
                                    template_obj = task.get('template', {})
                                    template_id = template_obj.get('id') if isinstance(template_obj, dict) else None
                                    if template_id:
                                        if isinstance(template_id, str) and ':' in str(template_id):
                                            template_id = int(str(template_id).split(':')[-1])
                                        else:
                                            template_id = int(template_id)
                                    
                                    # Проверяем, создана ли задача через бота (по имени или описанию)
                                    created_by_bot = False
                                    if task_name:
                                        task_name_lower = task_name.lower()
                                        if any(keyword in task_name_lower for keyword in ['заявка', 'задача', 'обращение']):
                                            created_by_bot = True
                                    
                                    # Дата последнего обновления
                                    date_of_last_update = None
                                    date_str = task.get('dateOfLastUpdate')
                                    if date_str:
                                        try:
                                            date_of_last_update = datetime.fromisoformat(date_str.replace('Z', '+00:00')).replace(tzinfo=None)
                                        except:
                                            pass
                                    
                                    # Проверяем, существует ли задача в кэше
                                    existing_task = db.query(TaskCache).filter(TaskCache.task_id == task_id).first()
                                    
                                    if existing_task:
                                        # Обновляем существующую задачу
                                        existing_task.name = task_name
                                        existing_task.status_id = status_id_task
                                        existing_task.status_name = status_name
                                        existing_task.counterparty_id = counterparty_id
                                        existing_task.project_id = project_id
                                        existing_task.template_id = template_id
                                        existing_task.created_by_bot = created_by_bot
                                        existing_task.date_of_last_update = date_of_last_update
                                        existing_task.updated_at = datetime.now()
                                        total_updated += 1
                                    else:
                                        # Создаем новую задачу
                                        new_task = TaskCache(
                                            task_id=task_id,
                                            name=task_name,
                                            status_id=status_id_task,
                                            status_name=status_name,
                                            counterparty_id=counterparty_id,
                                            project_id=project_id,
                                            template_id=template_id,
                                            created_by_bot=created_by_bot,
                                            date_of_last_update=date_of_last_update
                                        )
                                        db.add(new_task)
                                        total_created += 1
                                    
                                    total_synced += 1
                                    
                                    # Создаем записи в TaskAssignment для всех назначенных исполнителей
                                    try:
                                        from database import TaskAssignment, ExecutorProfile
                                        assignees = task.get('assignees', {})
                                        if isinstance(assignees, dict):
                                            assignee_users = assignees.get('users', []) or []
                                            assignee_contacts = assignees.get('contacts', []) or []
                                            
                                            # Собираем все назначенные ID (users и contacts)
                                            assigned_ids = set()
                                            for user in assignee_users:
                                                if isinstance(user, dict):
                                                    user_id = user.get('id')
                                                    if user_id:
                                                        if isinstance(user_id, str) and ':' in user_id:
                                                            assigned_ids.add(user_id.split(':')[-1])
                                                        else:
                                                            assigned_ids.add(str(user_id))
                                            
                                            for contact in assignee_contacts:
                                                if isinstance(contact, dict):
                                                    contact_id = contact.get('id')
                                                    if contact_id:
                                                        if isinstance(contact_id, str) and ':' in contact_id:
                                                            assigned_ids.add(contact_id.split(':')[-1])
                                                        else:
                                                            assigned_ids.add(str(contact_id))
                                            
                                            # Находим исполнителей по planfix_user_id или planfix_contact_id
                                            if assigned_ids:
                                                executors = db.query(ExecutorProfile).filter(
                                                    ExecutorProfile.profile_status == "активен"
                                                ).all()
                                                
                                                for executor in executors:
                                                    executor_user_id = str(executor.planfix_user_id) if executor.planfix_user_id else None
                                                    executor_contact_id = str(executor.planfix_contact_id) if executor.planfix_contact_id else None
                                                    
                                                    # Проверяем, назначен ли исполнитель
                                                    if (executor_user_id and executor_user_id in assigned_ids) or \
                                                       (executor_contact_id and executor_contact_id in assigned_ids):
                                                        # Проверяем, нет ли уже активного назначения
                                                        existing = db.query(TaskAssignment).filter(
                                                            TaskAssignment.task_id == task_id,
                                                            TaskAssignment.executor_telegram_id == executor.telegram_id,
                                                            TaskAssignment.status == "active"
                                                        ).first()
                                                        
                                                        if not existing:
                                                            assignment = TaskAssignment(
                                                                task_id=task_id,
                                                                executor_telegram_id=executor.telegram_id,
                                                                planfix_user_id=executor.planfix_user_id,
                                                                status="active"
                                                            )
                                                            db.add(assignment)
                                                            logger.debug(f"Created TaskAssignment: task {task_id} -> executor {executor.telegram_id}")
                                    except Exception as assign_err:
                                        logger.warning(f"Error creating TaskAssignment for task {task.get('id')}: {assign_err}")
                                    
                                except Exception as task_err:
                                    logger.error(f"Error processing task {task.get('id')}: {task_err}", exc_info=True)
                                    total_errors += 1
                            
                            db.commit()
                        
                        # Если получили меньше задач, чем page_size, значит это последняя страница
                        if len(tasks_list) < page_size:
                            break
                    else:
                        logger.warning(f"Failed to fetch tasks with status {status_id} at offset {offset}: {response}")
                        break
                    
                    offset += page_size
                    
                except Exception as e:
                    logger.error(f"Error fetching tasks with status {status_id} at offset {offset}: {e}", exc_info=True)
                    total_errors += 1
                    break
        
        result_message = (
            f"✅ <b>Синхронизация завершена</b>\n\n"
            f"📊 Всего обработано: {total_synced}\n"
            f"➕ Создано новых: {total_created}\n"
            f"🔄 Обновлено: {total_updated}\n"
            f"❌ Ошибок: {total_errors}"
        )
        
        await message.answer(result_message, parse_mode="HTML")
        logger.info(f"✅ Task sync completed: {total_synced} total, {total_created} created, {total_updated} updated, {total_errors} errors")
        
    except Exception as e:
        logger.error(f"Error syncing tasks: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка при синхронизации: {e}")


@router.message(AdminManagement.main_menu, F.text == "📥 Загрузить задачи без шаблона (10 дней)")
async def admin_sync_tasks_without_template(message: Message, state: FSMContext):
    """Синхронизация задач без шаблона за последние 10 дней, у которых есть исполнитель из бота."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для доступа к админ-командам.")
        return
    
    await message.answer("⏳ Начинаю загрузку задач без шаблона за последние 10 дней...")
    
    try:
        from planfix_client import planfix_client
        from datetime import datetime, timedelta
        from db_manager import DBManager
        from database import TaskCache, ExecutorProfile
        
        sync_db_manager = DBManager()
        
        # Получаем список всех исполнителей из бота с их Planfix ID
        with sync_db_manager.get_db() as db:
            executors = db.query(ExecutorProfile).filter(
                ExecutorProfile.profile_status == "активен"
            ).all()
        
        # Создаем словарь для быстрого поиска: planfix_id -> telegram_id
        executor_planfix_ids = set()
        for executor in executors:
            if executor.planfix_user_id:
                try:
                    planfix_id = int(str(executor.planfix_user_id).split(':')[-1])
                    executor_planfix_ids.add(('user', planfix_id))
                except (ValueError, TypeError):
                    pass
            if executor.planfix_contact_id:
                try:
                    planfix_id = int(str(executor.planfix_contact_id).split(':')[-1])
                    executor_planfix_ids.add(('contact', planfix_id))
                except (ValueError, TypeError):
                    pass
        
        logger.info(f"📋 Found {len(executor_planfix_ids)} executor Planfix IDs to check")
        
        if not executor_planfix_ids:
            await message.answer("❌ Не найдено активных исполнителей с Planfix ID.")
            return
        
        # Дата 10 дней назад
        date_from = (datetime.now() - timedelta(days=10)).strftime("%d-%m-%Y")
        
        # Фильтр по дате создания (последние 10 дней)
        filters = [
            {
                "type": 13,  # Start date filter
                "operator": "gt",  # greater than
                "value": {
                    "dateType": "otherDate",
                    "dateValue": date_from
                }
            }
        ]
        
        total_synced = 0
        total_updated = 0
        total_created = 0
        total_errors = 0
        total_with_executor = 0
        
        logger.info(f"🔄 Loading tasks created after {date_from}")
        await message.answer(f"🔄 Загрузка задач, созданных после {date_from}...")
        
        page_size = 100
        offset = 0
        max_pages = 50  # Максимум 50 страниц (5000 задач)
        
        while offset < max_pages * page_size:
            try:
                response = await planfix_client.get_task_list(
                    filters=filters,
                    fields="id,name,description,status,project,counterparty,dateTime,dateOfLastUpdate,template,assignees",
                    page_size=page_size,
                    offset=offset,
                    result_order=[{"field": "dateTime", "direction": "Desc"}]
                )
                
                if response and response.get('result') == 'success':
                    tasks_list = response.get('tasks', [])
                    if not tasks_list:
                        break
                    
                    logger.info(f"📥 Processing {len(tasks_list)} tasks (offset={offset})")
                    
                    with sync_db_manager.get_db() as db:
                        for task in tasks_list:
                            try:
                                task_id = task.get('id')
                                if not task_id:
                                    continue
                                
                                # Нормализуем task_id
                                if isinstance(task_id, str) and ':' in task_id:
                                    task_id = int(task_id.split(':')[-1])
                                else:
                                    task_id = int(task_id)
                                
                                # Проверяем, есть ли шаблон
                                template_obj = task.get('template', {})
                                template_id = template_obj.get('id') if isinstance(template_obj, dict) else None
                                
                                # Пропускаем задачи с шаблоном
                                if template_id:
                                    continue
                                
                                # Проверяем, есть ли среди назначенных исполнителей те, кто в боте
                                assignees = task.get('assignees', {}) or {}
                                assignee_users = assignees.get('users', []) or []
                                assignee_contacts = assignees.get('contacts', []) or []
                                all_assignees = assignee_users + assignee_contacts
                                
                                has_bot_executor = False
                                for assignee in all_assignees:
                                    if not isinstance(assignee, dict):
                                        continue
                                    
                                    assignee_id = assignee.get('id', '')
                                    if not assignee_id:
                                        continue
                                    
                                    if isinstance(assignee_id, str):
                                        if ':' in assignee_id:
                                            assignee_type, assignee_num = assignee_id.split(':', 1)
                                            try:
                                                assignee_num_int = int(assignee_num)
                                                if (assignee_type, assignee_num_int) in executor_planfix_ids:
                                                    has_bot_executor = True
                                                    break
                                            except (ValueError, TypeError):
                                                continue
                                    elif isinstance(assignee_id, int):
                                        if ('user', assignee_id) in executor_planfix_ids:
                                            has_bot_executor = True
                                            break
                                
                                # Пропускаем задачи без исполнителей из бота
                                if not has_bot_executor:
                                    continue
                                
                                total_with_executor += 1
                                
                                # Создаем записи в TaskAssignment для всех назначенных исполнителей
                                try:
                                    from database import TaskAssignment, ExecutorProfile
                                    assignees = task.get('assignees', {})
                                    if isinstance(assignees, dict):
                                        assignee_users = assignees.get('users', []) or []
                                        assignee_contacts = assignees.get('contacts', []) or []
                                        
                                        # Собираем все назначенные ID (users и contacts)
                                        assigned_ids = set()
                                        for user in assignee_users:
                                            if isinstance(user, dict):
                                                user_id = user.get('id')
                                                if user_id:
                                                    if isinstance(user_id, str) and ':' in user_id:
                                                        assigned_ids.add(user_id.split(':')[-1])
                                                    else:
                                                        assigned_ids.add(str(user_id))
                                        
                                        for contact in assignee_contacts:
                                            if isinstance(contact, dict):
                                                contact_id = contact.get('id')
                                                if contact_id:
                                                    if isinstance(contact_id, str) and ':' in contact_id:
                                                        assigned_ids.add(contact_id.split(':')[-1])
                                                    else:
                                                        assigned_ids.add(str(contact_id))
                                        
                                        # Находим исполнителей по planfix_user_id или planfix_contact_id
                                        if assigned_ids:
                                            executors = db.query(ExecutorProfile).filter(
                                                ExecutorProfile.profile_status == "активен"
                                            ).all()
                                            
                                            for executor in executors:
                                                executor_user_id = str(executor.planfix_user_id) if executor.planfix_user_id else None
                                                executor_contact_id = str(executor.planfix_contact_id) if executor.planfix_contact_id else None
                                                
                                                # Проверяем, назначен ли исполнитель
                                                if (executor_user_id and executor_user_id in assigned_ids) or \
                                                   (executor_contact_id and executor_contact_id in assigned_ids):
                                                    # Проверяем, нет ли уже активного назначения
                                                    existing = db.query(TaskAssignment).filter(
                                                        TaskAssignment.task_id == task_id,
                                                        TaskAssignment.executor_telegram_id == executor.telegram_id,
                                                        TaskAssignment.status == "active"
                                                    ).first()
                                                    
                                                    if not existing:
                                                        assignment = TaskAssignment(
                                                            task_id=task_id,
                                                            executor_telegram_id=executor.telegram_id,
                                                            planfix_user_id=executor.planfix_user_id,
                                                            status="active"
                                                        )
                                                        db.add(assignment)
                                                        logger.debug(f"Created TaskAssignment: task {task_id} -> executor {executor.telegram_id}")
                                except Exception as assign_err:
                                    logger.warning(f"Error creating TaskAssignment for task {task.get('id')}: {assign_err}")
                                
                                # Извлекаем данные задачи
                                task_name = task.get('name', '')
                                status_obj = task.get('status', {})
                                status_id_task = status_obj.get('id') if isinstance(status_obj, dict) else None
                                status_name = status_obj.get('name') if isinstance(status_obj, dict) else None
                                
                                # Нормализуем status_id
                                if isinstance(status_id_task, str) and ':' in str(status_id_task):
                                    status_id_task = int(str(status_id_task).split(':')[-1])
                                elif isinstance(status_id_task, int):
                                    pass
                                else:
                                    status_id_task = None
                                
                                counterparty_obj = task.get('counterparty', {})
                                counterparty_id = counterparty_obj.get('id') if isinstance(counterparty_obj, dict) else None
                                if counterparty_id:
                                    if isinstance(counterparty_id, str) and ':' in str(counterparty_id):
                                        counterparty_id = int(str(counterparty_id).split(':')[-1])
                                    else:
                                        counterparty_id = int(counterparty_id)
                                
                                project_obj = task.get('project', {})
                                project_id = project_obj.get('id') if isinstance(project_obj, dict) else None
                                if project_id:
                                    if isinstance(project_id, str) and ':' in str(project_id):
                                        project_id = int(str(project_id).split(':')[-1])
                                    else:
                                        project_id = int(project_id)
                                
                                # Дата последнего обновления
                                date_of_last_update = None
                                date_str = task.get('dateOfLastUpdate')
                                if date_str:
                                    try:
                                        date_of_last_update = datetime.fromisoformat(date_str.replace('Z', '+00:00')).replace(tzinfo=None)
                                    except:
                                        pass
                                
                                # Проверяем, существует ли задача в кэше
                                existing_task = db.query(TaskCache).filter(TaskCache.task_id == task_id).first()
                                
                                if existing_task:
                                    # Обновляем существующую задачу
                                    existing_task.name = task_name
                                    existing_task.status_id = status_id_task
                                    existing_task.status_name = status_name
                                    existing_task.counterparty_id = counterparty_id
                                    existing_task.project_id = project_id
                                    existing_task.template_id = None  # Без шаблона
                                    existing_task.created_by_bot = False  # Не создана через бота
                                    existing_task.date_of_last_update = date_of_last_update
                                    existing_task.updated_at = datetime.now()
                                    total_updated += 1
                                else:
                                    # Создаем новую задачу
                                    new_task = TaskCache(
                                        task_id=task_id,
                                        name=task_name,
                                        status_id=status_id_task,
                                        status_name=status_name,
                                        counterparty_id=counterparty_id,
                                        project_id=project_id,
                                        template_id=None,  # Без шаблона
                                        created_by_bot=False,  # Не создана через бота
                                        date_of_last_update=date_of_last_update
                                    )
                                    db.add(new_task)
                                    total_created += 1
                                
                                total_synced += 1
                                
                            except Exception as task_err:
                                logger.error(f"Error processing task {task.get('id')}: {task_err}", exc_info=True)
                                total_errors += 1
                        
                        db.commit()
                    
                    # Если получили меньше задач, чем page_size, значит это последняя страница
                    if len(tasks_list) < page_size:
                        break
                else:
                    logger.warning(f"Failed to fetch tasks at offset {offset}: {response}")
                    break
                
                offset += page_size
                
            except Exception as e:
                logger.error(f"Error fetching tasks at offset {offset}: {e}", exc_info=True)
                total_errors += 1
                break
        
        result_message = (
            f"✅ <b>Загрузка завершена</b>\n\n"
            f"📊 Всего обработано: {total_synced}\n"
            f"👷 С исполнителем из бота: {total_with_executor}\n"
            f"➕ Создано новых: {total_created}\n"
            f"🔄 Обновлено: {total_updated}\n"
            f"❌ Ошибок: {total_errors}"
        )
        
        await message.answer(result_message, parse_mode="HTML")
        logger.info(f"✅ Task sync (without template) completed: {total_synced} total, {total_with_executor} with executor, {total_created} created, {total_updated} updated, {total_errors} errors")
        
    except Exception as e:
        logger.error(f"Error syncing tasks without template: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка при загрузке: {e}")


@router.message(AdminManagement.main_menu, F.text == "📊 Статистика")
async def admin_statistics(message: Message, state: FSMContext):
    """Показывает статистику."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав.")
        return
    
    try:
        from db_manager import DBManager
        sync_db_manager = DBManager()
        
        with sync_db_manager.get_db() as db:
            users_count = db.query(UserProfile).count()
            executors_count = db.query(ExecutorProfile).count()
            active_executors = db.query(ExecutorProfile).filter(
                ExecutorProfile.profile_status == "активен"
            ).count()
            pending_executors = db.query(ExecutorProfile).filter(
                ExecutorProfile.profile_status == "ожидает подтверждения"
            ).count()
            assignments_count = db.query(TaskAssignment).filter(
                TaskAssignment.status == "active"
            ).count()
        
        stats_text = (
            "📊 <b>Статистика бота</b>\n\n"
            f"👤 Пользователей: {users_count}\n"
            f"👷 Исполнителей: {executors_count}\n"
            f"  ├─ Активных: {active_executors}\n"
            f"  └─ Ожидают подтверждения: {pending_executors}\n"
            f"📋 Активных назначений задач: {assignments_count}"
        )
        
        await message.answer(stats_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error getting statistics: {e}", exc_info=True)
        await message.answer("❌ Ошибка при получении статистики.")


# ============================================================================
# СПИСКИ ПОЛЬЗОВАТЕЛЕЙ И ИСПОЛНИТЕЛЕЙ
# ============================================================================

@router.callback_query(F.data == "admin_list_users")
async def admin_list_users(callback_query: CallbackQuery, state: FSMContext):
    """Показывает список пользователей."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        from db_manager import DBManager
        sync_db_manager = DBManager()
        
        with sync_db_manager.get_db() as db:
            users = db.query(UserProfile).order_by(UserProfile.telegram_id).all()
        
        if not users:
            await callback_query.message.edit_text(
                "👤 <b>Список пользователей</b>\n\n"
                "Пользователи не найдены.",
                reply_markup=get_admin_users_menu_keyboard(),
                parse_mode="HTML"
            )
            await callback_query.answer()
            return
        
        keyboard = create_users_list_keyboard(users, page=0)
        await callback_query.message.edit_text(
            f"👤 <b>Список пользователей</b>\n\n"
            f"Всего: {len(users)}\n"
            f"Выберите пользователя:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error listing users: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при загрузке списка.", show_alert=True)


@router.callback_query(F.data.startswith("admin_list_users_page:"))
async def admin_list_users_page(callback_query: CallbackQuery, state: FSMContext):
    """Пагинация списка пользователей."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        page = int(callback_query.data.split(":")[1])
        from db_manager import DBManager
        sync_db_manager = DBManager()
        
        with sync_db_manager.get_db() as db:
            users = db.query(UserProfile).order_by(UserProfile.telegram_id).all()
        
        keyboard = create_users_list_keyboard(users, page=page)
        await callback_query.message.edit_text(
            f"👤 <b>Список пользователей</b>\n\n"
            f"Всего: {len(users)}\n"
            f"Страница {page + 1}\n"
            f"Выберите пользователя:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error paginating users: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка.", show_alert=True)


@router.callback_query(F.data == "admin_list_executors")
async def admin_list_executors(callback_query: CallbackQuery, state: FSMContext):
    """Показывает список исполнителей."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        from db_manager import DBManager
        sync_db_manager = DBManager()
        
        with sync_db_manager.get_db() as db:
            executors = db.query(ExecutorProfile).order_by(ExecutorProfile.telegram_id).all()
        
        if not executors:
            await callback_query.message.edit_text(
                "👷 <b>Список исполнителей</b>\n\n"
                "Исполнители не найдены.",
                reply_markup=get_admin_executors_menu_keyboard(),
                parse_mode="HTML"
            )
            await callback_query.answer()
            return
        
        keyboard = create_executors_list_keyboard(executors, page=0)
        await callback_query.message.edit_text(
            f"👷 <b>Список исполнителей</b>\n\n"
            f"Всего: {len(executors)}\n"
            f"Выберите исполнителя:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error listing executors: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при загрузке списка.", show_alert=True)


@router.callback_query(F.data.startswith("admin_list_executors_page:"))
async def admin_list_executors_page(callback_query: CallbackQuery, state: FSMContext):
    """Пагинация списка исполнителей."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        page = int(callback_query.data.split(":")[1])
        from db_manager import DBManager
        sync_db_manager = DBManager()
        
        with sync_db_manager.get_db() as db:
            executors = db.query(ExecutorProfile).order_by(ExecutorProfile.telegram_id).all()
        
        keyboard = create_executors_list_keyboard(executors, page=page)
        await callback_query.message.edit_text(
            f"👷 <b>Список исполнителей</b>\n\n"
            f"Всего: {len(executors)}\n"
            f"Страница {page + 1}\n"
            f"Выберите исполнителя:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error paginating executors: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка.", show_alert=True)


# ============================================================================
# ПРОСМОТР ПРОФИЛЕЙ
# ============================================================================

@router.callback_query(F.data.startswith("admin_view_user:"))
async def admin_view_user(callback_query: CallbackQuery, state: FSMContext):
    """Показывает детали пользователя."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        user_id = int(callback_query.data.split(":")[1])
        user = await db_manager.get_user_profile(user_id)
        
        if not user:
            await callback_query.answer("❌ Пользователь не найден.", show_alert=True)
            return
        
        franchise_name = FRANCHISE_GROUPS.get(user.franchise_group_id, {}).get("name", f"ID: {user.franchise_group_id}")
        
        user_text = (
            f"👤 <b>Профиль пользователя</b>\n\n"
            f"🆔 Telegram ID: <code>{user.telegram_id}</code>\n"
            f"👤 ФИО: {user.full_name or 'не указано'}\n"
            f"📱 Телефон: {user.phone_number or 'не указан'}\n"
            f"📧 Email: {user.email or 'не указан'}\n"
            f"💼 Должность: {user.position or 'не указана'}\n"
            f"🏢 Концепция: {franchise_name}\n"
            f"🏪 ID ресторана: {user.restaurant_contact_id or 'не указан'}\n"
            f"🔗 Planfix Contact ID: {user.planfix_contact_id or 'не указан'}\n"
            f"📅 Дата регистрации: {user.registration_date.strftime('%Y-%m-%d %H:%M:%S') if user.registration_date else 'не указана'}\n"
            f"✅ Активен: {'Да' if user.is_active else 'Нет'}"
        )
        
        keyboard = get_admin_profile_actions_keyboard("user", user_id)
        await callback_query.message.edit_text(user_text, reply_markup=keyboard, parse_mode="HTML")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error viewing user: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при загрузке профиля.", show_alert=True)


@router.callback_query(F.data.startswith("admin_view_user_tasks:"))
async def admin_view_user_tasks(callback_query: CallbackQuery, state: FSMContext):
    """Показывает заявки выбранного пользователя."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        user_id = int(callback_query.data.split(":")[1])
        user = await db_manager.get_user_profile(user_id)
        
        if not user:
            await callback_query.answer("❌ Пользователь не найден.", show_alert=True)
            return
        
        # Показываем индикатор загрузки
        await callback_query.answer("⏳ Загрузка заявок...")
        
        # Получаем заявки пользователя
        from user_handlers import get_user_tasks
        tasks = await get_user_tasks(user_id, limit=50, only_active=False)
        
        if tasks is None:
            await callback_query.message.answer(
                "❌ Ошибка при получении заявок пользователя.",
                parse_mode="HTML"
            )
            return
        
        if not tasks:
            user_name = user.full_name or f"ID: {user_id}"
            await callback_query.message.answer(
                f"📋 <b>Заявки пользователя</b>\n\n"
                f"👤 <b>{user_name}</b> (ID: {user_id})\n\n"
                f"❌ У пользователя нет заявок.",
                parse_mode="HTML"
            )
            return
        
        # Формируем список заявок
        user_name = user.full_name or f"ID: {user_id}"
        lines = [
            f"📋 <b>Заявки пользователя</b>\n",
            f"👤 <b>{user_name}</b> (ID: {user_id})\n",
            f"Всего заявок: {len(tasks)}\n",
            "────────────────────\n"
        ]
        
        # Показываем первые 20 заявок
        for task in tasks[:20]:
            task_id = task.get('id')
            task_name = task.get('name', 'Без названия')[:50]
            status_obj = task.get('status', {})
            status_name = status_obj.get('name', 'Неизвестно') if isinstance(status_obj, dict) else 'Неизвестно'
            
            # Получаем название ресторана (если есть)
            counterparty_id = None
            counterparty_obj = task.get('counterparty', {})
            if isinstance(counterparty_obj, dict):
                counterparty_id = counterparty_obj.get('id')
            
            restaurant_info = ""
            if counterparty_id:
                restaurant_info = f"\n🏪 Ресторан ID: {counterparty_id}"
            
            lines.append(
                f"📋 <b>#{task_id}</b> – {status_name}\n"
                f"📝 {task_name}{restaurant_info}\n"
                f"────────────────────"
            )
        
        if len(tasks) > 20:
            lines.append(f"\n💡 <i>... и ещё {len(tasks) - 20} заявок</i>")
        
        # Создаем клавиатуру с кнопками для просмотра деталей заявок
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        task_buttons = []
        for task in tasks[:10]:  # Показываем кнопки для первых 10 заявок
            task_id = task.get('id')
            task_name = task.get('name', f'Заявка #{task_id}')[:30]
            task_buttons.append([
                InlineKeyboardButton(
                    text=f"#{task_id} - {task_name}",
                    callback_data=f"admin_view_task:{task_id}"
                )
            ])
        
        task_buttons.append([
            InlineKeyboardButton(text="◀️ Назад к профилю", callback_data=f"admin_view_user:{user_id}")
        ])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=task_buttons)
        
        await callback_query.message.answer(
            "\n".join(lines),
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error viewing user tasks: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при загрузке заявок.", show_alert=True)


@router.callback_query(F.data.startswith("admin_view_task:"))
async def admin_view_task_details(callback_query: CallbackQuery, state: FSMContext):
    """Показывает детали заявки для админа."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        task_id = int(callback_query.data.split(":")[1])
        
        # Получаем информацию о задаче из Planfix API
        from planfix_client import planfix_client
        task_response = await planfix_client.get_task_by_id(
            task_id,
            fields="id,name,description,status,statusId,project.id,project.name,template.id,template.name,counterparty.id,counterparty.name,assignees,customFieldData,files,dateTime,dateOfLastUpdate"
        )
        
        if not task_response or task_response.get('result') != 'success':
            await callback_query.answer("❌ Заявка не найдена.", show_alert=True)
            return
        
        task = task_response.get('task', {})
        
        # Форматируем информацию о задаче
        task_name = task.get('name', 'Без названия')
        task_desc = task.get('description', 'Нет описания')[:500]
        
        status_obj = task.get('status', {})
        status_name = status_obj.get('name', 'Неизвестно') if isinstance(status_obj, dict) else 'Неизвестно'
        
        counterparty_obj = task.get('counterparty', {})
        counterparty_name = counterparty_obj.get('name', 'Не указан') if isinstance(counterparty_obj, dict) else 'Не указан'
        
        project_obj = task.get('project', {})
        project_name = project_obj.get('name', 'Не указан') if isinstance(project_obj, dict) else 'Не указан'
        
        template_obj = task.get('template', {})
        template_name = template_obj.get('name', 'Не указан') if isinstance(template_obj, dict) else 'Не указан'
        
        assignees = task.get('assignees', {}).get('users', []) or []
        assignees_list = []
        for assignee in assignees:
            if isinstance(assignee, dict):
                assignee_name = assignee.get('name', 'Неизвестно')
                assignees_list.append(assignee_name)
        
        # Получаем чек-лист задачи
        checklist_text = ""
        try:
            checklist_response = await planfix_client.get_task_checklist(task_id)
            if checklist_response and checklist_response.get('result') == 'success':
                # Согласно swagger.json, ответ содержит поле 'items'
                checklist_items = checklist_response.get('items', []) or []
                if checklist_items:
                    checklist_lines = ["\n\n✅ <b>Чек-лист:</b>"]
                    for item in checklist_items:
                        if isinstance(item, dict):
                            item_name = item.get('name', 'Без названия')
                            
                            # Проверяем статус через поле 'status' (объект с id и name)
                            is_checked = False
                            status_obj = item.get('status', {})
                            if isinstance(status_obj, dict):
                                status_name = status_obj.get('name', '').lower() if status_obj.get('name') else ''
                                # Проверяем по названию статуса
                                if any(keyword in status_name for keyword in ['выполнен', 'checked', 'completed', 'done', 'готов']):
                                    is_checked = True
                            
                            checkbox = "☑️" if is_checked else "☐"
                            checklist_lines.append(f"{checkbox} {item_name}")
                    if len(checklist_lines) > 1:  # Если есть хотя бы один пункт
                        checklist_text = "\n".join(checklist_lines)
        except Exception as checklist_err:
            logger.debug(f"Error getting checklist for task {task_id}: {checklist_err}")
        
        task_text = (
            f"📋 <b>Заявка #{task_id}</b>\n\n"
            f"📝 <b>Название:</b> {task_name}\n"
            f"📄 <b>Описание:</b> {task_desc}\n"
            f"📊 <b>Статус:</b> {status_name}\n"
            f"🏢 <b>Проект:</b> {project_name}\n"
            f"📋 <b>Шаблон:</b> {template_name}\n"
            f"🏪 <b>Ресторан:</b> {counterparty_name}\n"
        )
        
        if assignees_list:
            task_text += f"👷 <b>Исполнители:</b> {', '.join(assignees_list)}\n"
        
        task_text += checklist_text
        
        # Получаем информацию о пользователе, создавшем заявку
        from db_manager import DBManager
        sync_db_manager = DBManager()
        with sync_db_manager.get_db() as db:
            from database import TaskCache
            task_cache = db.query(TaskCache).filter(TaskCache.task_id == task_id).first()
            if task_cache and task_cache.user_telegram_id:
                user = await db_manager.get_user_profile(task_cache.user_telegram_id)
                if user:
                    task_text += f"👤 <b>Создал:</b> {user.full_name or f'ID: {user.telegram_id}'}\n"
        
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        back_buttons = []
        
        # Определяем, откуда пришли (от пользователя или исполнителя)
        if task_cache and task_cache.user_telegram_id:
            back_buttons.append([
                InlineKeyboardButton(text="◀️ Назад к заявкам пользователя", callback_data=f"admin_view_user_tasks:{task_cache.user_telegram_id}")
            ])
        
        # Также проверяем, может быть это заявка исполнителя
        if task_cache:
            from db_manager import DBManager
            sync_db_manager = DBManager()
            with sync_db_manager.get_db() as db:
                from database import TaskAssignment
                assignment = db.query(TaskAssignment).filter(
                    TaskAssignment.task_id == task_id,
                    TaskAssignment.status == "active"
                ).first()
                if assignment:
                    back_buttons.append([
                        InlineKeyboardButton(text="◀️ Назад к заявкам исполнителя", callback_data=f"admin_view_executor_tasks:{assignment.executor_telegram_id}")
                    ])
        
        if not back_buttons:
            back_buttons.append([
                InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back_to_main")
            ])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=back_buttons)
        
        await callback_query.message.answer(task_text, reply_markup=keyboard, parse_mode="HTML")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error viewing task details: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при загрузке заявки.", show_alert=True)


@router.callback_query(F.data.startswith("admin_view_executor_tasks:"))
async def admin_view_executor_tasks(callback_query: CallbackQuery, state: FSMContext):
    """Показывает заявки выбранного исполнителя."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        executor_id = int(callback_query.data.split(":")[1])
        executor = await db_manager.get_executor_profile(executor_id)
        
        if not executor:
            await callback_query.answer("❌ Исполнитель не найден.", show_alert=True)
            return
        
        # Показываем индикатор загрузки
        await callback_query.answer("⏳ Загрузка заявок...")
        
        # Получаем planfix_user_id или planfix_contact_id исполнителя
        executor_planfix_id = None
        executor_planfix_id_type = None
        
        if executor.planfix_user_id:
            try:
                executor_planfix_id = int(str(executor.planfix_user_id).split(':')[-1])
                executor_planfix_id_type = "user"
            except (ValueError, TypeError):
                pass
        
        if not executor_planfix_id and executor.planfix_contact_id:
            try:
                executor_planfix_id = int(str(executor.planfix_contact_id).split(':')[-1])
                executor_planfix_id_type = "contact"
            except (ValueError, TypeError):
                pass
        
        tasks = []
        
        # Ищем задачи в БД бота (TaskCache) через TaskAssignment
        from db_manager import DBManager
        from database import TaskAssignment, TaskCache
        from services.status_registry import StatusKey, collect_status_ids, require_status_id
        
        sync_db_manager = DBManager()
        
        # Получаем статусы "Новая" и "В работе"
        working_status_ids = collect_status_ids(
            (StatusKey.NEW, StatusKey.IN_PROGRESS),
            required=False,
        )
        if not working_status_ids:
            try:
                working_status_ids = [
                    require_status_id(StatusKey.NEW),
                    require_status_id(StatusKey.IN_PROGRESS)
                ]
                working_status_ids = [sid for sid in working_status_ids if sid is not None]
            except Exception:
                working_status_ids = []
        
        if not working_status_ids:
            logger.warning(f"Could not get status IDs for NEW and IN_PROGRESS")
            executor_name = executor.full_name or f"ID: {executor_id}"
            await callback_query.message.answer(
                f"❌ Не удалось получить ID статусов. Проверьте настройки.",
                parse_mode="HTML"
            )
            return
        
        with sync_db_manager.get_db() as db:
            # Получаем все активные назначения для этого исполнителя
            assignments = db.query(TaskAssignment).filter(
                TaskAssignment.executor_telegram_id == executor_id,
                TaskAssignment.status == "active"
            ).all()
            
            logger.info(f"Found {len(assignments)} active task assignments for executor {executor_id}")
            
            if assignments:
                # Получаем task_id из назначений
                task_ids = [assignment.task_id for assignment in assignments]
                
                # Получаем задачи из TaskCache
                cached_tasks = db.query(TaskCache).filter(
                    TaskCache.task_id.in_(task_ids),
                    TaskCache.status_id.in_(working_status_ids)
                ).order_by(TaskCache.date_of_last_update.desc().nullslast()).all()
                
                logger.info(f"Found {len(cached_tasks)} tasks in TaskCache for executor {executor_id} with statuses {working_status_ids}")
                
                # Преобразуем TaskCache в формат, похожий на ответ API
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
                        'dateTime': cached_task.date_of_last_update.isoformat() if cached_task.date_of_last_update else None,
                        'dateOfLastUpdate': cached_task.date_of_last_update.isoformat() if cached_task.date_of_last_update else None
                    }
                    tasks.append(task_dict)
        
        if not tasks:
            executor_name = executor.full_name or f"ID: {executor_id}"
            await callback_query.message.answer(
                f"📋 <b>Заявки исполнителя</b>\n\n"
                f"👷 <b>{executor_name}</b> (ID: {executor_id})\n\n"
                f"❌ У исполнителя нет назначенных заявок.",
                parse_mode="HTML"
            )
            return
        
        # Сохраняем задачи в кэше для пагинации
        executor_name = executor.full_name or f"ID: {executor_id}"
        cache.set(f"admin_executor_tasks:{executor_id}", tasks, ttl_seconds=300)
        
        # Показываем первую страницу
        await _show_admin_executor_tasks_page(callback_query, executor_id, executor_name, tasks, page=0, is_callback=True)
    except Exception as e:
        logger.error(f"Error viewing executor tasks: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при загрузке заявок.", show_alert=True)


@router.callback_query(F.data.startswith("admin_executor_tasks_page:"))
async def admin_executor_tasks_page_callback(callback_query: CallbackQuery, state: FSMContext):
    """Обработчик переключения страниц списка задач исполнителя для админа."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        # Формат: admin_executor_tasks_page:executor_id:page
        parts = callback_query.data.split(":")
        executor_id = int(parts[1])
        page = int(parts[2])
        
        # Получаем список задач из кэша
        tasks = cache.get(f"admin_executor_tasks:{executor_id}")
        if not tasks:
            await callback_query.answer("❌ Список задач устарел. Обновите список.", show_alert=True)
            return
        
        # Получаем информацию об исполнителе
        executor = await db_manager.get_executor_profile(executor_id)
        if not executor:
            await callback_query.answer("❌ Исполнитель не найден.", show_alert=True)
            return
        
        executor_name = executor.full_name or f"ID: {executor_id}"
        
        await _show_admin_executor_tasks_page(callback_query, executor_id, executor_name, tasks, page=page, is_callback=True)
    except Exception as e:
        logger.error(f"Error paginating executor tasks for admin: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при переключении страницы.", show_alert=True)


@router.callback_query(F.data.startswith("admin_refresh_executor_tasks:"))
async def admin_refresh_executor_tasks_callback(callback_query: CallbackQuery, state: FSMContext):
    """Обработчик обновления списка задач исполнителя для админа."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        executor_id = int(callback_query.data.split(":")[1])
        await callback_query.answer("⏳ Обновление списка...")
        
        # Очищаем кэш для принудительного обновления
        cache.delete(f"admin_executor_tasks:{executor_id}")
        
        # Вызываем обработчик просмотра задач исполнителя
        # Создаем временный callback для вызова admin_view_executor_tasks
        class FakeCallback:
            def __init__(self, original_callback, data):
                self.from_user = original_callback.from_user
                self.message = original_callback.message
                self.data = data
                self.answer = original_callback.answer
        
        fake_callback = FakeCallback(callback_query, f"admin_view_executor_tasks:{executor_id}")
        await admin_view_executor_tasks(fake_callback, state)
    except Exception as e:
        logger.error(f"Error refreshing executor tasks for admin: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при обновлении списка.", show_alert=True)


@router.callback_query(F.data.startswith("admin_view_executor:"))
async def admin_view_executor(callback_query: CallbackQuery, state: FSMContext):
    """Показывает детали исполнителя."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        executor_id = int(callback_query.data.split(":")[1])
        executor = await db_manager.get_executor_profile(executor_id)
        
        if not executor:
            await callback_query.answer("❌ Исполнитель не найден.", show_alert=True)
            return
        
        # Получаем информацию о связанных данных
        from db_manager import DBManager
        sync_db_manager = DBManager()
        
        with sync_db_manager.get_db() as db:
            assignments_count = db.query(TaskAssignment).filter(
                TaskAssignment.executor_telegram_id == executor_id,
                TaskAssignment.status == "active"
            ).count()
        
        concept_names = []
        if executor.serving_franchise_groups:
            for cid in executor.serving_franchise_groups:
                name = FRANCHISE_GROUPS.get(cid, {}).get("name", f"ID: {cid}")
                concept_names.append(name)
        
        restaurants_count = len(executor.serving_restaurants) if executor.serving_restaurants else 0
        
        executor_text = (
            f"👷 <b>Профиль исполнителя</b>\n\n"
            f"🆔 Telegram ID: <code>{executor.telegram_id}</code>\n"
            f"👤 ФИО: {executor.full_name or 'не указано'}\n"
            f"📱 Телефон: {executor.phone_number or 'не указан'}\n"
            f"📧 Email: {executor.email or 'не указан'}\n"
            f"💼 Должность: {executor.position_role or 'не указана'}\n"
            f"🧭 Направление: {executor.service_direction or 'не указано'}\n"
            f"🏢 Концепции: {', '.join(concept_names) if concept_names else 'не указаны'}\n"
            f"🏪 Ресторанов: {restaurants_count}\n"
            f"🔗 Planfix Contact ID: {executor.planfix_contact_id or 'не указан'}\n"
            f"🔗 Planfix User ID: {executor.planfix_user_id or 'не указан'}\n"
            f"📋 Статус: {executor.profile_status or 'не указан'}\n"
            f"📅 Дата регистрации: {executor.registration_date.strftime('%Y-%m-%d %H:%M:%S') if executor.registration_date else 'не указана'}\n"
            f"📋 Активных назначений: {assignments_count}"
        )
        
        keyboard = get_admin_profile_actions_keyboard("executor", executor_id)
        await callback_query.message.edit_text(executor_text, reply_markup=keyboard, parse_mode="HTML")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error viewing executor: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при загрузке профиля.", show_alert=True)


# ============================================================================
# УДАЛЕНИЕ
# ============================================================================

@router.callback_query(F.data.startswith("admin_delete_user:") | F.data.startswith("admin_delete_executor:"))
async def admin_delete_profile(callback_query: CallbackQuery, state: FSMContext):
    """Запрашивает подтверждение удаления."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        # Формат: admin_delete_user:123 или admin_delete_executor:123
        parts = callback_query.data.split(":")
        if len(parts) < 2:
            raise ValueError(f"Invalid callback_data format: {callback_query.data}")
        
        # Извлекаем тип из первой части: "admin_delete_user" -> "user"
        prefix = parts[0]  # "admin_delete_user" or "admin_delete_executor"
        if "user" in prefix:
            profile_type = "user"
        elif "executor" in prefix:
            profile_type = "executor"
        else:
            raise ValueError(f"Unknown profile type in callback_data: {callback_query.data}")
        
        profile_id = int(parts[1])
        
        if profile_type == "user":
            profile = await db_manager.get_user_profile(profile_id)
            profile_name = profile.full_name if profile else f"ID: {profile_id}"
        else:
            profile = await db_manager.get_executor_profile(profile_id)
            profile_name = profile.full_name if profile else f"ID: {profile_id}"
        
        if not profile:
            await callback_query.answer("❌ Профиль не найден.", show_alert=True)
            return
        
        keyboard = get_admin_delete_confirmation_keyboard(profile_type, profile_id)
        await callback_query.message.edit_text(
            f"⚠️ <b>Подтверждение удаления</b>\n\n"
            f"Вы уверены, что хотите удалить {profile_type}:\n"
            f"<b>{profile_name}</b> (ID: {profile_id})?\n\n"
            f"Это действие нельзя отменить!",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error preparing delete: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка.", show_alert=True)


@router.callback_query(F.data.startswith("admin_confirm_delete_user:") | F.data.startswith("admin_confirm_delete_executor:"))
async def admin_confirm_delete(callback_query: CallbackQuery, state: FSMContext):
    """Подтверждает и выполняет удаление."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        # Формат: admin_confirm_delete_user:123 или admin_confirm_delete_executor:123
        parts = callback_query.data.split(":")
        if len(parts) < 2:
            raise ValueError(f"Invalid callback_data format: {callback_query.data}")
        
        # Извлекаем тип из первой части: "admin_confirm_delete_user" -> "user"
        prefix = parts[0]  # "admin_confirm_delete_user" or "admin_confirm_delete_executor"
        if "user" in prefix:
            profile_type = "user"
        elif "executor" in prefix:
            profile_type = "executor"
        else:
            raise ValueError(f"Unknown profile type in callback_data: {callback_query.data}")
        
        profile_id = int(parts[1])
        
        # Используем синхронный db_manager для удаления
        from db_manager import DBManager
        sync_db_manager = DBManager()
        
        with sync_db_manager.get_db() as db:
            if profile_type == "user":
                # Удаляем связанные логи
                logs = db.query(BotLog).filter(BotLog.telegram_id == profile_id).all()
                for log in logs:
                    db.delete(log)
                
                # Удаляем профиль
                sync_db_manager.delete_user_profile(db, profile_id)
                profile_name = "пользователя"
            else:
                # Удаляем назначения задач
                assignments = db.query(TaskAssignment).filter(
                    TaskAssignment.executor_telegram_id == profile_id
                ).all()
                for assignment in assignments:
                    db.delete(assignment)
                
                # Удаляем логи
                logs = db.query(BotLog).filter(BotLog.telegram_id == profile_id).all()
                for log in logs:
                    db.delete(log)
                
                # Удаляем профиль
                sync_db_manager.delete_executor_profile(db, profile_id)
                profile_name = "исполнителя"
            
            db.commit()
        
        await callback_query.message.edit_text(
            f"✅ <b>Удаление выполнено</b>\n\n"
            f"Профиль {profile_name} (ID: {profile_id}) успешно удалён.",
            parse_mode="HTML"
        )
        
        # Возвращаемся к списку
        await asyncio.sleep(2)
        if profile_type == "user":
            await admin_list_users(callback_query, state)
        else:
            await admin_list_executors(callback_query, state)
        
        await callback_query.answer("✅ Удалено")
    except Exception as e:
        logger.error(f"Error deleting profile: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка при удалении.", show_alert=True)


# ============================================================================
# РЕДАКТИРОВАНИЕ
# ============================================================================

@router.callback_query(F.data.startswith("admin_edit_user:") | F.data.startswith("admin_edit_executor:"))
async def admin_edit_profile(callback_query: CallbackQuery, state: FSMContext):
    """Показывает меню редактирования профиля."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        # Формат: admin_edit_user:123 или admin_edit_executor:123
        parts = callback_query.data.split(":")
        if len(parts) < 2:
            raise ValueError(f"Invalid callback_data format: {callback_query.data}")
        
        # Извлекаем тип из первой части: "admin_edit_user" -> "user"
        prefix = parts[0]  # "admin_edit_user" or "admin_edit_executor"
        if "user" in prefix:
            profile_type = "user"
        elif "executor" in prefix:
            profile_type = "executor"
        else:
            raise ValueError(f"Unknown profile type in callback_data: {callback_query.data}")
        
        profile_id = int(parts[1])
        
        if profile_type == "user":
            keyboard = get_admin_edit_user_keyboard(profile_id)
            text = f"✏️ <b>Редактирование пользователя</b>\n\nВыберите поле для редактирования:"
        else:
            keyboard = get_admin_edit_executor_keyboard(profile_id)
            text = f"✏️ <b>Редактирование исполнителя</b>\n\nВыберите поле для редактирования:"
        
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error opening edit menu: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка.", show_alert=True)


@router.callback_query(F.data.startswith("admin_edit_user_field:") | F.data.startswith("admin_edit_exec_field:"))
async def admin_edit_field_start(callback_query: CallbackQuery, state: FSMContext):
    """Начинает редактирование поля профиля."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        # Формат: admin_edit_user_field:123:full_name или admin_edit_exec_field:123:full_name
        parts = callback_query.data.split(":")
        if len(parts) < 3:
            raise ValueError(f"Invalid callback_data format: {callback_query.data}")
        
        profile_type = "user" if "user_field" in callback_query.data else "executor"
        profile_id = int(parts[1])  # ID находится во второй части
        field_name = parts[2]  # Название поля находится в третьей части
        
        # Сохраняем данные в state
        await state.update_data(
            admin_edit_profile_type=profile_type,
            admin_edit_profile_id=profile_id,
            admin_edit_field=field_name
        )
        
        # Определяем подсказку для ввода
        field_hints = {
            "full_name": "Введите новое ФИО:",
            "phone": "Введите новый номер телефона:",
            "email": "Введите новый email:",
            "franchise": "Выберите новую концепцию (ID группы):",
            "restaurant": "Введите новый ID ресторана:",
            "position": "Введите новую должность:",
            "concepts": "Редактирование концепций (пока не реализовано)",
            "restaurants": "Редактирование ресторанов (пока не реализовано)",
            "direction": "Выберите направление:",
            "planfix_id": "Введите новый Planfix Contact ID:",
            "status": "Выберите новый статус:",
        }
        
        hint = field_hints.get(field_name, "Введите новое значение:")
        
        # Для некоторых полей нужна специальная обработка
        if field_name == "direction":
            from keyboards import get_executor_direction_keyboard
            keyboard = get_executor_direction_keyboard(prefix="admin_edit_dir", include_cancel=True)
            await callback_query.message.edit_text(
                f"✏️ <b>Редактирование направления</b>\n\n{hint}",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            await state.set_state(AdminManagement.editing_executor_field)
            await callback_query.answer()
            return
        
        if field_name == "status":
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⏳ Ожидает подтверждения", callback_data="admin_status:pending")],
                    [InlineKeyboardButton(text="✅ Активен", callback_data="admin_status:active")],
                    [InlineKeyboardButton(text="❌ Отклонен", callback_data="admin_status:rejected")],
                    [InlineKeyboardButton(text="🚫 Заблокирован", callback_data="admin_status:blocked")],
                    [InlineKeyboardButton(text="◀️ Назад", callback_data=f"admin_view_{profile_type}:{profile_id}")]
                ]
            )
            await callback_query.message.edit_text(
                f"✏️ <b>Редактирование статуса</b>\n\n{hint}",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            await state.set_state(AdminManagement.editing_executor_field)
            await callback_query.answer()
            return
        
        if field_name in ["concepts", "restaurants"]:
            await callback_query.answer("🔧 Эта функция будет реализована позже", show_alert=True)
            return
        
        # Для остальных полей - запрашиваем текстовый ввод
        await callback_query.message.edit_text(
            f"✏️ <b>Редактирование поля</b>\n\n{hint}\n\n"
            f"Или нажмите /cancel для отмены",
            parse_mode="HTML"
        )
        await state.set_state(AdminManagement.editing_user_field if profile_type == "user" else AdminManagement.editing_executor_field)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error starting field edit: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка.", show_alert=True)


@router.message(AdminManagement.editing_user_field)
async def admin_edit_user_field_process(message: Message, state: FSMContext):
    """Обрабатывает ввод нового значения поля пользователя."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав.")
        await state.clear()
        return
    
    try:
        data = await state.get_data()
        profile_id = data.get("admin_edit_profile_id")
        field_name = data.get("admin_edit_field")
        new_value = message.text.strip()
        
        if not new_value:
            await message.answer("❌ Значение не может быть пустым. Попробуйте ещё раз или /cancel")
            return
        
        # Обновляем профиль
        update_kwargs = {}
        
        if field_name == "full_name":
            update_kwargs["full_name"] = new_value
        elif field_name == "phone":
            update_kwargs["phone_number"] = new_value
        elif field_name == "email":
            update_kwargs["email"] = new_value
        elif field_name == "franchise":
            try:
                franchise_id = int(new_value)
                if franchise_id not in FRANCHISE_GROUPS:
                    await message.answer(f"❌ Неверный ID концепции. Доступные: {list(FRANCHISE_GROUPS.keys())}")
                    return
                update_kwargs["franchise_group_id"] = franchise_id
            except ValueError:
                await message.answer("❌ ID концепции должен быть числом.")
                return
        elif field_name == "restaurant":
            try:
                restaurant_id = int(new_value)
                update_kwargs["restaurant_contact_id"] = restaurant_id
            except ValueError:
                await message.answer("❌ ID ресторана должен быть числом.")
                return
        
        if update_kwargs:
            # Используем async db_manager
            user = await db_manager.update_user_profile(profile_id, **update_kwargs)
            if user:
                await message.answer(f"✅ Поле '{field_name}' успешно обновлено!")
                await state.clear()
                # Показываем обновленный профиль
                user_text = await _format_user_profile(profile_id)
                keyboard = get_admin_profile_actions_keyboard("user", profile_id)
                await message.answer(user_text, reply_markup=keyboard, parse_mode="HTML")
            else:
                await message.answer("❌ Пользователь не найден.")
        else:
            await message.answer("❌ Не удалось обновить поле.")
    except Exception as e:
        logger.error(f"Error updating user field: {e}", exc_info=True)
        await message.answer("❌ Ошибка при обновлении поля.")
        await state.clear()


@router.message(AdminManagement.editing_executor_field)
async def admin_edit_executor_field_process(message: Message, state: FSMContext):
    """Обрабатывает ввод нового значения поля исполнителя."""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав.")
        await state.clear()
        return
    
    try:
        data = await state.get_data()
        profile_id = data.get("admin_edit_profile_id")
        field_name = data.get("admin_edit_field")
        new_value = message.text.strip()
        
        if not new_value:
            await message.answer("❌ Значение не может быть пустым. Попробуйте ещё раз или /cancel")
            return
        
        # Обновляем профиль
        update_kwargs = {}
        
        if field_name == "full_name":
            update_kwargs["full_name"] = new_value
        elif field_name == "phone":
            update_kwargs["phone_number"] = new_value
        elif field_name == "email":
            update_kwargs["email"] = new_value
        elif field_name == "position":
            update_kwargs["position_role"] = new_value
        elif field_name == "planfix_id":
            update_kwargs["planfix_user_id"] = new_value
            update_kwargs["planfix_contact_id"] = new_value
        
        if update_kwargs:
            # Используем async db_manager
            executor = await db_manager.update_executor_profile(profile_id, **update_kwargs)
            if executor:
                await message.answer(f"✅ Поле '{field_name}' успешно обновлено!")
                await state.clear()
                # Показываем обновленный профиль
                executor_text = await _format_executor_profile(profile_id)
                keyboard = get_admin_profile_actions_keyboard("executor", profile_id)
                await message.answer(executor_text, reply_markup=keyboard, parse_mode="HTML")
            else:
                await message.answer("❌ Исполнитель не найден.")
        else:
            await message.answer("❌ Не удалось обновить поле.")
    except Exception as e:
        logger.error(f"Error updating executor field: {e}", exc_info=True)
        await message.answer("❌ Ошибка при обновлении поля.")
        await state.clear()


@router.callback_query(F.data.startswith("admin_edit_dir:"))
async def admin_edit_direction(callback_query: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор направления исполнителя."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        # Формат: admin_edit_dir:it или admin_edit_dir:se
        parts = callback_query.data.split(":")
        if len(parts) < 2:
            raise ValueError(f"Invalid callback_data format: {callback_query.data}")
        
        direction = parts[1]  # "it" or "se"
        data = await state.get_data()
        profile_id = data.get("admin_edit_profile_id")
        
        if not profile_id:
            await callback_query.answer("❌ Не найден ID профиля. Попробуйте снова.", show_alert=True)
            return
        
        direction_map = {"it": "ИТ служба", "se": "Служба эксплуатации"}
        direction_value = direction_map.get(direction, direction)
        
        await db_manager.update_executor_profile(profile_id, service_direction=direction_value)
        
        await callback_query.message.edit_text(f"✅ Направление обновлено: {direction_value}")
        await state.clear()
        await asyncio.sleep(1)
        await admin_view_executor(callback_query, state)
        await callback_query.answer("✅ Обновлено")
    except Exception as e:
        logger.error(f"Error updating direction: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка.", show_alert=True)


@router.callback_query(F.data.startswith("admin_status:"))
async def admin_edit_status(callback_query: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор статуса исполнителя."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    try:
        # Формат: admin_status:pending, admin_status:active и т.д.
        parts = callback_query.data.split(":")
        if len(parts) < 2:
            raise ValueError(f"Invalid callback_data format: {callback_query.data}")
        
        status_key = parts[1]
        data = await state.get_data()
        profile_id = data.get("admin_edit_profile_id")
        
        if not profile_id:
            await callback_query.answer("❌ Не найден ID профиля. Попробуйте снова.", show_alert=True)
            return
        
        status_map = {
            "pending": "ожидает подтверждения",
            "active": "активен",
            "rejected": "отклонен",
            "blocked": "заблокирован"
        }
        status_value = status_map.get(status_key, status_key)
        
        await db_manager.update_executor_profile(profile_id, profile_status=status_value)
        
        await callback_query.message.edit_text(f"✅ Статус обновлён: {status_value}")
        await state.clear()
        await asyncio.sleep(1)
        await admin_view_executor(callback_query, state)
        await callback_query.answer("✅ Обновлено")
    except Exception as e:
        logger.error(f"Error updating status: {e}", exc_info=True)
        await callback_query.answer("❌ Ошибка.", show_alert=True)


# ============================================================================
# НАВИГАЦИЯ
# ============================================================================

@router.callback_query(F.data == "admin_back_to_main")
async def admin_back_to_main(callback_query: CallbackQuery, state: FSMContext):
    """Возврат в главное меню админа."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    await state.set_state(AdminManagement.main_menu)
    await callback_query.message.edit_text(
        "🔐 <b>Админ-панель</b>\n\n"
        "Выберите действие:",
        reply_markup=None,
        parse_mode="HTML"
    )
    await callback_query.message.answer(
        "Выберите действие:",
        reply_markup=get_admin_main_menu_keyboard()
    )
    await callback_query.answer()


# ============================================================================
# ПОИСК (заглушки для будущей реализации)
# ============================================================================

@router.callback_query(F.data == "admin_search_user")
async def admin_search_user(callback_query: CallbackQuery, state: FSMContext):
    """Поиск пользователя (заглушка)."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    await callback_query.answer("🔍 Функция поиска будет реализована позже", show_alert=True)


@router.callback_query(F.data == "admin_search_executor")
async def admin_search_executor(callback_query: CallbackQuery, state: FSMContext):
    """Поиск исполнителя (заглушка)."""
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ У вас нет прав.", show_alert=True)
        return
    
    await callback_query.answer("🔍 Функция поиска будет реализована позже", show_alert=True)

