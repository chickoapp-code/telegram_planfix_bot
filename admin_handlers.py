"""
Обработчики команд для администраторов
Версия: 1.0
"""

import logging
import asyncio
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
        parts = callback_query.data.split(":")
        profile_type = parts[1]  # "user" or "executor"
        profile_id = int(parts[2])
        
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
        parts = callback_query.data.split(":")
        profile_type = parts[2]  # "user" or "executor"
        profile_id = int(parts[3])
        
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
        parts = callback_query.data.split(":")
        profile_type = parts[1]  # "user" or "executor"
        profile_id = int(parts[2])
        
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
        parts = callback_query.data.split(":")
        profile_type = "user" if "user_field" in callback_query.data else "executor"
        profile_id = int(parts[2])
        field_name = parts[3]
        
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
        direction = callback_query.data.split(":")[2]  # "it" or "se"
        data = await state.get_data()
        profile_id = data.get("admin_edit_profile_id")
        
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
        status_key = callback_query.data.split(":")[1]
        data = await state.get_data()
        profile_id = data.get("admin_edit_profile_id")
        
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

