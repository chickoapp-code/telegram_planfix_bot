"""
Расширенные обработчики: редактирование профиля и отмена заявки
Версия: 1.0 
"""

import logging
import re
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext

from states import ProfileEdit, TaskCancellation
from keyboards import (
    get_phone_number_keyboard,
    create_dynamic_keyboard,
    get_main_menu_keyboard,
    get_profile_edit_keyboard,
    get_confirmation_keyboard
)
from db_manager import DBManager
from planfix_client import planfix_client
from services.status_registry import StatusKey, require_status_id

logger = logging.getLogger(__name__)
router = Router()
db_manager = DBManager()


# ============================================================================
# РЕДАКТИРОВАНИЕ ПРОФИЛЯ
# ============================================================================

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
        with db_manager.get_db() as db:
            db_manager.update_user_profile(db, user_id, phone_number=phone)
        
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
        groups_response = await planfix_client.get_contact_groups(fields="id,name")
        
        if not groups_response or groups_response.get('result') != 'success':
            await callback_query.message.edit_text("❌ Не удалось загрузить список концепций.")
            return
        
        all_groups = groups_response.get('groups', [])
        franchise_groups = [g for g in all_groups if 'Франчайзи' in g.get('name', '')]
        
        keyboard_items = [
            (str(g['id']), g['name'].replace('Франчайзи "', '').replace('"', ''))
            for g in franchise_groups
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
        contacts_response = await planfix_client.get_contact_list_by_group(
            franchise_group_id,
            fields="id,name",
            page_size=100
        )
        
        if not contacts_response or contacts_response.get('result') != 'success':
            await callback_query.message.edit_text("❌ Не удалось загрузить рестораны.")
            await state.clear()
            return
        
        contacts = contacts_response.get('contacts', [])
        keyboard_items = [(str(c['id']), c['name']) for c in contacts]
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
    with db_manager.get_db() as db:
        user = db_manager.get_user_profile(db, callback_query.from_user.id)
    
    if not user:
        await callback_query.message.edit_text("❌ Профиль не найден.")
        return
    
    try:
        contacts_response = await planfix_client.get_contact_list_by_group(
            user.franchise_group_id,
            fields="id,name",
            page_size=100
        )
        
        if not contacts_response or contacts_response.get('result') != 'success':
            await callback_query.message.edit_text("❌ Не удалось загрузить рестораны.")
            return
        
        contacts = contacts_response.get('contacts', [])
        keyboard_items = [(str(c['id']), c['name']) for c in contacts]
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
        with db_manager.get_db() as db:
            update_data = {"restaurant_contact_id": restaurant_contact_id}
            if new_franchise_id:
                update_data["franchise_group_id"] = new_franchise_id
            
            db_manager.update_user_profile(db, callback_query.from_user.id, **update_data)
        
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

@router.message(F.text == "Отменить заявку")
async def cancel_task_start(message: Message, state: FSMContext):
    """Начало отмены заявки."""
    with db_manager.get_db() as db:
        user = db_manager.get_user_profile(db, message.from_user.id)
    
    if not user:
        await message.answer("❌ Сначала пройдите регистрацию: /start")
        return
    
    await message.answer(
        "❌ Отмена заявки\n\n"
        "Введите номер заявки, которую хотите отменить:"
    )
    await state.set_state(TaskCancellation.waiting_for_task_id)


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
        with db_manager.get_db() as db:
            user = db_manager.get_user_profile(db, message.from_user.id)
            counterparty_id = task.get('counterparty', {}).get('id')
            
            if counterparty_id != user.restaurant_contact_id:
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
        # Получаем информацию о пользователе
        with db_manager.get_db() as db:
            user = db_manager.get_user_profile(db, callback_query.from_user.id)
        
        # Обновляем статус задачи на "Отменена"
        update_response = await planfix_client.update_task(
            task_id,
            status_id=require_status_id(StatusKey.CANCELLED)
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
