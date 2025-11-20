from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

def get_role_selection_keyboard():
    """Клавиатура для выбора роли при регистрации."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="👤 Сотрудник ресторана", 
                callback_data="role_user"
            )],
            [InlineKeyboardButton(
                text="👷 Исполнитель техподдержки", 
                callback_data="role_executor"
            )]
        ]
    )

def get_main_menu_keyboard():
    """Главное меню для сотрудников ресторанов."""
    buttons = [
        [KeyboardButton(text="📝 Создать заявку"), KeyboardButton(text="📋 Мои заявки")],
        [KeyboardButton(text="🔍 Уточнить статус"), KeyboardButton(text="💬 Написать комментарий")],
        [KeyboardButton(text="❌ Отменить заявку"), KeyboardButton(text="👤 Профиль")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_executor_main_menu_keyboard():
    """Главное меню для исполнителей."""
    buttons = [
        [KeyboardButton(text="🆕 Новые заявки"), KeyboardButton(text="📋 Мои задачи")],
        [KeyboardButton(text="👤 Профиль исполнителя")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_phone_number_keyboard():
    button = KeyboardButton(text="Поделиться номером телефона", request_contact=True)
    return ReplyKeyboardMarkup(keyboard=[[button]], resize_keyboard=True)

def create_dynamic_keyboard(items: list, add_cancel_button: bool = False) -> InlineKeyboardMarkup:
    """Создаёт простую inline-клавиатуру из (id, name), подрезая подписи до 64 символов."""
    def _short(text: str) -> str:
        return text if len(text) <= 64 else (text[:61] + "...")
    buttons = [[InlineKeyboardButton(text=_short(item_name), callback_data=item_id)] for item_id, item_name in items]
    if add_cancel_button:
        buttons.append([InlineKeyboardButton(text="Отмена", callback_data="cancel_registration")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_executor_confirmation_keyboard(executor_id):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_executor:{executor_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_executor:{executor_id}")
            ]
        ]
    )

def get_skip_or_done_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_file")]]
    )

def get_task_actions_keyboard(task_id: int, is_new: bool = False, is_waiting: bool = False, is_paused: bool = False):
    buttons = []
    if is_new:
        buttons.append([InlineKeyboardButton(text="✅ Принять в работу", callback_data=f"accept:{task_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="💬 Написать комментарий", callback_data=f"comment:{task_id}")])
        # Если задача в ожидании информации или на паузе — показываем «Возобновить»
        if is_waiting or is_paused:
            buttons.append([InlineKeyboardButton(text="▶️ Возобновить", callback_data=f"resume:{task_id}")])
        buttons.append([InlineKeyboardButton(text="✅ Завершить", callback_data=f"close:{task_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_profile_edit_keyboard():
    """Клавиатура для редактирования профиля."""
    buttons = [
        [InlineKeyboardButton(text="✏️ Изменить ФИО", callback_data="edit_name")],
        [InlineKeyboardButton(text="✏️ Изменить телефон", callback_data="edit_phone")],
        [InlineKeyboardButton(text="✏️ Изменить концепцию", callback_data="edit_franchise")],
        [InlineKeyboardButton(text="✏️ Изменить ресторан", callback_data="edit_restaurant")],
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="cancel_edit")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_executor_profile_edit_keyboard():
    """Клавиатура для редактирования профиля исполнителя."""
    buttons = [
        [InlineKeyboardButton(text="✏️ ФИО", callback_data="exec_edit_name")],
        [InlineKeyboardButton(text="✏️ Телефон", callback_data="exec_edit_phone")],
        [InlineKeyboardButton(text="✏️ Должность", callback_data="exec_edit_position")],
        [InlineKeyboardButton(text="✏️ Концепции", callback_data="exec_edit_concepts")],
        [InlineKeyboardButton(text="✏️ Рестораны", callback_data="exec_edit_restaurants")],
        [InlineKeyboardButton(text="✏️ Направление", callback_data="exec_edit_direction")],
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="exec_cancel_edit")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_executor_direction_keyboard(prefix: str = "exec_dir", include_cancel: bool = False):
    """Клавиатура для выбора направления исполнителя."""
    buttons = [
        [InlineKeyboardButton(text="💻 ИТ служба", callback_data=f"{prefix}:it")],
        [InlineKeyboardButton(text="🛠 Служба эксплуатации", callback_data=f"{prefix}:se")],
    ]
    if include_cancel:
        buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="exec_cancel_edit")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_cancel_keyboard():
    """Клавиатура с кнопкой отмены."""
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_action")]]
    )


def get_confirmation_keyboard(action: str, task_id: int = None):
    """Клавиатура подтверждения действия."""
    callback_data = f"confirm_{action}:{task_id}" if task_id else f"confirm_{action}"
    cancel_data = f"cancel_{action}:{task_id}" if task_id else f"cancel_{action}"
    
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, подтверждаю", callback_data=callback_data),
                InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_data)
            ]
        ]
    )


def create_tasks_keyboard(tasks: list, action_type: str = "select") -> InlineKeyboardMarkup:
    """Создает клавиатуру со списком заявок пользователя.
    ВАЖНО: подписи кнопок однострочные и не длиннее 64 символов (ограничение Telegram).
    """
    def _short(text: str) -> str:
        return text if len(text) <= 64 else (text[:61] + "...")

    buttons = []
    
    for task in tasks:
        task_id = task.get('id')
        task_name = task.get('name', 'Без названия')
        status_name = task.get('status', {}).get('name', 'Неизвестно')
        
        # Укорачиваем отображаемое имя задачи
        display_name = task_name[:40] + "..." if len(task_name) > 40 else task_name
        
        # Формируем однострочный текст к��опки с ограничением длины
        base_text = f"#{task_id} – {status_name}: {display_name}"
        button_text = _short(base_text)
        
        # Определяем callback_data в зависимости от типа действия
        if action_type == "status":
            callback_data = f"status_task:{task_id}"
        elif action_type == "comment":
            callback_data = f"comment_task:{task_id}"
        elif action_type == "cancel":
            callback_data = f"cancel_task:{task_id}"
        else:
            callback_data = f"select_task:{task_id}"
        
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])
    
    # Добавляем кнопку "Ввести номер вручную"
    buttons.append([InlineKeyboardButton(text="⌨️ Ввести номер вручную", callback_data="manual_input")])
    
    # Добавляем кнопку отмены
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="cancel_action")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_task_action_keyboard(task_id: int, action_type: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру с действиями для выбранной заявки."""
    buttons = []
    
    if action_type == "status":
        buttons.append([InlineKeyboardButton(text="🔍 Уточнить статус", callback_data=f"status_task:{task_id}")])
    elif action_type == "comment":
        buttons.append([InlineKeyboardButton(text="💬 Написать комментарий", callback_data=f"comment_task:{task_id}")])
    elif action_type == "cancel":
        buttons.append([InlineKeyboardButton(text="❌ Отменить заявку", callback_data=f"cancel_task:{task_id}")])
    
    # Добавляем кнопку "Назад к списку"
    buttons.append([InlineKeyboardButton(text="◀️ Назад к списку", callback_data="back_to_list")])
    
    # Добавляем кнопку отмены
    buttons.append([InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_action")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)
