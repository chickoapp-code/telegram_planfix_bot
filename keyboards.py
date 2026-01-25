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
        [KeyboardButton(text="❌ Отменить заявку"), KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="🔄 Перезапустить бот")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_executor_main_menu_keyboard():
    """Главное меню для исполнителей."""
    buttons = [
        [KeyboardButton(text="📋 Задачи")],
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
    # Убрали кнопку "Принять в работу" - все исполнители назначаются автоматически
    # Для новых задач показываем только комментарии и завершение
    buttons.append([InlineKeyboardButton(text="💬 Написать комментарий", callback_data=f"comment:{task_id}")])
    # Убрали кнопку "Возобновить" - не нужна
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


# ============================================================================
# АДМИН-КЛАВИАТУРЫ
# ============================================================================

def get_admin_main_menu_keyboard():
    """Главное меню администратора."""
    buttons = [
        [KeyboardButton(text="👥 Управление пользователями")],
        [KeyboardButton(text="👷 Управление исполнителями")],
        [KeyboardButton(text="📊 Статистика")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_admin_users_menu_keyboard():
    """Меню управления пользователями."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Список пользователей", callback_data="admin_list_users")],
            [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="admin_search_user")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back_to_main")]
        ]
    )


def get_admin_executors_menu_keyboard():
    """Меню управления исполнителями."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Список исполнителей", callback_data="admin_list_executors")],
            [InlineKeyboardButton(text="🔍 Найти исполнителя", callback_data="admin_search_executor")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back_to_main")]
        ]
    )


def get_admin_profile_actions_keyboard(profile_type: str, profile_id: int):
    """Клавиатура действий с профилем (пользователь/исполнитель)."""
    buttons = []
    
    # Для пользователей и исполнителей добавляем кнопку просмотра заявок
    if profile_type == "user":
        buttons.append([InlineKeyboardButton(text="📋 Заявки пользователя", callback_data=f"admin_view_user_tasks:{profile_id}")])
    elif profile_type == "executor":
        buttons.append([InlineKeyboardButton(text="📋 Заявки исполнителя", callback_data=f"admin_view_executor_tasks:{profile_id}")])
    
    buttons.extend([
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"admin_edit_{profile_type}:{profile_id}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_delete_{profile_type}:{profile_id}")],
        [InlineKeyboardButton(text="◀️ Назад к списку", callback_data=f"admin_list_{profile_type}s")]
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_admin_edit_user_keyboard(user_id: int):
    """Клавиатура редактирования пользователя."""
    buttons = [
        [InlineKeyboardButton(text="✏️ ФИО", callback_data=f"admin_edit_user_field:{user_id}:full_name")],
        [InlineKeyboardButton(text="✏️ Телефон", callback_data=f"admin_edit_user_field:{user_id}:phone")],
        [InlineKeyboardButton(text="✏️ Email", callback_data=f"admin_edit_user_field:{user_id}:email")],
        [InlineKeyboardButton(text="✏️ Концепция", callback_data=f"admin_edit_user_field:{user_id}:franchise")],
        [InlineKeyboardButton(text="✏️ Ресторан", callback_data=f"admin_edit_user_field:{user_id}:restaurant")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"admin_view_user:{user_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_admin_edit_executor_keyboard(executor_id: int):
    """Клавиатура редактирования исполнителя."""
    buttons = [
        [InlineKeyboardButton(text="✏️ ФИО", callback_data=f"admin_edit_exec_field:{executor_id}:full_name")],
        [InlineKeyboardButton(text="✏️ Телефон", callback_data=f"admin_edit_exec_field:{executor_id}:phone")],
        [InlineKeyboardButton(text="✏️ Email", callback_data=f"admin_edit_exec_field:{executor_id}:email")],
        [InlineKeyboardButton(text="✏️ Должность", callback_data=f"admin_edit_exec_field:{executor_id}:position")],
        [InlineKeyboardButton(text="✏️ Концепции", callback_data=f"admin_edit_exec_field:{executor_id}:concepts")],
        [InlineKeyboardButton(text="✏️ Рестораны", callback_data=f"admin_edit_exec_field:{executor_id}:restaurants")],
        [InlineKeyboardButton(text="✏️ Направление", callback_data=f"admin_edit_exec_field:{executor_id}:direction")],
        [InlineKeyboardButton(text="✏️ Planfix Contact ID", callback_data=f"admin_edit_exec_field:{executor_id}:planfix_id")],
        [InlineKeyboardButton(text="✏️ Статус", callback_data=f"admin_edit_exec_field:{executor_id}:status")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"admin_view_executor:{executor_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_admin_delete_confirmation_keyboard(profile_type: str, profile_id: int):
    """Клавиатура подтверждения удаления."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"admin_confirm_delete_{profile_type}:{profile_id}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_view_{profile_type}:{profile_id}")
            ]
        ]
    )


def create_users_list_keyboard(users: list, page: int = 0, per_page: int = 10):
    """Создает клавиатуру со списком пользователей с пагинацией."""
    def _short(text: str) -> str:
        return text if len(text) <= 64 else (text[:61] + "...")
    
    buttons = []
    start_idx = page * per_page
    end_idx = start_idx + per_page
    
    for user in users[start_idx:end_idx]:
        user_id = user.telegram_id
        name = user.full_name or f"ID: {user_id}"
        button_text = _short(f"{name} (ID: {user_id})")
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"admin_view_user:{user_id}")])
    
    # Кнопки пагинации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"admin_list_users_page:{page-1}"))
    if end_idx < len(users):
        nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"admin_list_users_page:{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    
    buttons.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="admin_back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def create_executors_list_keyboard(executors: list, page: int = 0, per_page: int = 10):
    """Создает клавиатуру со списком исполнителей с пагинацией."""
    def _short(text: str) -> str:
        return text if len(text) <= 64 else (text[:61] + "...")
    
    buttons = []
    start_idx = page * per_page
    end_idx = start_idx + per_page
    
    for executor in executors[start_idx:end_idx]:
        executor_id = executor.telegram_id
        name = executor.full_name or f"ID: {executor_id}"
        status = executor.profile_status or "неизвестно"
        button_text = _short(f"{name} ({status}) - ID: {executor_id}")
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"admin_view_executor:{executor_id}")])
    
    # Кнопки пагинации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"admin_list_executors_page:{page-1}"))
    if end_idx < len(executors):
        nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"admin_list_executors_page:{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    
    buttons.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="admin_back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)
