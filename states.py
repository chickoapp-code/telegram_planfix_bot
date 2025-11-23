"""
FSM состояния для диалогов бота
Версия: 2.1 
"""

from aiogram.fsm.state import StatesGroup, State


class RoleSelection(StatesGroup):
    """Состояние выбора роли при первой регистрации."""
    choosing_role = State()


class UserRegistration(StatesGroup):
    """Состояния регистрации сотрудника ресторана."""
    waiting_for_full_name = State()
    waiting_for_phone_number = State()
    waiting_for_franchise = State()  # Выбор группы франчайзи
    waiting_for_restaurant = State()  # Выбор ресторана (контакта)


class ExecutorRegistration(StatesGroup):
    """Состояния регистрации исполнителя (техника)."""
    waiting_for_full_name = State()
    waiting_for_phone_number = State()
    waiting_for_position = State()
    waiting_for_direction = State()
    waiting_for_concepts = State()  # Выбор концепций (групп франчайзи)
    waiting_for_restaurants = State()  # Выбор ресторанов по выбранным концепциям
    waiting_for_confirmation = State()


class TicketCreation(StatesGroup):
    """Состояния создания заявки."""
    choosing_template = State()  # Выбор шаблона (ИТ/СЭ)
    entering_description = State()  # Ввод описания проблемы
    attaching_photo = State()  # Прикрепление фото (опционально)


class StatusInquiry(StatesGroup):
    """Состояния уточнения статуса заявки."""
    choosing_from_list = State()  # Выбор заявки из списка
    waiting_for_task_id = State()  # Ввод номера заявки (если выбрано вручную)


class CommentFlow(StatesGroup):
    """Состояния добавления комментария к заявке."""
    choosing_from_list = State()  # Выбор заявки из списка
    waiting_for_task_id = State()  # Ввод номера заявки (если выбрано вручную)
    waiting_for_text = State()  # Ввод текста комментария
    waiting_for_file = State()  # Прикрепление файла (опционально)


class ExecutorTaskManagement(StatesGroup):
    """Состояния управления задачами исполнителем."""
    viewing_task = State()  # Просмотр задачи
    entering_comment = State()  # Ввод комментария/причины
    attaching_file = State()  # Прикрепление файла


class ProfileEdit(StatesGroup):
    """Состояния редактирования профиля пользователя."""
    choosing_field = State()  # Выбор поля для редактирования
    editing_full_name = State()  # Редактирование ФИО
    editing_phone = State()  # Редактирование телефона
    editing_franchise = State()  # Редактирование концепции
    editing_restaurant = State()  # Редактирование ресторана


class TaskCancellation(StatesGroup):
    """Состояния отмены заявки."""
    choosing_from_list = State()  # Выбор заявки из списка
    waiting_for_task_id = State()  # Ввод номера заявки (если выбрано вручную)
    confirming_cancellation = State()  # Подтверждение отмены


class AdminExecutorApproval(StatesGroup):
    """Состояния подтверждения исполнителя администратором."""
    waiting_for_planfix_user_id = State()  # Ввод Planfix User ID
    waiting_for_planfix_contact_id = State()  # Ввод Planfix Contact ID


class ExecutorProfileEdit(StatesGroup):
    """Состояния редактирования профиля исполнителя."""
    editing_full_name = State()
    editing_phone = State()
    editing_position = State()
    editing_concepts = State()
    editing_restaurants = State()
    editing_direction = State()
