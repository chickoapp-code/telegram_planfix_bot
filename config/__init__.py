from __future__ import annotations

from copy import deepcopy
from typing import Dict, List, Optional

from .settings import settings

BOT_TOKEN = settings.bot_token

PLANFIX_BASE_URL = settings.planfix_base_url
PLANFIX_API_KEY = settings.planfix_api_key
PLANFIX_API_SECRET = settings.planfix_api_secret
PLANFIX_ACCOUNT = settings.planfix_account
PLANFIX_API_SOURCE_ID = settings.planfix_api_source_id

PLANFIX_TASK_PROCESS_ID = settings.planfix_task_process_id
PLANFIX_MAX_CONCURRENCY = settings.planfix_max_concurrency

PLANFIX_STATUS_ID_NEW = settings.planfix_status_id_new
PLANFIX_STATUS_ID_DRAFT = settings.planfix_status_id_draft
PLANFIX_STATUS_ID_IN_PROGRESS = settings.planfix_status_id_in_progress
PLANFIX_STATUS_ID_INFO_SENT = settings.planfix_status_id_info_sent
PLANFIX_STATUS_ID_REPLY_RECEIVED = settings.planfix_status_id_reply_received
PLANFIX_STATUS_ID_TIMEOUT = settings.planfix_status_id_timeout
PLANFIX_STATUS_ID_COMPLETED = settings.planfix_status_id_completed
PLANFIX_STATUS_ID_POSTPONED = settings.planfix_status_id_postponed
PLANFIX_STATUS_ID_FINISHED = settings.planfix_status_id_finished
PLANFIX_STATUS_ID_CANCELLED = settings.planfix_status_id_cancelled
PLANFIX_STATUS_ID_REJECTED = settings.planfix_status_id_rejected

PLANFIX_STATUS_NAME_NEW = settings.planfix_status_name_new
PLANFIX_STATUS_NAME_DRAFT = settings.planfix_status_name_draft
PLANFIX_STATUS_NAME_IN_PROGRESS = settings.planfix_status_name_in_progress
PLANFIX_STATUS_NAME_INFO_SENT = settings.planfix_status_name_info_sent
PLANFIX_STATUS_NAME_REPLY_RECEIVED = settings.planfix_status_name_reply_received
PLANFIX_STATUS_NAME_TIMEOUT = settings.planfix_status_name_timeout
PLANFIX_STATUS_NAME_COMPLETED = settings.planfix_status_name_completed
PLANFIX_STATUS_NAME_POSTPONED = settings.planfix_status_name_postponed
PLANFIX_STATUS_NAME_FINISHED = settings.planfix_status_name_finished
PLANFIX_STATUS_NAME_CANCELLED = settings.planfix_status_name_cancelled
PLANFIX_STATUS_NAME_REJECTED = settings.planfix_status_name_rejected
PLANFIX_STATUS_NAME_PAUSED = settings.planfix_status_name_paused
PLANFIX_STATUS_NAME_WAITING_INFO = settings.planfix_status_name_waiting_info

PLANFIX_STATUS_NAMES: Dict[str, str | None] = {
    "new": PLANFIX_STATUS_NAME_NEW,
    "draft": PLANFIX_STATUS_NAME_DRAFT,
    "in_progress": PLANFIX_STATUS_NAME_IN_PROGRESS,
    "info_sent": PLANFIX_STATUS_NAME_INFO_SENT,
    "reply_received": PLANFIX_STATUS_NAME_REPLY_RECEIVED,
    "timeout": PLANFIX_STATUS_NAME_TIMEOUT,
    "completed": PLANFIX_STATUS_NAME_COMPLETED,
    "postponed": PLANFIX_STATUS_NAME_POSTPONED,
    "finished": PLANFIX_STATUS_NAME_FINISHED,
    "cancelled": PLANFIX_STATUS_NAME_CANCELLED,
    "rejected": PLANFIX_STATUS_NAME_REJECTED,
    "paused": PLANFIX_STATUS_NAME_PAUSED,
    "waiting_info": PLANFIX_STATUS_NAME_WAITING_INFO,
}

# Кастомные поля Planfix (опциональные для утилит, обязательные для основного бота)
CUSTOM_FIELD_RESTAURANT_ID = settings.custom_field_restaurant_id
CUSTOM_FIELD_CONTACT_ID = settings.custom_field_contact_id
CUSTOM_FIELD_PHONE_ID = settings.custom_field_phone_id
CUSTOM_FIELD_TYPE_ID = settings.custom_field_type_id
CUSTOM_FIELD_MOBILE_PHONE_ID = settings.custom_field_mobile_phone_id


def _require_custom_field(field_name: str, field_value: int | None) -> int:
    """Проверяет, что кастомное поле задано, и возвращает его значение.
    
    Используется в местах, где поле обязательно для работы функционала.
    """
    if field_value is None:
        raise ValueError(
            f"Кастомное поле {field_name} не задано в .env файле. "
            f"Это поле обязательно для работы данного функционала. "
            f"Пожалуйста, добавьте его в .env файл."
        )
    return field_value

DIRECTORY_RESTAURANTS_ID = settings.directory_restaurants_id

DB_PATH = str(settings.db_path)
LOG_LEVEL = settings.log_level
PLANFIX_POLL_INTERVAL = settings.planfix_poll_interval

TELEGRAM_ADMIN_IDS = settings.telegram_admin_ids

# --------------------------------------------------------------------------------------
# Domain dictionaries
# --------------------------------------------------------------------------------------

FRANCHISE_GROUPS: Dict[int, Dict[str, object]] = {
    12: {
        "name": "Мясоroob",
        "project_id": None,
    },
    14: {
        "name": "Аджикинежаль",
        "project_id": None,
    },
    16: {
        "name": "У Беллуччи Круче",
        "project_id": None,
    },
    20: {
        "name": "Хинкальцы",
        "project_id": None,
    },
    22: {
        "name": "CHICKO",
        "project_id": None,
    },
    28: {
        "name": "Во все ребра",
        "project_id": None,
    },
    30: {
        "name": "ДжоДжа",
        "project_id": None,
    },
}

# Шаблоны задач
PLANFIX_SE_TEMPLATES: Dict[int, Dict[str, object]] = {
    83454: {
        "id": 83454,
        "name": "Служба Эксплуатации",
        "full_name": "Служба Эксплуатации",
        "project_id": None,
        "franchise_group": None,
    },
}

PLANFIX_IT_TEMPLATES: Dict[int, Dict[str, object]] = {
    80839: {
        "id": 80839,
        "name": "ИТ отдел",
        "full_name": "ИТ отдел",
        "project_id": None,
        "franchise_group": None,
    },
}

PLANFIX_IT_TAG = "ИТ отдел"
PLANFIX_SE_TAG = "СЛУЖБА ЭКСПЛУАТАЦИИ"
UNIVERSAL_TEMPLATE_ENABLED = False

# Группа контактов для поддержки (исполнители и заявители)
# Чтобы получить ID группы "Поддержка", можно:
# 1. Использовать API: GET /contact/group/list
# 2. Или найти в интерфейсе Planfix в настройках групп контактов
# Настраивается через переменную окружения SUPPORT_CONTACT_GROUP_ID
SUPPORT_CONTACT_GROUP_ID = settings.support_contact_group_id
SUPPORT_CONTACT_TEMPLATE_ID = settings.support_contact_template_id

_TEMPLATE_REGISTRY: Dict[int, Dict[str, object]] = {
    **PLANFIX_SE_TEMPLATES,
    **PLANFIX_IT_TEMPLATES,
}


def get_available_templates(franchise_group_id: int, restaurant_contact_id: int) -> List[Dict[str, object]]:
    """
    Возвращает список шаблонов задач, доступных пользователю.

    Пока что все шаблоны доступны независимо от концепции — при необходимости
    можно добавить фильтрацию по franchise_group.
    """
    return [deepcopy(meta) for meta in _TEMPLATE_REGISTRY.values()]


def get_template_info(template_id: int) -> Dict[str, object] | None:
    """Возвращает информацию о шаблоне по идентификатору."""
    tpl = _TEMPLATE_REGISTRY.get(int(template_id))
    return deepcopy(tpl) if tpl else None


def get_template_direction(template_id: int | None) -> str | None:
    """Возвращает направление ('it' или 'se') для указанного шаблона."""
    if template_id is None:
        return None
    try:
        template_id = int(template_id)
    except (TypeError, ValueError):
        return None
    if template_id in PLANFIX_IT_TEMPLATES:
        return "it"
    if template_id in PLANFIX_SE_TEMPLATES:
        return "se"
    return None


def get_direction_tag(direction: str | None) -> str | None:
    """Возвращает человекочитаемый тег для направления задачи."""
    if not direction:
        return None
    norm = direction.strip().lower()
    if norm in ("it", "ит", "it отдел", "it-служба", "it служба"):
        return PLANFIX_IT_TAG
    if norm in ("se", "сэ", "служба эксплуатации", "эксплуатация", "отдел эксплуатации"):
        return PLANFIX_SE_TAG
    return None


async def get_contacts_by_group(planfix_client, group_id: int) -> Dict[int, str]:
    """
    Получает контакты из группы Planfix и возвращает словарь {contact_id: contact_name}.
    Автоматически исключает контакты из группы "Поддержка".
    
    Args:
        planfix_client: Экземпляр PlanfixAPIClient
        group_id: ID группы контактов
        
    Returns:
        Словарь {contact_id: contact_name} или пустой словарь при ошибке
    """
    try:
        from typing import Set
        
        # Множество контактов, которые находятся в группе "Поддержка" (для исключения)
        support_contact_ids: Set[int] = set()
        
        # Сначала получаем все контакты из группы "Поддержка", чтобы исключить их
        if SUPPORT_CONTACT_GROUP_ID:
            try:
                support_contacts_response = await planfix_client.get_contact_list_by_group(
                    SUPPORT_CONTACT_GROUP_ID, fields="id", page_size=1000
                )
                if support_contacts_response and support_contacts_response.get('result') == 'success':
                    for c in support_contacts_response.get('contacts', []):
                        try:
                            cid = int(c.get('id'))
                            support_contact_ids.add(cid)
                        except (TypeError, ValueError):
                            continue
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to load support group contacts for filtering: {e}")
        
        # Теперь загружаем контакты из запрошенной группы, исключая те, что в группе "Поддержка"
        contacts_response = await planfix_client.get_contact_list_by_group(
            group_id, fields="id,name", page_size=100
        )
        
        if not contacts_response or contacts_response.get('result') != 'success':
            return {}
        
        contacts = {}
        for c in contacts_response.get('contacts', []):
            try:
                cid = int(c.get('id'))
                
                # Пропускаем контакты, которые находятся в группе "Поддержка"
                if cid in support_contact_ids:
                    continue
                
                name = c.get('name') or f"Контакт {cid}"
                contacts[cid] = name
            except (TypeError, ValueError):
                continue
        
        return contacts
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error loading contacts for group {group_id}: {e}", exc_info=True)
        return {}


__all__ = [
    "BOT_TOKEN",
    "PLANFIX_BASE_URL",
    "PLANFIX_API_KEY",
    "PLANFIX_API_SECRET",
    "PLANFIX_ACCOUNT",
    "PLANFIX_API_SOURCE_ID",
    "PLANFIX_TASK_PROCESS_ID",
    "PLANFIX_MAX_CONCURRENCY",
    "PLANFIX_STATUS_ID_NEW",
    "PLANFIX_STATUS_ID_DRAFT",
    "PLANFIX_STATUS_ID_IN_PROGRESS",
    "PLANFIX_STATUS_ID_INFO_SENT",
    "PLANFIX_STATUS_ID_REPLY_RECEIVED",
    "PLANFIX_STATUS_ID_TIMEOUT",
    "PLANFIX_STATUS_ID_COMPLETED",
    "PLANFIX_STATUS_ID_POSTPONED",
    "PLANFIX_STATUS_ID_FINISHED",
    "PLANFIX_STATUS_ID_CANCELLED",
    "PLANFIX_STATUS_ID_REJECTED",
    "PLANFIX_STATUS_NAME_NEW",
    "PLANFIX_STATUS_NAME_DRAFT",
    "PLANFIX_STATUS_NAME_IN_PROGRESS",
    "PLANFIX_STATUS_NAME_INFO_SENT",
    "PLANFIX_STATUS_NAME_REPLY_RECEIVED",
    "PLANFIX_STATUS_NAME_TIMEOUT",
    "PLANFIX_STATUS_NAME_COMPLETED",
    "PLANFIX_STATUS_NAME_POSTPONED",
    "PLANFIX_STATUS_NAME_FINISHED",
    "PLANFIX_STATUS_NAME_CANCELLED",
    "PLANFIX_STATUS_NAME_REJECTED",
    "PLANFIX_STATUS_NAME_PAUSED",
    "PLANFIX_STATUS_NAME_WAITING_INFO",
    "PLANFIX_STATUS_NAMES",
    "CUSTOM_FIELD_RESTAURANT_ID",
    "CUSTOM_FIELD_CONTACT_ID",
    "CUSTOM_FIELD_PHONE_ID",
    "CUSTOM_FIELD_TYPE_ID",
    "CUSTOM_FIELD_MOBILE_PHONE_ID",
    "DIRECTORY_RESTAURANTS_ID",
    "DB_PATH",
    "LOG_LEVEL",
    "PLANFIX_POLL_INTERVAL",
    "TELEGRAM_ADMIN_IDS",
    "FRANCHISE_GROUPS",
    "PLANFIX_SE_TEMPLATES",
    "PLANFIX_IT_TEMPLATES",
    "PLANFIX_IT_TAG",
    "PLANFIX_SE_TAG",
    "UNIVERSAL_TEMPLATE_ENABLED",
    "get_available_templates",
    "get_template_info",
    "get_template_direction",
    "get_direction_tag",
    "get_contacts_by_group",
    "SUPPORT_CONTACT_GROUP_ID",
    "SUPPORT_CONTACT_TEMPLATE_ID",
]

