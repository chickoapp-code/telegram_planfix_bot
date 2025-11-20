from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def normalize_counterparty_id(raw_id: Any) -> Optional[int | str]:
    """
    Приводит идентификатор контрагента/контакта к удобному виду.

    Planfix может возвращать идентификаторы в форматах:
        - 123 (int)
        - "123" (str)
        - "contact:123" / "company:321"
        - {"id": "..."} (вложенный объект)
    """
    try:
        if raw_id is None:
            return None

        if isinstance(raw_id, dict):
            raw_id = raw_id.get("id")

        if isinstance(raw_id, str):
            value = raw_id.strip()
            if not value:
                return None
            if ":" in value:
                value = value.split(":")[-1]
            if value.isdigit():
                return int(value)
            return value

        if isinstance(raw_id, (int, float)):
            return int(raw_id)
    except Exception as exc:
        logger.debug("Failed to normalize counterparty id %s: %s", raw_id, exc)

    return raw_id


def _collect_phones(raw: Any) -> List[str]:
    phones: List[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                number = item.get("number") or item.get("value")
                if number:
                    phones.append(str(number).strip())
            elif isinstance(item, str):
                number = item.strip()
                if number:
                    phones.append(number)
    elif isinstance(raw, dict):
        number = raw.get("number") or raw.get("value")
        if number:
            phones.append(str(number).strip())
    elif isinstance(raw, str):
        number = raw.strip()
        if number:
            phones.append(number)
    return phones


def extract_contact_info(contact: Any) -> Dict[str, Any]:
    """
    Приводит ответ Planfix о контакте к единому словарю.

    Возвращаемый словарь всегда содержит ключи:
        id, raw_id, name, is_company, phones, phone, email
    """
    info: Dict[str, Any] = {
        "id": None,
        "raw_id": None,
        "name": None,
        "is_company": False,
        "phones": [],
        "phone": None,
        "email": None,
        "raw": contact,
    }

    if not isinstance(contact, dict):
        logger.debug("extract_contact_info expects dict, got %r", type(contact))
        return info

    raw_id = contact.get("id")
    info["raw_id"] = raw_id
    info["id"] = normalize_counterparty_id(raw_id)

    is_company = bool(contact.get("isCompany"))
    info["is_company"] = is_company

    # Имя контрагента: для компании берём name, для физлица комбинируем поля
    name_candidates: List[str] = []
    if is_company:
        for key in ("name", "fullName", "displayName"):
            value = contact.get(key)
            if isinstance(value, str) and value.strip():
                name_candidates.append(value.strip())
    else:
        for key in ("lastName", "name", "firstName", "midName"):
            value = contact.get(key)
            if isinstance(value, str) and value.strip():
                name_candidates.append(value.strip())
        # Если Planfix вернул собранное имя целиком
        for key in ("displayName", "fullName"):
            value = contact.get(key)
            if isinstance(value, str) and value.strip():
                name_candidates.append(value.strip())

    if name_candidates:
        info["name"] = " ".join(dict.fromkeys(name_candidates))  # сохраняем порядок, убирая дубли

    phones = _collect_phones(contact.get("phones"))
    if not phones and contact.get("phone"):
        phones = _collect_phones(contact.get("phone"))
    info["phones"] = phones
    if phones:
        info["phone"] = phones[0]

    email = contact.get("email")
    if isinstance(email, dict):
        email = email.get("value") or email.get("address")
    if isinstance(email, str) and email.strip():
        info["email"] = email.strip()

    return info


def extract_counterparty_from_task(task: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Возвращает словарь с информацией о контрагенте задачи.
    """
    counterparty: Dict[str, Any] = {
        "id": None,
        "raw_id": None,
        "name": None,
        "is_company": False,
        "contact": None,
    }

    if not isinstance(task, dict):
        return counterparty

    cp = task.get("counterparty")
    if not cp:
        return counterparty

    if isinstance(cp, dict):
        counterparty["raw_id"] = cp.get("id")
        counterparty["id"] = normalize_counterparty_id(cp.get("id"))
        info = extract_contact_info(cp)
        counterparty["name"] = info.get("name")
        counterparty["is_company"] = info.get("is_company", False)
        counterparty["contact"] = info
    else:
        counterparty["raw_id"] = cp
        counterparty["id"] = normalize_counterparty_id(cp)

    return counterparty


def format_counterparty_display(counterparty: Optional[Dict[str, Any]]) -> str:
    """
    Формирует компактное представление контрагента для сообщений.
    """
    if not counterparty:
        return "Не указан"

    name = counterparty.get("name")
    cp_id = counterparty.get("id") or counterparty.get("raw_id")
    phone = None

    contact = counterparty.get("contact")
    if isinstance(contact, dict):
        phone = contact.get("phone")
        if not name:
            name = contact.get("name")

    parts: List[str] = []
    if name:
        parts.append(str(name))
    if cp_id is not None:
        parts.append(f"ID: {cp_id}")
    if phone:
        parts.append(f"☎ {phone}")

    return " • ".join(parts) if parts else "Не указан"


