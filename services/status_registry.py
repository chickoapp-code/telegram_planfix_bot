from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, Optional

from database import PlanfixTaskStatus
from services.db_service import db_manager
from planfix_client import planfix_client
from config import (
    PLANFIX_STATUS_ID_CANCELLED,
    PLANFIX_STATUS_ID_COMPLETED,
    PLANFIX_STATUS_ID_DRAFT,
    PLANFIX_STATUS_ID_FINISHED,
    PLANFIX_STATUS_ID_INFO_SENT,
    PLANFIX_STATUS_ID_IN_PROGRESS,
    PLANFIX_STATUS_ID_NEW,
    PLANFIX_STATUS_ID_POSTPONED,
    PLANFIX_STATUS_ID_REJECTED,
    PLANFIX_STATUS_ID_REPLY_RECEIVED,
    PLANFIX_STATUS_ID_TIMEOUT,
    PLANFIX_STATUS_NAMES,
    PLANFIX_TASK_PROCESS_ID,
)

logger = logging.getLogger(__name__)


class StatusKey(str, Enum):
    NEW = "new"
    DRAFT = "draft"
    IN_PROGRESS = "in_progress"
    INFO_SENT = "info_sent"
    REPLY_RECEIVED = "reply_received"
    TIMEOUT = "timeout"
    COMPLETED = "completed"
    POSTPONED = "postponed"
    FINISHED = "finished"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


_ENV_STATUS_IDS: Dict[StatusKey, Optional[int]] = {
    StatusKey.NEW: PLANFIX_STATUS_ID_NEW,
    StatusKey.DRAFT: PLANFIX_STATUS_ID_DRAFT,
    StatusKey.IN_PROGRESS: PLANFIX_STATUS_ID_IN_PROGRESS,
    StatusKey.INFO_SENT: PLANFIX_STATUS_ID_INFO_SENT,
    StatusKey.REPLY_RECEIVED: PLANFIX_STATUS_ID_REPLY_RECEIVED,
    StatusKey.TIMEOUT: PLANFIX_STATUS_ID_TIMEOUT,
    StatusKey.COMPLETED: PLANFIX_STATUS_ID_COMPLETED,
    StatusKey.POSTPONED: PLANFIX_STATUS_ID_POSTPONED,
    StatusKey.FINISHED: PLANFIX_STATUS_ID_FINISHED,
    StatusKey.CANCELLED: PLANFIX_STATUS_ID_CANCELLED,
    StatusKey.REJECTED: PLANFIX_STATUS_ID_REJECTED,
}


def _normalize_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value).strip().lower()


def _normalize_system_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value).strip().upper().replace(" ", "")


@dataclass(slots=True)
class StatusRecord:
    id: int
    name: str
    is_final: bool = False
    system_name: Optional[str] = None


class PlanfixStatusRegistry:
    """
    Кэширует и предоставляет доступ к ID статусов процесса Planfix.

    Стратегия получения:
        1. Пытаемся использовать значения из окружения, если есть.
        2. Загружаем локальный кэш (SQLite) — таблица PlanfixTaskStatus.
        3. При отсутствии обязательных статусов — запрашиваем API Planfix и обновляем локальный кэш.
    """

    _required_statuses: tuple[StatusKey, ...] = (
        StatusKey.NEW,
        StatusKey.IN_PROGRESS,
        StatusKey.COMPLETED,
    )

    def __init__(self) -> None:
        self._ids: Dict[StatusKey, Optional[int]] = {}
        self._lock = asyncio.Lock()

    async def ensure_loaded(self, force_refresh: bool = False) -> None:
        """
        Гарантирует, что значения статусов загружены.
        """
        async with self._lock:
            if self._ids and not force_refresh:
                return

            # 1. Попытка использовать данные из окружения
            env_mapping = {key: value for key, value in _ENV_STATUS_IDS.items() if value is not None}
            if env_mapping and not force_refresh:
                logger.info("Using Planfix status IDs from environment variables")
                self._ids = env_mapping
                return

            # 2. Попробуем загрузить из локальной базы
            records = await self._load_from_db()
            mapping = self._build_mapping(records)

            missing_required = self._missing_required(mapping)
            if missing_required:
                logger.info(
                    "Not all required Planfix statuses found in DB (%s). "
                    "Fetching from Planfix API...",
                    ", ".join(key.value for key in missing_required),
                )
                api_records = await self._load_from_api()
                mapping = self._build_mapping(api_records)
                missing_required = self._missing_required(mapping)

                if missing_required:
                    raise RuntimeError(
                        "Failed to resolve required Planfix statuses from API: "
                        + ", ".join(key.value for key in missing_required)
                    )

            self._ids = mapping
            optional_missing = {
                key for key in StatusKey if key not in self._required_statuses and not mapping.get(key)
            }
            if optional_missing:
                logger.warning(
                    "Optional Planfix statuses not resolved: %s",
                    ", ".join(sorted(key.value for key in optional_missing)),
                )

    def _missing_required(self, mapping: Dict[StatusKey, Optional[int]]) -> set[StatusKey]:
        return {key for key in self._required_statuses if not mapping.get(key)}

    async def _load_from_db(self) -> list[StatusRecord]:
        statuses: list[PlanfixTaskStatus] = await db_manager.get_all_task_statuses()
        records: list[StatusRecord] = []
        for item in statuses:
            try:
                records.append(
                    StatusRecord(
                        id=int(item.id),
                        name=str(item.name),
                        is_final=bool(item.is_final),
                        system_name=None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to deserialize PlanfixTaskStatus row %s: %s", item, exc)
        return records

    async def _load_from_api(self) -> list[StatusRecord]:
        try:
            response = await planfix_client.get_process_task_statuses(
                PLANFIX_TASK_PROCESS_ID, fields="id,name,isFinal,systemName"
            )
        except Exception as exc:
            logger.error("Failed to fetch Planfix statuses from API: %s", exc, exc_info=True)
            raise

        if not response or response.get("result") != "success":
            raise RuntimeError("Planfix API returned unexpected response while fetching statuses")

        statuses = response.get("statuses", [])
        records: list[StatusRecord] = []
        for raw in statuses:
            sid_raw = raw.get("id")
            try:
                if isinstance(sid_raw, str) and ":" in sid_raw:
                    sid_raw = sid_raw.split(":")[-1]
                sid = int(sid_raw)
            except Exception:
                logger.warning("Skipping Planfix status with invalid ID: %s", raw)
                continue
            record = StatusRecord(
                id=sid,
                name=str(raw.get("name", f"Status {sid}")),
                is_final=bool(raw.get("isFinal", False)),
                system_name=raw.get("systemName"),
            )
            records.append(record)

        await self._persist_records(records)
        return records

    async def _persist_records(self, records: Iterable[StatusRecord]) -> None:
        tasks = []
        for record in records:
            tasks.append(
                db_manager.create_or_update_task_status(
                    status_id=record.id,
                    name=record.name,
                    is_final=record.is_final,
                )
            )
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=False)

    def _build_mapping(self, records: Iterable[StatusRecord]) -> Dict[StatusKey, Optional[int]]:
        name_lookup: Dict[str, int] = {}
        system_lookup: Dict[str, int] = {}

        for record in records:
            name_key = _normalize_name(record.name)
            if name_key and name_key not in name_lookup:
                name_lookup[name_key] = record.id

            sys_key = _normalize_system_name(record.system_name)
            if sys_key and sys_key not in system_lookup:
                system_lookup[sys_key] = record.id

        mapping: Dict[StatusKey, Optional[int]] = {}
        for key in StatusKey:
            # Сначала проверяем значения из окружения
            env_value = _ENV_STATUS_IDS.get(key)
            if env_value:
                mapping[key] = env_value
                continue

            spec_names = self._expected_names(key)
            spec_system_names = self._expected_system_names(key)

            resolved_id: Optional[int] = None

            # Пытаемся найти по системным именам
            for sys_name in spec_system_names:
                norm = _normalize_system_name(sys_name)
                if norm and norm in system_lookup:
                    resolved_id = system_lookup[norm]
                    break

            # Если не нашли по системному имени — ищем по человекочитаемому
            if resolved_id is None:
                for alias in spec_names:
                    norm = _normalize_name(alias)
                    if norm and norm in name_lookup:
                        resolved_id = name_lookup[norm]
                        break

            mapping[key] = resolved_id

        return mapping

    def _expected_names(self, key: StatusKey) -> set[str]:
        configured_name = PLANFIX_STATUS_NAMES.get(key.value)
        fallbacks: set[str] = set()

        if configured_name:
            fallbacks.add(configured_name)

        default_aliases: Dict[StatusKey, tuple[str, ...]] = {
            StatusKey.NEW: ("Новая", "New"),
            StatusKey.DRAFT: ("Черновик", "Draft"),
            StatusKey.IN_PROGRESS: ("В работе", "In progress", "Выполняется"),
            StatusKey.INFO_SENT: ("Отправлена информация", "Info sent", "Информация отправлена"),
            StatusKey.REPLY_RECEIVED: ("Получен ответ", "Reply received"),
            StatusKey.TIMEOUT: ("Истек срок ответа", "Timeout"),
            StatusKey.COMPLETED: ("Выполненная", "Завершена", "Completed", "Done"),
            StatusKey.POSTPONED: ("Отложенная", "Postponed"),
            StatusKey.FINISHED: ("Завершенная", "Finished"),
            StatusKey.CANCELLED: ("Отменена", "Cancelled", "Canceled"),
            StatusKey.REJECTED: ("Отклонена", "Rejected"),
        }

        fallbacks.update(default_aliases.get(key, ()))
        return fallbacks

    def _expected_system_names(self, key: StatusKey) -> set[str]:
        mapping: Dict[StatusKey, tuple[str, ...]] = {
            StatusKey.NEW: ("NEW",),
            StatusKey.IN_PROGRESS: ("INPROGRESS", "IN_PROGRESS"),
            StatusKey.COMPLETED: ("COMPLETED", "DONE", "FINISHED"),
            StatusKey.CANCELLED: ("CANCELLED", "CANCELED"),
            StatusKey.REJECTED: ("REJECTED",),
            StatusKey.POSTPONED: ("POSTPONED", "PAUSED"),
            StatusKey.INFO_SENT: ("INFO_SENT", "WAITINGINFO", "WAITING_INFORMATION"),
        }
        return set(mapping.get(key, ()))

    def get_id(self, key: StatusKey, *, required: bool = True) -> Optional[int]:
        if not self._ids:
            raise RuntimeError(
                "Planfix status registry is not initialized. Call ensure_loaded() before accessing IDs."
            )

        value = self._ids.get(key)
        if value is None and required:
            raise KeyError(f"Planfix status '{key.value}' is not available")
        return value

    def get_mapping(self) -> Dict[StatusKey, Optional[int]]:
        if not self._ids:
            raise RuntimeError(
                "Planfix status registry is not initialized. Call ensure_loaded() before accessing IDs."
            )
        return dict(self._ids)


status_registry = PlanfixStatusRegistry()


async def ensure_status_registry_loaded(force_refresh: bool = False) -> None:
    await status_registry.ensure_loaded(force_refresh=force_refresh)


def get_status_id(key: StatusKey, *, required: bool = True) -> Optional[int]:
    return status_registry.get_id(key, required=required)


def get_status_mapping() -> Dict[StatusKey, Optional[int]]:
    return status_registry.get_mapping()


def resolve_status_id(key: StatusKey, *, required: bool = True) -> Optional[int]:
    return get_status_id(key, required=required)


def require_status_id(key: StatusKey) -> int:
    value = get_status_id(key, required=True)
    if value is None:
        raise RuntimeError(f"Planfix status '{key.value}' is not configured")
    return value


def collect_status_ids(keys: Iterable[StatusKey], *, required: bool = False) -> list[int]:
    result: list[int] = []
    for key in keys:
        value = get_status_id(key, required=required)
        if value is not None:
            result.append(value)
    return result


def status_labels(pairs: Iterable[tuple[StatusKey, str]]) -> dict[int, str]:
    labels: dict[int, str] = {}
    for key, label in pairs:
        value = get_status_id(key, required=False)
        if value is not None:
            labels[value] = label
    return labels


def is_status(value: Optional[int], key: StatusKey) -> bool:
    resolved = get_status_id(key, required=False)
    return resolved is not None and value == resolved


def status_in(value: Optional[int], keys: Iterable[StatusKey]) -> bool:
    return any(is_status(value, key) for key in keys)


