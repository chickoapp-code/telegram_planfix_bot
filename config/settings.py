from __future__ import annotations

from pathlib import Path
from typing import List, Sequence

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    bot_token: str = Field(alias="BOT_TOKEN")

    planfix_base_url: str = Field(alias="PLANFIX_BASE_URL")
    planfix_api_key: str = Field(alias="PLANFIX_API_KEY")
    planfix_api_secret: str = Field(alias="PLANFIX_API_SECRET")
    planfix_account: str = Field(alias="PLANFIX_ACCOUNT")
    planfix_api_source_id: int = Field(alias="PLANFIX_API_SOURCE_ID")

    planfix_task_process_id: int = Field(alias="PLANFIX_TASK_PROCESS_ID")
    planfix_max_concurrency: int = Field(default=3, alias="PLANFIX_MAX_CONCURRENCY")

    planfix_status_id_new: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_NEW")
    planfix_status_id_draft: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_DRAFT")
    planfix_status_id_in_progress: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_IN_PROGRESS")
    planfix_status_id_info_sent: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_INFO_SENT")
    planfix_status_id_reply_received: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_REPLY_RECEIVED")
    planfix_status_id_timeout: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_TIMEOUT")
    planfix_status_id_completed: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_COMPLETED")
    planfix_status_id_postponed: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_POSTPONED")
    planfix_status_id_finished: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_FINISHED")
    planfix_status_id_cancelled: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_CANCELLED")
    planfix_status_id_rejected: int | None = Field(default=None, alias="PLANFIX_STATUS_ID_REJECTED")

    planfix_status_name_new: str = Field(default="Новая", alias="PLANFIX_STATUS_NAME_NEW")
    planfix_status_name_draft: str | None = Field(default=None, alias="PLANFIX_STATUS_NAME_DRAFT")
    planfix_status_name_in_progress: str = Field(default="В работе", alias="PLANFIX_STATUS_NAME_IN_PROGRESS")
    planfix_status_name_info_sent: str = Field(default="Отправлена информация", alias="PLANFIX_STATUS_NAME_INFO_SENT")
    planfix_status_name_reply_received: str | None = Field(
        default="Получен ответ", alias="PLANFIX_STATUS_NAME_REPLY_RECEIVED"
    )
    planfix_status_name_timeout: str | None = Field(default=None, alias="PLANFIX_STATUS_NAME_TIMEOUT")
    planfix_status_name_completed: str = Field(default="Завершена", alias="PLANFIX_STATUS_NAME_COMPLETED")
    planfix_status_name_postponed: str | None = Field(default="Отложена", alias="PLANFIX_STATUS_NAME_POSTPONED")
    planfix_status_name_finished: str | None = Field(default="Завершена", alias="PLANFIX_STATUS_NAME_FINISHED")
    planfix_status_name_cancelled: str = Field(default="Отменена", alias="PLANFIX_STATUS_NAME_CANCELLED")
    planfix_status_name_rejected: str | None = Field(default="Отклонена", alias="PLANFIX_STATUS_NAME_REJECTED")
    planfix_status_name_paused: str = Field(default="На паузе", alias="PLANFIX_STATUS_NAME_PAUSED")
    planfix_status_name_waiting_info: str = Field(
        default="Ожидает информации", alias="PLANFIX_STATUS_NAME_WAITING_INFO"
    )

    custom_field_restaurant_id: int | None = Field(default=None, alias="CUSTOM_FIELD_RESTAURANT_ID")
    custom_field_contact_id: int | None = Field(default=None, alias="CUSTOM_FIELD_CONTACT_ID")
    custom_field_phone_id: int | None = Field(default=None, alias="CUSTOM_FIELD_PHONE_ID")
    custom_field_type_id: int | None = Field(default=None, alias="CUSTOM_FIELD_TYPE_ID")
    custom_field_mobile_phone_id: int | None = Field(default=None, alias="CUSTOM_FIELD_MOBILE_PHONE_ID")

    directory_restaurants_id: int | None = Field(default=None, alias="DIRECTORY_RESTAURANTS_ID")

    support_contact_group_id: int | None = Field(default=32, alias="SUPPORT_CONTACT_GROUP_ID")
    support_contact_template_id: int = Field(default=1, alias="SUPPORT_CONTACT_TEMPLATE_ID")

    database_path: str = Field(default="bot.db", alias="DB_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    planfix_poll_interval: int = Field(default=60, alias="PLANFIX_POLL_INTERVAL")

    # Webhook server configuration
    webhook_host: str = Field(default="127.0.0.1", alias="WEBHOOK_HOST")
    webhook_port: int = Field(default=8080, alias="WEBHOOK_PORT")

    telegram_admin_ids_raw: str | None = Field(default=None, alias="TELEGRAM_ADMIN_IDS", exclude=True)
    
    @property
    def telegram_admin_ids(self) -> List[int]:
        """Парсит строку с ID через запятую или список в List[int]."""
        value = self.telegram_admin_ids_raw
        if value is None:
            return []
        if isinstance(value, str):
            # Обрабатываем пустую строку
            value = value.strip()
            if not value or value.startswith("#"):
                return []
            # Пробуем распарсить как JSON (если это JSON массив)
            try:
                import json
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [int(v) for v in parsed]
            except (json.JSONDecodeError, ValueError, TypeError):
                pass
            # Если не JSON, парсим как строку с запятыми (например: "123456789,987654321")
            parts = [part.strip() for part in value.split(",")]
            return [int(part) for part in parts if part]
        return []

    @property
    def db_path(self) -> Path:
        return Path(self.database_path).expanduser().resolve()

    @field_validator(
        "planfix_status_id_new",
        "planfix_status_id_draft",
        "planfix_status_id_in_progress",
        "planfix_status_id_info_sent",
        "planfix_status_id_reply_received",
        "planfix_status_id_timeout",
        "planfix_status_id_completed",
        "planfix_status_id_postponed",
        "planfix_status_id_finished",
        "planfix_status_id_cancelled",
        "planfix_status_id_rejected",
        mode="before",
    )
    @classmethod
    def _empty_str_to_none(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            sanitized = value.strip()
            if not sanitized:
                return None
            if sanitized.startswith("#"):
                return None
            return sanitized
        return value

    @field_validator("directory_restaurants_id", mode="before")
    @classmethod
    def _parse_optional_directory(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            sanitized = value.strip()
            if not sanitized or sanitized.startswith("#"):
                return None
            return sanitized
        return value

    @field_validator(
        "custom_field_restaurant_id",
        "custom_field_contact_id",
        "custom_field_phone_id",
        "custom_field_type_id",
        "custom_field_mobile_phone_id",
        mode="before",
    )
    @classmethod
    def _parse_optional_custom_field(cls, value):
        """Обрабатывает пустые строки и комментарии как None для опциональных кастомных полей."""
        if value is None:
            return None
        if isinstance(value, str):
            sanitized = value.strip()
            if not sanitized or sanitized.startswith("#"):
                return None
            try:
                return int(sanitized)
            except ValueError:
                return None
        if isinstance(value, int):
            return value
        return None


settings = Settings()

