"""
Модели базы данных
Версия: 3.0 
"""

import datetime
import logging
from pathlib import Path

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, JSON, String, Text, create_engine, text
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from config import DB_PATH

Base = declarative_base()


class UserProfile(Base):
    """
    Профиль сотрудника ресторана (заявителя).
    
    Хранит информацию о пользователе и его привязку к франчайзи и ресторану.
    """
    __tablename__ = 'user_profiles'

    telegram_id = Column(Integer, primary_key=True, unique=True, nullable=False, index=True)
    full_name = Column(String(255), nullable=False)
    phone_number = Column(String(50), nullable=False)
    email = Column(String(255), nullable=True)
    position = Column(String(100), nullable=True)
    
    # Привязка к Planfix (новая архитектура)
    franchise_group_id = Column(Integer, nullable=False, index=True)  # ID группы контактов (12, 14, 16, 18, 20, 22)
    restaurant_contact_id = Column(Integer, nullable=False, index=True)  # ID контакта ресторана в Planfix
    restaurant_directory_key = Column(String(50), nullable=True)  # Ключ записи справочника для поля 16
    
    # ID контакта пользователя в Planfix
    planfix_contact_id = Column(String(50), nullable=True, index=True)
    
    # Метаданные
    registration_date = Column(DateTime, default=datetime.datetime.now, nullable=False)
    last_activity = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    is_active = Column(Boolean, default=True, nullable=False)

    def __repr__(self):
        return f"<UserProfile(telegram_id={self.telegram_id}, full_name='{self.full_name}', restaurant={self.restaurant_contact_id})>"


class ExecutorProfile(Base):
    """
    Профиль исполнителя (техник/ИТ-специалист).
    
    Хранит информацию о сотруднике техподдержки и его зоны ответственности.
    """
    __tablename__ = 'executor_profiles'

    telegram_id = Column(Integer, primary_key=True, unique=True, nullable=False, index=True)
    full_name = Column(String(255), nullable=False)
    phone_number = Column(String(50), nullable=False)
    email = Column(String(255), nullable=True)
    position_role = Column(String(100), nullable=True)
    
    # Зоны ответственности (список ID групп франчайзи)
    serving_franchise_groups = Column(JSON, nullable=False, default=list)  # [12, 14, 16, 18, 20, 22]
    serving_restaurants = Column(JSON, nullable=True, default=list)
    service_direction = Column(String(50), nullable=True)
    
    # Дополнительная информация
    geography = Column(String(255), nullable=True)
    experience_notes = Column(Text, nullable=True)
    
    # Привязка к Planfix
    planfix_user_id = Column(String(50), nullable=True, index=True)  # ID пользователя (сотрудника) в Planfix
    planfix_contact_id = Column(String(50), nullable=True, index=True)  # ID контакта исполнителя в Planfix
    registration_task_id = Column(Integer, nullable=True, index=True)  # ID задачи регистрации в Planfix
    
    # Статус профиля
    profile_status = Column(String(50), default="ожидает подтверждения", nullable=False, index=True)
    # Возможные значения: "ожидает подтверждения", "активен", "отклонен", "заблокирован"
    
    # Метаданные
    registration_date = Column(DateTime, default=datetime.datetime.now, nullable=False)
    confirmation_date = Column(DateTime, nullable=True)
    last_activity = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __repr__(self):
        return f"<ExecutorProfile(telegram_id={self.telegram_id}, full_name='{self.full_name}', status='{self.profile_status}')>"


class PlanfixDirectory(Base):
    """
    Справочник Planfix.
    
    Кеш справочников из Planfix для быстрого доступа.
    """
    __tablename__ = 'planfix_directories'

    id = Column(Integer, primary_key=True, nullable=False)  # Planfix directory ID
    name = Column(String(255), nullable=False, index=True)
    group = Column(String(255), nullable=True)
    last_updated = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    
    # Связь с записями справочника
    entries = relationship("PlanfixDirectoryEntry", back_populates="directory", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<PlanfixDirectory(id={self.id}, name='{self.name}')>"


class PlanfixDirectoryEntry(Base):
    """
    Запись справочника Planfix.
    
    Кеш записей справочников для быстрого доступа.
    """
    __tablename__ = 'planfix_directory_entries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    directory_id = Column(Integer, ForeignKey('planfix_directories.id', ondelete='CASCADE'), nullable=False, index=True)
    key = Column(String(100), nullable=False, index=True)  # Planfix entry key
    name = Column(String(500), nullable=False)
    parent_key = Column(String(100), nullable=True, index=True)  # Для иерархических справочников
    custom_fields = Column(JSON, nullable=True)  # Дополнительные поля записи
    
    # Связь со справочником
    directory = relationship("PlanfixDirectory", back_populates="entries")
    
    # Индекс для уникальности ключа в рамках справочника
    __table_args__ = (
        Index('idx_directory_key', 'directory_id', 'key', unique=True),
    )

    def __repr__(self):
        return f"<PlanfixDirectoryEntry(key='{self.key}', name='{self.name}', directory_id={self.directory_id})>"


class PlanfixTaskStatus(Base):
    """
    Статус задачи Planfix.
    
    Кеш статусов задач из процесса Planfix.
    """
    __tablename__ = 'planfix_task_statuses'

    id = Column(Integer, primary_key=True, nullable=False)  # Planfix status ID
    name = Column(String(100), nullable=False, index=True)
    is_final = Column(Boolean, default=False, nullable=False)  # Является ли статус завершающим
    last_updated = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __repr__(self):
        return f"<PlanfixTaskStatus(id={self.id}, name='{self.name}', is_final={self.is_final})>"


class PlanfixTaskTemplate(Base):
    """
    Шаблон задачи Planfix.
    
    Кеш шаблонов задач для быстрого доступа.
    """
    __tablename__ = 'planfix_task_templates'

    id = Column(Integer, primary_key=True, nullable=False)  # Planfix template ID
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    project_id = Column(Integer, nullable=True, index=True)
    last_updated = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __repr__(self):
        return f"<PlanfixTaskTemplate(id={self.id}, name='{self.name}', project_id={self.project_id})>"


class TaskAssignment(Base):
    """
    Назначение задачи исполнителю.
    
    Хранит информацию о том, какой исполнитель работает над какой задачей.
    """
    __tablename__ = 'task_assignments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, nullable=False, index=True)  # ID задачи в Planfix
    executor_telegram_id = Column(Integer, ForeignKey('executor_profiles.telegram_id'), nullable=False, index=True)
    planfix_user_id = Column(String(50), nullable=True)  # ID исполнителя в Planfix
    
    # Метаданные
    assigned_at = Column(DateTime, default=datetime.datetime.now, nullable=False)
    status = Column(String(50), default="active", nullable=False)  # active, completed, cancelled
    
    # Индекс для быстрого поиска активных назначений
    __table_args__ = (
        Index('idx_task_executor', 'task_id', 'executor_telegram_id'),
        Index('idx_active_assignments', 'task_id', 'status'),
    )

    def __repr__(self):
        return f"<TaskAssignment(task_id={self.task_id}, executor={self.executor_telegram_id}, status='{self.status}')>"


class BotLog(Base):
    """
    Лог действий бота.
    
    Хранит историю важных действий для аудита и отладки.
    """
    __tablename__ = 'bot_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.datetime.now, nullable=False, index=True)
    telegram_id = Column(Integer, nullable=True, index=True)
    action = Column(String(100), nullable=False, index=True)  # registration, create_task, comment, etc.
    details = Column(JSON, nullable=True)  # Дополнительная информация
    success = Column(Boolean, default=True, nullable=False)
    error_message = Column(Text, nullable=True)

    def __repr__(self):
        return f"<BotLog(id={self.id}, action='{self.action}', success={self.success})>"


# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

db_file = Path(DB_PATH)
if db_file.parent and not db_file.parent.exists():
    db_file.parent.mkdir(parents=True, exist_ok=True)

# Создание движка БД
engine = create_engine(
    f"sqlite:///{db_file}",
    echo=False,  # Установите True для отладки SQL запросов
    pool_pre_ping=True,  # Проверка соединения перед использованием
    connect_args={"check_same_thread": False}  # Для SQLite
)

# Создание фабрики сессий
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def _ensure_column(table_name: str, column_name: str, column_type: str):
    """Добавляет колонку в таблицу, если она отсутствует (для SQLite)."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"PRAGMA table_info('{table_name}')"))
            columns = {row[1] for row in result}
            if column_name not in columns:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))
                logger = logging.getLogger(__name__)
                logger.info(f"Added column '{column_name}' to table '{table_name}'")
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to ensure column {column_name} on {table_name}: {e}", exc_info=True)


def _ensure_executor_profile_columns():
    _ensure_column("executor_profiles", "serving_restaurants", "TEXT")
    _ensure_column("executor_profiles", "service_direction", "VARCHAR(50)")
    _ensure_column("executor_profiles", "planfix_contact_id", "VARCHAR(50)")


def init_db():
    """
    Инициализирует базу данных, создавая все таблицы.
    
    Вызывается при первом запуске бота.
    """
    Base.metadata.create_all(bind=engine)
    _ensure_executor_profile_columns()
    print("✅ Database initialized successfully")


def drop_all_tables():
    """
    ВНИМАНИЕ: Удаляет все таблицы из базы данных!
    
    Используйте только для разработки/тестирования.
    """
    Base.metadata.drop_all(bind=engine)
    print("⚠️ All tables dropped")


def get_db():
    """
    Генератор для получения сессии БД.
    
    Использование:
        with get_db() as db:
            # работа с БД
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session():
    """
    Получить сессию БД напрямую.
    
    ВАЖНО: Не забудьте закрыть сессию после использования!
    
    Использование:
        db = get_db_session()
        try:
            # работа с БД
        finally:
            db.close()
    """
    return SessionLocal()
