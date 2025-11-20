from sqlalchemy.orm import Session
import contextlib
from database import SessionLocal, UserProfile, ExecutorProfile, PlanfixDirectory, PlanfixDirectoryEntry, PlanfixTaskStatus, PlanfixTaskTemplate, BotLog
import datetime
from typing import List, Dict, Optional

class DBManager:
    def __init__(self):
        self.db_session = SessionLocal

    @contextlib.contextmanager
    def get_db(self):
        db = self.db_session()
        try:
            yield db
            # Коммит выполняется в методах явно, но если что-то пошло не так - откатываем
        except Exception:
            db.rollback()  # Откатываем изменения при ошибке
            raise
        finally:
            db.close()

    # --- UserProfile operations ---
    def create_user_profile(self, db: Session, telegram_id: int, full_name: str, phone_number: str,
                            franchise_group_id: int, restaurant_contact_id: int,
                            email: Optional[str] = None, position: Optional[str] = None,
                            restaurant_directory_key: Optional[str] = None,
                            planfix_contact_id: Optional[str] = None) -> UserProfile:
        user = UserProfile(
            telegram_id=telegram_id,
            full_name=full_name,
            phone_number=phone_number,
            email=email,
            position=position,
            franchise_group_id=franchise_group_id,
            restaurant_contact_id=restaurant_contact_id,
            restaurant_directory_key=restaurant_directory_key,
            planfix_contact_id=planfix_contact_id
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        return user

    def get_user_profile(self, db: Session, telegram_id: int) -> Optional[UserProfile]:
        return db.query(UserProfile).filter(UserProfile.telegram_id == telegram_id).first()

    def update_user_profile(self, db: Session, telegram_id: int, **kwargs) -> Optional[UserProfile]:
        user = self.get_user_profile(db, telegram_id)
        if user:
            for key, value in kwargs.items():
                setattr(user, key, value)
            db.commit()
            db.refresh(user)
        return user

    def delete_user_profile(self, db: Session, telegram_id: int):
        user = self.get_user_profile(db, telegram_id)
        if user:
            db.delete(user)
            db.commit()

    # --- ExecutorProfile operations ---
    def create_executor_profile(self, db: Session, telegram_id: int, full_name: str, phone_number: str,
                                serving_franchise_groups: List[int], email: Optional[str] = None,
                                position_role: Optional[str] = None, geography: Optional[str] = None,
                                experience_notes: Optional[str] = None, planfix_user_id: Optional[str] = None,
                                profile_status: str = "ожидает подтверждения",
                                serving_restaurants: Optional[List[Dict]] = None,
                                service_direction: Optional[str] = None) -> ExecutorProfile:
        executor = ExecutorProfile(
            telegram_id=telegram_id,
            full_name=full_name,
            phone_number=phone_number,
            email=email,
            position_role=position_role,
            serving_franchise_groups=serving_franchise_groups,
            geography=geography,
            experience_notes=experience_notes,
            planfix_user_id=planfix_user_id,
            profile_status=profile_status,
            serving_restaurants=serving_restaurants or [],
            service_direction=service_direction
        )
        db.add(executor)
        db.commit()
        db.refresh(executor)
        return executor

    def get_executor_profile(self, db: Session, telegram_id: int) -> Optional[ExecutorProfile]:
        return db.query(ExecutorProfile).filter(ExecutorProfile.telegram_id == telegram_id).first()

    def update_executor_profile(self, db: Session, telegram_id: int, **kwargs) -> Optional[ExecutorProfile]:
        executor = self.get_executor_profile(db, telegram_id)
        if executor:
            for key, value in kwargs.items():
                setattr(executor, key, value)
            db.commit()
            db.refresh(executor)
        return executor

    def delete_executor_profile(self, db: Session, telegram_id: int):
        executor = self.get_executor_profile(db, telegram_id)
        if executor:
            db.delete(executor)
            db.commit()

    # --- PlanfixDirectory operations ---
    def create_or_update_directory(self, db: Session, directory_id: int, name: str, group: Optional[str] = None) -> PlanfixDirectory:
        directory = db.query(PlanfixDirectory).filter(PlanfixDirectory.id == directory_id).first()
        if directory:
            directory.name = name
            directory.group = group
        else:
            directory = PlanfixDirectory(id=directory_id, name=name, group=group)
            db.add(directory)
        db.commit()
        db.refresh(directory)
        return directory

    def get_directory(self, db: Session, directory_id: int) -> Optional[PlanfixDirectory]:
        return db.query(PlanfixDirectory).filter(PlanfixDirectory.id == directory_id).first()

    def get_all_directories(self, db: Session) -> List[PlanfixDirectory]:
        return db.query(PlanfixDirectory).all()

    # --- PlanfixDirectoryEntry operations ---
    def create_or_update_directory_entry(self, db: Session, directory_id: int, key: str, name: str,
                                         parent_key: Optional[str] = None, custom_fields: Optional[Dict] = None) -> PlanfixDirectoryEntry:
        entry = db.query(PlanfixDirectoryEntry).filter(PlanfixDirectoryEntry.key == key, PlanfixDirectoryEntry.directory_id == directory_id).first()
        if entry:
            entry.name = name
            entry.parent_key = parent_key
            entry.custom_fields = custom_fields
        else:
            entry = PlanfixDirectoryEntry(directory_id=directory_id, key=key, name=name, parent_key=parent_key, custom_fields=custom_fields)
            db.add(entry)
        db.commit()
        db.refresh(entry)
        return entry

    def get_directory_entry_by_key(self, db: Session, directory_id: int, key: str) -> Optional[PlanfixDirectoryEntry]:
        return db.query(PlanfixDirectoryEntry).filter(PlanfixDirectoryEntry.directory_id == directory_id, PlanfixDirectoryEntry.key == key).first()

    def get_directory_entries_by_directory_id(self, db: Session, directory_id: int) -> List[PlanfixDirectoryEntry]:
        return db.query(PlanfixDirectoryEntry).filter(PlanfixDirectoryEntry.directory_id == directory_id).all()

    def get_directory_entries_by_parent_key(self, db: Session, directory_id: int, parent_key: str) -> List[PlanfixDirectoryEntry]:
        return db.query(PlanfixDirectoryEntry).filter(PlanfixDirectoryEntry.directory_id == directory_id, PlanfixDirectoryEntry.parent_key == parent_key).all()

    def get_directory_entries_with_parent_null(self, db: Session, directory_id: int) -> List[PlanfixDirectoryEntry]:
        """Возвращает записи справочника верхнего уровня (parent_key IS NULL)."""
        return db.query(PlanfixDirectoryEntry).filter(PlanfixDirectoryEntry.directory_id == directory_id, PlanfixDirectoryEntry.parent_key.is_(None)).all()

    def get_directory_entries_by_keys(self, db: Session, directory_id: int, keys: List[str]) -> List[PlanfixDirectoryEntry]:
        if not keys:
            return []
        return db.query(PlanfixDirectoryEntry).filter(
            PlanfixDirectoryEntry.directory_id == directory_id,
            PlanfixDirectoryEntry.key.in_(keys)
        ).all()

    # --- PlanfixTaskStatus operations ---
    def create_or_update_task_status(self, db: Session, status_id: int, name: str, is_final: bool = False) -> PlanfixTaskStatus:
        status = db.query(PlanfixTaskStatus).filter(PlanfixTaskStatus.id == status_id).first()
        if status:
            status.name = name
            status.is_final = is_final
        else:
            status = PlanfixTaskStatus(id=status_id, name=name, is_final=is_final)
            db.add(status)
        db.commit()
        db.refresh(status)
        return status

    def get_task_status(self, db: Session, status_id: int) -> Optional[PlanfixTaskStatus]:
        return db.query(PlanfixTaskStatus).filter(PlanfixTaskStatus.id == status_id).first()

    def get_all_task_statuses(self, db: Session) -> List[PlanfixTaskStatus]:
        return db.query(PlanfixTaskStatus).all()

    # --- PlanfixTaskTemplate operations ---
    def create_or_update_task_template(self, db: Session, template_id: int, name: str,
                                       description: Optional[str] = None, project_id: Optional[int] = None) -> PlanfixTaskTemplate:
        template = db.query(PlanfixTaskTemplate).filter(PlanfixTaskTemplate.id == template_id).first()
        if template:
            template.name = name
            template.description = description
            template.project_id = project_id
        else:
            template = PlanfixTaskTemplate(id=template_id, name=name, description=description, project_id=project_id)
            db.add(template)
        db.commit()
        db.refresh(template)
        return template

    def get_task_template(self, db: Session, template_id: int) -> Optional[PlanfixTaskTemplate]:
        return db.query(PlanfixTaskTemplate).filter(PlanfixTaskTemplate.id == template_id).first()

    def get_all_task_templates(self, db: Session) -> List[PlanfixTaskTemplate]:
        return db.query(PlanfixTaskTemplate).all()

    # --- BotLog operations ---
    def get_bot_logs_by_telegram_id(self, db: Session, telegram_id: int, action: Optional[str] = None, limit: Optional[int] = None) -> List[BotLog]:
        """Возвращает логи бота для указанного telegram_id, опционально фильтруя по action."""
        query = db.query(BotLog).filter(BotLog.telegram_id == telegram_id)
        if action:
            query = query.filter(BotLog.action == action)
        query = query.order_by(BotLog.id.desc())
        if limit:
            query = query.limit(limit)
        return query.all()

    def create_bot_log(self, db: Session, telegram_id: Optional[int], action: str,
                       details: Optional[Dict] = None, success: bool = True,
                       error_message: Optional[str] = None) -> BotLog:
        """Создает запись лога бота."""
        log = BotLog(
            telegram_id=telegram_id,
            action=action,
            details=details,
            success=success,
            error_message=error_message
        )
        db.add(log)
        db.commit()
        db.refresh(log)
        return log