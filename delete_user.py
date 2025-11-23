#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для удаления пользователя из бота.

Использование:
    python delete_user.py <telegram_id>
    
Пример:
    python delete_user.py 123456789

Скрипт удаляет:
    - Профиль пользователя (UserProfile) или исполнителя (ExecutorProfile)
    - Связанные назначения задач (TaskAssignment) для исполнителей
    - Логи бота (BotLog) для данного пользователя
"""

import sys
from pathlib import Path
from database import SessionLocal, UserProfile, ExecutorProfile, TaskAssignment, BotLog
from db_manager import DBManager
from config import DB_PATH
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def format_user_info(user: UserProfile) -> str:
    """Форматирует информацию о пользователе для вывода."""
    info = [
        f"  👤 ФИО: {user.full_name}",
        f"  📱 Телефон: {user.phone_number}",
        f"  📧 Email: {user.email or 'не указан'}",
        f"  💼 Должность: {user.position or 'не указана'}",
        f"  🏢 ID группы франчайзи: {user.franchise_group_id}",
        f"  🏪 ID ресторана: {user.restaurant_contact_id}",
        f"  🔗 Planfix Contact ID: {user.planfix_contact_id or 'не указан'}",
        f"  📅 Дата регистрации: {user.registration_date.strftime('%Y-%m-%d %H:%M:%S') if user.registration_date else 'не указана'}",
        f"  ✅ Активен: {'Да' if user.is_active else 'Нет'}",
    ]
    return "\n".join(info)


def format_executor_info(executor: ExecutorProfile) -> str:
    """Форматирует информацию об исполнителе для вывода."""
    info = [
        f"  👤 ФИО: {executor.full_name}",
        f"  📱 Телефон: {executor.phone_number}",
        f"  📧 Email: {executor.email or 'не указан'}",
        f"  💼 Должность: {executor.position_role or 'не указана'}",
        f"  🏢 Группы франчайзи: {executor.serving_franchise_groups}",
        f"  🏪 Рестораны: {len(executor.serving_restaurants) if executor.serving_restaurants else 0}",
        f"  🧭 Направление: {executor.service_direction or 'не указано'}",
        f"  🔗 Planfix User ID: {executor.planfix_user_id or 'не указан'}",
        f"  🔗 Planfix Contact ID: {executor.planfix_contact_id or 'не указан'}",
        f"  📋 Статус профиля: {executor.profile_status}",
        f"  📅 Дата регистрации: {executor.registration_date.strftime('%Y-%m-%d %H:%M:%S') if executor.registration_date else 'не указана'}",
    ]
    return "\n".join(info)


def delete_user(telegram_id: int, force: bool = False) -> bool:
    """Удаляет пользователя из базы данных."""
    db_file = Path(DB_PATH)
    
    if not db_file.exists():
        print(f"❌ Файл БД не найден: {db_file.absolute()}")
        return False
    
    db = SessionLocal()
    db_manager = DBManager()
    
    try:
        # Ищем пользователя
        user = db_manager.get_user_profile(db, telegram_id)
        executor = db_manager.get_executor_profile(db, telegram_id)
        
        if not user and not executor:
            print(f"❌ Пользователь с Telegram ID {telegram_id} не найден в базе данных.")
            return False
        
        # Определяем тип профиля
        profile_type = None
        profile_info = None
        
        if user:
            profile_type = "user"
            profile_info = format_user_info(user)
        elif executor:
            profile_type = "executor"
            profile_info = format_executor_info(executor)
        
        # Показываем информацию о пользователе
        print("=" * 80)
        print(f"📋 ИНФОРМАЦИЯ О ПОЛЬЗОВАТЕЛЕ")
        print("=" * 80)
        print(f"Telegram ID: {telegram_id}")
        print(f"Тип профиля: {'👤 Пользователь (UserProfile)' if profile_type == 'user' else '👷 Исполнитель (ExecutorProfile)'}")
        print()
        print(profile_info)
        print()
        
        # Проверяем связанные данные
        related_data = []
        
        if executor:
            # Назначения задач для исполнителя
            assignments = db.query(TaskAssignment).filter(
                TaskAssignment.executor_telegram_id == telegram_id
            ).all()
            if assignments:
                related_data.append(f"  - Назначения задач: {len(assignments)} шт.")
        
        # Логи бота
        logs = db.query(BotLog).filter(BotLog.telegram_id == telegram_id).all()
        if logs:
            related_data.append(f"  - Записи в логах: {len(logs)} шт.")
        
        if related_data:
            print("📊 Связанные данные, которые будут удалены:")
            for item in related_data:
                print(item)
            print()
        
        # Запрашиваем подтверждение
        if not force:
            print("=" * 80)
            print("⚠️  ВНИМАНИЕ: Это действие нельзя отменить!")
            print("=" * 80)
            response = input("\nВы уверены, что хотите удалить этого пользователя? Введите 'YES' для подтверждения: ")
            if response != "YES":
                print("❌ Операция отменена.")
                return False
        
        # Удаляем связанные данные
        if executor:
            # Удаляем назначения задач
            assignments = db.query(TaskAssignment).filter(
                TaskAssignment.executor_telegram_id == telegram_id
            ).all()
            for assignment in assignments:
                db.delete(assignment)
                deleted_count += 1
            if assignments:
                db.commit()
                print(f"✅ Удалено назначений задач: {len(assignments)}")
        
        # Удаляем логи (опционально, можно закомментировать, если нужно сохранить историю)
        logs = db.query(BotLog).filter(BotLog.telegram_id == telegram_id).all()
        for log in logs:
            db.delete(log)
        if logs:
            db.commit()
            print(f"✅ Удалено записей в логах: {len(logs)}")
        
        # Удаляем профиль
        if user:
            db_manager.delete_user_profile(db, telegram_id)
            print(f"✅ Профиль пользователя удален")
        elif executor:
            db_manager.delete_executor_profile(db, telegram_id)
            print(f"✅ Профиль исполнителя удален")
        
        print()
        print("=" * 80)
        print("✅ ПОЛЬЗОВАТЕЛЬ УСПЕШНО УДАЛЕН")
        print("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при удалении пользователя: {e}", exc_info=True)
        print(f"\n❌ Ошибка: {e}")
        db.rollback()
        return False
    finally:
        db.close()


def list_all_users():
    """Показывает список всех пользователей для справки."""
    db_file = Path(DB_PATH)
    
    if not db_file.exists():
        print(f"❌ Файл БД не найден: {db_file.absolute()}")
        return
    
    db = SessionLocal()
    db_manager = DBManager()
    
    try:
        print("=" * 80)
        print("📋 СПИСОК ВСЕХ ПОЛЬЗОВАТЕЛЕЙ")
        print("=" * 80)
        
        # Пользователи
        users = db.query(UserProfile).all()
        print(f"\n👤 ПОЛЬЗОВАТЕЛИ ({len(users)}):")
        if users:
            for user in users:
                print(f"  - ID: {user.telegram_id}, Имя: {user.full_name}, Ресторан: {user.restaurant_contact_id}")
        else:
            print("  (нет пользователей)")
        
        # Исполнители
        executors = db.query(ExecutorProfile).all()
        print(f"\n👷 ИСПОЛНИТЕЛИ ({len(executors)}):")
        if executors:
            for executor in executors:
                print(f"  - ID: {executor.telegram_id}, Имя: {executor.full_name}, Статус: {executor.profile_status}")
        else:
            print("  (нет исполнителей)")
        
        print()
        
    except Exception as e:
        logger.error(f"❌ Ошибка при получении списка пользователей: {e}", exc_info=True)
        print(f"\n❌ Ошибка: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python delete_user.py <telegram_id>  - удалить пользователя")
        print("  python delete_user.py --list         - показать список всех пользователей")
        print()
        print("Пример:")
        print("  python delete_user.py 123456789")
        print("  python delete_user.py 123456789 --force  # без подтверждения")
        sys.exit(1)
    
    if sys.argv[1] == "--list":
        list_all_users()
        sys.exit(0)
    
    try:
        telegram_id = int(sys.argv[1])
    except ValueError:
        print(f"❌ Неверный формат Telegram ID: {sys.argv[1]}")
        print("Telegram ID должен быть числом.")
        sys.exit(1)
    
    force = "--force" in sys.argv or "-f" in sys.argv
    
    success = delete_user(telegram_id, force=force)
    sys.exit(0 if success else 1)

