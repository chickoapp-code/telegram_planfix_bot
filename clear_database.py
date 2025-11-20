#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для полной очистки базы данных.
ВНИМАНИЕ: Удаляет все данные из БД, включая всех пользователей, исполнителей и другую информацию!

Использование:
    python clear_database.py

Для безопасности скрипт требует подтверждения перед удалением данных.
"""

import sys
from pathlib import Path
from database import engine, Base, init_db, drop_all_tables
from config import DB_PATH
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clear_database():
    """Полностью очищает базу данных."""
    db_file = Path(DB_PATH)
    
    print("=" * 80)
    print("⚠️  ВНИМАНИЕ: ОПАСНАЯ ОПЕРАЦИЯ!")
    print("=" * 80)
    print(f"Будет удалена база данных: {db_file.absolute()}")
    print("\nЭто удалит:")
    print("  - Всех пользователей (user_profiles)")
    print("  - Всех исполнителей (executor_profiles)")
    print("  - Все назначения задач (task_assignments)")
    print("  - Все справочники Planfix (planfix_directories, planfix_directory_entries)")
    print("  - Все статусы задач (planfix_task_statuses)")
    print("  - Все шаблоны задач (planfix_task_templates)")
    print("  - Все логи бота (bot_logs)")
    print("=" * 80)
    
    # Запрашиваем подтверждение
    response = input("\nВы уверены? Введите 'YES' для подтверждения: ")
    if response != "YES":
        print("❌ Операция отменена.")
        return False
    
    try:
        # Проверяем, существует ли файл БД
        if db_file.exists():
            print(f"\n📁 Найден файл БД: {db_file.absolute()}")
            
            # Удаляем все таблицы
            print("🗑️  Удаление всех таблиц...")
            drop_all_tables()
            
            # Пересоздаем структуру БД
            print("🔨 Пересоздание структуры БД...")
            init_db()
            
            print("\n✅ База данных успешно очищена и пересоздана!")
            print(f"📁 Файл БД: {db_file.absolute()}")
        else:
            print(f"\n⚠️  Файл БД не найден: {db_file.absolute()}")
            print("🔨 Создание новой БД...")
            init_db()
            print("✅ База данных создана!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке БД: {e}", exc_info=True)
        print(f"\n❌ Ошибка: {e}")
        return False


def clear_database_force():
    """Принудительная очистка БД без подтверждения (для автоматизации)."""
    db_file = Path(DB_PATH)
    
    try:
        if db_file.exists():
            drop_all_tables()
        init_db()
        logger.info("✅ База данных успешно очищена")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке БД: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1 and sys.argv[1] == "--force":
        # Принудительная очистка без подтверждения
        success = clear_database_force()
        sys.exit(0 if success else 1)
    else:
        # Интерактивная очистка с подтверждением
        success = clear_database()
        sys.exit(0 if success else 1)

