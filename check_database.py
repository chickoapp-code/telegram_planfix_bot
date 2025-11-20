#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для проверки содержимого базы данных.
Показывает всех пользователей и исполнителей в БД.
"""

import sys
from pathlib import Path
from database import SessionLocal, UserProfile, ExecutorProfile
from config import DB_PATH

def check_database():
    """Проверяет содержимое базы данных."""
    db_file = Path(DB_PATH)
    
    print("=" * 80)
    print("📊 ПРОВЕРКА БАЗЫ ДАННЫХ")
    print("=" * 80)
    print(f"Файл БД: {db_file.absolute()}")
    print(f"Существует: {'✅ Да' if db_file.exists() else '❌ Нет'}")
    print()
    
    if not db_file.exists():
        print("⚠️  Файл БД не найден!")
        return
    
    # Размер файла
    size = db_file.stat().st_size
    print(f"Размер файла: {size:,} байт ({size / 1024:.2f} KB)")
    print()
    
    try:
        from sqlalchemy import inspect
        from database import engine
        
        # Проверяем таблицы
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f"📋 Таблицы в БД: {len(tables)}")
        for table in tables:
            print(f"  - {table}")
        print()
        
        # Подключаемся к БД
        db = SessionLocal()
        
        try:
            # Пользователи
            users = db.query(UserProfile).all()
            print(f"👤 ПОЛЬЗОВАТЕЛИ: {len(users)}")
            if users:
                for user in users:
                    print(f"  - ID: {user.telegram_id}, Имя: {user.full_name}, Ресторан: {user.restaurant_contact_id}")
            else:
                print("  (нет пользователей)")
            print()
            
            # Исполнители
            executors = db.query(ExecutorProfile).all()
            print(f"👷 ИСПОЛНИТЕЛИ: {len(executors)}")
            if executors:
                for executor in executors:
                    status = executor.profile_status
                    status_repr = repr(status)  # Показываем точное значение со всеми символами
                    print(f"  - ID: {executor.telegram_id}")
                    print(f"    Имя: {executor.full_name}")
                    print(f"    Статус: {status_repr} (длина: {len(status) if status else 0})")
                    print(f"    Статус (hex): {status.encode('utf-8').hex() if status else 'None'}")
                    print(f"    Активен? {status == 'активен'}")
                    print()
            else:
                print("  (нет исполнителей)")
            print()
            
        finally:
            db.close()
            
    except Exception as e:
        print(f"❌ Ошибка при проверке БД: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    check_database()

