#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилита для получения списка справочников из Planfix.
Помогает найти ID справочника ресторанов для настройки DIRECTORY_RESTAURANTS_ID.
"""

import asyncio
import json
import sys
from planfix_client import planfix_client


async def list_directories():
    """Получает и выводит список всех справочников Planfix."""
    try:
        print("🔍 Получение списка справочников из Planfix...\n")
        
        response = await planfix_client.get_directories(fields="id,name,group")
        
        if response.get("result") != "success":
            print(f"❌ Ошибка при получении справочников: {response}")
            return
        
        directories = response.get("directories", [])
        
        if not directories:
            print("⚠️  Справочники не найдены.")
            return
        
        print(f"✅ Найдено справочников: {len(directories)}\n")
        print("=" * 80)
        print(f"{'ID':<10} {'Название':<50} {'Группа':<20}")
        print("=" * 80)
        
        for directory in directories:
            dir_id = directory.get("id", "N/A")
            dir_name = directory.get("name", "Без названия")
            dir_group = directory.get("group", {})
            group_name = dir_group.get("name", "") if isinstance(dir_group, dict) else str(dir_group)
            
            print(f"{dir_id:<10} {dir_name:<50} {group_name:<20}")
        
        print("=" * 80)
        print("\n💡 Чтобы использовать справочник, скопируйте его ID в .env файл:")
        print("   DIRECTORY_RESTAURANTS_ID=<ID_справочника>")
        print("\n📝 Примечание: Если DIRECTORY_RESTAURANTS_ID не указан, бот будет")
        print("   использовать ID контакта ресторана как ключ справочника.")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        await planfix_client.close()


if __name__ == "__main__":
    asyncio.run(list_directories())

