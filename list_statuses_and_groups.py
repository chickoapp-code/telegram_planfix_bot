#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилита для получения списка статусов задач и групп контактов из Planfix.

Помогает найти:
- ID статусов задач для настройки PLANFIX_STATUS_ID_* в .env
- ID групп контактов для настройки SUPPORT_CONTACT_GROUP_ID и других параметров

Использование:
    python list_statuses_and_groups.py
"""

import asyncio
import sys
from planfix_client import planfix_client
from config import PLANFIX_TASK_PROCESS_ID


async def list_task_statuses():
    """Получает и выводит список статусов задач для процесса."""
    try:
        if not PLANFIX_TASK_PROCESS_ID:
            print("⚠️  PLANFIX_TASK_PROCESS_ID не задан в .env файле.")
            print("   Пропускаем получение статусов задач.\n")
            return
        
        print("🔍 Получение списка статусов задач из Planfix...")
        print(f"   Процесс ID: {PLANFIX_TASK_PROCESS_ID}\n")
        
        response = await planfix_client.get_process_task_statuses(
            PLANFIX_TASK_PROCESS_ID,
            fields="id,name,isFinal,systemName"
        )
        
        if response.get("result") != "success":
            print(f"❌ Ошибка при получении статусов: {response}")
            return
        
        statuses = response.get("statuses", [])
        
        if not statuses:
            print("⚠️  Статусы не найдены.")
            return
        
        print(f"✅ Найдено статусов: {len(statuses)}\n")
        print("=" * 100)
        print(f"{'ID':<15} {'Название':<40} {'Системное имя':<25} {'Финальный':<10}")
        print("=" * 100)
        
        for status in statuses:
            status_id = status.get("id", "N/A")
            status_name = status.get("name", "Без названия")
            system_name = status.get("systemName", "")
            is_final = "Да" if status.get("isFinal", False) else "Нет"
            
            # Форматируем ID (может быть строкой вида "status:3")
            if isinstance(status_id, str) and ":" in status_id:
                status_id_display = status_id
            else:
                status_id_display = str(status_id)
            
            print(f"{status_id_display:<15} {status_name:<40} {system_name:<25} {is_final:<10}")
        
        print("=" * 100)
        print("\n💡 Чтобы использовать статус, скопируйте его ID в .env файл:")
        print("   PLANFIX_STATUS_ID_NEW=<ID_статуса>")
        print("   PLANFIX_STATUS_ID_IN_PROGRESS=<ID_статуса>")
        print("   PLANFIX_STATUS_ID_COMPLETED=<ID_статуса>")
        print("   и т.д.")
        print("\n📝 Примечание: Если ID имеет формат 'status:3', используйте только число (3)")
        print("   или оставьте поле пустым - бот автоматически определит статусы по именам.")
        
    except Exception as e:
        print(f"❌ Ошибка при получении статусов: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()


async def list_contact_groups():
    """Получает и выводит список групп контактов."""
    try:
        print("\n" + "=" * 100)
        print("🔍 Получение списка групп контактов из Planfix...\n")
        
        response = await planfix_client.get_contact_groups(fields="id,name")
        
        if response.get("result") != "success":
            print(f"❌ Ошибка при получении групп контактов: {response}")
            return
        
        groups = response.get("groups", [])
        
        if not groups:
            print("⚠️  Группы контактов не найдены.")
            return
        
        print(f"✅ Найдено групп контактов: {len(groups)}\n")
        print("=" * 100)
        print(f"{'ID':<10} {'Название':<60}")
        print("=" * 100)
        
        for group in groups:
            group_id = group.get("id", "N/A")
            group_name = group.get("name", "Без названия")
            
            print(f"{group_id:<10} {group_name:<60}")
        
        print("=" * 100)
        print("\n💡 Чтобы использовать группу контактов, скопируйте её ID в .env файл:")
        print("   SUPPORT_CONTACT_GROUP_ID=<ID_группы>")
        print("\n📝 Примечание: По умолчанию используется группа с ID=32 (Поддержка)")
        
    except Exception as e:
        print(f"❌ Ошибка при получении групп контактов: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()


async def main():
    """Основная функция."""
    try:
        # Получаем статусы задач
        await list_task_statuses()
        
        # Получаем группы контактов
        await list_contact_groups()
        
        print("\n" + "=" * 100)
        print("✅ Завершено!")
        print("=" * 100)
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        await planfix_client.close()


if __name__ == "__main__":
    asyncio.run(main())

