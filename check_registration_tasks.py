#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилита для проверки и обработки завершенных задач регистрации исполнителей.

Использование:
    python check_registration_tasks.py  # Проверить все незавершенные задачи
    python check_registration_tasks.py --approve-all  # Автоматически подтвердить все завершенные
"""

import asyncio
import sys
from database import SessionLocal, ExecutorProfile
from db_manager import DBManager
from planfix_client import planfix_client
from services.status_registry import StatusKey, status_in
from logging_config import setup_logging
import logging

setup_logging()
logger = logging.getLogger(__name__)


async def check_registration_tasks(approve_all: bool = False):
    """Проверяет все незавершенные задачи регистрации."""
    db_manager = DBManager()
    
    try:
        with db_manager.get_db() as db:
            executors = db.query(ExecutorProfile).filter(
                ExecutorProfile.registration_task_id.isnot(None),
                ExecutorProfile.profile_status == "ожидает подтверждения"
            ).all()
            
            if not executors:
                print("✅ Нет незавершенных задач регистрации.")
                return
            
            print(f"📋 Найдено {len(executors)} незавершенных задач регистрации:\n")
            print("=" * 100)
            print(f"{'ID задачи':<15} {'Telegram ID':<15} {'Имя':<30} {'Статус задачи':<30} {'Действие':<20}")
            print("=" * 100)
            
            to_approve = []
            to_reject = []
            
            for executor in executors:
                task_id = executor.registration_task_id
                if not task_id:
                    continue
                
                try:
                    # Получаем статус задачи из Planfix
                    task_response = await planfix_client.get_task_by_id(
                        task_id,
                        fields="id,status,name"
                    )
                    
                    if not task_response or task_response.get('result') != 'success':
                        print(f"{task_id:<15} {executor.telegram_id:<15} {executor.full_name[:28]:<30} {'Ошибка получения':<30} {'-':<20}")
                        continue
                    
                    task = task_response.get('task', {})
                    status_raw = task.get('status', {})
                    status_id = None
                    
                    # Нормализуем статус
                    status_id_raw = status_raw.get('id')
                    if status_id_raw:
                        try:
                            if isinstance(status_id_raw, str) and ":" in status_id_raw:
                                status_id = int(status_id_raw.split(":")[-1])
                            else:
                                status_id = int(status_id_raw)
                        except (ValueError, TypeError):
                            pass
                    
                    status_name = status_raw.get('name', 'Неизвестно')
                    task_name = task.get('name', 'Без названия')
                    
                    action = "Ожидает"
                    if status_id:
                        if status_in(status_id, (StatusKey.COMPLETED, StatusKey.FINISHED)):
                            action = "✅ Завершена"
                            to_approve.append((executor, task_id))
                        elif status_in(status_id, (StatusKey.CANCELLED, StatusKey.REJECTED)):
                            action = "❌ Отменена"
                            to_reject.append((executor, task_id))
                        else:
                            action = f"Статус: {status_name}"
                    
                    print(f"{task_id:<15} {executor.telegram_id:<15} {executor.full_name[:28]:<30} {status_name[:28]:<30} {action:<20}")
                    
                except Exception as e:
                    logger.error(f"Error checking task {task_id} for executor {executor.telegram_id}: {e}", exc_info=True)
                    print(f"{task_id:<15} {executor.telegram_id:<15} {executor.full_name[:28]:<30} {'Ошибка':<30} {'-':<20}")
                    continue
            
            print("=" * 100)
            print()
            
            if to_approve:
                print(f"✅ Найдено {len(to_approve)} завершенных задач для подтверждения:")
                for executor, task_id in to_approve:
                    print(f"  - Задача {task_id}: {executor.full_name} (Telegram ID: {executor.telegram_id})")
                print()
                
                if approve_all:
                    print("Автоматическое подтверждение...")
                    for executor, task_id in to_approve:
                        try:
                            await approve_executor(executor.telegram_id, task_id, db_manager)
                            print(f"  ✅ Исполнитель {executor.telegram_id} подтвержден")
                        except Exception as e:
                            logger.error(f"Error approving executor {executor.telegram_id}: {e}", exc_info=True)
                            print(f"  ❌ Ошибка при подтверждении {executor.telegram_id}: {e}")
                else:
                    print("Для автоматического подтверждения запустите с флагом --approve-all")
            
            if to_reject:
                print(f"❌ Найдено {len(to_reject)} отмененных задач:")
                for executor, task_id in to_reject:
                    print(f"  - Задача {task_id}: {executor.full_name} (Telegram ID: {executor.telegram_id})")
                print()
                
                if approve_all:
                    print("Автоматическое отклонение...")
                    for executor, task_id in to_reject:
                        try:
                            await reject_executor(executor.telegram_id, task_id, db_manager)
                            print(f"  ✅ Исполнитель {executor.telegram_id} отклонен")
                        except Exception as e:
                            logger.error(f"Error rejecting executor {executor.telegram_id}: {e}", exc_info=True)
                            print(f"  ❌ Ошибка при отклонении {executor.telegram_id}: {e}")
            
    except Exception as e:
        logger.error(f"Error checking registration tasks: {e}", exc_info=True)
        print(f"❌ Ошибка: {e}")
    finally:
        await planfix_client.close()


async def approve_executor(telegram_id: int, task_id: int, db_manager: DBManager):
    """Подтверждает регистрацию исполнителя."""
    from config import FRANCHISE_GROUPS
    from datetime import datetime
    from aiogram import Bot
    from config import BOT_TOKEN
    from notifications import NotificationService
    
    bot = Bot(token=BOT_TOKEN)
    notification_service = NotificationService(bot)
    
    try:
        with db_manager.get_db() as db:
            executor = db_manager.get_executor_profile(db, telegram_id)
            
            if not executor:
                raise ValueError(f"Executor {telegram_id} not found")
            
            # Извлекаем planfix_user_id из задачи
            planfix_user_id = await extract_planfix_user_id(task_id)
            
            # Обновляем статус исполнителя
            db_manager.update_executor_profile(
                db,
                telegram_id,
                profile_status="активен",
                confirmation_date=datetime.now(),
                planfix_user_id=planfix_user_id
            )
            
            concept_names = [FRANCHISE_GROUPS[cid]["name"] for cid in executor.serving_franchise_groups]
            
            message = (
                f"✅ Ваша регистрация подтверждена!\n\n"
                f"Теперь вы будете получать заявки по концепциям:\n"
                f"🏢 {', '.join(concept_names)}\n\n"
                f"Используйте меню для работы с заявками."
            )
            # Отправляем сообщение с клавиатурой меню исполнителя
            from keyboards import get_executor_main_menu_keyboard
            await notification_service._send_notification(
                telegram_id, 
                message, 
                reply_markup=get_executor_main_menu_keyboard()
            )
            logger.info(f"✅ Executor {telegram_id} approved (planfix_user_id: {planfix_user_id})")
    finally:
        await bot.session.close()


async def reject_executor(telegram_id: int, task_id: int, db_manager: DBManager):
    """Отклоняет регистрацию исполнителя."""
    from aiogram import Bot
    from config import BOT_TOKEN
    from notifications import NotificationService
    
    bot = Bot(token=BOT_TOKEN)
    notification_service = NotificationService(bot)
    
    try:
        with db_manager.get_db() as db:
            db_manager.update_executor_profile(
                db,
                telegram_id,
                profile_status="отклонен"
            )
            
            message = (
                f"❌ Ваша регистрация отклонена.\n\n"
                f"Обратитесь к администратору для выяснения причин."
            )
            await notification_service._send_notification(telegram_id, message)
            logger.info(f"Executor {telegram_id} rejected")
    finally:
        await bot.session.close()


async def extract_planfix_user_id(task_id: int) -> str | None:
    """Извлекает planfix_user_id из задачи регистрации."""
    import re
    try:
        task_response = await planfix_client.get_task_by_id(
            task_id,
            fields="id,name,description,customFieldData,comments,assignees"
        )
        
        if not task_response or task_response.get('result') != 'success':
            return None
        
        task = task_response.get('task', {})
        
        # ПРИОРИТЕТ 1: Из назначенных исполнителей
        assignees = task.get('assignees', {})
        if isinstance(assignees, dict):
            users = assignees.get('users', [])
            if users and isinstance(users, list) and len(users) > 0:
                first_assignee = users[0]
                assignee_id = first_assignee.get('id')
                if assignee_id:
                    if isinstance(assignee_id, str) and ":" in assignee_id:
                        return assignee_id.split(":")[-1]
                    return str(assignee_id)
        
        # ПРИОРИТЕТ 2: Из описания
        description = task.get('description', '')
        match = re.search(r'[Pp]lanfix\s+[Uu]ser\s+ID[:\s]+(\d+)', description)
        if match:
            return match.group(1)
        
        return None
    except Exception as e:
        logger.error(f"Error extracting planfix_user_id from task {task_id}: {e}")
        return None


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Проверка задач регистрации исполнителей')
    parser.add_argument('--approve-all', action='store_true', help='Автоматически подтвердить все завершенные задачи')
    
    args = parser.parse_args()
    
    asyncio.run(check_registration_tasks(approve_all=args.approve_all))

