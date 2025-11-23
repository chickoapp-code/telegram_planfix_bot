# -*- coding: utf-8 -*-
"""
Клиент для работы с Planfix REST API
Версия: 2.3 (Обновлено 2025-11-19 - исправлен лимит запросов: с месячного на суточный)
"""

import aiohttp
import json
import logging
import asyncio
import time
import random
from datetime import datetime, timedelta
from config import (
    PLANFIX_ACCOUNT,
    PLANFIX_API_KEY,
    PLANFIX_API_SECRET,
    PLANFIX_API_SOURCE_ID,
    PLANFIX_BASE_URL,
    PLANFIX_MAX_CONCURRENCY,
)

logger = logging.getLogger(__name__)


class PlanfixRateLimitError(Exception):
    """Исключение для ошибок rate limit."""
    def __init__(self, wait_seconds: int, message: str = "Rate limit exceeded"):
        self.wait_seconds = wait_seconds
        self.message = message
        super().__init__(self.message)


class PlanfixAPIClient:
    """Клиент для взаимодействия с Planfix REST API."""
    
    # Глобальный семафор для ограничения одновременных запросов
    # Согласно документации Planfix API: не более 1 запроса в секунду
    _request_semaphore = asyncio.Semaphore(max(1, PLANFIX_MAX_CONCURRENCY))
    _last_request_time = 0
    # Минимальный интервал между запросами: 1 секунда (согласно документации Planfix API)
    # Если PLANFIX_MAX_CONCURRENCY > 1, то каждый поток должен делать запросы с интервалом 1 секунда
    # Но с учетом семафора, фактически будет не более 1 запроса в секунду глобально
    _min_request_interval = 1.0  # Минимум 1 секунда между запросами (согласно документации Planfix API)
    _rate_limit_lock = asyncio.Lock()
    _rate_limit_until = 0  # Timestamp до которого нужно ждать из-за rate limit
    
    # Отслеживание суточного лимита запросов (согласно документации Planfix API)
    _daily_request_limit = 20000  # Суточный лимит запросов (20 000 для базового пакета)
    _daily_request_count = 0  # Счетчик запросов за текущие сутки
    _daily_reset_time = 0  # Timestamp сброса счетчика (начало следующих суток)
    _last_remaining_requests = None  # Последнее известное количество оставшихся запросов из заголовка X-RateLimit-Remaining
    
    # Простой in-memory кэш для get_task_list
    _task_list_cache = {}
    _cache_ttl = 45  # TTL кэша в секундах
    
    def __init__(self):
        self.base_url = PLANFIX_BASE_URL
        self.api_key = PLANFIX_API_KEY
        self.api_secret = PLANFIX_API_SECRET
        self.account = PLANFIX_ACCOUNT
        self.source_id = PLANFIX_API_SOURCE_ID
        self.headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        # Единая сессия для всех запросов клиента
        self._session = None
        # Кэш статусов процесса (во избежание лишних запросов)
        self._status_cache = {}
        self._status_cache_ttl = 600  # seconds
        # Кэш контактов (для вывода контрагентов в списках)
        self._contact_cache = {}
        self._contact_cache_ttl = 600  # seconds
        # Кэш задач (для быстрого получения данных задач)
        self._task_cache = {}
        self._task_cache_ttl = 300  # seconds
    
    async def _get_session(self):
        """Получить или создать aiohttp сессию с таймаутами."""
        if self._session is None or self._session.closed:
            # Таймауты согласно документации Planfix API:
            # - connect: время на установление соединения
            # - total: общее время на выполнение запроса
            timeout = aiohttp.ClientTimeout(
                total=60,      # Общий таймаут: 60 секунд
                connect=15,    # Таймаут подключения: 15 секунд
                sock_read=30   # Таймаут чтения: 30 секунд
            )
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def close(self):
        """Закрыть сессию клиента."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
    
    async def __aenter__(self):
        """Поддержка async context manager."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие при выходе из контекста."""
        await self.close()

    async def _check_rate_limit_headers(self, response):
        """Проверяет заголовки ответа для отслеживания суточного лимита запросов."""
        try:
            # Проверяем заголовок X-RateLimit-Remaining (обновляется примерно раз в 10 минут)
            remaining_header = response.headers.get('X-RateLimit-Remaining')
            if remaining_header:
                try:
                    remaining = int(remaining_header)
                    async with self._rate_limit_lock:
                        old_remaining = PlanfixAPIClient._last_remaining_requests
                        PlanfixAPIClient._last_remaining_requests = remaining
                        
                        # Логируем только если значение изменилось или при приближении к лимиту
                        if old_remaining != remaining:
                            percentage = (remaining / PlanfixAPIClient._daily_request_limit) * 100
                            if remaining < PlanfixAPIClient._daily_request_limit * 0.1:
                                logger.warning(f"⚠️ Критически мало оставшихся запросов: {remaining}/{PlanfixAPIClient._daily_request_limit} ({percentage:.1f}%)")
                            elif remaining < PlanfixAPIClient._daily_request_limit * 0.2:
                                logger.warning(f"⚠️ Мало оставшихся запросов: {remaining}/{PlanfixAPIClient._daily_request_limit} ({percentage:.1f}%)")
                            elif remaining % 5000 == 0 or (old_remaining and abs(old_remaining - remaining) >= 5000):
                                logger.info(f"📊 Оставшихся запросов (из заголовка): {remaining}/{PlanfixAPIClient._daily_request_limit} ({percentage:.1f}%)")
                except (ValueError, TypeError):
                    pass
        except Exception:
            # Игнорируем ошибки при чтении заголовков
            pass

    # Вспомогательная функция: удаляет символы вне BMP (например, emoji),
    # которые некоторые JSON-десериализаторы не принимают
    def _sanitize_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            # Удаляем символы с кодом > 0xFFFF (включая суррогатные пары)
            sanitized = ''.join(ch for ch in str(value) if ord(ch) <= 0xFFFF)
            return sanitized
        except Exception:
            return value

    async def _request(self, method, endpoint, data=None, params=None, headers=None, retry_count=0, max_retries=3):
        """Базовый метод для выполнения HTTP запросов к API с управлением rate limit."""
        url = f"{self.base_url}{endpoint}"
        _headers = self.headers.copy()
        if headers:
            _headers.update(headers)
        
        # Добавляем account в параметры запроса (требуется для Planfix REST API)
        _params = params.copy() if params else {}
        if self.account and "account" not in _params:
            _params["account"] = self.account

        # Используем семафор для ограничения одновременных запросов
        async with self._request_semaphore:
            # Проверяем и ждем если rate limit активен
            while True:
                current_time = time.time()
                wait_until_reset = 0
                
                async with self._rate_limit_lock:
                    # Проверяем глобальную блокировку из-за rate limit
                    if PlanfixAPIClient._rate_limit_until > current_time:
                        wait_until_reset = PlanfixAPIClient._rate_limit_until - current_time
                
                # Ждем вне lock, чтобы другие запросы могли проверить статус
                if wait_until_reset > 0:
                    logger.info(f"⏳ Global rate limit active, waiting {wait_until_reset:.1f}s before request to {endpoint}")
                    await asyncio.sleep(wait_until_reset + 1)  # +1 секунда для безопасности
                    continue  # Проверяем снова после ожидания
                
                # Если rate limit не активен, продолжаем
                async with self._rate_limit_lock:
                    current_time = time.time()
                    time_since_last = current_time - PlanfixAPIClient._last_request_time
                    jitter_seconds = random.uniform(0.05, 0.25)
                    if time_since_last < self._min_request_interval:
                        base_wait = self._min_request_interval - time_since_last
                        wait_time = base_wait + jitter_seconds
                        logger.debug(f"Rate limiting: waiting {wait_time:.2f}s (base {base_wait:.2f}s + jitter {jitter_seconds:.2f}s) before request to {endpoint}")
                        await asyncio.sleep(wait_time)
                    else:
                        # Если ждать по базовому интервалу не требуется, добавляем небольшой джиттер
                        logger.debug(f"Adding jitter {jitter_seconds:.2f}s before request to {endpoint}")
                        await asyncio.sleep(jitter_seconds)
                    PlanfixAPIClient._last_request_time = time.time()
                break  # Выходим из цикла ожидания rate limit

            session = await self._get_session()
            response = None
            try:
                # Обновляем счетчик суточных запросов и проверяем лимит
                async with self._rate_limit_lock:
                    current_time = time.time()
                    current_datetime = datetime.now()
                    
                    # Вычисляем начало следующих суток для сброса счетчика
                    if PlanfixAPIClient._daily_reset_time == 0 or current_time >= PlanfixAPIClient._daily_reset_time:
                        # Вычисляем начало следующих суток (00:00:00 следующего дня)
                        next_day = (current_datetime.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
                        
                        PlanfixAPIClient._daily_request_count = 0
                        PlanfixAPIClient._daily_reset_time = next_day.timestamp()
                        
                        hours_until_reset = (next_day - current_datetime).total_seconds() / 3600
                        logger.info(f"📊 Суточный счетчик запросов сброшен. Лимит: {PlanfixAPIClient._daily_request_limit} запросов/сутки. Следующий сброс через {hours_until_reset:.1f} часов")
                    
                    # Проверяем, не превышен ли суточный лимит
                    if PlanfixAPIClient._daily_request_count >= PlanfixAPIClient._daily_request_limit:
                        remaining_until_reset = PlanfixAPIClient._daily_reset_time - current_time
                        hours_until_reset = remaining_until_reset / 3600
                        logger.error(
                            f"🚫 Суточный лимит запросов превышен! "
                            f"Использовано: {PlanfixAPIClient._daily_request_count}/{PlanfixAPIClient._daily_request_limit}. "
                            f"Сброс через {hours_until_reset:.1f} часов"
                        )
                        raise PlanfixRateLimitError(
                            wait_seconds=int(remaining_until_reset),
                            message=f"Daily request limit exceeded ({PlanfixAPIClient._daily_request_count}/{PlanfixAPIClient._daily_request_limit}). Reset in {hours_until_reset:.1f} hours"
                        )
                    
                    # Увеличиваем счетчик запросов
                    PlanfixAPIClient._daily_request_count += 1
                    
                    # Логируем предупреждение при приближении к лимиту
                    if PlanfixAPIClient._daily_request_count % 1000 == 0:
                        remaining = PlanfixAPIClient._daily_request_limit - PlanfixAPIClient._daily_request_count
                        percentage = (PlanfixAPIClient._daily_request_count / PlanfixAPIClient._daily_request_limit) * 100
                        logger.info(f"📊 Запросов за сутки: {PlanfixAPIClient._daily_request_count}/{PlanfixAPIClient._daily_request_limit} ({percentage:.1f}%), осталось: {remaining}")
                    elif PlanfixAPIClient._daily_request_count >= PlanfixAPIClient._daily_request_limit * 0.9:
                        remaining = PlanfixAPIClient._daily_request_limit - PlanfixAPIClient._daily_request_count
                        logger.warning(f"⚠️ Приближение к суточному лимиту: использовано {PlanfixAPIClient._daily_request_count}/{PlanfixAPIClient._daily_request_limit}, осталось: {remaining} запросов")
                
                if method == "GET":
                    async with session.get(url, headers=_headers, params=_params) as response:
                        # Проверяем заголовок X-RateLimit-Remaining для отслеживания оставшихся запросов
                        await self._check_rate_limit_headers(response)
                        
                        response_text = await response.text()
                        if response.status == 403:
                            try:
                                error_json = json.loads(response_text)
                                if error_json.get('code') == 22:  # Rate limit error
                                    time_to_reset = error_json.get('timeToReset')
                                    if time_to_reset:
                                        # timeToReset может быть в миллисекундах или секундах
                                        # Если больше 1000, значит в миллисекундах
                                        if time_to_reset > 1000:
                                            wait_time = (time_to_reset / 1000) + 15  # +15 секунд для безопасности
                                        else:
                                            wait_time = time_to_reset + 15  # +15 секунд для безопасности
                                    else:
                                        wait_time = 120  # 120 секунд по умолчанию (увеличено с 90)
                                    
                                    # Устанавливаем глобальную блокировку
                                    async with self._rate_limit_lock:
                                        PlanfixAPIClient._rate_limit_until = time.time() + wait_time
                                    
                                    logger.warning(f"⚠️ Rate limit exceeded (code 22), установлена глобальная блокировка на {wait_time:.1f}s")
                                    
                                    # Если не превышен лимит попыток, ждем и повторяем запрос
                                    if retry_count < max_retries:
                                        logger.info(f"⏳ Waiting {wait_time:.1f}s and retrying request to {endpoint} (attempt {retry_count + 1}/{max_retries})")
                                        await asyncio.sleep(wait_time)
                                        # Рекурсивный вызов проверит rate limit автоматически
                                        return await self._request(method, endpoint, data, params, headers, retry_count + 1, max_retries)
                                    else:
                                        # Превышен лимит попыток - выбрасываем исключение
                                        raise PlanfixRateLimitError(
                                            wait_seconds=int(wait_time),
                                            message=f"Rate limit exceeded after {max_retries} retries, please wait {int(wait_time)} seconds"
                                        )
                            except json.JSONDecodeError:
                                pass
                        response.raise_for_status()
                        return json.loads(response_text) if response_text else {}
                elif method == "POST":
                    # Логируем данные запроса для отладки
                    if data:
                        logger.debug(f"Request to {method} {url}")
                        logger.debug(f"Request data: {json.dumps(data, ensure_ascii=False, indent=2)}")
                    
                    post_kwargs = {"headers": _headers.copy(), "params": _params}
                    if data is not None:
                        # Используем json= для корректной сериализации тела запроса
                        post_kwargs["json"] = data
                        # Явно укажем charset в Content-Type (некоторые сервера чувствительны)
                        post_kwargs["headers"]["Content-Type"] = "application/json; charset=utf-8"
                    async with session.post(url, **post_kwargs) as response:
                        # Проверяем заголовок X-RateLimit-Remaining для отслеживания оставшихся запросов
                        await self._check_rate_limit_headers(response)
                        
                        # Логируем ответ для отладки
                        response_text = await response.text()
                        logger.debug(f"Response status: {response.status}")
                        logger.debug(f"Response body: {response_text}")
                        
                        if response.status == 403:
                            try:
                                error_json = json.loads(response_text)
                                if error_json.get('code') == 22:  # Rate limit error
                                    time_to_reset = error_json.get('timeToReset')
                                    if time_to_reset:
                                        # timeToReset может быть в миллисекундах или секундах
                                        # Если больше 1000, значит в миллисекундах
                                        if time_to_reset > 1000:
                                            wait_time = (time_to_reset / 1000) + 15  # +15 секунд для безопасности
                                        else:
                                            wait_time = time_to_reset + 15  # +15 секунд для безопасности
                                    else:
                                        wait_time = 120  # 120 секунд по умолчанию (увеличено с 90)
                                    
                                    # Устанавливаем глобальную блокировку
                                    async with self._rate_limit_lock:
                                        PlanfixAPIClient._rate_limit_until = time.time() + wait_time
                                    
                                    logger.warning(f"⚠️ Rate limit exceeded (code 22), установлена глобальная блокировка на {wait_time:.1f}s")
                                    
                                    # Если не превышен лимит попыток, ждем и повторяем запрос
                                    if retry_count < max_retries:
                                        logger.info(f"⏳ Waiting {wait_time:.1f}s and retrying request to {endpoint} (attempt {retry_count + 1}/{max_retries})")
                                        await asyncio.sleep(wait_time)
                                        # Рекурсивный вызов проверит rate limit автоматически
                                        return await self._request(method, endpoint, data, params, headers, retry_count + 1, max_retries)
                                    else:
                                        # Превышен лимит попыток - выбрасываем исключение
                                        raise PlanfixRateLimitError(
                                            wait_seconds=int(wait_time),
                                            message=f"Rate limit exceeded after {max_retries} retries, please wait {int(wait_time)} seconds"
                                        )
                            except json.JSONDecodeError:
                                pass
                        
                        response.raise_for_status()
                        return json.loads(response_text) if response_text else {}
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
            except PlanfixRateLimitError:
                # Пробрасываем исключение rate limit дальше
                raise
            except aiohttp.ClientResponseError as e:
                logger.error(f"Planfix API error for {method} {url}: {e.status} - {e.message}")
                # Пытаемся получить детали ошибки из исключения
                if hasattr(e, 'request_info') and e.request_info:
                    logger.error(f"Request URL: {e.request_info.real_url}")
                # response уже закрыт после выхода из async with, используем информацию из исключения
                if hasattr(e, 'message'):
                    logger.error(f"Error message: {e.message}")
                # Логируем отправленные данные при ошибке
                if data:
                    logger.error(f"Request data that caused error: {json.dumps(data, ensure_ascii=False, indent=2)}")
                
                # Специальная обработка ошибки 401 (Unauthorized)
                if e.status == 401:
                    logger.error("=" * 80)
                    logger.error("❌ ОШИБКА АВТОРИЗАЦИИ (401 Unauthorized)")
                    logger.error("=" * 80)
                    logger.error("Проверьте настройки в файле .env:")
                    logger.error(f"  - PLANFIX_BASE_URL: {self.base_url}")
                    logger.error(f"  - PLANFIX_ACCOUNT: {self.account}")
                    logger.error(f"  - PLANFIX_API_KEY: {'*' * min(10, len(self.api_key)) if self.api_key else 'НЕ УСТАНОВЛЕН'}")
                    logger.error("")
                    logger.error("Убедитесь, что:")
                    logger.error("  1. Вы используете реальные значения, а не примеры из env.example")
                    logger.error("  2. PLANFIX_BASE_URL указывает на ваш аккаунт Planfix (не example.planfix.ru)")
                    logger.error("  3. PLANFIX_ACCOUNT содержит правильное имя аккаунта")
                    logger.error("  4. PLANFIX_API_KEY содержит действительный API ключ")
                    logger.error("=" * 80)
                
                raise
            except aiohttp.ClientConnectorError as e:
                logger.error(f"Planfix API connection error for {method} {url}: {e}")
                raise
            except Exception as e:
                logger.error(f"An unexpected error occurred during Planfix API request: {e}")
                raise

    # ============================================================================
    # PROCESS & STATUSES
    # ============================================================================

    async def get_process_list(self):
        """Получает список процессов задач."""
        endpoint = "/process/task"
        return await self._request("GET", endpoint)

    async def get_process_task_statuses(self, process_id: int, fields: str = "id,name,isFinal"):
        """Получает список статусов задач для конкретного процесса."""
        endpoint = f"/process/task/{process_id}/statuses"
        params = {"fields": fields}
        return await self._request("GET", endpoint, params=params)

    async def get_terminal_status_ids(self, process_id: int) -> set[int]:
        """Возвращает множество ID терминальных статусов процесса без хардкода чисел.
        Предпочитаем системные имена COMPLETED/REJECTED/CANCELED/DONE/FINISHED, иначе используем isFinal."""
        # Проверяем кэш
        now = time.time()
        cache = self._status_cache.get(process_id)
        if cache and now - cache.get("ts", 0) < self._status_cache_ttl:
            return cache.get("terminal_ids", set())

        try:
            resp = await self.get_process_task_statuses(process_id, fields="id,name,isFinal,systemName")
        except Exception as e:
            logger.error(f"Failed to load process statuses for {process_id}: {e}")
            # Фолбэк: пустое множество, чтобы не отфильтровать всё
            return set()

        statuses = resp.get("statuses", []) if isinstance(resp, dict) else []
        terminal_names = {"COMPLETED", "REJECTED", "CANCELED", "CANCELLED", "DONE", "FINISHED"}
        terminal_ids: set[int] = set()

        for s in statuses:
            sid = s.get("id")
            sys_name = s.get("systemName") or s.get("name")
            sys_name_norm = str(sys_name).upper().replace(" ", "") if sys_name else ""
            is_final = bool(s.get("isFinal"))
            if sys_name_norm in terminal_names or is_final:
                # Нормализуем id (может приходить как "status:3")
                try:
                    terminal_ids.add(int(sid))
                except (TypeError, ValueError):
                    if isinstance(sid, str) and ":" in sid:
                        part = sid.split(":")[-1]
                        try:
                            terminal_ids.add(int(part))
                        except Exception:
                            pass

        # Кэшируем результат
        self._status_cache[process_id] = {"terminal_ids": terminal_ids, "ts": now}
        return terminal_ids

    # ============================================================================
    # DIRECTORIES
    # ============================================================================

    async def get_directories(self, fields="id,name,group,fields"):
        """Получает список всех справочников Planfix."""
        endpoint = "/directory/list"
        data = {"fields": fields}
        return await self._request("POST", endpoint, data=data)

    async def get_directory_by_id(self, directory_id: int, fields: str = "id,name,group"):
        """Получает информацию о справочнике по ID."""
        endpoint = f"/directory/{directory_id}"
        params = {"fields": fields}
        return await self._request("GET", endpoint, params=params)

    async def get_directory_entries(self, directory_id, fields="name,key,parentKey", offset=0, page_size=100):
        """Получает записи конкретного справочника Planfix."""
        endpoint = f"/directory/{directory_id}/entry/list"
        data = {
            "offset": offset,
            "pageSize": page_size,
            "fields": fields
        }
        return await self._request("POST", endpoint, data=data)

    async def get_directory_entry_by_key(self, directory_id: int, entry_key: str, fields: str = "key,name,parentKey"):
        """Получает запись справочника по ключу."""
        endpoint = f"/directory/{directory_id}/entry/{entry_key}"
        params = {"fields": fields}
        return await self._request("GET", endpoint, params=params)

    # ============================================================================
    # CONTACTS
    # ============================================================================

    async def get_contact_groups(self, fields: str = "id,name"):
        """
        Получает список групп контактов.
        
        Returns:
            dict: {"result": "success", "groups": [{"id": 12, "name": "..."}]}
        """
        endpoint = "/contact/groups"
        params = {"fields": fields}
        return await self._request("GET", endpoint, params=params)

    async def get_contact_list_by_group(self, group_id: int, fields: str = "id,name,group", offset: int = 0, page_size: int = 100):
        """
        Получает список контактов в группе.
        
        Args:
            group_id: ID группы контактов (например, 12 для "Мясоroob")
        """
        endpoint = "/contact/list"
        data = {
            "filters": [
                {
                    "type": 4008,  # Фильтр по группе контактов (Contact group)
                    "operator": "equal",  # ✅ Строка, не число!
                    "value": group_id
                }
            ],
            "fields": fields,
            "offset": offset,
            "pageSize": page_size
        }
        return await self._request("POST", endpoint, data=data)

    async def get_contact_by_id(self, contact_id: int, fields: str = "id,name,midName,lastName,isCompany,group,phones,email,customFieldData"):
        """Получа��т информацию о контакте по ID с кэшированием по TTL.
        
        Args:
            contact_id: ID контакта (целое число)
            fields: Список полей для получения (по умолчанию включены все основные поля)
            
        Returns:
            dict: Ответ API с информацией о контакте или пустой dict при ошибке
        """
        # Нормализуем contact_id (может приходить как "contact:123" или просто число)
        try:
            if isinstance(contact_id, str) and ':' in contact_id:
                contact_id = int(contact_id.split(':')[-1])
            else:
                contact_id = int(contact_id)
        except (TypeError, ValueError):
            logger.warning(f"Invalid contact_id format: {contact_id}")
            return {}
        
        # Ключ кэша учитывает набор полей
        cache_key = (contact_id, fields)
        try:
            cache_rec = self._contact_cache.get(cache_key)
            if cache_rec:
                if time.time() - cache_rec.get("ts", 0) < self._contact_cache_ttl:
                    logger.debug(f"Contact {contact_id} retrieved from cache")
                    return cache_rec.get("data")
        except Exception:
            pass
        
        endpoint = f"/contact/{contact_id}"
        params = {"fields": fields}
        try:
            data = await self._request("GET", endpoint, params=params)
            try:
                self._contact_cache[cache_key] = {"data": data, "ts": time.time()}
            except Exception:
                pass
            return data
        except Exception as e:
            logger.error(f"Failed to get contact {contact_id}: {e}")
            return {}
    
    async def get_contact_templates(self, fields: str = "id,name"):
        """
        Получает список шаблонов контактов.
        
        Returns:
            dict: {"result": "success", "templates": [{"id": 1, "name": "..."}]}
        """
        endpoint = "/contact/templates"
        params = {"fields": fields}
        return await self._request("GET", endpoint, params=params)

    async def create_contact(self, name, phone=None, email=None, template_id=None, custom_field_data=None, lastname=None, group_id=None, position=None, telegram=None, telegram_id=None):
        """Создает новый контакт в Planfix."""
        endpoint = "/contact/"
        
        # ��азделяем ФИО на части если передано полное имя
        name_parts = name.strip().split()
        if len(name_parts) >= 2 and not lastname:
            # Если передано "Фамилия Имя" или "Фамилия Имя Отчество"
            lastname = name_parts[0]
            name = " ".join(name_parts[1:])
        elif len(name_parts) == 1 and not lastname:
            # Если передано только одно слово - используем его как имя
            lastname = name_parts[0]
            name = name_parts[0]
        
        # Минимальные обязательные поля
        # Согласно swagger.json, нужно указать isCompany: false для контакта (не компании)
        data = {
            "name": name if name else "Контакт",
            "lastname": lastname if lastname else "Неизвестно",
            "isCompany": False  # Явно указываем, что это контакт, а не компания
        }
        
        # Добавляем группу если указана (пробуем без группы, если ошибка)
        # В некоторых случаях группа может быть необязательной или не подходить
        if group_id:
            try:
                data["group"] = {"id": int(group_id)}
            except (ValueError, TypeError):
                logger.warning(f"Invalid group_id: {group_id}, skipping group")
        
        # Добавляем телефон если есть
        # Согласно swagger.json, phones должен содержать number и type (1 - мобильный)
        if phone:
            data["phones"] = [{"number": phone, "type": 1}]
        
        # Добавляем email если есть
        if email:
            data["email"] = email
        
        # Добавляем должность если указана
        # Согласно swagger.json, поле position - это строка
        if position:
            data["position"] = position
        
        # Добавляем Telegram если указан
        # Согласно swagger.json, есть два поля: telegram (URL) и telegramId (ID)
        # telegram должен быть в формате "https://t.me/username"
        if telegram:
            # Если telegram начинается с @, убираем его
            telegram_clean = telegram.lstrip('@').strip()
            if telegram_clean:
                # Всегда формируем полный URL согласно swagger.json
                data["telegram"] = f"https://t.me/{telegram_clean}"
                logger.debug(f"Setting telegram field to: {data['telegram']}")
        elif telegram_id:
            # Если нет username, но есть telegram_id, создаем ссылку
            # Пробуем использовать формат https://t.me/user{id} или просто ID
            # Некоторые системы Planfix могут принимать просто ID как строку
            # Попробуем сначала формат с user{id}, если не сработает - можно будет изменить
            data["telegram"] = f"https://t.me/user{telegram_id}"
            logger.info(f"Setting telegram field to user link (no username): {data['telegram']}")
        
        if telegram_id:
            # telegramId должен быть строкой согласно swagger.json
            data["telegramId"] = str(telegram_id)
            logger.debug(f"Setting telegramId field to: {data['telegramId']}")
            
        # Добавляем кастомные поля если есть
        if custom_field_data:
            data["customFieldData"] = custom_field_data
            
        # Добавляем шаблон если есть
        if template_id:
            data["template"] = {"id": int(template_id)}
        
        # Логируем данные перед отправкой (для отладки)
        import json
        logger.debug(f"Creating contact with data keys: {list(data.keys())}")
        if "telegram" in data:
            logger.info(f"Telegram field will be set to: {data['telegram']}")
        if "telegramId" in data:
            logger.info(f"TelegramId field will be set to: {data['telegramId']}")
        if "position" in data:
            logger.info(f"Position field will be set to: {data['position']}")
        
        # Пробуем создать контакт
        try:
            result = await self._request("POST", endpoint, data=data)
            logger.info(f"Contact created successfully")
            return result
        except Exception as e:
            # Если ошибка 400 и есть группа или template, пробуем без них
            error_str = str(e).lower()
            is_bad_request = "400" in error_str or "bad request" in error_str
            
            if is_bad_request and (group_id or template_id):
                logger.warning(f"Failed to create contact with group/template, trying without them: {e}")
                # Создаем копию данных без группы и template
                # ВАЖНО: telegram, telegramId, position и другие поля должны остаться!
                data_fallback = data.copy()
                data_fallback.pop("group", None)
                data_fallback.pop("template", None)
                logger.info(f"Retrying contact creation without group/template. Telegram fields preserved: telegram={data_fallback.get('telegram')}, telegramId={data_fallback.get('telegramId')}, position={data_fallback.get('position')}")
                try:
                    result = await self._request("POST", endpoint, data=data_fallback)
                    logger.info(f"Contact created successfully (without group/template)")
                    return result
                except Exception as fallback_error:
                    logger.error(f"Failed to create contact even without group/template: {fallback_error}")
                    raise
            else:
                raise

    # ============================================================================
    # TASKS
    # ============================================================================

    async def get_task_templates(self, fields="id,name,description,project"):
        """Получает список всех доступных шаблонов задач Planfix."""
        endpoint = "/task/templates"
        params = {"fields": fields}
        return await self._request("GET", endpoint, params=params)

    async def create_task(self, name, description, template_id=None, project_id=None, counterparty_id=None,
                          custom_field_data=None, files=None, assignee_users=None, assignee_groups=None,
                          status_id=None, tags=None, process_id=None):
        """Создает новую задачу (заявку) в Planfix."""
        endpoint = "/task/"
        # Санитизируем текстовые поля (убираем emoji/вне BMP)
        name = self._sanitize_text(name) or ""
        description = self._sanitize_text(description) or ""

        # Базовые обязательные поля
        task_data = {
            "name": name,
            "description": description,
        }
        # Не включаем пустые/None поля — только если есть значения
        if template_id:
            task_data["template"] = {"id": int(template_id)}
        if project_id:
            task_data["project"] = {"id": int(project_id)}
        if counterparty_id:
            # В примерах swagger counterparty.id передается как число, хотя схема указывает string
            # Пробуем число, так как в примерах используется число
            task_data["counterparty"] = {"id": int(counterparty_id)}
        if process_id:
            # Согласно swagger.json, поле называется processId и это просто число (integer), а не объект
            task_data["processId"] = int(process_id)
        if custom_field_data:
            if isinstance(custom_field_data, list) and len(custom_field_data) > 0:
                task_data["customFieldData"] = custom_field_data
        if files:
            # Нормализуем ID файлов: убираем префикс если есть, преобразуем в int
            file_items = []
            for f_id in files:
                if f_id is None:
                    continue
                if isinstance(f_id, str) and ':' in f_id:
                    try:
                        f_id = int(f_id.split(':')[-1])
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse file_id: {f_id}")
                        continue
                elif not isinstance(f_id, int):
                    try:
                        f_id = int(f_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert file_id to int: {f_id}")
                        continue
                file_items.append(f_id)
            if file_items:
                task_data["files"] = [{"id": f_id} for f_id in file_items]

        # Формируем структуру исполнителей (assignees) согласно требованиям Planfix
        assignees_payload = {}
        if assignee_users:
            users = [{"id": f"user:{int(user_id)}"} for user_id in assignee_users if user_id is not None]
            if users:
                assignees_payload["users"] = users
        if assignee_groups:
            groups = [{"id": f"group:{int(group_id)}"} for group_id in assignee_groups if group_id is not None]
            if groups:
                assignees_payload["groups"] = groups
        if assignees_payload:
            task_data["assignees"] = assignees_payload
        if status_id is not None:
            # Передаем status_id только если он не None
            try:
                task_data["status"] = {"id": int(status_id)}
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid status_id format: {status_id}, error: {e}. Creating task without status.")
        if tags:
            normalized_tags = []
            if isinstance(tags, (list, tuple, set)):
                for tag in tags:
                    if isinstance(tag, str):
                        tag_name = tag.strip()
                        if tag_name:
                            normalized_tags.append(tag_name)
            elif isinstance(tags, str):
                tag_name = tags.strip()
                if tag_name:
                    normalized_tags.append(tag_name)
            if normalized_tags:
                task_data["tags"] = [{"name": tag} for tag in normalized_tags]

        # Отправляем task_data напрямую, без обёртки "task"
        return await self._request("POST", endpoint, data=task_data)

    async def update_task(self, task_id, status_id=None, assignee_users=None, assignee_groups=None, 
                         assignee_contacts=None, custom_field_data=None, files=None, assigner=None, **kwargs):
        """Обновляет существующую задачу в Planfix.
        
        Сначала получает текущие данные задачи, затем объединяет их с новыми данными,
        чтобы не потерять существующие поля (например, counterparty).
        """
        endpoint = f"/task/{task_id}"
        
        # Получаем текущие данные задачи, чтобы не потерять существующие поля
        try:
            current_task = await self.get_task_by_id(
                task_id, 
                fields="id,name,description,status,project,counterparty,assignees,customFieldData,files"
            )
            logger.debug(f"Current task data: {json.dumps(current_task, ensure_ascii=False, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to get current task data for {task_id}: {e}")
            current_task = {}
        
        # Начинаем с текущих данных задачи (если они получены)
        data = {}
        
        # Извлекаем объект задачи из ответа API
        task_obj = {}
        try:
            if isinstance(current_task, dict):
                task_obj = current_task.get("task") or {}
        except Exception:
            task_obj = {}
        
        # Сохраняем существующие поля, которые не переопределяются
        if task_obj:
            # Сохраняем counterparty если он валиден и не переопределяется
            if "counterparty" in task_obj and "counterparty" not in kwargs:
                try:
                    cp = task_obj.get("counterparty") or {}
                    rid = cp.get("id")
                    rid_int = None
                    if isinstance(rid, int):
                        rid_int = rid if rid > 0 else None
                    elif isinstance(rid, str):
                        part = rid.split(":")[-1]
                        rid_int = int(part) if part.isdigit() else None
                    if rid_int:
                        data["counterparty"] = cp
                    else:
                        # Пропускаем, чтобы не затирать существующее значение пустым
                        pass
                except Exception:
                    pass
            
            # Сохраняем project если он валиден и не переопределяется
            if "project" in task_obj and "project" not in kwargs:
                try:
                    pr = task_obj.get("project") or {}
                    pid = pr.get("id")
                    pid_int = None
                    if isinstance(pid, int):
                        pid_int = pid if pid > 0 else None
                    elif isinstance(pid, str):
                        part = pid.split(":")[-1]
                        pid_int = int(part) if part.isdigit() else None
                    if pid_int:
                        data["project"] = pr
                    else:
                        # Пропускаем, чтобы не затирать существующее значение пустым
                        pass
                except Exception:
                    pass
            
            # Сохраняем name и description если они есть и не переопределяются
            if "name" in task_obj and "name" not in kwargs:
                data["name"] = task_obj["name"]
            if "description" in task_obj and "description" not in kwargs:
                data["description"] = task_obj["description"]
        
        # Добавляем новые данные из kwargs
        data.update(kwargs)
        if status_id:
            data["status"] = {"id": status_id}
        
        # Устанавливаем поле "Постановщик" (assigner), если указано
        # В Planfix это может быть поле "Исполнитель"
        if assigner:
            data["assigner"] = assigner
            logger.info(f"✅ Setting assigner for task {task_id}: {assigner}")
        
        # Формируем структуру исполнителей (assignees) согласно требованиям Planfix
        # ВАЖНО: При передаче assignee_users, assignee_contacts или assignee_groups они полностью заменяют существующих
        # Согласно swagger.json, в assignees.users можно добавлять и user:ID, и contact:ID
        if assignee_users or assignee_contacts or assignee_groups:
            assignees_payload = {}
            users_list = []
            
            # Добавляем пользователей
            if assignee_users:
                users_list.extend([{"id": f"user:{int(user_id)}"} for user_id in assignee_users])
            
            # Добавляем контакты (в том же массиве users, но с префиксом contact:)
            if assignee_contacts:
                users_list.extend([{"id": f"contact:{int(contact_id)}"} for contact_id in assignee_contacts])
            
            if users_list:
                assignees_payload["users"] = users_list
                logger.debug(f"Setting assignees users: {assignees_payload['users']}")
            
            if assignee_groups:
                assignees_payload["groups"] = [{"id": f"group:{int(group_id)}"} for group_id in assignee_groups]
                logger.debug(f"Setting assignees groups: {assignees_payload['groups']}")
            
            data["assignees"] = assignees_payload
            logger.info(f"✅ Updated task {task_id} assignees: {assignees_payload}")
        elif task_obj and "assignees" in task_obj:
            # Сохраняем существующих исполнителей если новые не переданы
            data["assignees"] = task_obj["assignees"]
            logger.debug(f"Preserving existing assignees: {task_obj['assignees']}")
        
        # ВАЖНО: Обновляем кастомные поля
        # Planfix требует, чтобы customFieldData был массивом объектов с полями field и value
        # При обновлении нужно объединить существующие поля с новыми (новые переопределяют существующие)
        if custom_field_data:
            if isinstance(custom_field_data, list) and len(custom_field_data) > 0:
                # Валидируем структуру каждого элемента
                validated_fields = []
                for field_item in custom_field_data:
                    if isinstance(field_item, dict) and "field" in field_item and "value" in field_item:
                        validated_fields.append(field_item)
                    else:
                        logger.warning(f"Invalid custom field structure: {field_item}")
                
                if validated_fields:
                    # Объединяем с существующими кастомными полями
                    # Создаем словарь для быстрого поиска по ID поля
                    existing_fields_map = {}
                    if task_obj and "customFieldData" in task_obj:
                        existing_fields = task_obj.get("customFieldData", [])
                        if isinstance(existing_fields, list):
                            for existing_field in existing_fields:
                                if isinstance(existing_field, dict):
                                    field_id = existing_field.get("field", {}).get("id")
                                    if field_id is not None:
                                        existing_fields_map[field_id] = existing_field
                    
                    # Объединяем: новые поля переопределяют существующие с тем же ID
                    merged_fields_map = existing_fields_map.copy()
                    for new_field in validated_fields:
                        field_id = new_field.get("field", {}).get("id")
                        if field_id is not None:
                            merged_fields_map[field_id] = new_field
                    
                    # Преобразуем обратно в список
                    merged_fields = list(merged_fields_map.values())
                    
                    # Дополнительная валидация перед отправкой
                    # Проверяем формат каждого поля
                    final_validated_fields = []
                    for field_item in merged_fields:
                        field_id = field_item.get("field", {}).get("id")
                        field_value = field_item.get("value")
                        
                        if field_id is None:
                            logger.warning(f"Skipping merged field with missing field.id: {field_item}")
                            continue
                        
                        # Валидация значения
                        if isinstance(field_value, dict) and "id" in field_value:
                            # Для полей типа Directory entry или Contact
                            if field_value.get("id") is not None:
                                final_validated_fields.append(field_item)
                            else:
                                logger.warning(f"Skipping field {field_id} - id is None in merged fields")
                        elif isinstance(field_value, str):
                            # Для полей типа Phone или Text
                            if field_value.strip():
                                final_validated_fields.append(field_item)
                            else:
                                logger.warning(f"Skipping field {field_id} - empty string value")
                        elif field_value is None:
                            logger.warning(f"Skipping field {field_id} - value is None")
                        else:
                            # Другие типы - добавляем как есть
                            final_validated_fields.append(field_item)
                    
                    if final_validated_fields:
                        data["customFieldData"] = final_validated_fields
                        logger.info(f"✅ Updating task {task_id} with {len(validated_fields)} new custom fields (total: {len(final_validated_fields)} after merge and validation)")
                        logger.debug(f"Custom fields: {json.dumps(final_validated_fields, ensure_ascii=False, indent=2)}")
                    else:
                        logger.warning(f"No valid custom fields after validation for task {task_id}")
                else:
                    logger.warning(f"No valid custom fields found in: {custom_field_data}")
            else:
                logger.warning(f"custom_field_data is empty or not a list: {custom_field_data}")
        elif task_obj and "customFieldData" in task_obj:
            # Сохраняем существующие кастомные поля если новые не переданы
            data["customFieldData"] = task_obj["customFieldData"]
        
        # Обновляем файлы
        if files:
            # Нормализуем ID файлов: убираем префикс если есть, преобразуем в int
            normalized_files = []
            for f_id in files:
                if f_id is None:
                    continue
                if isinstance(f_id, str) and ':' in f_id:
                    try:
                        f_id = int(f_id.split(':')[-1])
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse file_id: {f_id}")
                        continue
                elif not isinstance(f_id, int):
                    try:
                        f_id = int(f_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert file_id to int: {f_id}")
                        continue
                normalized_files.append(f_id)
            
            if normalized_files:
                # Объединяем с существующими файлами из задачи
                existing_files = task_obj.get("files", []) if task_obj else []
                existing_file_ids = set()
                for f in existing_files:
                    if isinstance(f, dict):
                        fid = f.get('id')
                        if isinstance(fid, str) and ':' in fid:
                            try:
                                fid = int(fid.split(':')[-1])
                            except (ValueError, TypeError):
                                continue
                        elif not isinstance(fid, int):
                            try:
                                fid = int(fid)
                            except (ValueError, TypeError):
                                continue
                        if fid:
                            existing_file_ids.add(fid)
                
                # Добавляем новые файлы
                for fid in normalized_files:
                    existing_file_ids.add(fid)
                
                # Формируем список всех файлов
                data["files"] = [{"id": fid} for fid in existing_file_ids]
            elif task_obj and "files" in task_obj:
                # Если новых файлов нет после нормализации, сохраняем существующие
                data["files"] = task_obj["files"]
        elif task_obj and "files" in task_obj:
            # Сохраняем существующие файлы если новые не переданы
            data["files"] = task_obj["files"]
        
        # Обновляем tags (если переданы через kwargs)
        if "tags" in kwargs:
            tags = kwargs.pop("tags")  # Убираем из kwargs, чтобы не дублировать
            if tags:
                if isinstance(tags, list):
                    # Если уже в формате [{"name": "tag"}], используем как есть
                    if tags and isinstance(tags[0], dict):
                        data["tags"] = tags
                    else:
                        # Если список строк, преобразуем в нужный формат
                        data["tags"] = [{"name": str(tag)} for tag in tags if tag]
                elif isinstance(tags, str):
                    data["tags"] = [{"name": tags}]

        # Логируем полные данные обновления для отладки
        logger.debug(f"Updating task {task_id} with data: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        return await self._request("POST", endpoint, data=data)

    async def get_task_list(self, filters=None, fields="id,name,description,status,project,counterparty,workers,dateOfLastUpdate", 
                           offset=0, page_size=100, filter_id: str | None = None, result_order: list | None = None):
        """Получает список задач из Planfix с возможностью фильтрации."""
        endpoint = "/task/list"
        data = {
            "offset": offset,
            "pageSize": page_size,
            "fields": fields
        }
        if filter_id:
            data["filterId"] = filter_id
        else:
            data["filters"] = filters if filters else []
        if result_order:
            data["resultOrder"] = result_order
        return await self._request("POST", endpoint, data=data)

    async def get_task_by_id(self, task_id: int, fields: str = "id,name,description,status,project,counterparty,assignees,customFieldData,files,dateOfLastUpdate"):
        """Получает информацию о задаче по её номеру с кэшированием по TTL."""
        # Нормализуем task_id
        try:
            task_id = int(task_id)
        except (TypeError, ValueError):
            logger.warning(f"Invalid task_id format: {task_id}")
            return {}

        # Ключ кэша учитывает набор полей
        cache_key = (task_id, fields)
        try:
            cache_rec = self._task_cache.get(cache_key)
            if cache_rec:
                if time.time() - cache_rec.get("ts", 0) < self._task_cache_ttl:
                    logger.debug(f"Task {task_id} retrieved from cache")
                    return cache_rec.get("data")
        except Exception:
            pass

        endpoint = f"/task/{task_id}"
        params = {"fields": fields}
        try:
            data = await self._request("GET", endpoint, params=params)
            try:
                self._task_cache[cache_key] = {"data": data, "ts": time.time()}
            except Exception:
                pass
            return data
        except Exception as e:
            logger.error(f"Failed to get task {task_id}: {e}")
            return {}

    # ============================================================================
    # COMMENTS
    # ============================================================================

    async def add_comment_to_task(self, task_id, description, owner_id=None, files=None):
        """Добавляет комментарий к задаче в Planfix."""
        endpoint = f"/task/{task_id}/comments/"
        # Санитизируем текст (удаляем emoji и вне BMP)
        description = self._sanitize_text(description) or ""
        
        # Нормализуем ID файлов (убираем префикс "file:" и конвертируем в int)
        normalized_files = []
        if files:
            for f_id in files:
                if f_id is None:
                    continue
                if isinstance(f_id, str) and ':' in f_id:
                    try:
                        f_id = int(f_id.split(':')[-1])
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse file_id: {f_id}")
                        continue
                elif not isinstance(f_id, int):
                    try:
                        f_id = int(f_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert file_id to int: {f_id}")
                        continue
                normalized_files.append(f_id)
        
        data = {
            "description": description,
            "files": [{"id": f_id} for f_id in normalized_files] if normalized_files else []
        }
        if owner_id:
            data["owner"] = {"id": owner_id}  # user:X or contact:Y
        return await self._request("POST", endpoint, data=data)

    async def get_task_comments(self, task_id: int, fields: str = "id,description,owner,dateTime,files", 
                               offset: int = 0, page_size: int = 100):
        """Получает спис��к комментариев задачи."""
        endpoint = f"/task/{task_id}/comments/list"
        data = {
            "offset": offset,
            "pageSize": page_size,
            "fields": fields
        }
        return await self._request("POST", endpoint, data=data)

    # ============================================================================
    # FILES
    # ============================================================================

    async def download_file(self, file_id: int) -> bytes | None:
        """
        Скачивает файл из Planfix по его ID.
        
        Args:
            file_id: ID файла в Planfix
            
        Returns:
            bytes: Содержимое файла или None в случае ошибки
        """
        try:
            # Пробуем использовать REST API endpoint для скачивания файла
            endpoint = f"/file/{file_id}/download"
            logger.debug(f"Attempting to download file {file_id} via REST API endpoint")
            
            # Получаем сессию
            session = await self._get_session()
            
            # Используем прямой запрос к REST API endpoint (он возвращает 302 редирект)
            try:
                # Для загрузки файлов используем увеличенный таймаут
                api_url = f"{self.base_url}{endpoint}"
                file_timeout = aiohttp.ClientTimeout(total=120, connect=15, sock_read=90)
                async with session.get(
                    api_url,
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    allow_redirects=True,  # Следуем редиректу автоматически
                    timeout=file_timeout
                ) as response:
                    if response.status == 200:
                        file_data = await response.read()
                        logger.info(f"Downloaded file {file_id} via REST API, size: {len(file_data)} bytes")
                        return file_data
                    elif response.status == 302:
                        # Получаем URL из заголовка Location
                        redirect_url = response.headers.get('Location')
                        if redirect_url:
                            logger.debug(f"Got redirect URL: {redirect_url}")
                            # Скачиваем файл по редиректу (увеличенный таймаут для больших файлов)
                            file_timeout = aiohttp.ClientTimeout(total=120, connect=15, sock_read=90)
                            async with session.get(
                                redirect_url,
                                headers={"Authorization": f"Bearer {self.api_key}"},
                                timeout=file_timeout
                            ) as redirect_response:
                                if redirect_response.status == 200:
                                    file_data = await redirect_response.read()
                                    logger.info(f"Downloaded file {file_id} via redirect, size: {len(file_data)} bytes")
                                    return file_data
                    else:
                        error_text = await response.text()
                        logger.debug(f"REST API endpoint returned {response.status}: {error_text[:200]}")
            except Exception as api_err:
                logger.debug(f"REST API endpoint failed, trying direct URL: {api_err}")
            
            # Fallback: используем прямой URL (старый способ)
            # Для загрузки файлов используем увеличенный таймаут (файлы могут быть большими)
            file_url = f"{self.base_url.replace('/rest', '')}/?action=getfile&uniqueid={file_id}"
            logger.debug(f"Trying direct URL: {file_url}")
            
            # Увеличенный таймаут для загрузки файлов (до 120 секунд)
            file_timeout = aiohttp.ClientTimeout(total=120, connect=15, sock_read=90)
            async with session.get(
                file_url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=file_timeout
            ) as response:
                if response.status == 200:
                    file_data = await response.read()
                    logger.info(f"Downloaded file {file_id} via direct URL, size: {len(file_data)} bytes")
                    return file_data
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to download file {file_id}: HTTP {response.status}, response: {error_text[:200]}")
                    return None
        except Exception as e:
            logger.error(f"Error downloading file {file_id}: {e}", exc_info=True)
            return None
    
    async def get_file_info(self, file_id: int) -> dict | None:
        """
        Получает информацию о файле из Planfix.
        
        Args:
            file_id: ID файла в Planfix
            
        Returns:
            dict: Информация о файле (name, size, type) или None
        """
        try:
            endpoint = f"/file/{file_id}"
            response = await self._request("GET", endpoint, params={"fields": "id,name,size,type"})
            if response and response.get('result') == 'success':
                return response.get('file', {})
            return None
        except Exception as e:
            logger.error(f"Error getting file info for {file_id}: {e}")
            return None

    async def upload_file(self, file_data, filename, retry_count=0, max_retries=3):
        """Загружает файл в Planfix с обработкой rate limit."""
        endpoint = "/file/"
        url = f"{self.base_url}{endpoint}"
        
        # Используем семафор для ограничения одновременных запросов
        async with self._request_semaphore:
            # Проверяем и ждем если rate limit активен
            while True:
                current_time = time.time()
                wait_until_reset = 0
                
                async with self._rate_limit_lock:
                    # Проверяем глобальную блокировку из-за rate limit
                    if PlanfixAPIClient._rate_limit_until > current_time:
                        wait_until_reset = PlanfixAPIClient._rate_limit_until - current_time
                
                # Ждем вне lock, чтобы другие запросы могли проверить статус
                if wait_until_reset > 0:
                    logger.info(f"⏳ Global rate limit active, waiting {wait_until_reset:.1f}s before file upload to {endpoint}")
                    await asyncio.sleep(wait_until_reset + 1)  # +1 секунда для безопасности
                    continue  # Проверяем снова после ожидания
                
                # Если rate limit не активен, продолжаем
                async with self._rate_limit_lock:
                    current_time = time.time()
                    time_since_last = current_time - PlanfixAPIClient._last_request_time
                    jitter_seconds = random.uniform(0.05, 0.25)
                    if time_since_last < self._min_request_interval:
                        base_wait = self._min_request_interval - time_since_last
                        wait_time = base_wait + jitter_seconds
                        await asyncio.sleep(wait_time)
                    else:
                        await asyncio.sleep(jitter_seconds)
                    PlanfixAPIClient._last_request_time = time.time()
                break  # Выходим из цикла ожидания rate limit

            # Обновляем счетчик суточных запросов и проверяем лимит
            async with self._rate_limit_lock:
                current_time = time.time()
                current_datetime = datetime.now()
                
                # Вычисляем начало следующих суток для сброса счетчика
                if PlanfixAPIClient._daily_reset_time == 0 or current_time >= PlanfixAPIClient._daily_reset_time:
                    # Вычисляем начало следующих суток (00:00:00 следующего дня)
                    next_day = (current_datetime.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
                    
                    PlanfixAPIClient._daily_request_count = 0
                    PlanfixAPIClient._daily_reset_time = next_day.timestamp()
                    
                    hours_until_reset = (next_day - current_datetime).total_seconds() / 3600
                    logger.info(f"📊 Суточный счетчик запросов сброшен. Лимит: {PlanfixAPIClient._daily_request_limit} запросов/сутки. Следующий сброс через {hours_until_reset:.1f} часов")
                
                # Проверяем, не превышен ли суточный лимит
                if PlanfixAPIClient._daily_request_count >= PlanfixAPIClient._daily_request_limit:
                    remaining_until_reset = PlanfixAPIClient._daily_reset_time - current_time
                    hours_until_reset = remaining_until_reset / 3600
                    logger.error(
                        f"🚫 Суточный лимит запросов превышен! "
                        f"Использовано: {PlanfixAPIClient._daily_request_count}/{PlanfixAPIClient._daily_request_limit}. "
                        f"Сброс через {hours_until_reset:.1f} часов"
                    )
                    raise PlanfixRateLimitError(
                        wait_seconds=int(remaining_until_reset),
                        message=f"Daily request limit exceeded ({PlanfixAPIClient._daily_request_count}/{PlanfixAPIClient._daily_request_limit}). Reset in {hours_until_reset:.1f} hours"
                    )
                
                # Увеличиваем счетчик запросов
                PlanfixAPIClient._daily_request_count += 1
                
                # Логируем предупреждение при приближении к лимиту
                if PlanfixAPIClient._daily_request_count % 1000 == 0:
                    remaining = PlanfixAPIClient._daily_request_limit - PlanfixAPIClient._daily_request_count
                    percentage = (PlanfixAPIClient._daily_request_count / PlanfixAPIClient._daily_request_limit) * 100
                    logger.info(f"📊 Запросов за сутки: {PlanfixAPIClient._daily_request_count}/{PlanfixAPIClient._daily_request_limit} ({percentage:.1f}%), осталось: {remaining}")
                elif PlanfixAPIClient._daily_request_count >= PlanfixAPIClient._daily_request_limit * 0.9:
                    remaining = PlanfixAPIClient._daily_request_limit - PlanfixAPIClient._daily_request_count
                    logger.warning(f"⚠️ Приближение к суточному лимиту: использовано {PlanfixAPIClient._daily_request_count}/{PlanfixAPIClient._daily_request_limit}, осталось: {remaining} запросов")

            form = aiohttp.FormData()
            form.add_field('file', file_data, filename=filename, content_type='application/octet-stream')

            session = await self._get_session()
            try:
                async with session.post(
                    f"{self.base_url}{endpoint}", 
                    headers={"Authorization": f"Bearer {self.api_key}"}, 
                    data=form
                ) as response:
                    # Проверяем заголовок X-RateLimit-Remaining для отслеживания оставшихся запросов
                    await self._check_rate_limit_headers(response)
                    
                    response_text = await response.text()
                    
                    # Обрабатываем rate limit ошибки
                    if response.status == 403:
                        try:
                            error_json = json.loads(response_text)
                            if error_json.get('code') == 22:  # Rate limit error
                                time_to_reset = error_json.get('timeToReset')
                                if time_to_reset:
                                    # timeToReset может быть в миллисекундах или секундах
                                    # Если больше 1000, значит в миллисекундах
                                    if time_to_reset > 1000:
                                        wait_time = (time_to_reset / 1000) + 15  # +15 секунд для безопасности
                                    else:
                                        wait_time = time_to_reset + 15  # +15 секунд для безопасности
                                else:
                                    wait_time = 120  # 120 секунд по умолчанию (увеличено с 90)
                                
                                # Устанавливаем глобальную блокировку
                                async with self._rate_limit_lock:
                                    PlanfixAPIClient._rate_limit_until = time.time() + wait_time
                                
                                logger.warning(f"⚠️ Rate limit exceeded during file upload (code 22), установлена глобальная блокировка на {wait_time:.1f}s")
                                
                                # Если не превышен лимит попыток, ждем и повторяем запрос
                                if retry_count < max_retries:
                                    logger.info(f"⏳ Waiting {wait_time:.1f}s and retrying file upload (attempt {retry_count + 1}/{max_retries})")
                                    await asyncio.sleep(wait_time)
                                    # Рекурсивный вызов проверит rate limit автоматически
                                    return await self.upload_file(file_data, filename, retry_count + 1, max_retries)
                                else:
                                    # Превышен лимит попыток - выбрасываем исключение
                                    raise PlanfixRateLimitError(
                                        wait_seconds=int(wait_time),
                                        message=f"Rate limit exceeded after {max_retries} retries during file upload, please wait {int(wait_time)} seconds"
                                    )
                        except json.JSONDecodeError:
                            pass
                    
                    response.raise_for_status()
                    return json.loads(response_text) if response_text else {}
            except PlanfixRateLimitError:
                # Пробрасываем исключение rate limit дальше
                raise
            except aiohttp.ClientResponseError as e:
                logger.error(f"Planfix API file upload error: {e.status} - {e.message}")
                # response уже закрыт после выхода из async with, используем информацию из исключения
                if hasattr(e, 'request_info') and e.request_info:
                    logger.error(f"Request URL: {e.request_info.real_url}")
                if hasattr(e, 'message'):
                    logger.error(f"Error message: {e.message}")
                raise
            except aiohttp.ClientConnectorError as e:
                logger.error(f"Planfix API connection error during file upload: {e}")
                raise
            except Exception as e:
                logger.error(f"An unexpected error occurred during Planfix file upload: {e}")
                raise

    # ============================================================================
    # PROJECTS
    # ============================================================================

    async def get_project_list(self, fields: str = "id,name,description", offset: int = 0, page_size: int = 100):
        """
        Получает список проектов.
        
        Returns:
            dict: {"result": "success", "projects": [{"id": 31904, "name": "..."}]}
        """
        endpoint = "/project/list"
        data = {
            "fields": fields,
            "offset": offset,
            "pageSize": page_size
        }
        return await self._request("POST", endpoint, data=data)

    async def get_project_by_id(self, project_id: int, fields: str = "id,name,description"):
        """Получает информацию о проекте по ID."""
        endpoint = f"/project/{project_id}"
        params = {"fields": fields}
        return await self._request("GET", endpoint, params=params)

    # ============================================================================
    # CUSTOM FIELDS
    # ============================================================================

    async def get_custom_field_info(self, field_id: int):
        """
        Получает информацию о кастомном поле задачи.
        
        Returns:
            dict: Информация о поле (включая directory.id если это Directory entry)
        """
        endpoint = f"/customfield/task/{field_id}"
        return await self._request("GET", endpoint)

    async def get_custom_fields_list(self, fields: str = "id,name,type,directory"):
        """Получает список всех кастомных полей задач."""
        endpoint = "/customfield/task"
        params = {"fields": fields}
        return await self._request("GET", endpoint, params=params)

    async def find_status_id_by_system_names(self, process_id: int, system_names: set[str]) -> int | None:
        """
        Находит ID статуса по ��истемным именам (например, CANCELED, CANCELLED).
        
        Args:
            process_id: ID процесса
            system_names: Множество системных имен для поиска (например, {"CANCELED", "CANCELLED"})
            
        Returns:
            ID статуса или None если не найден
        """
        try:
            resp = await self.get_process_task_statuses(process_id, fields="id,name,systemName")
            statuses = resp.get("statuses", []) if isinstance(resp, dict) else []
            
            for status in statuses:
                sys_name = status.get("systemName") or status.get("name")
                sys_name_norm = str(sys_name).upper().replace(" ", "") if sys_name else ""
                if sys_name_norm in system_names:
                    sid = status.get("id")
                    try:
                        return int(sid)
                    except (TypeError, ValueError):
                        if isinstance(sid, str) and ":" in sid:
                            try:
                                return int(sid.split(":")[-1])
                            except Exception:
                                pass
            return None
        except Exception as e:
            logger.error(f"Error finding status by system names: {e}")
            return None
    
    async def find_status_id_by_names(self, process_id: int, names: set[str]) -> int | None:
        """
        Находит ID статуса по именам (например, "Отменена", "Cancelled", "Canceled").
        
        Args:
            process_id: ID процесса
            names: Множество имен для поиска (например, {"Отменена", "Cancelled", "Canceled"})
            
        Returns:
            ID статуса или None если не найден
        """
        try:
            resp = await self.get_process_task_statuses(process_id, fields="id,name,systemName")
            statuses = resp.get("statuses", []) if isinstance(resp, dict) else []
            
            # Нормализуем искомые имена (приводим к нижнему регистру, убираем пробелы)
            normalized_search_names = {str(name).lower().strip().replace(" ", "") for name in names}
            # Извлекаем корни слов для более гибкого поиска (например, "отмен" из "отменена", "отмененная")
            search_roots = set()
            for name in normalized_search_names:
                search_roots.add(name)
                # Извлекаем корень для русских слов (первые 5-6 символов обычно содержат корень)
                if len(name) > 4:
                    # Для слов типа "отменена", "отмененная" - корень "отмен"
                    if name.startswith("отмен"):
                        search_roots.add("отмен")
                    elif name.startswith("cancel"):
                        search_roots.add("cancel")
            
            logger.debug(f"Searching for status with normalized names: {normalized_search_names}, roots: {search_roots}")
            
            for status in statuses:
                status_name = status.get("name", "")
                status_name_norm = str(status_name).lower().strip().replace(" ", "")
                logger.debug(f"Checking status: '{status_name}' (normalized: '{status_name_norm}')")
                
                # Проверяем точное совпадение
                if status_name_norm in normalized_search_names:
                    sid = status.get("id")
                    try:
                        result_id = int(sid)
                        logger.info(f"Found status by exact match: {result_id} ('{status_name}')")
                        return result_id
                    except (TypeError, ValueError):
                        if isinstance(sid, str) and ":" in sid:
                            try:
                                result_id = int(sid.split(":")[-1])
                                logger.info(f"Found status by exact match: {result_id} ('{status_name}')")
                                return result_id
                            except Exception:
                                pass
                
                # Проверяем частичное совпадение (содержит одно из искомых имен)
                for search_name in normalized_search_names:
                    if search_name in status_name_norm or status_name_norm in search_name:
                        sid = status.get("id")
                        try:
                            result_id = int(sid)
                            logger.info(f"Found status by partial match: {result_id} ('{status_name}') matches '{search_name}'")
                            return result_id
                        except (TypeError, ValueError):
                            if isinstance(sid, str) and ":" in sid:
                                try:
                                    result_id = int(sid.split(":")[-1])
                                    logger.info(f"Found status by partial match: {result_id} ('{status_name}') matches '{search_name}'")
                                    return result_id
                                except Exception:
                                    pass
                
                # Проверяем совпадение по корню слова
                for root in search_roots:
                    if root in status_name_norm:
                        sid = status.get("id")
                        try:
                            result_id = int(sid)
                            logger.info(f"Found status by root match: {result_id} ('{status_name}') contains root '{root}'")
                            return result_id
                        except (TypeError, ValueError):
                            if isinstance(sid, str) and ":" in sid:
                                try:
                                    result_id = int(sid.split(":")[-1])
                                    logger.info(f"Found status by root match: {result_id} ('{status_name}') contains root '{root}'")
                                    return result_id
                                except Exception:
                                    pass
            logger.debug(f"No matching status found among {len(statuses)} statuses")
            return None
        except Exception as e:
            logger.error(f"Error finding status by names: {e}", exc_info=True)
            return None
