# -*- coding: utf-8 -*-
"""
Общий TTL-кэш для всего бота.
Используется для хранения вычисленных значений, например имени ресторана по task_id.

Версия: 1.0
"""

import time
from typing import Any, Optional


class TTLCache:
    def __init__(self):
        self._store = {}

    def get(self, key: str) -> Optional[Any]:
        item = self._store.get(key)
        if not item:
            return None
        value, exp = item
        if exp is not None and exp < time.time():
            try:
                del self._store[key]
            except Exception:
                pass
            return None
        return value

    def set(self, key: str, value: Any, ttl_seconds: int = 300):
        exp = (time.time() + ttl_seconds) if ttl_seconds else None
        self._store[key] = (value, exp)


# Глобальный экземпляр кэша, импортируемый из других модулей
cache = TTLCache()
