from __future__ import annotations

import asyncio
from typing import Any, Callable

from db_manager import DBManager


class AsyncDBManager:
    """Асинхронная обёртка над синхронным DBManager с выполнением операций в пуле потоков."""

    _sync_attrs = {"db_session", "get_db"}

    def __init__(self, manager: DBManager | None = None):
        self._manager = manager or DBManager()

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._manager, name)
        if name in self._sync_attrs or not callable(attr):
            return attr

        async def async_wrapper(*args, **kwargs):
            return await asyncio.to_thread(self._call_with_session, attr, args, kwargs)

        return async_wrapper

    async def run(self, func: Callable, *args, **kwargs) -> Any:
        """Выполнить произвольную функцию с сессией БД в пуле потоков."""
        return await asyncio.to_thread(self._call_with_session, func, args, kwargs)

    def _call_with_session(self, method: Callable, args: tuple, kwargs: dict) -> Any:
        with self._manager.get_db() as db:
            return method(db, *args, **kwargs)


db_manager = AsyncDBManager()

__all__ = ["db_manager", "AsyncDBManager"]

