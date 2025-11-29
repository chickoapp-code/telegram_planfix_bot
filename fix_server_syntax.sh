#!/bin/bash
# Скрипт для исправления проблем с синтаксисом на сервере

cd /home/dev_bot/telegram_planfix_bot || exit 1

echo "🔍 Проверка текущего состояния..."
echo ""

# 1. Проверяем статус git
echo "📋 Git status:"
git status --short
echo ""

# 2. Убеждаемся, что мы на правильной ветке и версии
echo "🔄 Обновление из репозитория..."
git fetch origin
git reset --hard origin/main
echo ""

# 3. Удаляем все .pyc файлы и __pycache__ директории
echo "🧹 Очистка кэша Python..."
find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
find . -type f -name "*.pyo" -delete 2>/dev/null || true
echo "✅ Кэш очищен"
echo ""

# 4. Проверяем синтаксис файлов
echo "🔍 Проверка синтаксиса..."
if [ -f ".venv/bin/python3" ]; then
    PYTHON=".venv/bin/python3"
elif [ -f "venv/bin/python3" ]; then
    PYTHON="venv/bin/python3"
else
    PYTHON="python3"
fi

echo "Используется Python: $PYTHON"
$PYTHON -m py_compile webhook_server.py 2>&1 && echo "✅ webhook_server.py: синтаксис корректен" || echo "❌ webhook_server.py: ошибка синтаксиса"
$PYTHON -m py_compile main.py 2>&1 && echo "✅ main.py: синтаксис корректен" || echo "❌ main.py: ошибка синтаксиса"
echo ""

# 5. Показываем строку 742 из webhook_server.py
echo "📄 Строка 742 из webhook_server.py:"
sed -n '740,745p' webhook_server.py
echo ""

# 6. Проверяем структуру try-except вокруг строки 742
echo "📄 Структура try-except вокруг строки 742:"
sed -n '735,750p' webhook_server.py
echo ""

echo "✅ Проверка завершена. Если все файлы корректны, перезапустите сервис:"
echo "   sudo systemctl restart telegram-planfix-bot"
echo "   sudo journalctl -u telegram-planfix-bot -f"

