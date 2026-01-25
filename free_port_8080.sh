#!/bin/bash
# Скрипт для освобождения порта 8080

set -e

echo "=========================================="
echo "Освобождение порта 8080"
echo "=========================================="
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "⚠️  Запустите с sudo для полного доступа"
    echo ""
fi

# 1. Проверяем systemd сервис
echo "1. Проверка systemd сервиса:"
if systemctl is-active --quiet telegram-planfix-bot 2>/dev/null; then
    echo "   ✓ Сервис telegram-planfix-bot активен"
    echo "   Останавливаем сервис..."
    sudo systemctl stop telegram-planfix-bot
    sleep 2
    echo "   ✓ Сервис остановлен"
else
    echo "   ✗ Сервис telegram-planfix-bot не активен"
fi

echo ""
echo "2. Поиск процессов на порту 8080:"

# Используем lsof, если доступен
if command -v lsof &> /dev/null; then
    PIDS=$(sudo lsof -ti:8080 2>/dev/null || true)
    if [ -n "$PIDS" ]; then
        echo "   Найдены процессы: $PIDS"
        echo "   Останавливаем процессы..."
        echo "$PIDS" | xargs sudo kill -9 2>/dev/null || true
        sleep 1
        echo "   ✓ Процессы остановлены"
    else
        echo "   ✓ Порт 8080 свободен"
    fi
# Используем netstat, если lsof недоступен
elif command -v netstat &> /dev/null; then
    PID=$(sudo netstat -tulpn 2>/dev/null | grep :8080 | awk '{print $7}' | cut -d'/' -f1 | head -1)
    if [ -n "$PID" ] && [ "$PID" != "-" ]; then
        echo "   Найден процесс: $PID"
        echo "   Останавливаем процесс..."
        sudo kill -9 "$PID" 2>/dev/null || true
        sleep 1
        echo "   ✓ Процесс остановлен"
    else
        echo "   ✓ Порт 8080 свободен"
    fi
# Используем ss, если netstat недоступен
elif command -v ss &> /dev/null; then
    PID=$(sudo ss -tulpn 2>/dev/null | grep :8080 | awk '{print $6}' | cut -d',' -f2 | cut -d'=' -f2 | head -1)
    if [ -n "$PID" ] && [ "$PID" != "-" ]; then
        echo "   Найден процесс: $PID"
        echo "   Останавливаем процесс..."
        sudo kill -9 "$PID" 2>/dev/null || true
        sleep 1
        echo "   ✓ Процесс остановлен"
    else
        echo "   ✓ Порт 8080 свободен"
    fi
else
    echo "   ⚠️  Не найдены утилиты для проверки порта"
fi

echo ""
echo "3. Поиск процессов Python (main.py):"
PYTHON_PIDS=$(ps aux | grep -E "python.*main\.py|python3.*main\.py" | grep -v grep | awk '{print $2}' || true)
if [ -n "$PYTHON_PIDS" ]; then
    echo "   Найдены процессы Python: $PYTHON_PIDS"
    echo "   Останавливаем процессы..."
    echo "$PYTHON_PIDS" | xargs kill -9 2>/dev/null || true
    sleep 1
    echo "   ✓ Процессы остановлены"
else
    echo "   ✓ Процессы Python не найдены"
fi

echo ""
echo "4. Финальная проверка порта 8080:"
sleep 1
if command -v lsof &> /dev/null; then
    if sudo lsof -i :8080 2>/dev/null | grep -q LISTEN; then
        echo "   ⚠️  Порт 8080 все еще занят"
        sudo lsof -i :8080
    else
        echo "   ✓ Порт 8080 свободен"
    fi
elif command -v netstat &> /dev/null; then
    if sudo netstat -tulpn 2>/dev/null | grep -q :8080; then
        echo "   ⚠️  Порт 8080 все еще занят"
        sudo netstat -tulpn | grep :8080
    else
        echo "   ✓ Порт 8080 свободен"
    fi
elif command -v ss &> /dev/null; then
    if sudo ss -tulpn 2>/dev/null | grep -q :8080; then
        echo "   ⚠️  Порт 8080 все еще занят"
        sudo ss -tulpn | grep :8080
    else
        echo "   ✓ Порт 8080 свободен"
    fi
fi

echo ""
echo "=========================================="
echo "Готово!"
echo "=========================================="
echo ""
echo "Теперь вы можете запустить бота:"
echo "  python main.py"
echo ""
echo "Или через systemd:"
echo "  sudo systemctl start telegram-planfix-bot"
echo ""

