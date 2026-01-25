#!/bin/bash
# Скрипт для проверки и освобождения порта 8080

echo "=========================================="
echo "Проверка порта 8080"
echo "=========================================="
echo ""

# Проверка процессов на порту 8080
echo "1. Процессы, использующие порт 8080:"
if command -v lsof &> /dev/null; then
    sudo lsof -i :8080 2>/dev/null || echo "   Порт 8080 свободен (lsof)"
elif command -v netstat &> /dev/null; then
    sudo netstat -tulpn | grep :8080 || echo "   Порт 8080 свободен (netstat)"
elif command -v ss &> /dev/null; then
    sudo ss -tulpn | grep :8080 || echo "   Порт 8080 свободен (ss)"
else
    echo "   Не найдены утилиты для проверки порта"
fi

echo ""
echo "2. Процессы Python (main.py):"
ps aux | grep -E "python.*main\.py|python3.*main\.py" | grep -v grep || echo "   Процессы не найдены"

echo ""
echo "3. Статус systemd сервиса:"
if systemctl is-active --quiet telegram-planfix-bot 2>/dev/null; then
    echo "   ✓ Сервис telegram-planfix-bot активен"
    systemctl status telegram-planfix-bot --no-pager -l | head -15
else
    echo "   ✗ Сервис telegram-planfix-bot не активен"
fi

echo ""
echo "=========================================="
echo "РЕШЕНИЕ:"
echo "=========================================="
echo ""
echo "Если порт занят другим процессом бота:"
echo "  1. Остановите systemd сервис:"
echo "     sudo systemctl stop telegram-planfix-bot"
echo ""
echo "  2. Или найдите и остановите процесс вручную:"
echo "     sudo lsof -ti:8080 | xargs kill -9"
echo "     # или"
echo "     sudo pkill -f 'python.*main.py'"
echo ""
echo "  3. Проверьте, что порт свободен:"
echo "     sudo lsof -i :8080"
echo ""
echo "  4. Запустите бота снова:"
echo "     python main.py"
echo "     # или через systemd:"
echo "     sudo systemctl start telegram-planfix-bot"
echo ""

