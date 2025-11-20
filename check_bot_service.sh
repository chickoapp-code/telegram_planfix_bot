#!/bin/bash
# Скрипт для проверки systemd сервисов бота

echo "🔍 Проверка systemd сервисов..."
echo ""

# Ищем сервисы, связанные с ботом
SERVICES=$(systemctl list-units --type=service --all | grep -E "(bot|telegram|planfix)" | awk '{print $1}')

if [ -z "$SERVICES" ]; then
    echo "❌ Сервисы бота не найдены в systemd"
else
    echo "Найдены сервисы:"
    for service in $SERVICES; do
        echo "  - $service"
        systemctl status $service --no-pager | head -5
        echo ""
    done
    
    echo "💡 Для остановки сервиса:"
    echo "   sudo systemctl stop <service_name>"
    echo ""
    echo "💡 Для отключения автозапуска:"
    echo "   sudo systemctl disable <service_name>"
fi

echo ""
echo "🔍 Проверка supervisor..."
if command -v supervisorctl &> /dev/null; then
    echo "Найдены процессы в supervisor:"
    supervisorctl status | grep -E "(bot|telegram|planfix)" || echo "  (не найдено)"
    echo ""
    echo "💡 Для остановки:"
    echo "   supervisorctl stop <process_name>"
else
    echo "❌ Supervisor не установлен"
fi

echo ""
echo "🔍 Проверка screen сессий..."
SCREENS=$(screen -ls | grep -E "(bot|telegram|planfix)" || echo "")
if [ -n "$SCREENS" ]; then
    echo "Найдены screen сессии:"
    screen -ls | grep -E "(bot|telegram|planfix)"
    echo ""
    echo "💡 Для остановки:"
    echo "   screen -S <session_name> -X quit"
else
    echo "❌ Screen сессии не найдены"
fi

echo ""
echo "🔍 Проверка tmux сессий..."
TMUX_SESSIONS=$(tmux ls 2>/dev/null | grep -E "(bot|telegram|planfix)" || echo "")
if [ -n "$TMUX_SESSIONS" ]; then
    echo "Найдены tmux сессии:"
    tmux ls | grep -E "(bot|telegram|planfix)"
    echo ""
    echo "💡 Для остановки:"
    echo "   tmux kill-session -t <session_name>"
else
    echo "❌ Tmux сессии не найдены"
fi

