#!/bin/bash
# Скрипт для остановки найденных процессов бота

echo "🛑 Остановка процессов бота..."
echo ""

# Процесс от root
ROOT_PID=1179
if ps -p $ROOT_PID > /dev/null 2>&1; then
    echo "Останавливаю процесс root (PID $ROOT_PID)..."
    sudo kill $ROOT_PID
    sleep 2
    if ps -p $ROOT_PID > /dev/null 2>&1; then
        echo "Принудительная остановка процесса $ROOT_PID..."
        sudo kill -9 $ROOT_PID
    fi
    echo "✅ Процесс root остановлен"
else
    echo "⚠️  Процесс root (PID $ROOT_PID) не найден"
fi

echo ""

# Screen сессия
SCREEN_PID=7431
if ps -p $SCREEN_PID > /dev/null 2>&1; then
    echo "Останавливаю screen сессию (PID $SCREEN_PID)..."
    # Останавливаем screen сессию по имени
    screen -S planfix_bot -X quit 2>/dev/null || kill $SCREEN_PID
    sleep 2
    if ps -p $SCREEN_PID > /dev/null 2>&1; then
        echo "Принудительная остановка screen сессии..."
        kill -9 $SCREEN_PID
    fi
    echo "✅ Screen сессия остановлена"
else
    echo "⚠️  Screen сессия (PID $SCREEN_PID) не найдена"
fi

echo ""

# Python процесс main.py
PYTHON_PID=7433
if ps -p $PYTHON_PID > /dev/null 2>&1; then
    echo "Останавливаю Python процесс (PID $PYTHON_PID)..."
    kill $PYTHON_PID
    sleep 2
    if ps -p $PYTHON_PID > /dev/null 2>&1; then
        echo "Принудительная остановка процесса $PYTHON_PID..."
        kill -9 $PYTHON_PID
    fi
    echo "✅ Python процесс остановлен"
else
    echo "⚠️  Python процесс (PID $PYTHON_PID) не найден"
fi

echo ""
echo "🔍 Проверка оставшихся процессов..."
REMAINING=$(ps aux | grep -E "(main\.py|python.*main|bot\.main)" | grep -v grep)
if [ -z "$REMAINING" ]; then
    echo "✅ Все процессы бота остановлены!"
else
    echo "⚠️  Найдены оставшиеся процессы:"
    echo "$REMAINING"
    echo ""
    echo "💡 Для остановки используйте:"
    echo "   kill <PID>"
fi

