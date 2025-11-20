#!/bin/bash
# Скрипт для остановки всех процессов бота

echo "🛑 Остановка процессов бота..."
echo ""

# Ищем процессы
PIDS=$(ps aux | grep -E "(main\.py|telegram.*bot|planfix.*bot)" | grep -v grep | awk '{print $2}')

if [ -z "$PIDS" ]; then
    echo "❌ Процессы бота не найдены"
    exit 0
fi

echo "Найдены процессы:"
ps aux | grep -E "(main\.py|telegram.*bot|planfix.*bot)" | grep -v grep

echo ""
read -p "Остановить эти процессы? (yes/no): " confirm

if [ "$confirm" = "yes" ]; then
    for PID in $PIDS; do
        echo "Останавливаю процесс $PID..."
        kill $PID
        sleep 1
        # Проверяем, остановился ли
        if ps -p $PID > /dev/null 2>&1; then
            echo "Принудительная остановка процесса $PID..."
            kill -9 $PID
        fi
    done
    echo "✅ Процессы остановлены"
else
    echo "❌ Отменено"
fi

