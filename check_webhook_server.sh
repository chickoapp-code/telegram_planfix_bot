#!/bin/bash
# Скрипт для проверки состояния webhook сервера

echo "=========================================="
echo "Проверка webhook сервера"
echo "=========================================="
echo ""

# Проверка, запущен ли webhook сервер на порту 8080
echo "1. Проверка порта 8080:"
if netstat -tuln 2>/dev/null | grep -q ":8080"; then
    echo "   ✓ Порт 8080 открыт"
    netstat -tuln | grep ":8080"
else
    echo "   ✗ Порт 8080 не слушается"
fi

echo ""
echo "2. Проверка процессов Python:"
ps aux | grep -E "(python|python3)" | grep -v grep | head -5

echo ""
echo "3. Проверка подключения к localhost:8080:"
if curl -s -o /dev/null -w "%{http_code}" --connect-timeout 2 http://127.0.0.1:8080/health 2>/dev/null | grep -q "200\|OK"; then
    echo "   ✓ Webhook сервер отвечает на /health"
    curl -s http://127.0.0.1:8080/health
    echo ""
else
    echo "   ✗ Webhook сервер не отвечает на http://127.0.0.1:8080/health"
    echo "   Попытка подключения..."
    curl -v http://127.0.0.1:8080/health 2>&1 | head -10
fi

echo ""
echo "4. Проверка логов nginx (последние ошибки):"
if [ -f /var/log/nginx/error.log ]; then
    echo "   Последние ошибки из /var/log/nginx/error.log:"
    sudo tail -5 /var/log/nginx/error.log | grep -i "webhook\|8080\|502" || echo "   Нет ошибок, связанных с webhook"
else
    echo "   Файл /var/log/nginx/error.log не найден"
fi

echo ""
echo "5. Проверка конфигурации nginx:"
if [ -f /etc/nginx/sites-enabled/telegram-bot-webhook ]; then
    echo "   ✓ Конфигурация найдена"
    echo "   Проверка proxy_pass:"
    grep "proxy_pass" /etc/nginx/sites-enabled/telegram-bot-webhook
else
    echo "   ✗ Конфигурация /etc/nginx/sites-enabled/telegram-bot-webhook не найдена"
fi

echo ""
echo "=========================================="
echo "Рекомендации:"
echo "=========================================="
echo "1. Убедитесь, что webhook сервер запущен:"
echo "   - Проверьте, запущен ли бот: ps aux | grep main.py"
echo "   - Проверьте логи бота"
echo ""
echo "2. Проверьте настройки в .env файле:"
echo "   - WEBHOOK_HOST должен быть 127.0.0.1"
echo "   - WEBHOOK_PORT должен быть 8080"
echo ""
echo "3. Проверьте, что бот запущен с webhook сервером:"
echo "   - Бот должен быть запущен в режиме 'BOTH' (Polling + Webhook)"
echo ""

