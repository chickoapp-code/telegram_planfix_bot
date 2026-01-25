#!/bin/bash
# Диагностика проблемы 502 Bad Gateway для webhook

echo "=========================================="
echo "Диагностика 502 Bad Gateway"
echo "=========================================="
echo ""

# 1. Проверка, запущен ли webhook сервер
echo "1. Проверка процессов бота:"
BOT_PROCESSES=$(ps aux | grep -E "python.*main\.py|python3.*main\.py" | grep -v grep)
if [ -z "$BOT_PROCESSES" ]; then
    echo "   ✗ Бот НЕ запущен!"
    echo "   Запустите бота: python main.py"
else
    echo "   ✓ Бот запущен:"
    echo "$BOT_PROCESSES" | head -3
fi

echo ""
echo "2. Проверка порта 8080:"
if command -v ss &> /dev/null; then
    PORT_INFO=$(ss -tuln | grep ":8080")
elif command -v netstat &> /dev/null; then
    PORT_INFO=$(netstat -tuln | grep ":8080")
else
    PORT_INFO=""
fi

if [ -z "$PORT_INFO" ]; then
    echo "   ✗ Порт 8080 НЕ слушается!"
    echo "   Webhook сервер не запущен или слушает другой порт"
else
    echo "   ✓ Порт 8080 слушается:"
    echo "   $PORT_INFO"
fi

echo ""
echo "3. Проверка подключения к localhost:8080:"
HEALTH_RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}" --connect-timeout 2 http://127.0.0.1:8080/health 2>&1)
if echo "$HEALTH_RESPONSE" | grep -q "HTTP_CODE:200\|OK"; then
    echo "   ✓ Webhook сервер отвечает!"
    echo "$HEALTH_RESPONSE" | grep -v "HTTP_CODE"
else
    echo "   ✗ Webhook сервер НЕ отвечает на http://127.0.0.1:8080/health"
    echo "   Попытка подключения:"
    curl -v http://127.0.0.1:8080/health 2>&1 | head -15
fi

echo ""
echo "4. Проверка логов nginx (последние ошибки):"
if [ -f /var/log/nginx/error.log ]; then
    echo "   Последние ошибки, связанные с webhook:"
    sudo tail -20 /var/log/nginx/error.log | grep -i "webhook\|8080\|502\|upstream\|connect" || echo "   Нет ошибок в логах"
else
    echo "   Файл /var/log/nginx/error.log не найден"
fi

echo ""
echo "5. Проверка конфигурации nginx:"
if [ -f /etc/nginx/sites-enabled/telegram-bot-webhook ]; then
    echo "   ✓ Конфигурация найдена"
    echo "   proxy_pass:"
    grep "proxy_pass" /etc/nginx/sites-enabled/telegram-bot-webhook | head -1
else
    echo "   ✗ Конфигурация не найдена в /etc/nginx/sites-enabled/"
    echo "   Проверьте, создан ли симлинк"
fi

echo ""
echo "6. Проверка .env файла:"
if [ -f .env ]; then
    echo "   WEBHOOK_HOST: $(grep -E '^WEBHOOK_HOST=' .env | cut -d'=' -f2 || echo 'не найден')"
    echo "   WEBHOOK_PORT: $(grep -E '^WEBHOOK_PORT=' .env | cut -d'=' -f2 || echo 'не найден')"
else
    echo "   ✗ Файл .env не найден"
fi

echo ""
echo "=========================================="
echo "РЕШЕНИЕ ПРОБЛЕМЫ:"
echo "=========================================="
echo ""
echo "Если бот не запущен, запустите его:"
echo "  cd ~/telegram_planfix_bot"
echo "  source venv/bin/activate  # если используете venv"
echo "  python main.py"
echo ""
echo "Или через systemd (если настроен сервис):"
echo "  sudo systemctl status telegram-planfix-bot"
echo "  sudo systemctl start telegram-planfix-bot"
echo ""
echo "Проверьте, что в .env файле указано:"
echo "  WEBHOOK_HOST=127.0.0.1"
echo "  WEBHOOK_PORT=8080"
echo ""

