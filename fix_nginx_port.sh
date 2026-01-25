#!/bin/bash
# Скрипт для исправления порта в конфигурации nginx

set -e

echo "=========================================="
echo "Исправление порта в конфигурации nginx"
echo "=========================================="
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "Ошибка: Запустите скрипт с правами root (sudo)"
    exit 1
fi

# Поиск конфигурации с неправильным портом
CONFIG_FILE=""
if [ -f /etc/nginx/sites-enabled/telegram-bot-webhook ]; then
    CONFIG_FILE="/etc/nginx/sites-enabled/telegram-bot-webhook"
elif [ -f /etc/nginx/sites-enabled/default ]; then
    CONFIG_FILE="/etc/nginx/sites-enabled/default"
else
    # Ищем все конфиги с упоминанием webhook или 8001
    CONFIG_FILE=$(grep -r "8001\|webhook" /etc/nginx/sites-enabled/ 2>/dev/null | head -1 | cut -d: -f1)
fi

if [ -z "$CONFIG_FILE" ]; then
    echo "Ошибка: Не найдена конфигурация nginx"
    echo "Проверьте файлы в /etc/nginx/sites-enabled/"
    exit 1
fi

echo "Найден конфигурационный файл: $CONFIG_FILE"
echo ""

# Проверка текущего порта
CURRENT_PORT=$(grep -o "127.0.0.1:[0-9]*" "$CONFIG_FILE" | head -1 | cut -d: -f2)
echo "Текущий порт в конфигурации: $CURRENT_PORT"

if [ "$CURRENT_PORT" = "8080" ]; then
    echo "✓ Порт уже правильный (8080)"
    exit 0
fi

echo ""
echo "Исправление порта с $CURRENT_PORT на 8080..."

# Создаем резервную копию
BACKUP_FILE="${CONFIG_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
cp "$CONFIG_FILE" "$BACKUP_FILE"
echo "Создана резервная копия: $BACKUP_FILE"

# Заменяем порт
if [[ "$CONFIG_FILE" == *"telegram-bot-webhook"* ]]; then
    # Если это наш конфиг, просто заменим порт
    sed -i 's/127.0.0.1:8001/127.0.0.1:8080/g' "$CONFIG_FILE"
    sed -i 's/localhost:8001/localhost:8080/g' "$CONFIG_FILE"
else
    # Если это другой конфиг, нужно быть осторожнее
    sed -i 's/127.0.0.1:8001/127.0.0.1:8080/g' "$CONFIG_FILE"
    sed -i 's/localhost:8001/localhost:8080/g' "$CONFIG_FILE"
fi

echo "✓ Порт исправлен"

# Проверка результата
NEW_PORT=$(grep -o "127.0.0.1:[0-9]*" "$CONFIG_FILE" | head -1 | cut -d: -f2)
echo "Новый порт в конфигурации: $NEW_PORT"

if [ "$NEW_PORT" != "8080" ]; then
    echo "⚠️  Внимание: Порт не изменился. Проверьте конфигурацию вручную"
    echo "Файл: $CONFIG_FILE"
    exit 1
fi

# Проверка конфигурации nginx
echo ""
echo "Проверка конфигурации nginx..."
if nginx -t; then
    echo "✓ Конфигурация nginx корректна"
else
    echo "✗ Ошибка в конфигурации nginx!"
    echo "Восстанавливаем резервную копию..."
    cp "$BACKUP_FILE" "$CONFIG_FILE"
    exit 1
fi

# Перезагрузка nginx
echo ""
echo "Перезагрузка nginx..."
if systemctl reload nginx; then
    echo "✓ nginx успешно перезагружен"
else
    echo "✗ Ошибка при перезагрузке nginx!"
    exit 1
fi

echo ""
echo "=========================================="
echo "Готово! Порт исправлен на 8080"
echo "=========================================="
echo ""
echo "Проверьте работу:"
echo "  curl -u webhook_user:ваш_пароль http://crmbot.restme.pro/planfix/webhook"
echo ""

