#!/bin/bash
# Скрипт для исправления конфликтующих конфигураций nginx

set -e

echo "=========================================="
echo "Исправление конфликтующих конфигураций nginx"
echo "=========================================="
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "Ошибка: Запустите скрипт с правами root (sudo)"
    exit 1
fi

# Файлы для исправления
FILES_TO_FIX=(
    "/etc/nginx/sites-available/planfix-webhook"
    "/etc/nginx/sites-available/crmbot"
)

# Создаем резервную копию
BACKUP_DIR="/tmp/nginx_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
echo "Создана резервная копия в: $BACKUP_DIR"
echo ""

# Исправляем каждый файл
for FILE in "${FILES_TO_FIX[@]}"; do
    if [ ! -f "$FILE" ]; then
        echo "⚠️  Файл не найден: $FILE"
        continue
    fi
    
    echo "Обработка: $FILE"
    
    # Создаем резервную копию
    BACKUP_FILE="$BACKUP_DIR/$(basename $FILE).backup"
    cp "$FILE" "$BACKUP_FILE"
    echo "  Резервная копия: $BACKUP_FILE"
    
    # Проверяем, есть ли порт 8001
    if grep -q "8001" "$FILE"; then
        echo "  Найден порт 8001, исправляем на 8080..."
        
        # Заменяем порт
        sed -i 's/127.0.0.1:8001/127.0.0.1:8080/g' "$FILE"
        sed -i 's/localhost:8001/localhost:8080/g' "$FILE"
        sed -i 's/:8001/:8080/g' "$FILE"
        
        # Проверяем результат
        if grep -q "8001" "$FILE"; then
            echo "  ⚠️  Внимание: В файле все еще есть упоминания 8001"
            grep "8001" "$FILE" | head -3
        else
            echo "  ✓ Порт исправлен на 8080"
        fi
        
        # Показываем изменения
        echo "  Изменения:"
        grep "proxy_pass.*8080" "$FILE" | head -3 | sed 's/^/    /'
    else
        echo "  ✓ Порт 8001 не найден в файле"
    fi
    
    echo ""
done

# Проверка конфигурации nginx
echo "Проверка конфигурации nginx..."
if nginx -t 2>&1 | tee /tmp/nginx_test.log; then
    echo ""
    echo "✓ Конфигурация nginx корректна"
    
    # Показываем, какие конфиги активны для crmbot.restme.pro
    echo ""
    echo "Активные конфигурации для crmbot.restme.pro:"
    for FILE in /etc/nginx/sites-enabled/*; do
        if [ -L "$FILE" ] && grep -q "crmbot.restme.pro" "$FILE" 2>/dev/null; then
            echo "  - $FILE -> $(readlink -f $FILE)"
            echo "    proxy_pass: $(grep 'proxy_pass' "$FILE" | head -1 | sed 's/^[[:space:]]*//')"
        fi
    done
    
    echo ""
    echo "⚠️  ВНИМАНИЕ: У вас несколько активных конфигов для одного домена!"
    echo "   Это может вызывать конфликты. Рекомендуется:"
    echo "   1. Оставить только один конфиг (telegram-bot-webhook)"
    echo "   2. Или объединить конфигурации в один файл"
    echo ""
    echo "   Для отключения старых конфигов:"
    echo "   sudo rm /etc/nginx/sites-enabled/planfix-webhook"
    echo "   sudo rm /etc/nginx/sites-enabled/crmbot"
    echo ""
    
    read -p "Перезагрузить nginx? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if systemctl reload nginx; then
            echo "✓ nginx успешно перезагружен"
        else
            echo "✗ Ошибка при перезагрузке nginx!"
            exit 1
        fi
    else
        echo "Пропущена перезагрузка. Выполните вручную: sudo systemctl reload nginx"
    fi
else
    echo ""
    echo "✗ Ошибка в конфигурации nginx!"
    echo "Восстанавливаем из резервной копии..."
    
    for FILE in "${FILES_TO_FIX[@]}"; do
        if [ ! -f "$FILE" ]; then
            continue
        fi
        BACKUP_FILE="$BACKUP_DIR/$(basename $FILE).backup"
        if [ -f "$BACKUP_FILE" ]; then
            cp "$BACKUP_FILE" "$FILE"
            echo "  Восстановлен: $FILE"
        fi
    done
    
    exit 1
fi

echo ""
echo "=========================================="
echo "Готово!"
echo "=========================================="
echo ""
echo "Резервная копия сохранена в: $BACKUP_DIR"
echo ""
echo "Проверьте работу:"
echo "  curl http://crmbot.restme.pro/health"
echo "  curl -u webhook_user:ваш_пароль http://crmbot.restme.pro/planfix/webhook"
echo ""

