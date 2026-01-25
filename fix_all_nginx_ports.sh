#!/bin/bash
# Скрипт для автоматического исправления всех портов 8001 на 8080 в nginx

set -e

echo "=========================================="
echo "Исправление всех портов 8001 -> 8080 в nginx"
echo "=========================================="
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "Ошибка: Запустите скрипт с правами root (sudo)"
    exit 1
fi

# Поиск всех файлов с портом 8001
echo "Поиск файлов с портом 8001..."
FILES_WITH_8001=$(grep -r "8001" /etc/nginx/ 2>/dev/null | cut -d: -f1 | sort -u)

if [ -z "$FILES_WITH_8001" ]; then
    echo "✓ Файлов с портом 8001 не найдено"
    exit 0
fi

echo "Найдено файлов: $(echo "$FILES_WITH_8001" | wc -l)"
echo ""

# Создаем резервную копию всех файлов
BACKUP_DIR="/tmp/nginx_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
echo "Создана резервная копия в: $BACKUP_DIR"

# Обрабатываем каждый файл
for FILE in $FILES_WITH_8001; do
    if [ ! -f "$FILE" ]; then
        continue
    fi
    
    echo "Обработка: $FILE"
    
    # Создаем резервную копию
    RELATIVE_PATH=$(echo "$FILE" | sed 's|/etc/nginx/||')
    BACKUP_FILE="$BACKUP_DIR/${RELATIVE_PATH//\//_}"
    mkdir -p "$(dirname "$BACKUP_FILE")" 2>/dev/null || true
    cp "$FILE" "$BACKUP_FILE"
    
    # Заменяем порт
    sed -i 's/127.0.0.1:8001/127.0.0.1:8080/g' "$FILE"
    sed -i 's/localhost:8001/localhost:8080/g' "$FILE"
    sed -i 's/:8001/:8080/g' "$FILE"
    
    # Проверяем, что замена произошла
    if grep -q "8001" "$FILE"; then
        echo "  ⚠️  Внимание: В файле все еще есть упоминания 8001"
        grep "8001" "$FILE" | head -3
    else
        echo "  ✓ Порт исправлен"
    fi
done

echo ""
echo "Проверка конфигурации nginx..."
if nginx -t 2>&1 | tee /tmp/nginx_test.log; then
    echo ""
    echo "✓ Конфигурация nginx корректна"
    
    # Показываем, что было изменено
    echo ""
    echo "Изменения:"
    for FILE in $FILES_WITH_8001; do
        if [ -f "$FILE" ]; then
            echo "  $FILE:"
            grep "8080\|proxy_pass" "$FILE" | grep -v "^#" | head -2 | sed 's/^/    /'
        fi
    done
    
    echo ""
    echo "Перезагрузка nginx..."
    if systemctl reload nginx; then
        echo "✓ nginx успешно перезагружен"
    else
        echo "✗ Ошибка при перезагрузке nginx!"
        echo "Восстановите из резервной копии: $BACKUP_DIR"
        exit 1
    fi
else
    echo ""
    echo "✗ Ошибка в конфигурации nginx!"
    echo "Восстанавливаем из резервной копии..."
    
    for FILE in $FILES_WITH_8001; do
        if [ ! -f "$FILE" ]; then
            continue
        fi
        RELATIVE_PATH=$(echo "$FILE" | sed 's|/etc/nginx/||')
        BACKUP_FILE="$BACKUP_DIR/${RELATIVE_PATH//\//_}"
        if [ -f "$BACKUP_FILE" ]; then
            cp "$BACKUP_FILE" "$FILE"
            echo "  Восстановлен: $FILE"
        fi
    done
    
    exit 1
fi

echo ""
echo "=========================================="
echo "Готово! Все порты исправлены на 8080"
echo "=========================================="
echo ""
echo "Резервная копия сохранена в: $BACKUP_DIR"
echo ""
echo "Проверьте работу:"
echo "  curl http://crmbot.restme.pro/health"
echo "  curl -u webhook_user:ваш_пароль http://crmbot.restme.pro/planfix/webhook"
echo ""

