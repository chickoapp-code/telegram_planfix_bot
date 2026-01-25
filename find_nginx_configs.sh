#!/bin/bash
# Скрипт для поиска всех конфигураций nginx с портом 8001

echo "=========================================="
echo "Поиск всех конфигураций nginx"
echo "=========================================="
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "⚠️  Запустите с sudo для полного доступа"
    echo ""
fi

echo "1. Поиск всех упоминаний порта 8001:"
echo "----------------------------------------"
grep -r "8001" /etc/nginx/ 2>/dev/null | grep -v ".swp" | grep -v "~" || echo "   Не найдено"

echo ""
echo "2. Поиск всех упоминаний порта 8080:"
echo "----------------------------------------"
grep -r "8080" /etc/nginx/ 2>/dev/null | grep -v ".swp" | grep -v "~" || echo "   Не найдено"

echo ""
echo "3. Все активные конфигурации в sites-enabled:"
echo "----------------------------------------"
ls -la /etc/nginx/sites-enabled/ 2>/dev/null || echo "   Директория не найдена"

echo ""
echo "4. Все конфигурации в sites-available:"
echo "----------------------------------------"
ls -la /etc/nginx/sites-available/ 2>/dev/null || echo "   Директория не найдена"

echo ""
echo "5. Проверка основного конфига nginx.conf:"
echo "----------------------------------------"
if [ -f /etc/nginx/nginx.conf ]; then
    echo "   Файл найден"
    # Проверяем, есть ли include директивы
    grep -E "include|server_name.*crmbot" /etc/nginx/nginx.conf | head -10
else
    echo "   Файл не найден"
fi

echo ""
echo "6. Поиск всех server блоков с crmbot.restme.pro:"
echo "----------------------------------------"
grep -r "crmbot.restme.pro" /etc/nginx/ 2>/dev/null | grep -v ".swp" | grep -v "~" || echo "   Не найдено"

echo ""
echo "7. Поиск всех proxy_pass директив:"
echo "----------------------------------------"
grep -r "proxy_pass" /etc/nginx/sites-enabled/ 2>/dev/null | grep -v ".swp" | grep -v "~" || echo "   Не найдено"

echo ""
echo "8. Проверка конфигурации по умолчанию (default):"
echo "----------------------------------------"
if [ -f /etc/nginx/sites-enabled/default ]; then
    echo "   Файл найден, проверка proxy_pass:"
    grep "proxy_pass" /etc/nginx/sites-enabled/default | head -5
elif [ -L /etc/nginx/sites-enabled/default ]; then
    REAL_FILE=$(readlink -f /etc/nginx/sites-enabled/default)
    echo "   Симлинк найден -> $REAL_FILE"
    grep "proxy_pass" "$REAL_FILE" | head -5
else
    echo "   Файл не найден"
fi

echo ""
echo "=========================================="
echo "Рекомендации:"
echo "=========================================="
echo "1. Проверьте все найденные файлы с портом 8001"
echo "2. Замените 8001 на 8080 во всех найденных файлах"
echo "3. Проверьте конфигурацию: sudo nginx -t"
echo "4. Перезагрузите nginx: sudo systemctl reload nginx"
echo ""

