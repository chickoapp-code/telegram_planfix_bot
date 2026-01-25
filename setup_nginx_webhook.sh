#!/bin/bash
# Скрипт для быстрой настройки nginx webhook с Basic Authentication

set -e

echo "=========================================="
echo "Настройка nginx для webhook с Basic Auth"
echo "=========================================="
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "Ошибка: Запустите скрипт с правами root (sudo)"
    exit 1
fi

# Проверка наличия nginx
if ! command -v nginx &> /dev/null; then
    echo "Ошибка: nginx не установлен"
    echo "Установите: sudo apt-get install nginx"
    exit 1
fi

# Проверка наличия htpasswd
if ! command -v htpasswd &> /dev/null; then
    echo "Установка apache2-utils для создания файла паролей..."
    apt-get update
    apt-get install -y apache2-utils
fi

# Создание файла с паролями
PASSWD_FILE="/etc/nginx/.htpasswd"
echo ""
echo "Создание файла с паролями: $PASSWD_FILE"
echo "Введите имя пользователя для webhook (по умолчанию: webhook_user):"
read -r USERNAME
USERNAME=${USERNAME:-webhook_user}

if [ -f "$PASSWD_FILE" ]; then
    echo "Файл $PASSWD_FILE уже существует."
    echo "Добавить пользователя $USERNAME? (y/n)"
    read -r ADD_USER
    if [ "$ADD_USER" = "y" ] || [ "$ADD_USER" = "Y" ]; then
        htpasswd "$PASSWD_FILE" "$USERNAME"
    else
        echo "Пропускаем создание пользователя"
    fi
else
    htpasswd -c "$PASSWD_FILE" "$USERNAME"
fi

# Настройка прав доступа
chmod 644 "$PASSWD_FILE"
chown root:www-data "$PASSWD_FILE"

# Копирование конфигурации
CONFIG_FILE="/etc/nginx/sites-available/telegram-bot-webhook"
echo ""
echo "Копирование конфигурации в $CONFIG_FILE..."

# Проверяем, существует ли файл
if [ -f "$CONFIG_FILE" ]; then
    echo "Файл $CONFIG_FILE уже существует."
    echo "Перезаписать? (y/n)"
    read -r OVERWRITE
    if [ "$OVERWRITE" != "y" ] && [ "$OVERWRITE" != "Y" ]; then
        echo "Пропускаем копирование конфигурации"
        CONFIG_EXISTS=true
    fi
fi

if [ "$CONFIG_EXISTS" != "true" ]; then
    # Используем текущую директорию или ищем файл
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    SOURCE_CONFIG="$SCRIPT_DIR/nginx_webhook.conf.example"
    
    if [ ! -f "$SOURCE_CONFIG" ]; then
        echo "Ошибка: Файл nginx_webhook.conf.example не найден в $SCRIPT_DIR"
        echo "Убедитесь, что вы запускаете скрипт из директории проекта"
        exit 1
    fi
    
    cp "$SOURCE_CONFIG" "$CONFIG_FILE"
    echo "Конфигурация скопирована"
fi

# Создание симлинка
LINK_FILE="/etc/nginx/sites-enabled/telegram-bot-webhook"
echo ""
if [ -L "$LINK_FILE" ]; then
    echo "Симлинк $LINK_FILE уже существует"
elif [ -f "$LINK_FILE" ]; then
    echo "Внимание: $LINK_FILE существует как обычный файл (не симлинк)"
    echo "Удалить и создать симлинк? (y/n)"
    read -r REMOVE
    if [ "$REMOVE" = "y" ] || [ "$REMOVE" = "Y" ]; then
        rm "$LINK_FILE"
        ln -s "$CONFIG_FILE" "$LINK_FILE"
        echo "Симлинк создан"
    fi
else
    ln -s "$CONFIG_FILE" "$LINK_FILE"
    echo "Симлинк создан: $LINK_FILE -> $CONFIG_FILE"
fi

# Проверка конфигурации
echo ""
echo "Проверка конфигурации nginx..."
if nginx -t; then
    echo "✓ Конфигурация nginx корректна"
else
    echo "✗ Ошибка в конфигурации nginx!"
    echo "Проверьте файл: $CONFIG_FILE"
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

# Итоговая информация
echo ""
echo "=========================================="
echo "Настройка завершена!"
echo "=========================================="
echo ""
echo "Информация для настройки в Planfix:"
echo "  URL: http://$(hostname -I | awk '{print $1}')/planfix/webhook"
echo "  Username: $USERNAME"
echo "  Password: (пароль, который вы указали)"
echo ""
echo "Файлы:"
echo "  Конфигурация: $CONFIG_FILE"
echo "  Пароли: $PASSWD_FILE"
echo ""
echo "Проверка работы:"
echo "  curl -u $USERNAME:your_password http://localhost/planfix/webhook"
echo ""

