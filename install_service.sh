#!/bin/bash
# Скрипт установки systemd сервиса
# Использование: sudo ./install_service.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "📂 Текущая директория: $SCRIPT_DIR"
echo ""

# Проверяем, что мы в правильной директории (должны быть файлы run.py, config/, и т.д.)
if [ ! -f "run.py" ]; then
    echo "❌ Ошибка: файл run.py не найден в текущей директории"
    echo "   Убедитесь, что вы находитесь в корневой директории проекта"
    echo "   Ожидаемый путь: /home/dev_bot/telegram_planfix_bot"
    exit 1
fi
echo "✅ Проверка директории: файл run.py найден"
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "❌ Этот скрипт должен быть запущен с правами root (sudo)"
    exit 1
fi

# Определяем пользователя и группу из текущей директории
PROJECT_USER=$(stat -c '%U' "$SCRIPT_DIR")
PROJECT_GROUP=$(stat -c '%G' "$SCRIPT_DIR")
PROJECT_DIR="$SCRIPT_DIR"

echo "📋 Параметры установки:"
echo "   Пользователь: $PROJECT_USER"
echo "   Группа: $PROJECT_GROUP"
echo "   Директория проекта: $PROJECT_DIR"
echo ""

# Создаем директорию для логов
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"
chown "$PROJECT_USER:$PROJECT_GROUP" "$LOG_DIR"
chmod 755 "$LOG_DIR"
echo "✅ Создана директория для логов: $LOG_DIR"

# Определяем путь к Python в venv (пробуем оба варианта: venv и .venv)
# Проверяем от имени пользователя проекта, чтобы избежать проблем с правами доступа
VENV_PYTHON=""
if sudo -u "$PROJECT_USER" test -f "$PROJECT_DIR/venv/bin/python3"; then
    VENV_PYTHON="$PROJECT_DIR/venv/bin/python3"
    echo "✅ Найден Python в venv: $VENV_PYTHON"
elif sudo -u "$PROJECT_USER" test -f "$PROJECT_DIR/.venv/bin/python3"; then
    VENV_PYTHON="$PROJECT_DIR/.venv/bin/python3"
    echo "✅ Найден Python в .venv: $VENV_PYTHON"
else
    echo "❌ Python в venv не найден"
    echo "   Проверялись пути:"
    echo "   - $PROJECT_DIR/venv/bin/python3"
    echo "   - $PROJECT_DIR/.venv/bin/python3"
    echo ""
    echo "   Отладочная информация:"
    sudo -u "$PROJECT_USER" ls -la "$PROJECT_DIR" | grep -E "(venv|\.venv)" || echo "   (директории venv не найдены)"
    echo ""
    echo "   Убедитесь, что виртуальное окружение создано:"
    echo "   python3 -m venv .venv"
    echo "   source .venv/bin/activate"
    echo "   pip install -r requirements.txt"
    exit 1
fi

# Создаем временный service файл с правильными путями
SERVICE_FILE="/tmp/telegram-planfix-bot.service"
cat > "$SERVICE_FILE" << EOF
[Unit]
Description=Telegram Planfix Bot Service
After=network.target

[Service]
Type=simple
User=$PROJECT_USER
Group=$PROJECT_GROUP
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$(dirname $VENV_PYTHON):/usr/local/bin:/usr/bin:/bin"
Environment="SYSTEMD_SERVICE=1"
Environment="LOG_DIR=$LOG_DIR"
ExecStart=$VENV_PYTHON $PROJECT_DIR/run.py --mode both
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# Ограничения ресурсов (опционально)
# LimitNOFILE=65536
# MemoryMax=512M

[Install]
WantedBy=multi-user.target
EOF

# Копируем service файл
SYSTEMD_SERVICE="/etc/systemd/system/telegram-planfix-bot.service"
cp "$SERVICE_FILE" "$SYSTEMD_SERVICE"
echo "✅ Service файл скопирован в $SYSTEMD_SERVICE"

# Перезагружаем systemd
systemctl daemon-reload
echo "✅ Systemd перезагружен"

# Включаем автозапуск
systemctl enable telegram-planfix-bot
echo "✅ Автозапуск включен"

echo ""
echo "✅ Установка завершена!"
echo ""
echo "Для управления сервисом используйте:"
echo "  sudo systemctl start telegram-planfix-bot    # Запуск"
echo "  sudo systemctl stop telegram-planfix-bot     # Остановка"
echo "  sudo systemctl restart telegram-planfix-bot  # Перезапуск"
echo "  sudo systemctl status telegram-planfix-bot   # Статус"
echo "  sudo journalctl -u telegram-planfix-bot -f   # Логи"
echo ""
echo "Для запуска сервиса выполните:"
echo "  sudo systemctl start telegram-planfix-bot"

