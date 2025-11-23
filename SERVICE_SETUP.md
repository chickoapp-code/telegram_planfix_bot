# Настройка автозапуска бота как системного сервиса

Этот документ описывает, как настроить бота для автоматического запуска после перезагрузки системы и работы в фоновом режиме.

## Вариант 1: Systemd Service (рекомендуется для Linux)

### Шаг 1: Подготовка

1. Убедитесь, что вы находитесь в директории проекта:
   ```bash
   cd /home/dev_bot/telegram_planfix_bot/telegram_planfix_bot
   ```

2. Создайте директорию для логов:
   ```bash
   mkdir -p logs
   chmod 755 logs
   ```

3. Убедитесь, что скрипт `start_bot.sh` исполняемый:
   ```bash
   chmod +x start_bot.sh
   ```

### Шаг 2: Установка service файла

**Автоматическая установка (рекомендуется):**

1. Запустите скрипт установки:
   ```bash
   sudo ./install_service.sh
   ```

   Скрипт автоматически:
   - Определит пользователя и группу
   - Создаст директорию для логов
   - Настроит пути в service файле
   - Скопирует service файл в systemd
   - Включит автозапуск

**Ручная установка:**

1. Откройте файл `telegram-planfix-bot.service` и отредактируйте пути:
   - `User` и `Group` - ваш пользователь и группа
   - `WorkingDirectory` - полный путь к директории проекта
   - `ExecStart` - путь к Python в venv и к `run.py`
   - `LOG_DIR` - путь к директории логов

2. Скопируйте service файл в systemd:
   ```bash
   sudo cp telegram-planfix-bot.service /etc/systemd/system/
   ```

3. Перезагрузите systemd:
   ```bash
   sudo systemctl daemon-reload
   ```

### Шаг 3: Управление сервисом

**Запуск сервиса:**
```bash
sudo systemctl start telegram-planfix-bot
```

**Остановка сервиса:**
```bash
sudo systemctl stop telegram-planfix-bot
```

**Перезапуск сервиса:**
```bash
sudo systemctl restart telegram-planfix-bot
```

**Проверка статуса:**
```bash
sudo systemctl status telegram-planfix-bot
```

**Просмотр логов:**
```bash
# Логи systemd
sudo journalctl -u telegram-planfix-bot -f

# Логи приложения
tail -f logs/bot.log
tail -f logs/bot_errors.log
```

**Включение автозапуска при загрузке системы:**
```bash
sudo systemctl enable telegram-planfix-bot
```

**Отключение автозапуска:**
```bash
sudo systemctl disable telegram-planfix-bot
```

### Шаг 4: Проверка

После запуска проверьте:
1. Статус сервиса: `sudo systemctl status telegram-planfix-bot`
2. Логи: `tail -f logs/bot.log`
3. Работу бота: отправьте команду `/start` боту в Telegram

## Вариант 2: Screen (альтернативный способ)

Если systemd недоступен, можно использовать screen:

1. Установите screen (если не установлен):
   ```bash
   sudo apt-get install screen  # для Debian/Ubuntu
   ```

2. Создайте screen сессию:
   ```bash
   screen -S telegram_bot
   ```

3. Запустите бота:
   ```bash
   cd /home/dev_bot/telegram_planfix_bot/telegram_planfix_bot
   source venv/bin/activate
   python3 run.py --mode both
   ```

4. Отключитесь от screen (бот продолжит работать):
   - Нажмите `Ctrl+A`, затем `D`

5. Для повторного подключения:
   ```bash
   screen -r telegram_bot
   ```

6. Для автозапуска через screen добавьте в `~/.bashrc` или создайте cron job:
   ```bash
   @reboot screen -dmS telegram_bot bash -c 'cd /home/dev_bot/telegram_planfix_bot/telegram_planfix_bot && source venv/bin/activate && python3 run.py --mode both'
   ```

## Вариант 3: Tmux (альтернативный способ)

Аналогично screen:

1. Установите tmux:
   ```bash
   sudo apt-get install tmux
   ```

2. Создайте tmux сессию:
   ```bash
   tmux new -s telegram_bot
   ```

3. Запустите бота (внутри tmux):
   ```bash
   cd /home/dev_bot/telegram_planfix_bot/telegram_planfix_bot
   source venv/bin/activate
   python3 run.py --mode both
   ```

4. Отключитесь: `Ctrl+B`, затем `D`

5. Подключитесь обратно: `tmux attach -t telegram_bot`

## Настройка переменных окружения

Убедитесь, что файл `.env` содержит все необходимые переменные:
- `BOT_TOKEN`
- `PLANFIX_*` настройки
- `WEBHOOK_HOST` и `WEBHOOK_PORT`
- И другие необходимые параметры

## Решение проблем

### Сервис не запускается

1. Проверьте логи:
   ```bash
   sudo journalctl -u telegram-planfix-bot -n 50
   ```

2. Проверьте права доступа:
   ```bash
   ls -la /home/dev_bot/telegram_planfix_bot/telegram_planfix_bot
   ```

3. Проверьте, что виртуальное окружение активируется:
   ```bash
   /home/dev_bot/telegram_planfix_bot/telegram_planfix_bot/venv/bin/python3 --version
   ```

### Бот не отвечает

1. Проверьте логи приложения: `tail -f logs/bot.log`
2. Проверьте, что бот запущен: `sudo systemctl status telegram-planfix-bot`
3. Проверьте подключение к интернету
4. Проверьте токен бота в `.env`

### Webhook не работает

1. Убедитесь, что webhook сервер запущен (режим `both` или `webhook`)
2. Проверьте настройки `WEBHOOK_HOST` и `WEBHOOK_PORT` в `.env`
3. Проверьте настройки nginx (если используется)
4. Проверьте логи: `tail -f logs/bot.log | grep webhook`

## Обновление бота

При обновлении кода:

1. Остановите сервис:
   ```bash
   sudo systemctl stop telegram-planfix-bot
   ```

2. Обновите код (git pull, и т.д.)

3. Перезапустите сервис:
   ```bash
   sudo systemctl start telegram-planfix-bot
   ```

Или просто перезапустите:
```bash
sudo systemctl restart telegram-planfix-bot
```

