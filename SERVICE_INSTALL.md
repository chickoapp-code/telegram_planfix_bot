# Установка бота как системной службы

Эта инструкция поможет вам установить Telegram Planfix Bot как systemd службу для автоматического запуска при перезагрузке сервера.

## Быстрая установка

1. Перейдите в директорию проекта:
   ```bash
   cd /home/dev_bot/telegram_planfix_bot
   ```

2. Убедитесь, что скрипт имеет права на выполнение:
   ```bash
   chmod +x install_service.sh
   ```

3. Запустите скрипт установки:
   ```bash
   sudo ./install_service.sh
   ```
   
   **Альтернативный способ** (если права не устанавливаются):
   ```bash
   sudo bash install_service.sh
   ```

3. Запустите службу:
   ```bash
   sudo systemctl start telegram-planfix-bot
   ```

4. Проверьте статус:
   ```bash
   sudo systemctl status telegram-planfix-bot
   ```

Готово! Бот теперь будет автоматически запускаться при перезагрузке сервера.

## Управление службой

```bash
# Запуск
sudo systemctl start telegram-planfix-bot

# Остановка
sudo systemctl stop telegram-planfix-bot

# Перезапуск
sudo systemctl restart telegram-planfix-bot

# Статус
sudo systemctl status telegram-planfix-bot

# Логи в реальном времени
sudo journalctl -u telegram-planfix-bot -f

# Последние 100 строк логов
sudo journalctl -u telegram-planfix-bot -n 100
```

## Что делает скрипт установки

- ✅ Определяет пользователя и группу проекта
- ✅ Находит виртуальное окружение (venv или .venv)
- ✅ Создает директорию для логов
- ✅ Создает systemd service файл с правильными путями
- ✅ Включает автозапуск при загрузке системы
- ✅ Настраивает автоматический перезапуск при сбоях

## Проверка работы

После установки:
- Бот автоматически запускается при перезагрузке сервера
- Бот автоматически перезапускается при сбоях (через 10 секунд)
- Бот работает в фоновом режиме
- Логи доступны через `journalctl`

Вы можете безопасно завершать SSH сеанс и перезагружать сервер — бот будет работать автоматически.

