# Быстрый запуск бота как системного сервиса

## Шаг 1: Перейдите в директорию проекта

```bash
cd ~/telegram_planfix_bot
```

Проверьте текущую директорию:
```bash
pwd
```
Должно быть: `/home/dev_bot/telegram_planfix_bot`

Проверьте, что файл `install_service.sh` существует:
```bash
ls -la install_service.sh
```

Проверьте наличие файла `run.py`:
```bash
ls -la run.py
```

## Шаг 2: Сделайте скрипт исполняемым

```bash
chmod +x install_service.sh
```

## Шаг 3: Запустите установку

```bash
sudo ./install_service.sh
```

Скрипт автоматически:
- Определит пользователя и группу
- Создаст директорию для логов
- Настроит все пути
- Установит и включит автозапуск сервиса

## Шаг 4: Запустите сервис

```bash
sudo systemctl start telegram-planfix-bot
```

## Шаг 5: Проверьте статус

```bash
sudo systemctl status telegram-planfix-bot
```

## Полезные команды

**Просмотр логов:**
```bash
# Логи systemd
sudo journalctl -u telegram-planfix-bot -f

# Логи приложения
tail -f logs/bot.log
tail -f logs/bot_errors.log
```

**Управление сервисом:**
```bash
sudo systemctl start telegram-planfix-bot    # Запуск
sudo systemctl stop telegram-planfix-bot     # Остановка
sudo systemctl restart telegram-planfix-bot  # Перезапуск
sudo systemctl status telegram-planfix-bot   # Статус
```

**Отключение автозапуска:**
```bash
sudo systemctl disable telegram-planfix-bot
```

**Включение автозапуска:**
```bash
sudo systemctl enable telegram-planfix-bot
```

## Решение проблем

### Скрипт не найден

Если вы получаете ошибку `command not found`:

1. Проверьте текущую директорию:
   ```bash
   pwd
   ```
   Должно быть: `/home/dev_bot/telegram_planfix_bot`

2. Если вы в другой директории, перейдите в правильную:
   ```bash
   cd ~/telegram_planfix_bot
   ```

3. Проверьте наличие файла:
   ```bash
   ls -la install_service.sh
   ```

4. Если файл не найден, проверьте структуру директорий:
   ```bash
   find ~/telegram_planfix_bot -name "install_service.sh" -type f
   ```

### Права доступа

Если скрипт не запускается, проверьте права:
```bash
chmod +x install_service.sh
ls -la install_service.sh
```

Должно быть что-то вроде: `-rwxr-xr-x` (x означает исполняемый)

### Сервис не запускается

1. Проверьте логи:
   ```bash
   sudo journalctl -u telegram-planfix-bot -n 50
   ```

2. Проверьте, что виртуальное окружение существует:
   ```bash
   ls -la venv/bin/python3
   ```

3. Проверьте, что файл `.env` существует:
   ```bash
   ls -la .env
   ```

