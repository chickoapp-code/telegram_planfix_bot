# 📋 Инструкция по обновлению бота на сервере

## Шаг 1: Подключение к серверу

```bash
ssh dev_bot@your_server_ip
# или
ssh dev_bot@your_server_hostname
```

## Шаг 2: Переход в директорию проекта

```bash
cd /home/dev_bot/telegram_planfix_bot
```

## Шаг 3: Остановка сервиса

```bash
sudo systemctl stop telegram-planfix-bot
```

Проверьте, что сервис остановлен:
```bash
sudo systemctl status telegram-planfix-bot
```

Должно быть: `Active: inactive (dead)`

## Шаг 4: Создание резервной копии (рекомендуется)

```bash
# Создаем папку для бэкапов
mkdir -p ~/backups

# Создаем бэкап текущей версии
cp -r /home/dev_bot/telegram_planfix_bot ~/backups/telegram_planfix_bot_backup_$(date +%Y%m%d_%H%M%S)
```

## Шаг 5: Обновление кода

### Вариант A: Если используется Git

```bash
# Проверяем текущую ветку
git status

# Получаем последние изменения
git pull origin main
# или
git pull origin master
```

### Вариант B: Если код копируется вручную

```bash
# Скопируйте обновленные файлы на сервер через scp или rsync
# Например, с локального компьютера:
# scp -r user_handlers.py executor_handlers.py task_notification_service.py keyboards.py dev_bot@server:/home/dev_bot/telegram_planfix_bot/
```

## Шаг 6: Активация виртуального окружения

```bash
source venv/bin/activate
```

## Шаг 7: Обновление зависимостей (если нужно)

```bash
# Проверяем, есть ли обновления в requirements.txt
pip install -r requirements.txt --upgrade
```

## Шаг 8: Проверка синтаксиса Python (опционально)

```bash
python3 -m py_compile main.py user_handlers.py executor_handlers.py task_notification_service.py keyboards.py
```

Если ошибок нет - команда завершится без вывода.

## Шаг 9: Проверка конфигурации

Убедитесь, что файл `.env` существует и содержит все необходимые переменные:

```bash
ls -la .env
cat .env | grep -v "PASSWORD\|SECRET\|KEY"  # Показываем без секретов
```

## Шаг 10: Запуск сервиса

```bash
sudo systemctl start telegram-planfix-bot
```

## Шаг 11: Проверка статуса

```bash
sudo systemctl status telegram-planfix-bot
```

Должно быть: `Active: active (running)`

## Шаг 12: Просмотр логов

```bash
# Просмотр последних логов
sudo journalctl -u telegram-planfix-bot -n 50

# Просмотр логов в реальном времени
sudo journalctl -u telegram-planfix-bot -f
```

## Шаг 13: Проверка работы бота

1. Отправьте команду `/start` боту в Telegram
2. Проверьте, что бот отвечает
3. Создайте тестовую заявку (если вы заявитель)
4. Проверьте, что исполнители получили уведомления

---

## 🔧 Полезные команды для управления сервисом

### Остановка сервиса
```bash
sudo systemctl stop telegram-planfix-bot
```

### Запуск сервиса
```bash
sudo systemctl start telegram-planfix-bot
```

### Перезапуск сервиса
```bash
sudo systemctl restart telegram-planfix-bot
```

### Просмотр статуса
```bash
sudo systemctl status telegram-planfix-bot
```

### Просмотр логов
```bash
# Последние 100 строк
sudo journalctl -u telegram-planfix-bot -n 100

# Логи за последний час
sudo journalctl -u telegram-planfix-bot --since "1 hour ago"

# Логи в реальном времени
sudo journalctl -u telegram-planfix-bot -f

# Логи с фильтром по уровню (только ошибки)
sudo journalctl -u telegram-planfix-bot -p err
```

### Отключение автозапуска
```bash
sudo systemctl disable telegram-planfix-bot
```

### Включение автозапуска
```bash
sudo systemctl enable telegram-planfix-bot
```

---

## ⚠️ Решение проблем

### Проблема: Сервис не запускается

1. Проверьте логи:
```bash
sudo journalctl -u telegram-planfix-bot -n 100
```

2. Проверьте синтаксис Python:
```bash
cd /home/dev_bot/telegram_planfix_bot
source venv/bin/activate
python3 -m py_compile main.py
```

3. Проверьте права доступа:
```bash
ls -la /home/dev_bot/telegram_planfix_bot
```

4. Проверьте конфигурацию сервиса:
```bash
sudo systemctl cat telegram-planfix-bot
```

### Проблема: Бот не отвечает

1. Проверьте, что сервис запущен:
```bash
sudo systemctl status telegram-planfix-bot
```

2. Проверьте логи на ошибки:
```bash
sudo journalctl -u telegram-planfix-bot -p err -n 50
```

3. Проверьте токен бота в `.env`:
```bash
grep BOT_TOKEN .env
```

### Проблема: Ошибки импорта

1. Убедитесь, что виртуальное окружение активировано
2. Переустановите зависимости:
```bash
source venv/bin/activate
pip install -r requirements.txt --force-reinstall
```

---

## 📝 Быстрая команда для обновления (все в одной строке)

Если вы используете Git и хотите быстро обновить:

```bash
cd /home/dev_bot/telegram_planfix_bot && \
sudo systemctl stop telegram-planfix-bot && \
git pull && \
source venv/bin/activate && \
pip install -r requirements.txt --upgrade && \
sudo systemctl start telegram-planfix-bot && \
sudo systemctl status telegram-planfix-bot
```

---

## 🔄 Откат к предыдущей версии (если что-то пошло не так)

```bash
# Остановите сервис
sudo systemctl stop telegram-planfix-bot

# Восстановите из бэкапа
cd /home/dev_bot
cp -r backups/telegram_planfix_bot_backup_YYYYMMDD_HHMMSS/telegram_planfix_bot/* telegram_planfix_bot/

# Запустите сервис
sudo systemctl start telegram-planfix-bot
```

---

**Примечание:** Замените `YYYYMMDD_HHMMSS` на дату и время нужного бэкапа.

