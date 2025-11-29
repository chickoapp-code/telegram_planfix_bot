# Исправление синтаксической ошибки в webhook_server.py

## Проблема
В логах видна ошибка:
```
SyntaxError: expected 'except' or 'finally' block
File "/home/dev_bot/telegram_planfix_bot/webhook_server.py", line 742
```

## Решение

Файл `webhook_server.py` на сервере устарел. Нужно обновить его до актуальной версии.

### Вариант 1: Обновить через git (рекомендуется)
```bash
cd ~/telegram_planfix_bot
git pull origin main  # или master, в зависимости от вашей ветки
sudo systemctl restart telegram-planfix-bot
```

### Вариант 2: Скопировать файл вручную
Если git недоступен, скопируйте актуальный файл `webhook_server.py` на сервер.

### Вариант 3: Проверить синтаксис на сервере
```bash
cd ~/telegram_planfix_bot
python3 -m py_compile webhook_server.py
```

Если есть ошибки, они будут показаны.

## Проверка после исправления

```bash
# Проверка статуса
sudo systemctl status telegram-planfix-bot

# Просмотр логов
sudo journalctl -u telegram-planfix-bot -f
```

## Примечание

В текущей версии файла все try блоки правильно закрыты. Ошибка возникает из-за устаревшей версии файла на сервере.

