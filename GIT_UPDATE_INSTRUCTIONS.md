# Инструкция по обновлению кода на сервере

## Быстрое обновление (если нет локальных изменений)

```bash
cd /home/dev_bot/telegram_planfix_bot
git pull
```

## Если есть локальные изменения

### Вариант 1: Сохранить локальные изменения (stash)

```bash
cd /home/dev_bot/telegram_planfix_bot

# Сохранить локальные изменения
git stash save "Local changes before update"

# Получить обновления
git pull

# Применить сохраненные изменения обратно (если нужно)
git stash pop
```

### Вариант 2: Отменить локальные изменения (если они не нужны)

```bash
cd /home/dev_bot/telegram_planfix_bot

# Отменить все локальные изменения
git checkout -- .

# Получить обновления
git pull
```

### Вариант 3: Закоммитить локальные изменения

```bash
cd /home/dev_bot/telegram_planfix_bot

# Добавить изменения
git add .

# Закоммитить
git commit -m "Local changes"

# Получить обновления (может потребоваться merge)
git pull
```

## После обновления кода

### 1. Активировать виртуальное окружение (если нужно)

```bash
cd /home/dev_bot/telegram_planfix_bot
source .venv/bin/activate
```

### 2. Установить новые зависимости (если есть)

```bash
pip install -r requirements.txt
```

### 3. Перезапустить сервис

```bash
# Если используется systemd сервис
sudo systemctl restart telegram-planfix-bot

# Проверить статус
sudo systemctl status telegram-planfix-bot

# Посмотреть логи
sudo journalctl -u telegram-planfix-bot -f
```

### 4. Или перезапустить вручную (если не используется systemd)

```bash
# Остановить текущий процесс (Ctrl+C или kill)
# Затем запустить снова
python run.py --mode both
```

## Проверка статуса git

```bash
cd /home/dev_bot/telegram_planfix_bot

# Проверить статус репозитория
git status

# Посмотреть последние коммиты
git log --oneline -10

# Посмотреть изменения
git diff
```

## Типичная последовательность команд

```bash
# 1. Перейти в директорию проекта
cd /home/dev_bot/telegram_planfix_bot

# 2. Проверить статус
git status

# 3. Если есть изменения, сохранить их
git stash save "Before update $(date +%Y%m%d_%H%M%S)"

# 4. Получить обновления
git pull

# 5. Перезапустить сервис
sudo systemctl restart telegram-planfix-bot

# 6. Проверить логи
sudo journalctl -u telegram-planfix-bot -n 50 --no-pager
```

## Если возникли конфликты при merge

```bash
# Посмотреть конфликтующие файлы
git status

# Разрешить конфликты вручную в файлах
# Затем:
git add .
git commit -m "Resolved merge conflicts"
```

## Откат к предыдущей версии (если что-то пошло не так)

```bash
cd /home/dev_bot/telegram_planfix_bot

# Посмотреть историю коммитов
git log --oneline -10

# Откатиться к предыдущему коммиту
git reset --hard HEAD~1

# Или к конкретному коммиту
git reset --hard <commit_hash>

# Перезапустить сервис
sudo systemctl restart telegram-planfix-bot
```



