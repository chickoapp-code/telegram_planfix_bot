# Инструкция по остановке найденных процессов

## Найденные процессы:

1. **root процесс** (PID 1179): `python -m bot.main`
2. **Screen сессия** (PID 7431): `planfix_bot`
3. **Bash процесс** (PID 7432): внутри screen
4. **Python процесс** (PID 7433): `python main.py`

## Способ 1: Использовать скрипт

```bash
bash stop_found_bot.sh
```

## Способ 2: Остановить вручную

### 1. Остановить screen сессию (это остановит процессы 7432 и 7433)

```bash
screen -S planfix_bot -X quit
```

Или если не работает:
```bash
kill 7431
```

### 2. Остановить процесс от root

```bash
sudo kill 1179
```

Если не останавливается:
```bash
sudo kill -9 1179
```

### 3. Проверить, что все остановлено

```bash
ps aux | grep -E "(main\.py|python.*main|bot\.main)" | grep -v grep
```

Не должно быть вывода.

## Способ 3: Остановить все сразу

```bash
# Остановить screen сессию
screen -S planfix_bot -X quit

# Остановить root процесс
sudo kill 1179

# Принудительно остановить все процессы Python с main.py
sudo pkill -9 -f "python.*main.py"
sudo pkill -9 -f "bot.main"
```

## После остановки

1. **Проверьте, что процессы остановлены:**
   ```bash
   ps aux | grep -E "(main\.py|python.*main|bot\.main)" | grep -v grep
   ```

2. **Проверьте screen сессии:**
   ```bash
   screen -ls
   ```
   Не должно быть сессии `planfix_bot`

3. **Проверьте systemd сервисы (если есть):**
   ```bash
   systemctl list-units --type=service --all | grep bot
   ```

4. **Удалите старую папку** (если еще не удалили)

5. **Загрузите новую версию** и запустите бота заново

