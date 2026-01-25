# Настройка Basic Authentication для Webhook в nginx

## Быстрый старт

### 1. Установка утилиты для создания файла паролей

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install apache2-utils
```

**CentOS/RHEL:**
```bash
sudo yum install httpd-tools
```

### 2. Создание файла с паролями

Создайте файл с паролями (первый пользователь):
```bash
sudo htpasswd -c /etc/nginx/.htpasswd webhook_user
```
Введите пароль при запросе.

**Важно:** Флаг `-c` создает новый файл. Используйте его только для первого пользователя!

Для добавления дополнительных пользователей (без `-c`):
```bash
sudo htpasswd /etc/nginx/.htpasswd webhook_user2
```

### 3. Настройка прав доступа

Убедитесь, что файл доступен для чтения nginx:
```bash
sudo chmod 644 /etc/nginx/.htpasswd
sudo chown root:www-data /etc/nginx/.htpasswd  # Ubuntu/Debian
# или
sudo chown root:nginx /etc/nginx/.htpasswd      # CentOS/RHEL
```

### 4. Конфигурация nginx

Скопируйте пример конфигурации из `nginx_webhook.conf.example` в ваш конфиг nginx:

```bash
sudo cp nginx_webhook.conf.example /etc/nginx/sites-available/telegram-bot
sudo ln -s /etc/nginx/sites-available/telegram-bot /etc/nginx/sites-enabled/
```

Или добавьте блок `location` в существующий конфиг nginx.

**Важно:** Замените в конфиге:
- `your-domain.com` на ваш домен или IP
- `127.0.0.1:8080` на адрес вашего webhook сервера (если отличается)
- Путь к `.htpasswd` если используете другой путь

### 5. Проверка и перезагрузка

Проверьте конфигурацию:
```bash
sudo nginx -t
```

Если все ОК, перезагрузите nginx:
```bash
sudo systemctl reload nginx
```

## Настройка в Planfix

В настройках webhook в Planfix укажите:
- **URL:** `http://your-domain.com/planfix/webhook` (или `https://` для HTTPS)
- **Username:** `webhook_user` (или другой созданный пользователь)
- **Password:** пароль, который вы указали при создании

## Проверка работы

### Тест через curl:

```bash
# Без аутентификации (должен вернуть 401)
curl -v http://your-domain.com/planfix/webhook

# С правильной аутентификацией (должен вернуть 200 OK)
curl -u webhook_user:your_password -X POST http://your-domain.com/planfix/webhook \
  -H "Content-Type: application/json" \
  -d '{"test": "data"}'
```

### Тест health check:

```bash
curl http://your-domain.com/health
```

## Безопасность

1. **Используйте HTTPS** для production окружения
2. **Используйте сильные пароли** для webhook пользователей
3. **Ограничьте доступ** по IP, если возможно (добавьте `allow`/`deny` директивы в nginx)
4. **Регулярно обновляйте пароли**

## Дополнительная защита по IP (опционально)

Если вы знаете IP-адреса Planfix, можете добавить ограничение:

```nginx
location /planfix/webhook {
    # Разрешаем только IP Planfix (замените на реальные IP)
    allow 1.2.3.4;
    allow 5.6.7.8;
    deny all;
    
    auth_basic "Planfix Webhook Authentication";
    auth_basic_user_file /etc/nginx/.htpasswd;
    
    # ... остальная конфигурация
}
```

## Устранение проблем

### Ошибка 401 Unauthorized
- Проверьте, что логин и пароль в Planfix совпадают с созданными в `.htpasswd`
- Проверьте права доступа к файлу `.htpasswd`
- Проверьте логи nginx: `sudo tail -f /var/log/nginx/webhook_error.log`

### Ошибка 502 Bad Gateway
- Проверьте, что webhook сервер запущен на `127.0.0.1:8080`
- Проверьте логи nginx: `sudo tail -f /var/log/nginx/webhook_error.log`

### Проверка файла паролей
```bash
# Просмотр содержимого (зашифрованные пароли)
cat /etc/nginx/.htpasswd

# Проверка конкретного пользователя
htpasswd -v /etc/nginx/.htpasswd webhook_user
```

## Отключение Basic Auth в приложении

Если вы используете nginx для Basic Auth, вы можете отключить проверку в `webhook_server.py`, 
удалив или закомментировав переменные окружения:
```bash
# В .env файле закомментируйте или удалите:
# PLANFIX_WEBHOOK_USERNAME=
# PLANFIX_WEBHOOK_PASSWORD=
```

Однако рекомендуется оставить проверку на обоих уровнях для дополнительной безопасности.

