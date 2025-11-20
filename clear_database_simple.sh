#!/bin/bash
# Простой скрипт для удаления файла БД (Linux/Mac)
# Использование: bash clear_database_simple.sh

DB_FILE="${DB_PATH:-bot.db}"

echo "⚠️  ВНИМАНИЕ: Будет удален файл БД: $DB_FILE"
read -p "Вы уверены? (yes/no): " confirm

if [ "$confirm" = "yes" ]; then
    if [ -f "$DB_FILE" ]; then
        rm "$DB_FILE"
        echo "✅ Файл БД удален: $DB_FILE"
        echo "ℹ️  При следующем запуске бота БД будет создана автоматически"
    else
        echo "⚠️  Файл БД не найден: $DB_FILE"
    fi
else
    echo "❌ Операция отменена"
fi

