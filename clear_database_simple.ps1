# Простой скрипт для удаления файла БД (Windows PowerShell)
# Использование: .\clear_database_simple.ps1

$DB_FILE = if ($env:DB_PATH) { $env:DB_PATH } else { "bot.db" }

Write-Host "⚠️  ВНИМАНИЕ: Будет удален файл БД: $DB_FILE" -ForegroundColor Yellow
$confirm = Read-Host "Вы уверены? (yes/no)"

if ($confirm -eq "yes") {
    if (Test-Path $DB_FILE) {
        Remove-Item $DB_FILE
        Write-Host "✅ Файл БД удален: $DB_FILE" -ForegroundColor Green
        Write-Host "ℹ️  При следующем запуске бота БД будет создана автоматически" -ForegroundColor Cyan
    } else {
        Write-Host "⚠️  Файл БД не найден: $DB_FILE" -ForegroundColor Yellow
    }
} else {
    Write-Host "❌ Операция отменена" -ForegroundColor Red
}

