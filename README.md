# Миграция на Postgres

Скрипт создан для полного переноса структуры базы и всех данных из mysql в postgres.

## Для запуска
- `pip install -r requirements.txt`
- создать новую пустую базу `postgres`
- в `.env` прописать переменные для подключения к этой базе
- `python manage.py mysql2pg <mysql_user> <mysql_password> <mysql_host> <mysql_db_name> <postgres_user>
<postgres_password> <postgres_host> <postgres_db_name> <дополнительные ключи>`

Список ключей:
- `-c` continue. Запустив скрипт с этим флагов можно продолжить прерванную ранее миграцию.
- `-i` info. Позволяет найти отсутствующие таблицы и таблицы с различным количеством записей.
- `-u` use csv. Позволяет использовать миграцию через csv для существенного ускорения миграции.
- `-r` repair. Позволяет починить индексы, последовательности, автоинкременты.