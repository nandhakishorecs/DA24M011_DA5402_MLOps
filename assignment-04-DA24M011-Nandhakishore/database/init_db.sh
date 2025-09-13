#!/bin/bash
set -e

echo "Checking database initialization..."

PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d $POSTGRES_DB -tc "SELECT 1 FROM pg_database WHERE datname='$POSTGRES_DB';" | grep -q 1 || {
    echo "Database $POSTGRES_DB does not exist, initializing..."
    psql -U $POSTGRES_USER -d postgres -c "CREATE DATABASE $POSTGRES_DB;"
}

TABLE_EXISTS=$(PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d $POSTGRES_DB -tc "SELECT to_regclass('public.news_articles');")

if [[ "$TABLE_EXISTS" != "news_articles" ]]; then
    echo "Table does not exist. Creating..."
    psql -U $POSTGRES_USER -d $POSTGRES_DB -f /docker-entrypoint-initdb.d/init.sql
else
    echo "Table exists. Skipping initialization."
fi

echo "Database is ready."

exec docker-entrypoint.sh postgres
