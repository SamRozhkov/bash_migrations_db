#!/bin/bash
set -euo pipefail
source ./env
global_id=$(uuidgen) #guid транзакции
execute_dt=$(date)  #дата и время запуска скрипта

function message(){
    echo "$1"
}

function run_sql(){
    result=$(PGPASSWORD=$GP_PASSWORD psql -t -h $GP_SOURCE -U $GP_USER -d $GP_DATABASE -c "$1") 2>/dev/null
    echo $result
}

#Выполняет проверку подключения к базе
function check_conn(){
    conn=$(run_sql "select 1;") #$(PGPASSWORD=$DB_PASSWORD psql -t -h $DB_SOURCE -U $DB_USER -d $DB_DATABASE -c "select 1;") 2>/dev/null
    if [[ $conn != "1" ]]
    then
        echo "$(date +%s) Ошибка подключения к базе" | tee -a /tmp/migration_history
        exit 1
    fi
}

#Инициализирует таблицу с метаданными миграций
function init(){
    search_init_table=$(run_sql "select tablename from pg_tables where tablename = 'migrations';")
    run_sql "delete from migrations where execute=False;" #Удаляем неудавшиеся миграции

    if [[ $search_init_table != "migrations" ]]
    then
        message "Initialize metadata storage"
        init_table=$(run_sql "create table migrations(id uuid, timestamp_at timestamp, migration text, hash text, execute bool default false) WITH (appendoptimized=true, compresslevel=1);")
        echo "$init_table"
    else
        message "Metadata storage already exixst"
    fi
}

function search_migration(){
    result=$(run_sql "select count(*) from migrations where hash = '$2';")
    if [[ $result -eq '0' ]]
    then
        run_sql "insert into migrations values ('$3', '$4', '$1', '$2')"
    else
        message "Skip file ($1) migration already exist"
    fi
}

function execute_migration(){
    files=$(run_sql "select string_agg(migration, ' ') from migrations where id = '$1' group by id")
    if [[ $files != "" ]]
    then
        consume="BEGIN; $(cat  $files) COMMIT;"
        run_sql "$consume"
        run_sql "update migrations set execute=true where id ='$1'"
    else
        message "Not migrations"
    fi
}

check_conn
init

function find_files(){
    files=$(find $1 -type f -name '*.sql' )
    echo "$files"
}

files=$(find_files $MIGRATION_PATH_ADB)

while IFS= read -r file; do
     hash=$(md5 -q $file)

     search_migration $file $hash $global_id "$execute_dt"

     #echo $hash $file $(date +%s) | tee -a /tmp/migration_history
done <<< "$files"

execute_migration $global_id
