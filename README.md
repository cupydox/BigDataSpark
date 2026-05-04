# BigDataSpark


Реализован пайплайн:

```text
CSV → PostgreSQL → Spark → PostgreSQL Star Schema → Spark → ClickHouse Reports
```

Используются:

- PostgreSQL — хранение исходных данных и модели звезда/снежинка;
- Apache Spark — обработка данных;
- ClickHouse — хранение отчетных таблиц;
- Docker Compose — запуск окружения.

## Запуск

Склонировать репозиторий:

```bash
git clone https://github.com/cupydox/BigDataSpark.git
cd BigDataSpark
```

Запустить контейнеры:

```bash
docker compose up -d
```

Проверить, что контейнеры запущены:

```bash
docker ps
```

Должны быть контейнеры:

```text
bdspark_postgres
bdspark_clickhouse
bdspark_spark
```

## Загрузка исходных данных в PostgreSQL

Создать таблицу для сырых данных:

```bash
docker compose exec -T postgres psql -U bigdata -d bigdata -f /init/01_create_raw_table.sql
```

Загрузить CSV-файлы в PostgreSQL.

Для PowerShell:

```powershell
Get-ChildItem ".\исходные данные" -Filter "*.csv" | ForEach-Object {
    $name = $_.Name
    docker compose exec -T postgres psql -U bigdata -d bigdata -c "\copy raw_sales FROM '/data/$name' WITH (FORMAT csv, HEADER true)"
}
```

Проверить количество строк:

```bash
docker compose exec -T postgres psql -U bigdata -d bigdata -c "SELECT COUNT(*) FROM raw_sales;"
```

Ожидаемый результат:

```text
10000
```

## Создание модели звезда/снежинка в PostgreSQL

Запустить Spark-задачу:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.4 /app/jobs/01_raw_to_star.py
```

После выполнения создаются таблицы:

```text
dim_customers
dim_dates
dim_products
dim_sellers
dim_stores
dim_suppliers
fact_sales
```

Проверка:

```bash
docker compose exec -T postgres psql -U bigdata -d bigdata -c "\dt"
docker compose exec -T postgres psql -U bigdata -d bigdata -c "SELECT COUNT(*) FROM fact_sales;"
```

Ожидаемый результат для `fact_sales`:

```text
10000
```

## Создание отчетов в ClickHouse

Запустить Spark-задачу:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.4 /app/jobs/02_star_to_clickhouse_reports.py
```

После выполнения создаются отчетные таблицы:

```text
report_customer_sales
report_product_quality
report_product_sales
report_store_sales
report_supplier_sales
report_time_sales
```

Проверить таблицы в ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --database bigdata --query "SHOW TABLES"
```

Проверить количество строк в отчетах:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --database bigdata --query "SELECT 'product_sales', count() FROM report_product_sales UNION ALL SELECT 'customer_sales', count() FROM report_customer_sales UNION ALL SELECT 'time_sales', count() FROM report_time_sales UNION ALL SELECT 'store_sales', count() FROM report_store_sales UNION ALL SELECT 'supplier_sales', count() FROM report_supplier_sales UNION ALL SELECT 'product_quality', count() FROM report_product_quality"
```

Посмотреть пример отчета:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --database bigdata --query "SELECT * FROM report_product_sales LIMIT 5"
```

## Остановка

```bash
docker compose down
```

Для полной очистки данных контейнеров:

```bash
docker compose down -v
```