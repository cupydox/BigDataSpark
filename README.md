# BigDataSpark

Реализован пайплайн:

```text
CSV → Spark → PostgreSQL → Spark → PostgreSQL Star Schema → Spark → ClickHouse Reports
```

Используются:

- PostgreSQL — хранение исходных данных и модели звезда/снежинка;
- Apache Spark — чтение CSV, обработка данных и запись результатов;
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

Проверить контейнеры:

```bash
docker ps
```

Должны быть запущены:

```text
bdspark_postgres
bdspark_clickhouse
bdspark_spark
```

## 1. Загрузка CSV в PostgreSQL через Spark

```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.4 /app/jobs/00_csv_to_postgres.py
```

Проверить количество строк:

```bash
docker compose exec -T postgres psql -U bigdata -d bigdata -c "SELECT COUNT(*) FROM raw_sales;"
```

Ожидаемый результат:

```text
10000
```

## 2. Создание модели звезда/снежинка в PostgreSQL через Spark

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

## 3. Создание отчетов в ClickHouse через Spark

```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.4 /app/jobs/02_star_to_clickhouse_reports.py
```

После выполнения создаются таблицы:

```text
report_customer_sales
report_product_quality
report_product_sales
report_store_sales
report_supplier_sales
report_time_sales
```

Проверка таблиц ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --database bigdata --query "SHOW TABLES"
```

Проверка количества строк в отчетах:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --database bigdata --query "SELECT 'product_sales', count() FROM report_product_sales UNION ALL SELECT 'customer_sales', count() FROM report_customer_sales UNION ALL SELECT 'time_sales', count() FROM report_time_sales UNION ALL SELECT 'store_sales', count() FROM report_store_sales UNION ALL SELECT 'supplier_sales', count() FROM report_supplier_sales UNION ALL SELECT 'product_quality', count() FROM report_product_quality"
```

Ожидаемый результат:

```text
product_sales     10
customer_sales    10
time_sales        12
store_sales       5
supplier_sales    5
product_quality   10000
```

Посмотреть пример отчета:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --database bigdata --query "SELECT * FROM report_product_sales LIMIT 5"
```

## Остановка

```bash
docker compose down
```

Полная очистка данных контейнеров:

```bash
docker compose down -v
```