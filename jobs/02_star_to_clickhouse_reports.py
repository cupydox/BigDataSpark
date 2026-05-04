import json
import urllib.parse
import urllib.request
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdata"
POSTGRES_USER = "bigdata"
POSTGRES_PASSWORD = "bigdata"
POSTGRES_DRIVER = "org.postgresql.Driver"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "bigdata"
CLICKHOUSE_USER = "bigdata"
CLICKHOUSE_PASSWORD = "bigdata"


def read_postgres_table(spark: SparkSession, table_name: str):
    return (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .load()
    )


def clickhouse_url():
    params = urllib.parse.urlencode(
        {
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
            "database": CLICKHOUSE_DATABASE,
        }
    )
    return f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?{params}"


def execute_clickhouse_query(query: str):
    request = urllib.request.Request(
        clickhouse_url(),
        data=query.encode("utf-8"),
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        return response.read().decode("utf-8")


def json_serializer(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def recreate_clickhouse_table(table_name: str, ddl: str):
    execute_clickhouse_query(f"DROP TABLE IF EXISTS {table_name}")
    execute_clickhouse_query(ddl)


def insert_dataframe_to_clickhouse(df, table_name: str):
    rows = df.collect()

    if not rows:
        return

    json_lines = []
    for row in rows:
        json_lines.append(
            json.dumps(
                row.asDict(recursive=True),
                default=json_serializer,
                ensure_ascii=False,
            )
        )

    body = f"INSERT INTO {table_name} FORMAT JSONEachRow\n" + "\n".join(json_lines)
    execute_clickhouse_query(body)


def write_report(df, table_name: str, ddl: str):
    recreate_clickhouse_table(table_name, ddl)
    insert_dataframe_to_clickhouse(df, table_name)
    print(f"Report {table_name} has been written to ClickHouse. Rows: {df.count()}")


def main():
    spark = (
        SparkSession.builder
        .appName("StarSchemaToClickHouseReports")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    fact_sales = read_postgres_table(spark, "fact_sales")
    dim_products = read_postgres_table(spark, "dim_products")
    dim_customers = read_postgres_table(spark, "dim_customers")
    dim_stores = read_postgres_table(spark, "dim_stores")
    dim_suppliers = read_postgres_table(spark, "dim_suppliers")
    dim_dates = read_postgres_table(spark, "dim_dates")

    report_product_sales = (
        fact_sales
        .join(dim_products, on="product_key", how="left")
        .groupBy(
            "product_key",
            "product_name",
            "product_category",
            "product_brand",
        )
        .agg(
            F.count("*").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_quantity_sold"),
            F.round(F.sum("sale_total_price"), 2).alias("total_sales_amount"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_sale_amount"),
        )
        .orderBy(F.col("total_sales_amount").desc())
        .limit(10)
    )

    report_customer_sales = (
        fact_sales
        .join(dim_customers, on="customer_key", how="left")
        .withColumn(
            "customer_name",
            F.concat_ws(
                " ",
                F.col("customer_first_name"),
                F.col("customer_last_name"),
            ),
        )
        .groupBy(
            "customer_key",
            "customer_name",
            "customer_email",
            "customer_country",
        )
        .agg(
            F.count("*").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_quantity"),
            F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .orderBy(F.col("total_spent").desc())
        .limit(10)
    )

    report_time_sales = (
        fact_sales
        .join(dim_dates, on="date_key", how="left")
        .groupBy(
            "year",
            "quarter",
            "month",
        )
        .agg(
            F.count("*").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_quantity"),
            F.round(F.sum("sale_total_price"), 2).alias("total_sales_amount"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .orderBy("year", "quarter", "month")
    )

    report_store_sales = (
        fact_sales
        .join(dim_stores, on="store_key", how="left")
        .groupBy(
            "store_key",
            "store_name",
            "store_city",
            "store_country",
        )
        .agg(
            F.count("*").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_quantity"),
            F.round(F.sum("sale_total_price"), 2).alias("total_sales_amount"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .orderBy(F.col("total_sales_amount").desc())
        .limit(5)
    )

    report_supplier_sales = (
        fact_sales
        .join(dim_suppliers, on="supplier_key", how="left")
        .groupBy(
            "supplier_key",
            "supplier_name",
            "supplier_city",
            "supplier_country",
        )
        .agg(
            F.countDistinct("product_key").cast("long").alias("products_count"),
            F.count("*").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_quantity"),
            F.round(F.sum("sale_total_price"), 2).alias("total_sales_amount"),
        )
        .orderBy(F.col("total_sales_amount").desc())
        .limit(5)
    )

    report_product_quality = (
        fact_sales
        .join(dim_products, on="product_key", how="left")
        .groupBy(
            "product_key",
            "product_name",
            "product_category",
            "product_brand",
        )
        .agg(
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.round(F.avg("product_reviews"), 2).alias("avg_reviews"),
            F.count("*").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_quantity_sold"),
            F.round(F.sum("sale_total_price"), 2).alias("total_sales_amount"),
        )
        .orderBy(
            F.col("avg_rating").desc_nulls_last(),
            F.col("avg_reviews").desc_nulls_last(),
        )
    )

    write_report(
        report_product_sales,
        "report_product_sales",
        """
        CREATE TABLE report_product_sales (
            product_key UInt64,
            product_name Nullable(String),
            product_category Nullable(String),
            product_brand Nullable(String),
            orders_count UInt64,
            total_quantity_sold UInt64,
            total_sales_amount Float64,
            avg_sale_amount Float64
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )

    write_report(
        report_customer_sales,
        "report_customer_sales",
        """
        CREATE TABLE report_customer_sales (
            customer_key UInt64,
            customer_name Nullable(String),
            customer_email Nullable(String),
            customer_country Nullable(String),
            orders_count UInt64,
            total_quantity UInt64,
            total_spent Float64,
            avg_check Float64
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )

    write_report(
        report_time_sales,
        "report_time_sales",
        """
        CREATE TABLE report_time_sales (
            year UInt16,
            quarter UInt8,
            month UInt8,
            orders_count UInt64,
            total_quantity UInt64,
            total_sales_amount Float64,
            avg_check Float64
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )

    write_report(
        report_store_sales,
        "report_store_sales",
        """
        CREATE TABLE report_store_sales (
            store_key UInt64,
            store_name Nullable(String),
            store_city Nullable(String),
            store_country Nullable(String),
            orders_count UInt64,
            total_quantity UInt64,
            total_sales_amount Float64,
            avg_check Float64
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )

    write_report(
        report_supplier_sales,
        "report_supplier_sales",
        """
        CREATE TABLE report_supplier_sales (
            supplier_key UInt64,
            supplier_name Nullable(String),
            supplier_city Nullable(String),
            supplier_country Nullable(String),
            products_count UInt64,
            orders_count UInt64,
            total_quantity UInt64,
            total_sales_amount Float64
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )

    write_report(
        report_product_quality,
        "report_product_quality",
        """
        CREATE TABLE report_product_quality (
            product_key UInt64,
            product_name Nullable(String),
            product_category Nullable(String),
            product_brand Nullable(String),
            avg_rating Nullable(Float64),
            avg_reviews Nullable(Float64),
            orders_count UInt64,
            total_quantity_sold UInt64,
            total_sales_amount Float64
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )

    print("All ClickHouse reports have been created successfully.")

    spark.stop()


if __name__ == "__main__":
    main()