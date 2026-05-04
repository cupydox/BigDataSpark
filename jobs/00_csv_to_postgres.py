from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CSV_PATH = "/app/исходные данные/*.csv"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdata"
POSTGRES_USER = "bigdata"
POSTGRES_PASSWORD = "bigdata"
POSTGRES_DRIVER = "org.postgresql.Driver"

RAW_COLUMNS = [
    "id",
    "customer_first_name",
    "customer_last_name",
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
    "product_name",
    "product_category",
    "product_price",
    "product_quantity",
    "sale_date",
    "sale_customer_id",
    "sale_seller_id",
    "sale_product_id",
    "sale_quantity",
    "sale_total_price",
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
    "pet_category",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_date",
    "product_expiry_date",
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
]


def write_postgres_table(df, table_name: str):
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .mode("overwrite")
        .save()
    )


def main():
    spark = (
        SparkSession.builder
        .appName("CsvToPostgresRawSales")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(CSV_PATH)
    )

    raw = raw.toDF(*[column.strip() for column in raw.columns])

    missing_columns = [column for column in RAW_COLUMNS if column not in raw.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in CSV files: {missing_columns}")

    raw_sales = raw.select(
        *[
            F.col(column).cast("string").alias(column)
            for column in RAW_COLUMNS
        ]
    )

    write_postgres_table(raw_sales, "raw_sales")

    print("CSV files have been loaded to PostgreSQL by Spark.")
    print(f"raw_sales count: {raw_sales.count()}")

    spark.stop()


if __name__ == "__main__":
    main()