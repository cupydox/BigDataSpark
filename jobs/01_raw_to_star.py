from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdata"
POSTGRES_PROPERTIES = {
    "user": "bigdata",
    "password": "bigdata",
    "driver": "org.postgresql.Driver",
}


def read_postgres_table(spark: SparkSession, table_name: str):
    return (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .load()
    )


def write_postgres_table(df, table_name: str):
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .mode("overwrite")
        .save()
    )


def add_surrogate_key(df, key_name: str, order_columns: list):
    window = Window.orderBy(*[F.col(column).asc_nulls_last() for column in order_columns])
    return df.withColumn(key_name, F.row_number().over(window))


def join_dimension(fact_df, dim_df, dim_key: str, join_columns: list):
    fact_alias = fact_df.alias("fact")
    dim_alias = dim_df.alias("dim")

    join_condition = reduce(
        lambda left, right: left & right,
        [
            F.col(f"fact.{column}").eqNullSafe(F.col(f"dim.{column}"))
            for column in join_columns
        ],
    )

    return (
        fact_alias
        .join(dim_alias, join_condition, "left")
        .select(
            *[F.col(f"fact.{column}") for column in fact_df.columns],
            F.col(f"dim.{dim_key}"),
        )
    )


def main():
    spark = (
        SparkSession.builder
        .appName("RawSalesToStarSchema")
        .getOrCreate()
    )

    raw = read_postgres_table(spark, "raw_sales")

    sales = (
        raw
        .withColumn("raw_row_id", F.monotonically_increasing_id())
        .withColumn("source_id", F.col("id").cast("int"))
        .withColumn("customer_age", F.col("customer_age").cast("int"))
        .withColumn("product_price", F.col("product_price").cast("double"))
        .withColumn("product_quantity", F.col("product_quantity").cast("int"))
        .withColumn("sale_date", F.to_date(F.col("sale_date"), "M/d/yyyy"))
        .withColumn("sale_customer_id", F.col("sale_customer_id").cast("int"))
        .withColumn("sale_seller_id", F.col("sale_seller_id").cast("int"))
        .withColumn("sale_product_id", F.col("sale_product_id").cast("int"))
        .withColumn("sale_quantity", F.col("sale_quantity").cast("int"))
        .withColumn("sale_total_price", F.col("sale_total_price").cast("double"))
        .withColumn("product_weight", F.col("product_weight").cast("double"))
        .withColumn("product_rating", F.col("product_rating").cast("double"))
        .withColumn("product_reviews", F.col("product_reviews").cast("int"))
        .withColumn("product_release_date", F.to_date(F.col("product_release_date"), "M/d/yyyy"))
        .withColumn("product_expiry_date", F.to_date(F.col("product_expiry_date"), "M/d/yyyy"))
    )

    customer_columns = [
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
    ]

    seller_columns = [
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code",
    ]

    product_columns = [
        "product_name",
        "product_category",
        "product_price",
        "product_quantity",
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
    ]

    store_columns = [
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ]

    supplier_columns = [
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ]

    dim_customers = (
        add_surrogate_key(
            sales.select(*customer_columns).distinct(),
            "customer_key",
            customer_columns,
        )
        .select("customer_key", *customer_columns)
    )

    dim_sellers = (
        add_surrogate_key(
            sales.select(*seller_columns).distinct(),
            "seller_key",
            seller_columns,
        )
        .select("seller_key", *seller_columns)
    )

    dim_products = (
        add_surrogate_key(
            sales.select(*product_columns).distinct(),
            "product_key",
            product_columns,
        )
        .select("product_key", *product_columns)
    )

    dim_stores = (
        add_surrogate_key(
            sales.select(*store_columns).distinct(),
            "store_key",
            store_columns,
        )
        .select("store_key", *store_columns)
    )

    dim_suppliers = (
        add_surrogate_key(
            sales.select(*supplier_columns).distinct(),
            "supplier_key",
            supplier_columns,
        )
        .select("supplier_key", *supplier_columns)
    )

    dim_dates = (
        sales
        .select("sale_date")
        .distinct()
        .withColumn("date_key", F.date_format(F.col("sale_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("sale_date"))
        .withColumn("quarter", F.quarter("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .withColumn("day", F.dayofmonth("sale_date"))
        .withColumn("day_of_week", F.date_format(F.col("sale_date"), "E"))
        .select(
            "date_key",
            "sale_date",
            "year",
            "quarter",
            "month",
            "day",
            "day_of_week",
        )
    )

    fact = sales

    fact = join_dimension(fact, dim_customers, "customer_key", customer_columns)
    fact = join_dimension(fact, dim_sellers, "seller_key", seller_columns)
    fact = join_dimension(fact, dim_products, "product_key", product_columns)
    fact = join_dimension(fact, dim_stores, "store_key", store_columns)
    fact = join_dimension(fact, dim_suppliers, "supplier_key", supplier_columns)

    fact_sales = (
        fact
        .join(dim_dates.select("date_key", "sale_date"), on="sale_date", how="left")
        .withColumn(
            "sale_key",
            F.row_number().over(Window.orderBy(F.col("raw_row_id"))),
        )
        .select(
            "sale_key",
            "source_id",
            "customer_key",
            "seller_key",
            "product_key",
            "store_key",
            "supplier_key",
            "date_key",
            "sale_customer_id",
            "sale_seller_id",
            "sale_product_id",
            "sale_quantity",
            "sale_total_price",
        )
    )

    write_postgres_table(dim_customers, "dim_customers")
    write_postgres_table(dim_sellers, "dim_sellers")
    write_postgres_table(dim_products, "dim_products")
    write_postgres_table(dim_stores, "dim_stores")
    write_postgres_table(dim_suppliers, "dim_suppliers")
    write_postgres_table(dim_dates, "dim_dates")
    write_postgres_table(fact_sales, "fact_sales")

    print("Star schema has been created successfully.")
    print(f"raw_sales count: {sales.count()}")
    print(f"fact_sales count: {fact_sales.count()}")

    spark.stop()


if __name__ == "__main__":
    main()