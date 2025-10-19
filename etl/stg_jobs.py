import os
import argparse
from datetime import date, timedelta
import calendar

from pyspark.sql import SparkSession, functions as F, types as T
import psycopg

# ---------------------------------------------------------------------
# Config qua ENV
# ---------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB", "retaildb")
PG_USER = os.getenv("PG_USER", "retail")
PG_PW   = os.getenv("PG_PASSWORD", "retailpw")
PG_URL  = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_NATIVE_URL = f"postgresql://{PG_USER}:{PG_PW}@{PG_HOST}:{PG_PORT}/{PG_DB}"

PG_DRIVER = "org.postgresql.Driver"
STG_SCHEMA = "stg"      # staging schema
SRC_SCHEMA = "public"   # source schema

## --------------------------------------------------------------------
UNKNOWN_VALUE = 'UNKNOWN'
UNKNOWN_ID = 0

## --------------------------------------------------------------------

# ---------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------
def get_spark(app="Retail Staging Jobs"):
    return (
        SparkSession.builder.appName(app)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def pg_execute(sql: str, params=None):
    """Run a SQL statement on Postgres using psycopg."""
    with psycopg.connect(PG_NATIVE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()

def ensure_schema_and_tables():
    """Create schema/tables if missing (safe to call repeatedly)."""
    ddl_sales_tx = f"""
    CREATE SCHEMA IF NOT EXISTS {STG_SCHEMA};

    CREATE TABLE IF NOT EXISTS {STG_SCHEMA}.stg_sales_transactions (
      run_date         date,
      order_date       date,
      order_id         text ,
      order_item_id    text ,
      customer_id      text,
      store_id         text,
      employee_id      text,
      channel_id        int,
      product_id       text,
      qty              int,
      unit_price       numeric(18,2),
      discount_amount  numeric(18,2),
      subtotal         numeric(18,2),
      cost_at_sale     numeric(18,2),
      profit_amount    numeric(18,2),
      status           text,
      payment_method   text,
      created_at       timestamptz DEFAULT now(),
      updated_at       timestamptz DEFAULT now(), 
      PRIMARY KEY (order_id, order_item_id)
    );

    CREATE TABLE IF NOT EXISTS {STG_SCHEMA}.stg_channel_performance (
      run_date             date,
      channel_id            int, 
      order_date           date,
      channel_name         text,
      channel_type         text,
      commission_rate      numeric(6,4),
      monthly_fixed_cost   numeric(18,2),
      customer_acquisition_cost numeric(18,2),
      order_count               int,
      revenue          numeric(18,2),
      created_at           timestamptz DEFAULT now(),
      updated_at           timestamptz DEFAULT now(),
      PRIMARY KEY (order_date, channel_name)
    );
    """
    pg_execute(ddl_sales_tx)

def delete_partition(table: str, part_col: str, part_value: str):
    """Idempotent: delete that day's data before appending."""
    sql = f'DELETE FROM {table} WHERE {part_col} = %s;'
    pg_execute(sql, (part_value,))

def write_df_to_pg(df, table: str):
    (df.write
       .format("jdbc")
       .option("url", PG_URL)
       .option("dbtable", table)
       .option("user", PG_USER)
       .option("password", PG_PW)
       .option("driver", PG_DRIVER)
       .mode("append")
       .save())

# ---------------------------------------------------------------------
# Sources as DataFrames (with pushdown predicates)
# ---------------------------------------------------------------------
def read_orders(spark, d_str):
    return (spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", f"(SELECT * FROM {SRC_SCHEMA}.orders WHERE order_date = DATE '{d_str}') o")
            .option("user", PG_USER).option("password", PG_PW)
            .option("driver", PG_DRIVER).load())

def read_order_items(spark):
    return (spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", f"(SELECT * FROM {SRC_SCHEMA}.order_items) i")
            .option("user", PG_USER).option("password", PG_PW)
            .option("driver", PG_DRIVER).load())

def read_products(spark):
    return (spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", f"(SELECT * FROM {SRC_SCHEMA}.products) p")
            .option("user", PG_USER).option("password", PG_PW)
            .option("driver", PG_DRIVER).load())

def read_channels(spark):
    return (spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", f"(SELECT * FROM {SRC_SCHEMA}.channels) c")
            .option("user", PG_USER).option("password", PG_PW)
            .option("driver", PG_DRIVER).load())

# ---------------------------------------------------------------------
# Job 1: STG_SalesTransactions
# ---------------------------------------------------------------------
def job_stg_sales_transactions(spark, run_date: str):
    ensure_schema_and_tables()



    o = read_orders(spark, run_date)
    i = read_order_items(spark)
    p = read_products(spark)
    c = read_channels(spark)

    oA = o.alias("o")
    iA = i.alias("i")
    pA = p.alias("p")
    cA = c.alias("c")
    # join
    df = (
        oA.join(iA, F.col("o.order_id") == F.col("i.order_id"), "inner")
        .join(pA, F.col("i.product_id") == F.col("p.product_id"), "left")
        .join(cA, F.col("o.channel") == F.col("c.channel_id"), "left")
    )

    # compute fields
    df2 = (
        df.select(

            F.lit(run_date).cast(T.DateType()).alias("run_date"),
            F.col("o.order_date").cast(T.DateType()).alias("order_date"),
            F.col("o.order_id").alias("order_id"),
            F.col("i.item_id").alias("order_item_id"),

            F.col("o.customer_id").alias("customer_id"),
            F.col("o.store_id").alias("store_id"),
            F.col("o.employee_id").alias("employee_id"),
            F.col("c.channel_id").alias("channel_id"),
            F.col("i.product_id").alias("product_id"),

            F.col("i.quantity").cast("int").alias("qty"),
            F.col("i.unit_price").cast(T.DecimalType(18, 2)).alias("unit_price"),
            F.coalesce(F.col("i.discount"), F.lit(0)).cast(T.DecimalType(18, 2)).alias("discount_amount"),

            (F.col("i.quantity") * F.col("i.unit_price") - F.coalesce(F.col("i.discount"), F.lit(0)))
            .cast(T.DecimalType(18, 2)).alias("subtotal"),

            (F.col("i.quantity") * F.coalesce(F.col("p.cost_price"), F.lit(0)))
            .cast(T.DecimalType(18, 2)).alias("cost_at_sale"),

            ((F.col("i.quantity") * F.col("i.unit_price") - F.coalesce(F.col("i.discount"), F.lit(0)))
             - (F.col("i.quantity") * F.coalesce(F.col("p.cost_price"), F.lit(0))))
            .cast(T.DecimalType(18, 2)).alias("profit_amount"),

            F.col("o.payment_method").alias("payment_method"),
            F.col("o.status").alias("status"),

            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
    )
    ## handle null value
    df2 = df2.fillna({"channel_id": UNKNOWN_ID , "product_id": UNKNOWN_ID})


    # idempotent write
    target = f"{STG_SCHEMA}.stg_sales_transactions"
    delete_partition(target, "order_date", run_date)
    write_df_to_pg(df2, target)

# ---------------------------------------------------------------------
# Job 2: STG_ChannelPerformance
# ---------------------------------------------------------------------
def job_stg_channel_performance(spark, run_date: str):
    ensure_schema_and_tables()

    o = read_orders(spark, run_date)
    i = read_order_items(spark)
    ch = read_channels(spark)

    # base joins
    items_of_day = (o.select("order_id","order_date","channel")
                      .join(i.select("order_id","item_id"),
                            on="order_id", how="inner"))

    items_agg = (items_of_day.groupBy("order_date","channel")
                 .agg(F.count("*").alias("items")))

    orders_agg = (o.groupBy("order_date","channel")
                    .agg(F.countDistinct("order_id").alias("orders"),
                         F.sum("final_amount").cast(T.DecimalType(18,2)).alias("gross_revenue")))

    # join channel costs
    joined = (orders_agg
              .join(items_agg, ["order_date","channel"], "left")
              .join(ch.select(
                        F.col("channel_name"),
                        F.col("channel_id"),
                        "type",
                        "commission_rate","monthly_fixed_cost","avg_acquisition_cost"),
                    orders_agg.channel == F.col("channel_id"), "left")
             )

    # days in month for fixed cost allocation
    y, m, d = [int(x) for x in run_date.split("-")]
    dim = calendar.monthrange(y, m)[1]
    days_in_month = float(dim)

    df2 = (joined
        .withColumn("run_date", F.lit(run_date).cast(T.DateType()))
        .withColumn("commission_rate", F.coalesce(F.col("commission_rate"), F.lit(0.0)))
        .withColumn("monthly_fixed_cost", F.coalesce(F.col("monthly_fixed_cost"), F.lit(0.0)))
        .withColumn("customer_acquisition_cost", F.coalesce(F.col("avg_acquisition_cost"), F.lit(0.0)))
        .withColumn("order_count", F.coalesce(F.col("orders"), F.lit(0)))
        .withColumn("revenue", F.coalesce(F.col("gross_revenue"), F.lit(0)).cast(T.DecimalType(18,2)))
        .withColumn("commission_cost",
                    (F.col("gross_revenue") * F.col("commission_rate")).cast(T.DecimalType(18,2)))
        .withColumn("cac_cost",
                    (F.col("orders") * F.col("avg_acquisition_cost")).cast(T.DecimalType(18,2)))
        .withColumn("fixed_cost_alloc",
                    (F.col("monthly_fixed_cost") / F.lit(days_in_month)).cast(T.DecimalType(18,2)))
        .withColumn("total_cost",
                    (F.col("commission_cost") + F.col("cac_cost") + F.col("fixed_cost_alloc")).cast(T.DecimalType(18,2)))
        .withColumn("net_revenue",
                    (F.col("gross_revenue") - F.col("total_cost")).cast(T.DecimalType(18,2)))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "run_date",
            F.col("order_date"),
            F.col("channel_name"),
            F.col("type").alias("channel_type"),
            F.col("channel_id"),
            "commission_rate","monthly_fixed_cost","customer_acquisition_cost",
            "order_count","revenue",
            "created_at","updated_at"
        )
    )
    df2 = df2.fillna({'channel_name' : UNKNOWN_VALUE , "channel_id" : UNKNOWN_ID , "channel_type" : UNKNOWN_VALUE})

    # df2.printSchema()
    target = f"{STG_SCHEMA}.stg_channel_performance"
    delete_partition(target, "order_date", run_date)
    write_df_to_pg(df2, target)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

if __name__ == "__main__":
    # how to run this file :
    # python ./etl/stg_jobs.py --date 2025-01-15 --job sales_tx
    # replace --date value and --job value

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="yyyy-mm-dd, ví dụ 2025-01-15")
    parser.add_argument("--job", required=True,
                        choices=["sales_tx","channel_perf","both"],
                        help="chạy job nào")
    args = parser.parse_args()

    spark = get_spark()

    if args.job in ("sales_tx","both"):
        job_stg_sales_transactions(spark, args.date)

    if args.job in ("channel_perf","both") :
        job_stg_channel_performance(spark, args.date)

    spark.stop()
