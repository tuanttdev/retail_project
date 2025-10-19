import os
from datetime import datetime, date, timedelta

from py4j.protocol import NULL_TYPE
from pyspark.sql import SparkSession, functions as F, types as T
import clickhouse_connect

PG_URL  = f"jdbc:postgresql://{os.getenv('PG_HOST','localhost')}:{os.getenv('PG_PORT','5432')}/{os.getenv('PG_DB','retaildb')}"
PG_USER = os.getenv("PG_USER","retail")
PG_PW   = os.getenv("PG_PASSWORD","retailpw")
PG_SCHEMA_SRC   = os.getenv("PG_SCHEMA_SRC","public")
PG_SCHEMA_STG   = os.getenv("PG_SCHEMA_STG","stg")

CH_URL  = f"jdbc:clickhouse://{os.getenv('CH_HOST','localhost')}:{os.getenv('CH_PORT','8123')}/{os.getenv('CH_DB','retail')}"
CH_USER = os.getenv("CH_USER","default")
CH_PW   = os.getenv("CH_PASSWORD","")

def spark(app="Retail Spark Loader"):
    return (SparkSession.builder.appName(app)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages",
                    "org.postgresql:postgresql:42.7.4,"
                    "com.clickhouse:clickhouse-jdbc:0.6.1,"
                    "com.clickhouse:clickhouse-http-client:0.6.1,"
                    # "org.apache.httpcomponents.client5:httpclient5:5.2.1,"
                    # "org.apache.httpcomponents.core5:httpcore5:5.2.1,"
                    # "org.apache.httpcomponents.core5:httpcore5-h2:5.2.1"
                    )
            .getOrCreate())

def ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST","localhost"),
        port=int(os.getenv("CH_PORT","8123")),
        username=CH_USER, password=CH_PW, database="retail"
    )

def ch_exec(sql:str):
    ch_client().command(sql)


def _read_pg_query(spark, sql: str):
    return (spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("user", PG_USER).option("password", PG_PW)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", 10000)
            .option("query", sql).load())

def _read_ch_query(spark, sql: str):
    return (spark.read.format("jdbc")
            .option("url", CH_URL)
            .option("user", CH_USER).option("password", CH_PW)
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("fetchsize", 10000)
            .option("query", sql).load())


def _write_ch(df, table: str, mode="append"):
    (df.write.format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", table)
        .option("user", CH_USER).option("password", CH_PW)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode(mode).save())

# ---------- DDL (chạy 1 lần) ----------
def ensure_wh_schema():
    ch_exec("CREATE DATABASE IF NOT EXISTS retail")
    ch_exec("""
    CREATE TABLE IF NOT EXISTS retail.dim_channel(
        channel_key UUID, channel_id UInt8, channel_name String, channel_type String,
        commission_rate Decimal(6,4), monthly_fixed_cost Decimal(18,2),
        customer_acquisition_cost Decimal(18,2),
        is_active UInt8, is_digital UInt8, 
        from_date Date,
        to_date Date NULL,
    ) ENGINE=MergeTree ORDER BY channel_key
    """)
    ch_exec("""
    CREATE TABLE IF NOT EXISTS retail.dim_product(
      product_key UUID DEFAULT generateUUIDv4(), product_id String, product_name String,
      category String, brand String, cost_price Decimal(18,2),
      selling_price Decimal(18,2), 
        is_new UInt8,
        update_date Date,
    ) ENGINE=MergeTree ORDER BY product_key
    """)
    ch_exec("""
    CREATE TABLE IF NOT EXISTS retail.dim_date(
      date_key UInt32, full_date Date, day_of_week UInt8, day_name String,
      week_of_year UInt8, month UInt8, month_name String, quarter UInt8,
      year UInt16, is_weekend UInt8
    ) ENGINE=MergeTree ORDER BY date_key
    """)
    ch_exec("""
    CREATE TABLE IF NOT EXISTS retail.fact_sales(
      order_date Date, date_key UInt32, channel_key UUID, product_key UUID,
      order_id String, order_item_id String,
      qty UInt32, unit_price Decimal(18,2), discount Decimal(18,2), amount Decimal(18,2), 
        cost_amount Decimal(18,2), profit_amount Decimal(18,2)
        
    ) ENGINE=MergeTree PARTITION BY toYYYYMM(order_date)
      ORDER BY (order_date, order_id, order_item_id)
    """)

# ---------- Loaders (Spark) ----------
def load_dim_channel_spark(run_date: str):
    ensure_wh_schema()
    s = spark("Load DIM Channel")
    s.sparkContext.setLogLevel("ERROR")

    d = run_date
    df = (_read_pg_query(s, f"""
        SELECT channel_id, channel_name, channel_type, commission_rate, monthly_fixed_cost, customer_acquisition_cost
        FROM {PG_SCHEMA_STG}.stg_channel_performance
        WHERE order_date = DATE'{d}'
    """)
        .select(
        "channel_id",
        "channel_name",

        F.col("channel_type"),
        F.col("commission_rate").cast(T.DecimalType(6,4)),
        F.col("monthly_fixed_cost").cast(T.DecimalType(18,2)),
        F.col("customer_acquisition_cost").cast(T.DecimalType(18,2)),
        F.lit(1).cast(T.IntegerType()).alias("is_active"),
        F.lit(1).cast(T.IntegerType()).alias("is_digital")

    ).withColumn("channel_key", F.expr("uuid()"))
    .withColumn('from_date', F.now()))

    # wh
    dim_channel = (_read_ch_query(s, f"""
                    select  channel_id, channel_key,channel_name, channel_type, commission_rate, monthly_fixed_cost, customer_acquisition_cost, is_digital, is_active, from_date, to_date
                    from dim_channel where to_date is null """) )

    dim_channel = dim_channel.withColumn("is_new", F.lit(0))
    #


    join_condition = """dim.channel_id = ch.channel_id
                        AND dim.channel_name = ch.channel_name
                        AND dim.channel_type = ch.channel_type
                        AND dim.commission_rate = ch.commission_rate
                        AND dim.monthly_fixed_cost = ch.monthly_fixed_cost
                        AND dim.customer_acquisition_cost = ch.customer_acquisition_cost
                        AND dim.is_active = ch.is_active
                        AND dim.is_digital = ch.is_digital"""

    joined  = df.alias('ch').join(
                        dim_channel.alias('dim'),
                        on = F.expr(join_condition),
                        how = 'inner' ).select(F.col("ch.channel_id"),
                        "dim.channel_key", 'ch.channel_name', 'ch.channel_type', 'ch.commission_rate','ch.monthly_fixed_cost', 'ch.customer_acquisition_cost', 'ch.is_active', 'ch.is_digital'
                        , "dim.from_date", "dim.to_date")
    #

    new_records = df.alias('ch').join(
                        dim_channel.alias('dim'),
                        on = F.expr(join_condition),
                        how = 'left' ).filter("dim.channel_key is null ").select(F.col("ch.channel_id"),
                        "ch.channel_key", 'ch.channel_name', 'ch.channel_type', 'ch.commission_rate','ch.monthly_fixed_cost', 'ch.customer_acquisition_cost', 'ch.is_active', 'ch.is_digital'
                        , "ch.from_date", F.lit(None).alias("to_date"))
    #

    old_records = dim_channel.alias('dim').join(
                        df.alias('ch'),
                        on = F.expr(join_condition),
                        how='left').filter("ch.channel_key is null ").select(F.col("dim.channel_id"),
                            "dim.channel_key", 'dim.channel_name', 'dim.channel_type', 'dim.commission_rate', 'dim.monthly_fixed_cost',
                            'dim.customer_acquisition_cost', 'dim.is_active', 'dim.is_digital'
                            , "dim.from_date", F.date_sub(F.current_date(), 1).alias("to_date"))
    #
    # joined.show()
    # new_records.show()
    # old_records.show()

    new_dim = joined.union(new_records).union(old_records)
    new_dim = new_dim.fillna({"channel_name" : "UNKNOWN" , "channel_type" : "UNKNOWN" , "channel_id" : 0})
    # union to old and new value to get rid of duplication


    # replace (truncate + append)

    new_dim_cache = new_dim.cache()
    new_dim_cache.count()
    ch_exec("DELETE FROM retail.dim_channel WHERE to_date is NULL")
    # new_dim_cache.show()
    _write_ch(new_dim_cache, "retail.dim_channel")
    # new_dim_cache.show()
    s.stop()

def load_dim_product_spark(run_date: str):
    ensure_wh_schema()
    s = spark("Load DIM Product")

    df = _read_pg_query(s, f"""
        SELECT product_id, name, category, brand, cost_price, selling_price
        FROM {PG_SCHEMA_SRC}.products
    """).select(
        "product_id",
        F.col("name").alias("product_name"),
        "category","brand",
        F.col("cost_price").cast(T.DecimalType(18,2)),
        F.col("selling_price").cast(T.DecimalType(18,2))
    )
    ch_exec("TRUNCATE TABLE retail.dim_product")
    _write_ch(df, "retail.dim_product")
    s.stop()

def load_dim_date_spark(start_date="2024-01-01", end_date="2026-12-31"):
    ensure_wh_schema()
    s = spark("Load DIM Date")
    sd = datetime.strptime(start_date, "%Y-%m-%d").date()
    ed = datetime.strptime(end_date, "%Y-%m-%d").date()
    days = (ed - sd).days + 1
    df = (s.range(0, days)
            .withColumn("full_date", F.expr(f"date_add(to_date('{start_date}'), CAST(id AS INT))"))
            .select(
                # date_key = yyyymmdd (Spark: date_format) → int/long để CH map sang UInt32
                F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
                F.col("full_date").cast(T.DateType()).alias("full_date"),
                F.dayofweek("full_date").cast("int").alias("day_of_week"),      # 1=Sun..7=Sat trong Spark
                F.date_format("full_date", "E").alias("day_name"),
                F.weekofyear("full_date").cast("int").alias("week_of_year"),
                F.month("full_date").cast("int").alias("month"),
                F.date_format("full_date", "MMM").alias("month_name"),
                F.quarter("full_date").cast("int").alias("quarter"),
                F.year("full_date").cast("int").alias("year"),
                F.when(F.dayofweek("full_date").isin(1, 7), F.lit(1)).otherwise(F.lit(0)).alias("is_weekend")
            ))
    ch_exec("TRUNCATE TABLE retail.dim_date")
    _write_ch(df, "retail.dim_date")
    s.stop()

def load_fact_sales_spark(run_date:str):
    """
    Idempotent load for a specific day from staging.stg_sales_transactions.
    """
    ensure_wh_schema()
    s = spark("Load FACT Sales")
    d = run_date
    # lọc theo ngày để pushdown ở Postgres
    q = f"""
      SELECT order_date, order_id, order_item_id, channel_id, product_id,
             qty, unit_price, discount_amount, subtotal, cost_at_sale, profit_amount
      FROM {PG_SCHEMA_STG}.stg_sales_transactions
      WHERE order_date = DATE '{d}'
    """

    df = _read_pg_query(s, q).select(
        F.col("order_date").cast(T.DateType()).alias("order_date"),

        # date_key: yyyymmdd -> int
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int").alias("date_key"),

        "channel_id",
        "product_id",
        "order_id",
        "order_item_id",

        F.col("qty").cast("int").alias("qty"),

        # tiền tệ dùng Decimal
        F.col("unit_price").cast(T.DecimalType(18, 2)).alias("unit_price"),
        F.col("discount_amount").cast(T.DecimalType(18, 2)).alias("discount"),
        F.col("subtotal").cast(T.DecimalType(18, 2)).alias("amount"),
        F.col("cost_at_sale").cast(T.DecimalType(18, 2)).alias("cost_amount"),
        F.col("profit_amount").cast(T.DecimalType(18, 2)).alias("profit_amount"),
    )

    ## i don't know what i am doing
    ## get product_id, channel_id, order_id, order_item_id
    channel_ids = df.select("channel_id").distinct()
    order_ids = df.select("order_id").distinct()
    product_ids = df.select("product_id").distinct()
    order_item_ids = df.select("order_item_id").distinct()
    s = spark("Load DIM Channel")
    dim_channel = _read_ch_query(s, """select channel_id, channel_key from dim_channel where to_date is NULL """) #.alias("ch")
    dim_product = _read_ch_query(s, """select product_id, product_key from dim_product """) #.alias("pr")
    #
    # dim = dim_channel.alias("ch")
    # dim = dim_channel.alias("ch")
    dim_channel.show()
    df.printSchema()
    df = (df.join(dim_channel, "channel_id", how="left" )
            .join(dim_product, "product_id", how="left" )
            .select('order_date', 'date_key', dim_channel["channel_key"] , dim_product['product_key'] , 'order_id', 'order_item_id', 'qty', 'unit_price', 'discount', 'amount', 'cost_amount', 'profit_amount')
            .drop(dim_channel["channel_id"] , dim_product["product_id"]))
    #
    # UNKNOWN_KEY = dim_channel.filter("channel_id = 0 ").first()["channel_key"]
    # print(UNKNOWN_KEY)
    # df.fillna({'channel_id' : UNKNOWN_KEY})

    # dimension_key_sql = f"""select """
    # _read_ch_query("Load Dimensions key for fact sales", dimension_key_sql)

    # idempotent: xoá ngày cũ ở CH rồi append

    df.show(truncate=False)
    ch_exec(f"ALTER TABLE retail.fact_sales DELETE WHERE order_date = toDate('{d}')")

    _write_ch(df, "retail.fact_sales")
    s.stop()
## 2febdb53-79c5-446f-8625-fdd100bf3e4

def load_warehouse_for_date_spark(run_date:str, date_start="2024-01-01", date_end="2027-12-31"):
    load_dim_channel_spark(run_date)
    load_dim_product_spark(run_date)
    load_dim_date_spark(date_start, date_end)
    load_fact_sales_spark(run_date)

if __name__ == '__main__':
    # d = date(day = 14, month = 1 , year = 2025)
    # d2 = date(day = 23, month = 1 , year = 2025)
    # for i in range(11):
    #     d1 = d + timedelta(days=i)
    #     load_warehouse_for_date_spark(d1.strftime("%Y%m%d"))


    # load_dim_channel_spark("2025-01-20")
    d1 = date(day=24, month=1, year=2025)
    load_warehouse_for_date_spark(d1.strftime("%Y%m%d"))