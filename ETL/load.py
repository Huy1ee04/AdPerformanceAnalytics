from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, hash, input_file_name, regexp_extract, sum as _sum, when
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv("/opt/spark/.env")

# Configure environment variables
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = "vdt2025-ads-data"

MARIADB_HOST = os.getenv("MARIADB_HOST", "mariadb")
MARIADB_PORT = os.getenv("MARIADB_PORT", "3306")
MARIADB_USER = os.getenv("MARIADB_USER")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD")
MARIADB_DATABASE = "ads_schema"

if not all([MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MARIADB_USER, MARIADB_PASSWORD]):
    raise ValueError("Missing required credentials in .env file!")

MARIADB_URL = f"jdbc:mariadb://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DATABASE}"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Aggregated Ad Performance") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/opt/bitnami/spark/jars/mariadb-java-client-3.3.3.jar,/opt/bitnami/spark/jars/hadoop-aws-3.4.1.jar,/opt/bitnami/spark/jars/aws-java-sdk-1.12.785.jar") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# exchange_rates_schema = StructType([
#     StructField("date_id", StringType(), True),
#     StructField("currency", StringType(), True),
#     StructField("exchange_rate_usd", FloatType(), True)
# ])

# Read exchange_rates from MariaDB without manual schema
exchange_rates_df = spark.read \
    .format("jdbc") \
    .option("url", MARIADB_URL) \
    .option("dbtable", "exchange_rates") \
    .option("user", MARIADB_USER) \
    .option("password", MARIADB_PASSWORD) \
    .option("numPartitions", 4) \
    .option("header", "false") \
    .load()
print("Schema of exchange_rates_df:")
exchange_rates_df.printSchema()
print("Loaded exchange_rates from MariaDB:")
exchange_rates_df.show(10)

# Process data from MinIO
prefixes = ["facebook_data", "google_data"]
all_dfs = []
for prefix in prefixes:
    df = spark.read.option("basePath", f"s3a://{MINIO_BUCKET}/{prefix}") \
                   .option("mergeSchema", "true") \
                   .parquet(f"s3a://{MINIO_BUCKET}/{prefix}/year=2024/month=6/*/batch_*.parquet")
    print(f"Schema của {prefix}:")
    df.printSchema()
    print(f"Số bản ghi trong {prefix} trước khi lọc: {df.count()}")

    # đặt alias cho 2 DataFrame
    ads_df = df.alias("ads")               # dữ liệu quảng cáo đã lọc
    er_df  = exchange_rates_df.alias("er") # bảng tỷ giá

    # điều kiện join
    join_cond = (
        col("ads.day") == col("er.date_id")
    ) & (
        col("ads.currency") == col("er.currency")
    )

    # thực hiện join
    df_joined = (
        ads_df.join(er_df, join_cond, "left")
            .withColumn("channel_id", lit(prefix.split("_")[0]))
            .select(
                "channel_id",
                col("ads.country").alias("country"),
                col("er.date_id").alias("date_id"),
                col("er.currency").alias("currency"),
                "impressions",
                "cost",
                col("er.exchange_rate_usd").alias("exchange_rate_usd")
            )
    )

    print(f"Sau khi join với exchange_rates_df '{prefix}':")
    df_joined.printSchema()
    df_joined.show(5)

    aggregated_df = df_joined.groupBy("channel_id", "country", "date_id", "currency","exchange_rate_usd") \
                      .agg(
                          _sum("impressions").alias("impressions"),
                          _sum("cost").alias("cost_original")
                      )
    print(f"Dữ liệu đã aggregate '{prefix}':")
    aggregated_df.printSchema()
    aggregated_df.show(5)

    all_dfs.append(aggregated_df)

if not all_dfs:
    print("Không có dữ liệu nào từ các parquet files.")
    spark.stop()
    exit()

# Combine all DataFrames
combined_df = all_dfs[0].union(all_dfs[1]) if len(all_dfs) > 1 else all_dfs[0]

# Convert cost_original to cost_usd
combined_df = combined_df \
    .withColumn("effective_rate", 
                when(col("currency") == "USD", lit(1.0))
                .when(col("exchange_rate_usd").isNotNull(), col("exchange_rate_usd"))
                .otherwise(lit(1.0))) \
    .withColumn("cost_usd", 
                when(col("effective_rate").isNotNull(), 
                     col("cost_original") * col("effective_rate"))  
                .otherwise(col("cost_original"))) \
    .withColumn("cpi_usd", 
                when(col("impressions") != 0, col("cost_usd") / col("impressions"))
                .otherwise(None)) \
    .drop("exchange_rate_usd", "effective_rate")

# Create fact_ad_performance
fact_ad_performance = combined_df \
    .withColumn("channel_id", 
                when(col("channel_id") == "facebook", lit("facebook"))
                .when(col("channel_id") == "google", lit("google"))
                .otherwise(lit("unknown"))) \
    .select(
        col("channel_id").cast(StringType()),
        col("date_id").cast(StringType()),
        col("country").cast(StringType()),
        col("currency").cast(StringType()),
        col("cost_original").cast("double"),
        col("impressions").cast("int"),
        col("cost_usd").cast("double"),
        col("cpi_usd").cast("double")
    )
print("Dữ liệu chuẩn bị ghi vào fact_ad_performance:")
fact_ad_performance.show(5)
if fact_ad_performance.rdd.isEmpty():
    print("Không có bản ghi nào để ghi vào MariaDB.")
    spark.stop()
    exit()

# Write to MariaDB
try:
    fact_ad_performance.write \
        .format("jdbc") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "fact_ad_performance") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
        .option("batchsize", "10000") \
        .mode("append") \
        .save()
    print("Dữ liệu đã ghi vào bảng fact_ad_performance thành công.")
except Exception as e:
    print(f"Lỗi khi ghi vào MariaDB: {str(e)}")
    spark.stop()
    exit(1)

# Stop Spark session
spark.stop()