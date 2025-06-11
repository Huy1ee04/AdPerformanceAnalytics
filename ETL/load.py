from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, hash, input_file_name, regexp_extract, sum as _sum, when
from pyspark.sql.functions import concat, lpad
from pyspark.sql.types import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv
import pymysql
import pandas as pd

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
    .config("spark.jars", "/opt/bitnami/spark/jars/mariadb-java-client-3.3.2.jar,/opt/bitnami/spark/jars/hadoop-aws-3.4.1.jar,/opt/bitnami/spark/jars/aws-java-sdk-1.12.785.jar") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


# Cách 1: Đọc exchange_rates từ MariaDB<->Spark
# exchange_rates_df = spark.read \
#     .format("jdbc") \
#     .option("url", MARIADB_URL) \
#     .option("dbtable", "exchange_rates") \
#     .option("user", MARIADB_USER) \
#     .option("password", MARIADB_PASSWORD) \
#     .option("numPartitions", 4) \
#     .load()

#Cách 2: Đọc exchange_rates từ MariaDB->pandas->Spark
# Lấy thông tin kết nối từ biến môi trường
host = os.getenv("MARIADB_HOST", "mariadb")
port = int(os.getenv("MARIADB_PORT", "3306"))
user = os.getenv("MARIADB_USER")
password = os.getenv("MARIADB_PASSWORD")
database = "ads_schema"
if not all([user, password]):
    raise ValueError("Missing DB credentials in environment variables")
# Kết nối MariaDB và truy vấn dữ liệu bằng pymysql
connection = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    database=database
)
query = "SELECT date_id, currency, exchange_rate_usd FROM exchange_rates"
# Đọc vào pandas DataFrame
pandas_df = pd.read_sql(query, connection)
connection.close()
# Chuyển thành Spark DataFrame
exchange_rates_df = spark.createDataFrame(pandas_df)
# Hiển thị kết quả
exchange_rates_df.show()
exchange_rates_df.printSchema()

# Process data from MinIO
prefixes = ["facebook_data", "google_data"]
all_dfs = []
for prefix in prefixes:
    df = spark.read.option("basePath", f"s3a://{MINIO_BUCKET}/{prefix}") \
                   .option("mergeSchema", "true") \
                   .parquet(f"s3a://{MINIO_BUCKET}/{prefix}/year=2024/month=6/*/batch_*.parquet")
    
    df = df.withColumn("date_id", concat(
                      col("year").cast("string"),
                      lpad(col("month").cast("string"), 2, "0"),
                      lpad(col("day").cast("string"), 2, "0")
                  ))

    # Chọn và định nghĩa lại các cột mong muốn, loại bỏ year và month cũ
    df = df.select(
        col("currency").cast("string"),
        col("country").cast("string"),
        col("date_id").alias("day"),  # Giữ day mới đã gộp
        col("impressions").cast("int"),
        col("cost").cast("double")).drop("year", "month")

    print(f"Schema của {prefix}:")
    df.printSchema()
    print(f"Số bản ghi trong {prefix} trước khi lọc: {df.count()}")
    df.show(4)

    # đặt alias cho 2 DataFrame
    ads_df = df.alias("ads")               # dữ liệu quảng cáo đã lọc
    er_df  = exchange_rates_df.alias("er") # bảng tỷ giá

    # điều kiện join
    join_cond = (
        col("ads.day") == col("er.date_id")
    ) & (
        (col("ads.currency") == col("er.currency")) & (col("ads.currency") != "USD")
    )

    # thực hiện join
    df_joined = (
        ads_df.join(er_df, join_cond, "left")
            .withColumn("channel_id", lit(prefix.split("_")[0]))
            .withColumn("exchange_rate_usd",
                        when(col("ads.currency") == "USD", lit(1.0))
                        .otherwise(col("er.exchange_rate_usd")))
            .select(
                "channel_id",
                col("ads.country").alias("country"),
                col("ads.day").alias("date_id"),
                col("ads.currency").alias("currency"),
                "impressions",
                "cost",
                "exchange_rate_usd"
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
    aggregated_df.show(6)

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
    .select(
        col("channel_id"),
        col("date_id"),
        col("country"),
        col("currency"),
        col("cost_original"),
        col("impressions").cast("int"),
        col("cost_usd").cast("double"),
        col("cpi_usd").cast("double")
    )

# Ensure NOT NULL columns do not contain NULL values
# fact_ad_performance = fact_ad_performance.na.fill({
#     "channel_id": "unknown",
#     "date_id": "00000000",  # Giá trị mặc định cho date_id (ví dụ: "YYYYMMDD")
#     "country": "XX",        # Giá trị mặc định cho country (ví dụ: "XX" cho không xác định)
#     "currency": "USD",      # Giá trị mặc định cho currency
#     "cost_original": 0.0,   # Giá trị mặc định cho cost_original
#     "impressions": 0        # Giá trị mặc định cho impressions
# })

print("Dữ liệu chuẩn bị ghi vào fact_ad_performance:")
fact_ad_performance.show(7)

# Write to MariaDB
#.option("driver", "org.mariadb.jdbc.Driver") \
try:
    fact_ad_performance.write \
        .format("jdbc") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "fact_ad_performance") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
        .option("driver", "org.mariadb.jdbc.Driver") \
        .option("batchsize", "10000") \
        .option("quoteIdentifiers", "true") \
        .mode("append") \
        .save()
    print("Dữ liệu đã ghi vào bảng fact_ad_performance thành công.")
except Exception as e:
    print(f"Lỗi khi ghi vào MariaDB: {str(e)}")
    spark.stop()
    exit(1)

# Stop Spark session
spark.stop()