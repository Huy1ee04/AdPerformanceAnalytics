from pyspark.sql import SparkSession
from pyspark.sql.functions import col,length, lit, hash, input_file_name, regexp_extract, sum as _sum, when
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
MARIADB_URL = f"jdbc:mysql://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DATABASE}"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Aggregated Ad Performance") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/opt/bitnami/spark/jars/mysql-connector-j-9.3.0.jar,/opt/bitnami/spark/jars/hadoop-aws-3.4.1.jar,/opt/bitnami/spark/jars/aws-java-sdk-1.12.785.jar") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

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
pandas_df = pd.read_sql(query, connection)
exchange_rates_df = spark.createDataFrame(pandas_df)

query2 = "SELECT country_id,country, country_name FROM dim_country"
pandas_df2 = pd.read_sql(query2, connection)
country_df = spark.createDataFrame(pandas_df2)

query3 = "SELECT channel_id, channel FROM dim_channel"
pandas_df3 = pd.read_sql(query3, connection)
channel_df = spark.createDataFrame(pandas_df3)

connection.close()

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
        col("date_id"),
        col("impressions").cast("int"),
        col("cost").cast("double")).drop("year", "month")
    
    print(f"Schema của {prefix}:")
    df.printSchema()
    print(f"Số bản ghi trong {prefix} trước khi lọc: {df.count()}")
    df.show(4)

    # đặt alias cho 2 DataFrame
    ads_df = df.alias("ads")               # dữ liệu quảng cáo đã lọc
    er_df  = exchange_rates_df.alias("er") # bảng tỷ giá
    co_df = country_df.alias("co") # bảng quốc gia

    # thực hiện join
    df_joined = (
    ads_df.join(co_df, col("ads.country") == col("co.country"), "left")  # Join với bảng country trước
          .join(er_df, [col("ads.date_id") == col("er.date_id"), col("ads.currency") == col("er.currency")], "left")  # Join với bảng exchange_rates
          .withColumn("channel", lit(prefix.split("_")[0]))  # Thêm channel_id từ prefix
          .select(
              col("channel"),
              col("ads.date_id").alias("date_id"),
              col("co.country_id").alias("country_id"),  # Lấy country_id từ bảng dim_country
              col("ads.currency").alias("currency"), 
              col("er.exchange_rate_usd").alias("exchange_rate_usd"),  # Tỷ giá USD
              col("ads.cost").alias("cost_original"),  # Đặt biệt danh cho cost
              col("ads.impressions").alias("impressions")
          )
    )        
    df_joined.printSchema()
    df_joined.show(5)
    all_dfs.append(df_joined)

if not all_dfs:
    print("Không có dữ liệu nào từ các parquet files.")
    spark.stop()
    exit()

# Combine all DataFrames
combined_df = all_dfs[0].union(all_dfs[1]) if len(all_dfs) > 1 else all_dfs[0]

# Convert cost_original to cost_usd
combined_df = combined_df.alias("combined").join(
    channel_df.alias("ch"), 
    col("combined.channel") == col("ch.channel"), 
    "left"
) \
.withColumn("cost_usd", col("cost_original") * col("exchange_rate_usd"))
                 
# Create fact_ad_performance
fact_ad_performance = combined_df \
    .select(
        col("ch.channel_id"),
        col("date_id"),
        col("country_id"),
        col("currency"),
        col("exchange_rate_usd"),
        col("cost_original"),
        col("impressions").cast("int"),
        col("cost_usd").cast("double"),
    )

print("Dữ liệu chuẩn bị ghi vào fact_ad_performance:")
fact_ad_performance.show(10)


# Write to MariaDB
try:
    fact_ad_performance.write \
        .format("jdbc") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "fact_ad_performance") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
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