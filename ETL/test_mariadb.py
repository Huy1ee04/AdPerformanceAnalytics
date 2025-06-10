from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, hash, input_file_name, regexp_extract, sum as _sum, when
from pyspark.sql.types import *
from pyspark.sql.types import StringType, StructType, StructField
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

# # 1. Lấy danh sách cột trong bảng (ví dụ thủ công hoặc từ metadata)
# columns = ["col1", "col2", "exchange_rate_usd", "col4"]  # thay bằng tên cột thật

# # 2. Tạo schema với tất cả cột kiểu String
# schema = StructType([StructField(c, StringType(), True) for c in columns])

exchange_rates_df = spark.read \
    .format("jdbc") \
    .option("url", MARIADB_URL) \
    .option("dbtable", "exchange_rates") \
    .option("user", MARIADB_USER) \
    .option("password", MARIADB_PASSWORD) \
    .option("numPartitions", 4) \
    .load()
    # .option("customSchema", ",".join([f"{c} STRING" for c in columns])) \


print("Schema of exchange_rates_df:")
exchange_rates_df.printSchema()
print("Loaded exchange_rates from MariaDB:")
# exchange_rates_df = exchange_rates_df.filter(col("exchange_rate_usd") != "exchange_rate_usd")
exchange_rates_df.show(10)
print("Number of rows in exchange_rates_df:", exchange_rates_df.count())