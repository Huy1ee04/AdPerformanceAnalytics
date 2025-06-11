from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, hash, input_file_name, regexp_extract, sum as _sum, when
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

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Load Exchange Rates without JDBC Spark") \
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

# Đọc vào pandas DataFrame
pandas_df = pd.read_sql(query, connection)
connection.close()

# Chuyển thành Spark DataFrame
exchange_rates_df = spark.createDataFrame(pandas_df)

# Hiển thị kết quả
exchange_rates_df.show(truncate=False)
exchange_rates_df.printSchema()

spark.stop()





# MARIADB_URL = f"jdbc:mariadb://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DATABASE}"

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("Load Aggregated Ad Performance") \
#     .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
#     .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
#     .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.jars", "/opt/bitnami/spark/jars/mariadb-java-client-3.3.2.jar,/opt/bitnami/spark/jars/hadoop-aws-3.4.1.jar,/opt/bitnami/spark/jars/aws-java-sdk-1.12.785.jar") \
#     .config("spark.driver.memory", "4g") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.executor.cores", "2") \
#     .master("spark://spark-master:7077") \
#     .getOrCreate()

# # exchange_rates_schema = StructType([
# #     StructField("date_id", StringType(), True),
# #     StructField("currency", StringType(), True),
# #     StructField("exchange_rate_usd", FloatType(), True)
# # ])

# # Lấy danh sách JARs từ cấu hình
# jars = spark.sparkContext.getConf().get("spark.jars")
# print("JARs used in Spark:", jars)

# exchange_rates_df = spark.read \
#     .format("jdbc") \
#     .option("url", MARIADB_URL) \
#     .option("dbtable", "exchange_rates") \
#     .option("user", MARIADB_USER) \
#     .option("password", MARIADB_PASSWORD) \
#     .option("numPartitions", 4) \
#     .option("customSchema",
#             "date_id STRING, currency STRING, exchange_rate_usd STRING") \
#     .load()

# exchange_rates_df.show()
# exchange_rates_df.printSchema()
