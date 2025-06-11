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

geo_path = "s3a://vdt2025-ads-data/geo_id_2.csv"   
geo_df = (
    spark.read.option("header", "true")
               .option("inferSchema", "true")
               .csv(geo_path)
)

# Rename & chọn đúng cột cho dim_country
geo_df = (
    geo_df.select("country", "country_name")
          .withColumn("country", col("country").cast("string"))
          .withColumn("country_name", col("country_name").cast("string"))
)


print("Dữ liệu chuẩn bị ghi vào dim_country:")
geo_df.show(7)

# Write to MariaDB
try:
    geo_df.write \
        .format("jdbc") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "dim_country") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
        .option("quoteIdentifiers", "true") \
        .mode("append") \
        .save()

except Exception as e:
    print(f"Lỗi khi ghi vào MariaDB: {str(e)}")
    spark.stop()
    exit(1)

# Stop Spark session
spark.stop()