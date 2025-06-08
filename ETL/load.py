from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, lit, when, hash
from pyspark.sql.types import LongType, StringType
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import re

# Load environment variables
load_dotenv()

# MinIO configuration (Dockerized)
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = "vdt2025-ads-data"

# MariaDB configuration (Dockerized)
MARIADB_URL = os.getenv("MARIADB_URL")
MARIADB_USER = os.getenv("MARIADB_USER")
MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD")

if not all([MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MARIADB_USER, MARIADB_PASSWORD]):
    raise ValueError("Missing required credentials in .env file!")

# Initialize Spark session with MinIO and MariaDB support
spark = SparkSession.builder \
    .appName("Load Ad Performance to MariaDB") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.mariadb.jdbc:mariadb-java-client:3.5.3") \
    .getOrCreate()

# Read start date from file
with open("start_date.txt", "r") as f:
    start_date_str = f.read().strip()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

# Current date (for filtering partitions)
current_date = datetime(2025, 6, 5, 15, 0)  # 03:00 PM +07 on 05/06/2025
latest_date = None

# List partitions for facebook_data and google_data
prefixes = ["facebook_data", "google_data"]
folders = set()

for prefix in prefixes:
    # Simulate listing partitions (adjust if using MinIO client)
    for y in range(start_date.year, current_date.year + 1):
        for m in range(1, 13):
            for d in range(1, 32):
                try:
                    folder_date = datetime(y, m, d)
                    if start_date <= folder_date <= current_date:
                        folder_path = f"{prefix}/year={y}/month={m}/day={d}"
                        folders.add((prefix, folder_path))
                        if latest_date is None or folder_date > latest_date:
                            latest_date = folder_date
                except ValueError:
                    continue

print(f"✅ Found {len(folders)} partitions to process.")

# Function to extract date from path for sorting
def extract_date_from_path(path):
    match = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", path)
    if match:
        y, m, d = map(int, match.groups())
        return datetime(y, m, d)
    return None

# Sort folders by date
folders_sorted = sorted(folders, key=lambda x: extract_date_from_path(x[1]))

# Process each partition
for prefix, folder in folders_sorted:
    full_path = f"s3a://{MINIO_BUCKET}/{folder}"
    print(f"🚀 Processing partition: {full_path}")

    # Read Parquet data
    df = spark.read.parquet(full_path)

    # Preprocess data
    df = df.withColumn("date", to_date(col("Day"), "yyyyMMdd")) \
           .withColumn("channel_name", lit(prefix.split("/")[0])) \
           .withColumnRenamed("Currency", "currency_name") \
           .withColumnRenamed("Country", "country_code") \
           .withColumnRenamed("Cost", "cost_original") \
           .withColumn("country_name", lit("Unknown"))  # Placeholder, update if mapping available

    # Create dim_date
    dim_date = df.select("date").distinct() \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("date_id", (col("year")*10000 + col("month")*100 + col("day")).cast(LongType()))

    # Create dim_country
    dim_country = df.select("country_code", "country_name").distinct() \
        .withColumn("country_id", hash("country_code"))

    # Create dim_currency
    dim_currency = df.select("currency_name").distinct() \
        .withColumn("currency_id", hash("currency_name"))

    # Create dim_channel
    dim_channel = df.select("channel_name").distinct() \
        .withColumn("channel_id", hash("channel_name"))

    # Create fact_ad_performance
    # Simulate exchange rate (for demo purposes, assume 1 USD = 1 USD, 1 THB = 0.03 USD)
    df = df.withColumn("exchange_rate", when(col("currency_name") == "USD", 1.0)
                                     .when(col("currency_name") == "THB", 0.03)
                                     .otherwise(1.0)) \
           .withColumn("cost_usd", col("cost_original") * col("exchange_rate")) \
           .withColumn("cpi_usd", when(col("impressions") > 0, col("cost_usd") * 1000 / col("impressions"))
                                  .otherwise(0.0))

    # Join with dimension tables to get IDs
    df_with_ids = df.join(dim_date, on="date", how="left") \
                    .join(dim_country, on=["country_code", "country_name"], how="left") \
                    .join(dim_currency, on="currency_name", how="left") \
                    .join(dim_channel, on="channel_name", how="left")

    fact_ad_performance = df_with_ids.select(
        "date_id",
        "country_id",
        "channel_id",
        "currency_id",
        col("currency_name").alias("currency_code"),
        "cost_original",
        "impressions",
        "exchange_rate",
        "cost_usd",
        "cpi_usd"
    ).withColumn("fact_id", hash("date_id", "country_id", "channel_id", "currency_id"))

    # Write to MariaDB
    dim_date.write \
        .format("jdbc") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "dim_date") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
        .mode("append") \
        .save()

    dim_country.write \
        .format("jdbc") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "dim_country") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
        .mode("append") \
        .save()

    dim_currency.write \
        .format("jdbc") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "dim_currency") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
        .mode("append") \
        .save()

    dim_channel.write \
        .format("jdbc") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "dim_channel") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
        .mode("append") \
        .save()

    fact_ad_performance.write \
        .format("jdbc") \
        .option("url", MARIADB_URL) \
        .option("dbtable", "fact_ad_performance") \
        .option("user", MARIADB_USER) \
        .option("password", MARIADB_PASSWORD) \
        .mode("append") \
        .save()

    print(f"✅ Successfully wrote data from {full_path} to MariaDB!")

# Update start_date.txt with the next day after the latest date
if latest_date:
    next_date = latest_date + timedelta(days=1)
    with open("start_date.txt", "w") as f:
        f.write(next_date.strftime("%Y-%m-%d"))
    print(f"✅ Updated start_date.txt with next date: {next_date.strftime('%Y-%m-%d')}")
else:
    print("⚠️ No suitable dates found to update start_date.txt.")

# Stop Spark session
spark.stop()