import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO

# Load environment variables
load_dotenv()

# MinIO configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SOURCE_BUCKET = os.getenv("MINIO_SOURCE_BUCKET")
MINIO_DEST_BUCKET = os.getenv("MINIO_DEST_BUCKET")
SOURCE_PREFIX_FB = "facebook_data/"
SOURCE_PREFIX_GOOGLE = "google_data/"

if not all([MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    raise ValueError("MinIO credentials (MINIO_ACCESS_KEY or MINIO_SECRET_KEY) not found in .env file")

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False       # default to HTTP
)

# Ensure buckets exist
try:
    if not minio_client.bucket_exists(MINIO_SOURCE_BUCKET):
        raise Exception(f"Source bucket {MINIO_SOURCE_BUCKET} does not exist")
    if not minio_client.bucket_exists(MINIO_DEST_BUCKET):
        minio_client.make_bucket(MINIO_DEST_BUCKET)
        print(f"Created destination bucket {MINIO_DEST_BUCKET}")
except S3Error as e:
    raise Exception(f"Error checking/creating bucket: {e}")

def process_json_to_parquet(source_bucket, dest_bucket, source_prefix, batch_size=10000):
    # Define schema for Parquet files
    schema = pa.schema([
        ("Currency", pa.string()),
        ("Country", pa.string()),
        ("Day", pa.string()),
        ("Impressions", pa.int64()),
        ("Cost", pa.float64())
    ])

    def write_batch_to_minio(batch, batch_num, file_date):
        df = pd.DataFrame(batch)
        
        # Kiểm tra sự tồn tại của các cột bắt buộc
        missing_columns = [col for col in required_fields if col not in df.columns]
        if missing_columns:
            return

        # Kiểm tra và xóa các hàng có trường Day không hợp lệ
        df["Day"] = pd.to_datetime(df["Day"], format="%Y%m%d", errors='coerce')
        invalid_day_rows = df["Day"].isna()
        if invalid_day_rows.any():
            df = df[~invalid_day_rows]

        # Kiểm tra và xóa các hàng có trường Cost hoặc Impressions không hợp lệ
        df["Cost"] = pd.to_numeric(df["Cost"], errors='coerce')
        df["Impressions"] = pd.to_numeric(df["Impressions"], errors='coerce')
        invalid_cost_rows = df["Cost"].isna()
        invalid_impressions_rows = df["Impressions"].isna()
        invalid_rows = invalid_cost_rows | invalid_impressions_rows
        if invalid_rows.any():

            df = df[~invalid_rows]
        # Convert 'Day' to datetime and extract year, month, day
        df["Day"] = pd.to_datetime(df["Day"], format="%Y%m%d", errors='coerce')
        df["year"] = df["Day"].dt.year
        df["month"] = df["Day"].dt.month
        df["day"] = df["Day"].dt.day
        df["Day"] = df["Day"].dt.strftime("%Y%m%d")  # Convert back to string for storage

        # Convert data types
        df["Cost"] = pd.to_numeric(df["Cost"], errors='coerce').astype(float).round(6)
        df["Impressions"] = pd.to_numeric(df["Impressions"], errors='coerce').astype("int64")

        # Group by year, month, day for partitioning
        grouped = df.groupby(["year", "month", "day"])

        for (year, month, day), group_df in grouped:
            # Drop partitioning columns
            group_df = group_df.drop(columns=["year", "month", "day"])

            # Convert to PyArrow Table
            table = pa.Table.from_pandas(group_df, schema=schema, preserve_index=False)
            
            # Define partition path
            partition_path = f"facebook_data/year={year}/month={month}/day={day}/batch_{batch_num}.parquet"
            
            # Write Parquet to BytesIO
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            # Upload to MinIO destination bucket
            try:
                minio_client.put_object(
                    dest_bucket,
                    partition_path,
                    buffer,
                    length=buffer.getbuffer().nbytes,
                    content_type="application/octet-stream"
                )
                print(f"Batch {batch_num}: Wrote {len(group_df)} records to {partition_path} in {dest_bucket}")
            except S3Error as e:
                print(f"Error uploading to MinIO: {e}")

    # List JSON files in source bucket
    try:
        objects = minio_client.list_objects(source_bucket, prefix=source_prefix, recursive=True)
        for obj in objects:
            if obj.object_name.endswith(".txt"):  
                try:
                    # Extract date from filename (e.g., raw_data_fb_fb_YYYYMMDD.txt)
                    file_date_str = obj.object_name.split("_")[-1].replace(".txt", "")
                    file_date = datetime.strptime(file_date_str, "%Y%m%d")

                    # Read JSON file from source bucket
                    response = minio_client.get_object(source_bucket, obj.object_name)
                    batch = []
                    count = 0

                    # Process JSON lines
                    for line in response.read().decode('utf-8').splitlines():
                        try:
                            record = json.loads(line.strip())
                            batch.append(record)
                            if len(batch) >= batch_size:
                                write_batch_to_minio(batch, count, file_date)
                                count += 1
                                batch = []
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON in {obj.object_name}: {e}")
                            continue
                    response.close()
                    response.release_conn()

                    # Process remaining records
                    if batch:
                        write_batch_to_minio(batch, count, file_date)

                    print(f"Finished processing {obj.object_name}")
                except Exception as e:
                    print(f"Error processing {obj.object_name}: {e}")
    except S3Error as e:
        print(f"Error listing objects in source bucket: {e}")

if __name__ == "__main__":
    # Process Facebook data
    process_json_to_parquet(MINIO_SOURCE_BUCKET, MINIO_DEST_BUCKET, SOURCE_PREFIX_FB)
    print(f"Completed processing all Facebook JSON files from {MINIO_SOURCE_BUCKET}/{SOURCE_PREFIX_FB} and uploaded to {MINIO_DEST_BUCKET}/facebook_data!")
    
    # Process Google data
    process_json_to_parquet(MINIO_SOURCE_BUCKET, MINIO_DEST_BUCKET, SOURCE_PREFIX_GOOGLE)
    print(f"Completed processing all Google JSON files from {MINIO_SOURCE_BUCKET}/{SOURCE_PREFIX_GOOGLE} and uploaded to {MINIO_DEST_BUCKET}/google_data!")