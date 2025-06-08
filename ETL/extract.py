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
MINIO_DEST_BUCKET = "vdt2025-ads-data"
SOURCE_PREFIX_FB = "facebook_data"  
SOURCE_PREFIX_GOOGLE = "google_data"  

if not all([MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    raise ValueError("MinIO credentials (MINIO_ACCESS_KEY or MINIO_SECRET_KEY) not found in .env file")

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
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


# Load country code mapping from CSV in raw-data bucket
def load_country_mapping():
    try:
        # Assume the mapping file is named 'country_mapping.csv' in the raw-data bucket
        response = minio_client.get_object(MINIO_SOURCE_BUCKET, "geo_id.csv")
        mapping_df = pd.read_csv(BytesIO(response.read()))
        response.close()
        response.release_conn()
        
        # Create dictionary mapping numeric country_code to ISO alpha-2 Country
        return dict(zip(mapping_df["country_code"].astype(str), mapping_df["Country"]))
    except S3Error as e:
        print(f"Error loading country mapping: {e}")
        return {}
    except Exception as e:
        print(f"Error processing country mapping CSV: {e}")
        return {}
    
# Load country mapping
country_mapping = load_country_mapping()

def normalize_record(record, source_type):
    """Normalize fields from different data sources to a common schema."""
    normalized = {}
    if source_type == "facebook":
        normalized = {
            "currency": record.get("Currency", ""),
            "country": record.get("Country", ""),  # Already ISO alpha-2
            "day": record.get("Day", ""),
            "impressions": record.get("Impressions", 0),
            "cost": record.get("Amount spent", 0.0)
        }
    elif source_type == "google":
        country_code = str(record.get("country_code", ""))
        normalized = {
            "currency": record.get("Currency code", ""),
            "country": country_mapping.get(country_code, "Unknown"),  # Map numeric to ISO alpha-2
            "day": record.get("Day", ""),
            "impressions": record.get("Impr.", 0),
            "cost": record.get("Cost", 0.0)
        }
    return normalized

def process_json_to_parquet(source_bucket, dest_bucket, source_prefix, source_type, batch_size=10000):
    # Define schema for Parquet files
    schema = pa.schema([
        ("currency", pa.string()),
        ("country", pa.string()),
        ("day", pa.string()),
        ("impressions", pa.int64()),
        ("cost", pa.float64())
    ])

    # def write_batch_to_minio(batch, batch_num, file_date, prefix):
    #     df = pd.DataFrame(batch)
        
    #     # Check for required columns
    #     required_fields = ["currency", "country", "day", "impressions", "cost"]
    #     missing_columns = [col for col in required_fields if col not in df.columns]
    #     if missing_columns:
    #         print(f"Missing columns {missing_columns} in batch {batch_num}")
    #         return

    #     # Validate and clean 'day' column
    #     df["day"] = pd.to_datetime(df["day"], format="%Y%m%d", errors='coerce')
    #     invalid_day_rows = df["day"].isna()
    #     if invalid_day_rows.any():
    #         print(f"Batch {batch_num}: Dropped {invalid_day_rows.sum()} rows with invalid 'day' values")
    #         df = df[~invalid_day_rows]

    #     # Validate and clean 'cost' and 'impressions' columns
    #     df["cost"] = pd.to_numeric(df["cost"], errors='coerce')
    #     df["impressions"] = pd.to_numeric(df["impressions"], errors='coerce')
    #     invalid_rows = df["cost"].isna() | df["impressions"].isna()
    #     if invalid_rows.any():
    #         print(f"Batch {batch_num}: Dropped {invalid_rows.sum()} rows with invalid 'cost' or 'impressions' values")
    #         df = df[~invalid_rows]

    #     # Convert 'day' to datetime and extract year, month, day for partitioning
    #     df["day"] = pd.to_datetime(df["day"], format="%Y%m%d", errors='coerce')
    #     df["year"] = df["day"].dt.year
    #     df["month"] = df["day"].dt.month
    #     df["day"] = df["day"].dt.day
    #     df["day"] = df["day"].dt.strftime("%Y%m%d")

    #     # Convert data types
    #     df["cost"] = pd.to_numeric(df["cost"], errors='coerce').astype(float).round(6)
    #     df["impressions"] = pd.to_numeric(df["impressions"], errors='coerce').astype("int64")

    #     # Group by year, month, day for partitioning
    #     grouped = df.groupby(["year", "month", "day"])

    #     for (year, month, day), group_df in grouped:
    #         # Drop partitioning columns
    #         group_df = group_df.drop(columns=["year", "month", "day"])

    #         # Convert to PyArrow Table
    #         table = pa.Table.from_pandas(group_df, schema=schema, preserve_index=False)
            
    #         # Define partition path
    #         partition_path = f"{prefix}/year={year}/month={month}/day={day}/batch_{batch_num}.parquet"
            
    #         # Write Parquet to BytesIO
    #         buffer = BytesIO()
    #         pq.write_table(table, buffer)
    #         buffer.seek(0)

    #         # Upload to MinIO destination bucket
    #         try:
    #             minio_client.put_object(
    #                 dest_bucket,
    #                 partition_path,
    #                 buffer,
    #                 length=buffer.getbuffer().nbytes,
    #                 content_type="application/octet-stream"
    #             )
    #             print(f"Batch {batch_num}: Wrote {len(group_df)} records to {partition_path} in {dest_bucket}")
    #         except S3Error as e:
    #             print(f"Error uploading to MinIO: {e}")


    def write_batch_to_minio(batch, batch_num, file_date, prefix):
        df = pd.DataFrame(batch)
        
        # Kiểm tra các cột bắt buộc
        required_fields = ["currency", "country", "day", "impressions", "cost"]
        missing_columns = [col for col in required_fields if col not in df.columns]
        if missing_columns:
            print(f"Thiếu cột {missing_columns} trong batch {batch_num}")
            return

        # Xác thực và làm sạch cột 'day' (chuyển đổi sang datetime chỉ một lần)
        df["day"] = pd.to_datetime(df["day"], format="%Y%m%d", errors='coerce')
        invalid_day_rows = df["day"].isna()
        if invalid_day_rows.any():
            print(f"Batch {batch_num}: Loại bỏ {invalid_day_rows.sum()} dòng có giá trị 'day' không hợp lệ")
            df = df[~invalid_day_rows]

        # Xác thực và làm sạch cột 'cost' và 'impressions'
        df["cost"] = pd.to_numeric(df["cost"], errors='coerce')
        df["impressions"] = pd.to_numeric(df["impressions"], errors='coerce')
        invalid_rows = df["cost"].isna() | df["impressions"].isna()
        if invalid_rows.any():
            print(f"Batch {batch_num}: Loại bỏ {invalid_rows.sum()} dòng có giá trị 'cost' hoặc 'impressions' không hợp lệ")
            df = df[~invalid_rows]

        # Trích xuất year, month, day_of_month để phân vùng
        df["year"] = df["day"].dt.year
        df["month"] = df["day"].dt.month
        df["day_of_month"] = df["day"].dt.day  # Cột tạm để phân vùng

        # Định dạng lại cột 'day' về chuỗi cho lưu trữ
        df["day"] = df["day"].dt.strftime("%Y%m%d")

        # Chuyển đổi kiểu dữ liệu
        df["cost"] = pd.to_numeric(df["cost"], errors='coerce').astype(float).round(6)
        df["impressions"] = pd.to_numeric(df["impressions"], errors='coerce').astype("int64")

        # Nhóm theo year, month, day_of_month để phân vùng
        grouped = df.groupby(["year", "month", "day_of_month"])

        for (year, month, day), group_df in grouped:
            # Loại bỏ các cột phân vùng
            group_df = group_df.drop(columns=["year", "month", "day_of_month"])

            # Chuyển thành bảng PyArrow
            table = pa.Table.from_pandas(group_df, schema=schema, preserve_index=False)
            
            # Định nghĩa đường dẫn phân vùng
            partition_path = f"{prefix}/year={year}/month={month}/day={day}/batch_{batch_num}.parquet"
            
            # Ghi file Parquet vào BytesIO
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            # Tải lên MinIO
            try:
                minio_client.put_object(
                    dest_bucket,
                    partition_path,
                    buffer,
                    length=buffer.getbuffer().nbytes,
                    content_type="application/octet-stream"
                )
                print(f"Batch {batch_num}: Ghi {len(group_df)} bản ghi vào {partition_path} trong {dest_bucket}")
            except S3Error as e:
                print(f"Lỗi khi tải lên MinIO: {e}")

    # List JSON files in source bucket
    try:
        objects = minio_client.list_objects(source_bucket, prefix=source_prefix, recursive=True)
        for obj in objects:
            if obj.object_name.endswith(".txt"):
                try:
                    # Extract date from filename
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
                            normalized_record = normalize_record(record, source_type)
                            batch.append(normalized_record)
                            if len(batch) >= batch_size:
                                write_batch_to_minio(batch, count, file_date, source_prefix)
                                count += 1
                                batch = []
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON in {obj.object_name}: {e}")
                            continue
                    response.close()
                    response.release_conn()

                    # Process remaining records
                    if batch:
                        write_batch_to_minio(batch, count, file_date, source_prefix)

                    print(f"Finished processing {obj.object_name}")
                except Exception as e:
                    print(f"Error processing {obj.object_name}: {e}")
    except S3Error as e:
        print(f"Error listing objects in source bucket: {e}")

if __name__ == "__main__":
    # Process Facebook data
    process_json_to_parquet(MINIO_SOURCE_BUCKET, MINIO_DEST_BUCKET, SOURCE_PREFIX_FB, "facebook")
    print(f"Completed processing all Facebook JSON files from {MINIO_SOURCE_BUCKET}/{SOURCE_PREFIX_FB}/ and uploaded to {MINIO_DEST_BUCKET}/{SOURCE_PREFIX_FB}/")
    
    # Process Google data
    process_json_to_parquet(MINIO_SOURCE_BUCKET, MINIO_DEST_BUCKET, SOURCE_PREFIX_GOOGLE, "google")
    print(f"Completed processing all Google JSON files from {MINIO_SOURCE_BUCKET}/{SOURCE_PREFIX_GOOGLE}/ and uploaded to {MINIO_DEST_BUCKET}/{SOURCE_PREFIX_GOOGLE}/")
