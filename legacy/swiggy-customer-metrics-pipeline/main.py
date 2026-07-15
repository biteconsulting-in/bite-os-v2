import os
import re
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
import functions_framework

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "o2-data-s-z"
DATASET_ID = "swiggy_daily_powerbi"
TABLE_NAME = "s_customer_metrics_daily"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
BUCKET_NAME = "o2-data-raw-ingestion"
LANDING_FOLDER = "swiggy_o2/swiggy_powerbi_data/swiggy_customer_metrics"
ARCHIVE_FOLDER = "swiggy_o2/swiggy_powerbi_data/swiggy_customer_metrics/archive"

HEADER_MAP = {
    "Restaurant Name": "res_name",
    "Restaurant ID": "res_id",
    "City": "city",
    "Area": "area",
    "Impressions": "impressions",
    "Menu Sessions": "menu_sessions",
    "Cart Sessions": "cart_sessions",
    "Order Sessions": "order_sessions",
    "Impression to Menu Conversion": "i2m",
    "Menu to Cart Conversion": "m2c",
    "Cart to Order Conversion": "c2o",
    "Menu to Order Conversion": "m2o",
    "New User Orders": "new_user_orders",
    "Repeat User Orders": "repeat_user_orders",
    "New User Orders (%)": "new_user_orders_pct",
    "Repeat User Orders (%)": "repeat_user_orders_pct",
    "User Ratings": "user_ratings",
    "Rated Orders": "rated_orders",
    "% Rated Orders": "rated_orders_pct",
    "Poor Rated Orders": "poor_rated_orders",
    "% Poor Rated Orders": "poor_rated_orders_pct",
    "Items without Image (%)": "items_no_image",
    "Items without Description (%)": "items_no_desc"
}

BQ_SCHEMA = [
    ("res_name", "STRING"),
    ("res_id", "INTEGER"),
    ("city", "STRING"),
    ("area", "STRING"),
    ("impressions", "INTEGER"),
    ("menu_sessions", "INTEGER"),
    ("cart_sessions", "INTEGER"),
    ("order_sessions", "INTEGER"),
    ("i2m", "FLOAT"),
    ("m2c", "FLOAT"),
    ("c2o", "FLOAT"),
    ("m2o", "FLOAT"),
    ("new_user_orders", "INTEGER"),
    ("repeat_user_orders", "INTEGER"),
    ("new_user_orders_pct", "FLOAT"),
    ("repeat_user_orders_pct", "FLOAT"),
    ("user_ratings", "FLOAT"),
    ("rated_orders", "INTEGER"),
    ("rated_orders_pct", "FLOAT"),
    ("poor_rated_orders", "INTEGER"),
    ("poor_rated_orders_pct", "FLOAT"),
    ("items_no_image", "FLOAT"),
    ("items_no_desc", "FLOAT"),
    ("date", "DATE")
]

def clean_percentage_value(val):
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str.endswith("%"):
        val_str = val_str.rstrip("%")
    try:
        return float(val_str) / 100.0
    except Exception:
        return None

@functions_framework.cloud_event
def run_pipeline(cloud_event):
    logger.info("Pipeline Started")
    
    event_data = cloud_event.data
    bucket_incoming = event_data.get("bucket")
    file_path = event_data.get("name")
    
    if bucket_incoming != BUCKET_NAME:
        logger.info(f"Ignored trigger from irrelevant bucket: {bucket_incoming}")
        return "Ignored Bucket", 200
        
    if "archive/" in file_path:
        logger.info("Archive trigger ignored to prevent recursive loops.")
        return "Ignored Archive", 200
        
    if not file_path.startswith(LANDING_FOLDER):
        logger.info(f"Ignored file outside target landing directory: {file_path}")
        return "Ignored Path", 200

    filename = os.path.basename(file_path)
    match = re.match(r"^customer_metrics_(\d{8})\.csv$", filename)
    if not match:
        logger.error(f"Invalid filename pattern: {filename}")
        raise ValueError(f"Invalid filename pattern: {filename}")
        
    date_str = match.group(1)
    try:
        extracted_date = datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError as e:
        logger.error(f"Invalid date value in filename: {date_str}")
        raise ValueError(f"Invalid date format in filename: {date_str}") from e

    logger.info("File Validated")

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_path)
    
    try:
        content = blob.download_as_bytes()
        df = pd.read_csv(pd.io.common.BytesIO(content))
        logger.info("Worksheet Loaded")
        logger.info(f"Rows Read: {len(df)}")
    except Exception as e:
        logger.error(f"GCS Object retrieval failed: {e}")
        raise RuntimeError(f"Read file failure: {e}")

    incoming_headers = list(df.columns)
    expected_headers = list(HEADER_MAP.keys())
    
    if len(incoming_headers) != len(set(incoming_headers)):
        logger.error("Duplicate headers found in source file.")
        raise ValueError("Duplicate headers found in source file.")
        
    missing = [h for h in expected_headers if h not in incoming_headers]
    if missing:
        logger.error(f"Missing mandatory headers: {missing}")
        raise ValueError(f"Missing headers: {missing}")
        
    unknown = [h for h in incoming_headers if h not in expected_headers]
    if unknown:
        logger.error(f"Unknown headers detected: {unknown}")
        raise ValueError(f"Unknown headers: {unknown}")
        
    logger.info("Columns Validated")

    transformed_payload = {}
    
    transformed_payload["res_name"] = df["Restaurant Name"].replace("", pd.NA).astype("string")
    transformed_payload["res_id"] = pd.to_numeric(df["Restaurant ID"], errors="coerce").astype("Int64")
    transformed_payload["city"] = df["City"].replace("", pd.NA).astype("string")
    transformed_payload["area"] = df["Area"].replace("", pd.NA).astype("string")
    
    transformed_payload["impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").astype("Int64")
    transformed_payload["menu_sessions"] = pd.to_numeric(df["Menu Sessions"], errors="coerce").astype("Int64")
    transformed_payload["cart_sessions"] = pd.to_numeric(df["Cart Sessions"], errors="coerce").astype("Int64")
    transformed_payload["order_sessions"] = pd.to_numeric(df["Order Sessions"], errors="coerce").astype("Int64")
    
    # Process float metrics properly aligned with standard rule
    transformed_payload["i2m"] = pd.to_numeric(df["Impression to Menu Conversion"].apply(clean_percentage_value), errors="coerce")
    transformed_payload["m2c"] = pd.to_numeric(df["Menu to Cart Conversion"].apply(clean_percentage_value), errors="coerce")
    transformed_payload["c2o"] = pd.to_numeric(df["Cart to Order Conversion"].apply(clean_percentage_value), errors="coerce")
    transformed_payload["m2o"] = pd.to_numeric(df["Menu to Order Conversion"].apply(clean_percentage_value), errors="coerce")
    
    transformed_payload["new_user_orders"] = pd.to_numeric(df["New User Orders"], errors="coerce").astype("Int64")
    transformed_payload["repeat_user_orders"] = pd.to_numeric(df["Repeat User Orders"], errors="coerce").astype("Int64")
    transformed_payload["new_user_orders_pct"] = pd.to_numeric(df["New User Orders (%)"].apply(clean_percentage_value), errors="coerce")
    transformed_payload["repeat_user_orders_pct"] = pd.to_numeric(df["Repeat User Orders (%)"].apply(clean_percentage_value), errors="coerce")
    
    transformed_payload["user_ratings"] = pd.to_numeric(df["User Ratings"], errors="coerce")
    transformed_payload["rated_orders"] = pd.to_numeric(df["Rated Orders"], errors="coerce").astype("Int64")
    transformed_payload["rated_orders_pct"] = pd.to_numeric(df["% Rated Orders"].apply(clean_percentage_value), errors="coerce")
    transformed_payload["poor_rated_orders"] = pd.to_numeric(df["Poor Rated Orders"], errors="coerce").astype("Int64")
    transformed_payload["poor_rated_orders_pct"] = pd.to_numeric(df["% Poor Rated Orders"].apply(clean_percentage_value), errors="coerce")
    transformed_payload["items_no_image"] = pd.to_numeric(df["Items without Image (%)"].apply(clean_percentage_value), errors="coerce")
    transformed_payload["items_no_desc"] = pd.to_numeric(df["Items without Description (%)"].apply(clean_percentage_value), errors="coerce")
    
    transformed_payload["date"] = pd.Series([extracted_date] * len(df))
    
    transformed_df = pd.DataFrame(transformed_payload)
    logger.info("Datatypes Converted")

    bq_client = bigquery.Client(project=PROJECT_ID)
    try:
        table = bq_client.get_table(TABLE_ID)
        live_schema_map = {field.name: field.field_type for field in table.schema}
    except Exception as e:
        logger.error(f"Live BigQuery schema validation failed: {e}")
        raise RuntimeError(f"Schema validation failure: {e}")
        
    for schema_col, _ in BQ_SCHEMA:
        if schema_col not in live_schema_map:
            logger.error(f"Live target table schema mismatch. Missing column: {schema_col}")
            raise RuntimeError(f"Schema mismatch: missing {schema_col}")

    column_ordering = [name for name, _ in BQ_SCHEMA]
    transformed_df = transformed_df[[col for col in column_ordering if col in transformed_df.columns]]
    transformed_df["date"] = pd.to_datetime(transformed_df["date"]).dt.date

    logger.info("BigQuery Started")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    try:
        job = bq_client.load_table_from_dataframe(transformed_df, table, job_config=job_config)
        job.result()
        
        if job.errors:
            logger.error(f"BigQuery execution returned write errors: {job.errors}")
            raise RuntimeError(job.errors)
        logger.info("BigQuery Completed")
    except Exception as e:
        logger.error(f"BigQuery load operation critically failed: {e}")
        raise RuntimeError(f"BigQuery load failure: {e}")

    archive_path = f"{ARCHIVE_FOLDER}/{filename}"
    try:
        bucket.copy_blob(blob, bucket, archive_path)
        archive_verification_blob = bucket.blob(archive_path)
        if not archive_verification_blob.exists():
            raise RuntimeError("Verification failed: Target file missing from archive folder.")
        logger.info("Archive Completed")
    except Exception as e:
        logger.error(f"File archive phase failed: {e}")
        raise RuntimeError(f"Archive phase failure: {e}")

    try:
        blob.delete()
        fresh_blob_check = bucket.get_blob(file_path)
        if fresh_blob_check is not None:
            raise RuntimeError("Verification failed: Landing file still exists in processing directory.")
        logger.info("Landing Deleted")
    except Exception as e:
        logger.error(f"Landing cleanup phase failed: {e}")
        raise RuntimeError(f"Delete phase failure: {e}")

    logger.info("Pipeline Completed")
    return "Success", 200
