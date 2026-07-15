import functions_framework
import pandas as pd
import re
import os
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
PROJECT_ID = "o2-data-s-z"
DATASET_ID = "swiggy_daily_powerbi"
TABLE_NAME = "s_spend_metrics_daily"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# GCS Paths
LANDING_PREFIX = "swiggy_o2/swiggy_powerbi_data/swiggy_spend_metrics_overview/"
ARCHIVE_PREFIX = "swiggy_o2/swiggy_powerbi_data/archive/swiggy_spend_metrics_overview/"

# Filename pattern: spend_metrics_YYYYMMDD.csv
FILENAME_PATTERN = r"^spend_metrics_(\d{8})\.csv$"

HEADER_MAP = {
    "Restaurant Name": "res_name",
    "Restaurant ID": "res_id",
    "City": "city",
    "Area": "area",
    "Completed Orders": "completed_orders",
    "Discounted Orders": "discounted_orders",
    "% Order on Discount": "order_on_discount_pct",
    "Discounted Orders Revenue": "discount_orders_revenue",
    "Discount Spend": "discount_spend",
    "Average Order Value (Discounted Orders)": "discounted_aov",
    "New User Orders": "new_user_orders",
    "Returning User Orders": "returning_user_orders",
    "Dormant User Orders": "dormant_user_orders",
    "Ads Orders": "ads_orders",
    "Orders with Ads (%)": "orders_with_ads_pct",
    "Revenue from Ads Orders": "ads_orders_revenue",
    "Ads Spend": "ads_spend",
    "New User Orders via Ads": "new_user_ads_orders",
    "Ads Impressions": "ads_impressions",
    "Click Through Rate (CTR %)- Impressions to Menu Conversion ": "ctr"
}

# -------------------------------------------------------------------------
# PIPELINE EXECUTION
# -------------------------------------------------------------------------
@functions_framework.cloud_event
def run_pipeline(cloud_event):
    print("Pipeline Started")
    
    # 1. Validate Event
    data = cloud_event.data
    bucket_name = data.get("bucket")
    file_name = data.get("name")
    
    if not bucket_name or not file_name:
        raise RuntimeError("Invalid event payload: Missing bucket or file name.")
        
    # Ignore files outside landing folder and ignore archive folder
    if not file_name.startswith(LANDING_PREFIX) or "archive/" in file_name:
        print(f"Ignored file outside landing zone or in archive: {file_name}")
        return "Ignored"

    # 2. Validate Filename & Extract Date
    base_name = file_name.replace(LANDING_PREFIX, "")
    match = re.match(FILENAME_PATTERN, base_name)
    if not match:
        raise RuntimeError(f"Invalid filename format: {base_name}. Expected format: spend_metrics_YYYYMMDD.csv")
    
    date_str = match.group(1)
    try:
        file_date = datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError as e:
        raise RuntimeError(f"Invalid date in filename: {date_str}. Error: {e}")
        
    print(f"File Validated: {base_name}")

    # Initialize Clients
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # 3. Read File
    gcs_uri = f"gs://{bucket_name}/{file_name}"
    try:
        df = pd.read_csv(gcs_uri)
        print(f"Rows Read: {len(df)}")
    except Exception as e:
        raise RuntimeError(f"Failed to read CSV file {gcs_uri}: {e}")

    # 4. Validate Structure (Headers)
    file_headers = list(df.columns)
    missing_headers = [h for h in HEADER_MAP.keys() if h not in file_headers]
    if missing_headers:
        raise RuntimeError(f"Missing expected headers: {missing_headers}")
        
    if len(file_headers) != len(set(file_headers)):
        raise RuntimeError("Duplicate headers found in the file.")
        
    # Rename columns based on mapping
    df = df.rename(columns=HEADER_MAP)
    
    # Add new date column extracted from filename
    df["date"] = file_date
    print("Columns Validated")

    # 5. Transform (Datatypes)
    try:
        # STRING
        string_cols = ["res_name", "city", "area"]
        for col in string_cols:
            df[col] = df[col].astype(str).replace(["", "nan", "None"], pd.NA).astype("string")
            
        # INTEGER
        int_cols = [
            "res_id", "completed_orders", "discounted_orders", "discount_orders_revenue",
            "discount_spend", "discounted_aov", "new_user_orders", "returning_user_orders",
            "dormant_user_orders", "ads_orders", "ads_orders_revenue", "ads_spend",
            "new_user_ads_orders", "ads_impressions"
        ]
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            
        # FLOAT
        float_cols = ["order_on_discount_pct", "orders_with_ads_pct", "ctr"]
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
            
        # DATE
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        
        print("Datatypes Converted")
    except Exception as e:
        raise RuntimeError(f"Data transformation failed: {e}")

    # 6. Validate Output against Live BigQuery Schema
    try:
        table = bq_client.get_table(TABLE_ID)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch live BigQuery schema for {TABLE_ID}: {e}")
        
    bq_schema = {field.name: field.field_type for field in table.schema}
    
    # Check if our dataframe contains all required BQ columns
    missing_bq_cols = [col for col in bq_schema.keys() if col not in df.columns]
    if missing_bq_cols:
        raise RuntimeError(f"Schema mismatch. DataFrame is missing columns required by BigQuery: {missing_bq_cols}")
        
    # Reorder DataFrame exactly as BigQuery expects
    df = df[[field.name for field in table.schema]]

    # 7. Append BigQuery
    print("BigQuery Started")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    load_job = bq_client.load_table_from_dataframe(
        df, TABLE_ID, job_config=job_config
    )
    
    # 8. Verify Load
    load_job.result()
    if load_job.errors:
        raise RuntimeError(f"BigQuery load failed: {load_job.errors}")
    print("BigQuery Completed")

    # 9. Archive Process
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(file_name)
    archive_file_name = f"{ARCHIVE_PREFIX}{base_name}"
    
    # Copy to Archive
    archive_blob = bucket.copy_blob(source_blob, bucket, archive_file_name)
    
    # 10. Verify Archive
    if not archive_blob.exists():
        raise RuntimeError("Archive verification failed. Aborting deletion of landing file.")
    print("Archive Completed")

    # 11. Delete Landing File
    source_blob.delete()
    
    # 12. Verify Delete
    if source_blob.exists():
        raise RuntimeError("Landing file deletion verification failed.")
    print("Landing Deleted")
    
    print("Pipeline Completed")
    return "Success", 200
