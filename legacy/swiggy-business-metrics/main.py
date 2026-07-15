import io
import re
import os
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import functions_framework

# -------------------------------------------------------------------------
# CONSTANTS & CONFIGURATION (Standard Infrastructure)
# -------------------------------------------------------------------------
PROJECT_ID = "o2-data-s-z"
EXPECTED_BUCKET = "o2-data-raw-ingestion"
LANDING_FOLDER = "swiggy_o2/swiggy_powerbi_data/swiggy_business_metrics/"
ARCHIVE_FOLDER = "swiggy_o2/swiggy_powerbi_data/swiggy_business_metrics/archive/"
TABLE_ID = "o2-data-s-z.swiggy_daily_powerbi.s_bizmetrics_daily"

BQ_SCHEMA = [
    ("res_name", "STRING"),
    ("res_id", "INTEGER"),
    ("city", "STRING"),
    ("area", "STRING"),
    ("gmv", "INTEGER"),
    ("discount_spend", "INTEGER"),
    ("total_orders", "INTEGER"),
    ("delivered_orders", "INTEGER"),
    ("rejected_orders", "INTEGER"),
    ("aov", "INTEGER"),
    ("revenue_per_day_per_res", "INTEGER"),
    ("avg_item_price", "INTEGER"),
    ("date", "DATE")
]

HEADER_MAP = {
    "Restaurant Name": "res_name",
    "Restaurant ID": "res_id",
    "City": "city",
    "Area": "area",
    "Revenue": "gmv",
    "Discount Spends": "discount_spend",
    "Total Orders": "total_orders",
    "Delivered Orders": "delivered_orders",
    "Rejected Orders": "rejected_orders",
    "Average Order Value": "aov",
    "Revenue per Day per Transacting Restaurant": "revenue_per_day_per_res",
    "Average Item Price": "avg_item_price"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def clean_currency_and_k(val):
    if pd.isna(val):
        return pd.NA
    val_str = str(val).strip().replace("₹", "").strip()
    if "K" in val_str or "k" in val_str:
        val_str = val_str.replace("K", "").replace("k", "").strip()
        try:
            return int(float(val_str) * 1000)
        except ValueError:
            return pd.NA
    try:
        return int(float(val_str))
    except ValueError:
        return pd.NA

@functions_framework.cloud_event
def run_pipeline(cloud_event):
    logging.info("Pipeline Event Received")
    
    # -------------------------------------------------------------------------
    # 1. VALIDATE EVENT
    # -------------------------------------------------------------------------
    data = cloud_event.data
    bucket_name = data.get("bucket")
    file_name = data.get("name")
    
    if not bucket_name or not file_name:
        logging.error("Invalid Event: Missing bucket or file name.")
        return
        
    if bucket_name != EXPECTED_BUCKET:
        logging.info(f"Ignored: Event from bucket '{bucket_name}' matches no rule.")
        return

    # -------------------------------------------------------------------------
    # 2. VALIDATE FILENAME & DUPLICATES
    # -------------------------------------------------------------------------
    if "archive/" in file_name or file_name.endswith("/"):
        logging.info(f"Ignored Path: {file_name}")
        return
        
    if not file_name.startswith(LANDING_FOLDER):
        logging.info(f"Ignored Path (Outside Landing): {file_name}")
        return

    if not file_name.lower().endswith(".csv"):
        logging.error(f"Fail immediately: Unsupported file extension in {file_name}")
        return

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    
    # -- DUPLICATION CHECK --
    base_filename = os.path.basename(file_name)
    archive_path = f"{ARCHIVE_FOLDER}{base_filename}"
    archive_blob = bucket.blob(archive_path)
    
    if archive_blob.exists():
        logging.warning(f"Duplication Prevented: '{base_filename}' already exists in archive. Skipping execution.")
        return

    # Match yyyymmdd pattern
    date_match = re.search(r"swiggy_bizmetrics_(\d{8})", file_name)
    if not date_match:
        logging.error(f"Fail immediately: Filename pattern mismatch on '{file_name}'")
        return
        
    date_str = date_match.group(1)
    try:
        extracted_date = pd.to_datetime(date_str, format="%Y%m%d").date()
    except Exception as e:
        logging.error(f"Fail immediately: Could not parse date block: {e}")
        return

    # -------------------------------------------------------------------------
    # 3. SAFE READ FILE
    # -------------------------------------------------------------------------
    blob = bucket.blob(file_name)
    
    if not blob.exists():
        logging.info(f"Ignored: File {file_name} does not exist. Likely already deleted.")
        return

    content = blob.download_as_bytes()
    
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8")
        logging.info(f"Worksheet Loaded: {len(df)} rows")
    except Exception as e:
        logging.error(f"CSV Parsing Error: {e}")
        return

    # -------------------------------------------------------------------------
    # 4. TRANSFORM & VALIDATE
    # -------------------------------------------------------------------------
    incoming_headers = list(df.columns)
    expected_incoming = list(HEADER_MAP.keys())
    
    if any(h not in incoming_headers for h in expected_incoming):
        logging.error("Structure mismatch: Missing headers.")
        return
        
    df = df.rename(columns=HEADER_MAP)
    
    df["res_name"] = df["res_name"].replace("", pd.NA).astype("string")
    df["res_id"] = pd.to_numeric(df["res_id"], errors="coerce").astype("Int64")
    df["city"] = df["city"].replace("", pd.NA).astype("string")
    df["area"] = df["area"].replace("", pd.NA).astype("string")
    
    df["gmv"] = df["gmv"].apply(clean_currency_and_k).astype("Int64")
    df["discount_spend"] = df["discount_spend"].apply(clean_currency_and_k).astype("Int64")
    df["aov"] = df["aov"].apply(clean_currency_and_k).astype("Int64")
    df["revenue_per_day_per_res"] = df["revenue_per_day_per_res"].apply(clean_currency_and_k).astype("Int64")
    df["avg_item_price"] = df["avg_item_price"].apply(clean_currency_and_k).astype("Int64")
    
    df["total_orders"] = pd.to_numeric(df["total_orders"], errors="coerce").astype("Int64")
    df["delivered_orders"] = pd.to_numeric(df["delivered_orders"], errors="coerce").astype("Int64")
    df["rejected_orders"] = pd.to_numeric(df["rejected_orders"], errors="coerce").astype("Int64")
    
    df["date"] = extracted_date
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # -------------------------------------------------------------------------
    # 5. APPEND BIGQUERY
    # -------------------------------------------------------------------------
    bq_client = bigquery.Client(project=PROJECT_ID)
    try:
        table = bq_client.get_table(TABLE_ID)
    except Exception as e:
        logging.error(f"BigQuery Table Unavailable: {e}")
        return
            
    df = df[[name for name, _ in BQ_SCHEMA]]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[bigquery.SchemaField(name, dtype) for name, dtype in BQ_SCHEMA]
    )
    
    try:
        job = bq_client.load_table_from_dataframe(df, table, job_config=job_config)
        job.result()
        if job.errors:
            logging.error(f"BigQuery Load Errors: {job.errors}")
            return
    except Exception as e:
        logging.error(f"Database Exception: {e}")
        return
        
    logging.info("BigQuery Data Insertion Complete")

    # -------------------------------------------------------------------------
    # 6. ARCHIVE & DELETE
    # -------------------------------------------------------------------------
    try:
        bucket.copy_blob(blob, bucket, archive_path)
        if not bucket.blob(archive_path).exists():
            raise RuntimeError("Archive copy failed.")
        logging.info("Archive Completed")
        
        blob.delete()
        logging.info("Landing Deleted. Pipeline Finished Successfully.")
    except Exception as e:
        logging.error(f"Lifecycle Failure: {e}")
