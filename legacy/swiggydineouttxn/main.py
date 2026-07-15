import os
import io
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import functions_framework

# Production Target Infrastructure Configurations
PROJECT_ID = "o2-data-s-z"
TABLE_ID = "o2-data-s-z.swiggy_dineout.dineout_transaction_raw"
LANDING_FOLDER = "swiggy_dineout/transaction_history/"
ARCHIVE_FOLDER = "swiggy_dineout/txn_history/archive/"

# Strict BigQuery Contract Schema Definition
BQ_SCHEMA = [
    ("restaurant_id", "INTEGER"),
    ("restaurant_name", "STRING"),
    ("city", "STRING"),
    ("location", "STRING"),
    ("transaction_id", "INTEGER"),
    ("transaction_date", "TIMESTAMP"),
    ("utr_id", "STRING"),
    ("utr_date", "TIMESTAMP"),
    ("bill_amount", "INTEGER"),
    ("base_discount_amount", "FLOAT"),
    ("coupon_discount_amount", "FLOAT"),
    ("dinecash_discount", "FLOAT"),
    ("net_amount", "FLOAT"),
    ("commission", "FLOAT"),
    ("gst_g", "FLOAT"),
    ("tip_amount", "INTEGER"),
    ("amount_receivable", "FLOAT"),
    ("transaction_status", "STRING"),
    ("refund_status", "STRING"),
    ("order_type", "STRING"),
    ("offer_claimed_title", "STRING")
]

# Explicit Mapping Configuration
HEADER_MAP = {
    "Restaurant Id": "restaurant_id",
    "Restaurant Name": "restaurant_name",
    "City": "city",
    "Location": "location",
    "Transaction ID": "transaction_id",
    "Transaction Date": "transaction_date",
    "UTR ID": "utr_id",
    "UTR Date": "utr_date",
    "Bill Amount (A)": "bill_amount",
    "Base Discount Amount (B)": "base_discount_amount",
    "Coupon Discount Amount (C)": "coupon_discount_amount",
    "DineCash discount (D)": "dinecash_discount",
    "Net Amount (E = A-B-C-D)": "net_amount",
    "Commission (F)": "commission",
    "GST (G)": "gst_g",
    "Tip Amount (H)": "tip_amount",
    "Amount Receivable (E-F-G+H)": "amount_receivable",
    "Transaction Status": "transaction_status",
    "Refund Status": "refund_status",
    "Order Type": "order_type",
    "Offer Claimed Title": "offer_claimed_title"
}

@functions_framework.cloud_event
def run_pipeline(cloud_event):
    print("Pipeline Started")
    
    # Validate Event Structure
    event_data = cloud_event.data
    bucket_name = event_data.get("bucket")
    file_name = event_data.get("name")
    
    if not bucket_name or not file_name:
        raise RuntimeError("Invalid Cloud Event: Missing bucket or file metadata.")
        
    # Ignore archive/ path changes and files outside target landing zone
    if "archive/" in file_name or not file_name.startswith(LANDING_FOLDER):
        print(f"File skipped: {file_name} falls outside target operational folder prefix.")
        return "Skipped"
        
    print("File Validated")
    
    # Validate Filename Structural Pattern
    filename_only = os.path.basename(file_name)
    if not filename_only.startswith("Swiggy_DineoutBillPayment_Report_") or not filename_only.endswith(".csv"):
        raise RuntimeError(f"Invalid Filename Pattern rejected: {filename_only}")
        
    # Read File from GCS Landing Path
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    raw_content = blob.download_as_bytes()
    
    df = pd.read_csv(io.BytesIO(raw_content))
    print("Worksheet Loaded")
    
    rows_count = len(df)
    print(f"Rows Read: {rows_count}")
    
    # Validate Structure and Strip Column A
    csv_headers = list(df.columns)
    if not csv_headers:
        raise RuntimeError("Structural Failure: Input file contains no header elements.")
        
    # Hardcoded complete removal of Column A (index/serial layout column)
    col_a_identifier = csv_headers[0]
    df = df.drop(columns=[col_a_identifier])
    
    # Review remaining operational columns
    remaining_headers = list(df.columns)
    
    for mandatory_key in HEADER_MAP.keys():
        if mandatory_key not in remaining_headers:
            raise RuntimeError(f"Structural Failure: Missing critical required layout column '{mandatory_key}'")
            
    for actual_col in remaining_headers:
        if actual_col not in HEADER_MAP:
            raise RuntimeError(f"Structural Failure: Unknown layout column detected: '{actual_col}'")
            
    if len(remaining_headers) != len(set(remaining_headers)):
        raise RuntimeError("Structural Failure: Duplicate column fields detected inside file schema layout.")
        
    # Direct Contract Verification against Live BigQuery Schema
    bq_client = bigquery.Client(project=PROJECT_ID)
    try:
        live_table = bq_client.get_table(TABLE_ID)
    except Exception as exc:
        raise RuntimeError(f"Live Schema Verification Failed: Could not fetch table metadata. Error: {str(exc)}")
        
    live_schema_fields = {field.name: field.field_type for field in live_table.schema}
    for field_name, expected_type in BQ_SCHEMA:
        if field_name not in live_schema_fields:
            raise RuntimeError(f"Schema Contract Mismatch: Column '{field_name}' not found in target BigQuery table.")
            
    print("Columns Validated")
    
    # Transform: Remap Header Names
    df = df.rename(columns=HEADER_MAP)
    
    # Transform: Enforce Explicit Datatype Coercions
    for field_name, expected_type in BQ_SCHEMA:
        if expected_type == "INTEGER":
            df[field_name] = pd.to_numeric(df[field_name], errors="coerce").astype("Int64")
        elif expected_type == "FLOAT":
            df[field_name] = pd.to_numeric(df[field_name], errors="coerce")
        elif expected_type == "STRING":
            df[field_name] = df[field_name].replace("", pd.NA).astype("string")
        elif expected_type == "TIMESTAMP":
            df[field_name] = pd.to_datetime(df[field_name], errors="coerce")
            if df[field_name].dt.tz is None:
                df[field_name] = df[field_name].dt.tz_localize("UTC")
            else:
                df[field_name] = df[field_name].dt.tz_convert("UTC")
        elif expected_type == "DATE":
            df[field_name] = pd.to_datetime(df[field_name], errors="coerce").dt.date
            
    print("Datatypes Converted")
    
    # Enforce Explicit Column Ordering Match
    df = df[[column_name for column_name, _ in BQ_SCHEMA]]
    
    # Load to BigQuery in Append Mode Only
    print("BigQuery Started")
    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[bigquery.SchemaField(name, dtype) for name, dtype in BQ_SCHEMA]
    )
    
    load_job = bq_client.load_table_from_dataframe(df, TABLE_ID, job_config=load_config)
    load_job.result()
    
    if load_job.errors:
        raise RuntimeError(f"BigQuery Execution Failure: Append operation failed. Errors: {load_job.errors}")
        
    print("BigQuery Completed")
    
    # Execute File Archival Flow
    archive_path = f"{ARCHIVE_FOLDER}{filename_only}"
    bucket.copy_blob(blob, bucket, archive_path)
    
    # Verify Archival Status
    archive_blob = bucket.blob(archive_path)
    if not archive_blob.exists():
        raise RuntimeError(f"Archival Verification Failure: Target artifact not found at path: {archive_path}")
        
    print("Archive Completed")
    
    # Clean Up Ingestion Landing Zone
    blob.delete()
    
    # Verify Cleanup Status
    if blob.exists():
        raise RuntimeError(f"Cleanup Verification Failure: Landing artifact still present at path: {file_name}")
        
    print("Landing Deleted")
    print("Pipeline Completed")
    return "Pipeline Executed Successfully", 200
