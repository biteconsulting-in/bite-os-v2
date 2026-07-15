import io
import os
import pandas as pd
import functions_framework
from google.cloud import storage, bigquery

# Standard Infrastructure Configuration
PROJECT_ID = "o2-data-s-z"
REGION = "asia-south1"
BUCKET_NAME = "o2-data-raw-ingestion"
LANDING_FOLDER = "district/transaction_history/"
ARCHIVE_FOLDER = "district/archive/"
TABLE_ID = "o2-data-s-z.district.district_transactions"
WORKSHEET_NAME = "Transactions summary"

# Strict Schema Mapping Contract (The structural target blueprint)
BQ_SCHEMA = [
    ("S_no_", "INTEGER"),
    ("Transaction ID", "INTEGER"),
    ("Date and time", "TIMESTAMP"),
    ("Transaction Type", "STRING"),
    ("Type", "STRING"),
    ("Res_ Name", "STRING"),
    ("Res_ ID", "INTEGER"),
    ("Currency", "STRING"),
    ("Bill Amount", "FLOAT"),
    ("Cover Charge", "FLOAT"),
    ("Discount type", "STRING"),
    ("Instant discount", "FLOAT"),
    ("Promo share", "FLOAT"),
    ("Commissionable amount", "FLOAT"),
    ("Commission %", "FLOAT"),
    ("Commission Amount", "FLOAT"),
    ("Tax on commission", "FLOAT"),
    ("Tips", "FLOAT"),
    ("Adjustment type", "STRING"),
    ("Adjustment", "FLOAT"),
    ("Net receivable ", "FLOAT"),  # Matches the literal live column with trailing space
    ("Settlement status", "STRING"),
    ("Settlement date", "DATE"),
    ("UTR Number _ Reference ID", "STRING"),
    ("Remark", "STRING")
]

# Explicit Translation Map: From Cleaned Raw Excel Header -> Precise Destination Column
HEADER_MAP = {
    "S.no.": "S_no_",
    "Transaction ID": "Transaction ID",
    "Date and time": "Date and time",
    "Transaction Type": "Transaction Type",
    "Type": "Type",
    "Res. Name": "Res_ Name",
    "Res. ID": "Res_ ID",
    "Currency": "Currency",
    "Bill Amount": "Bill Amount",
    "Cover Charge": "Cover Charge",
    "Discount type": "Discount type",
    "Instant discount": "Instant discount",
    "Promo share": "Promo share",
    "Commissionable amount": "Commissionable amount",
    "Commission %": "Commission %",
    "Commission Amount": "Commission Amount",
    "Tax on commission": "Tax on commission",
    "Tips": "Tips",
    "Adjustment type": "Adjustment type",
    "Adjustment": "Adjustment",
    "Net receivable": "Net receivable ",  # Maps stripped input to trailing-space destination
    "Settlement status": "Settlement status",
    "Settlement date": "Settlement date",
    "UTR Number / Reference ID": "UTR Number _ Reference ID",
    "Remark": "Remark"
}

@functions_framework.cloud_event
def run_pipeline(cloud_event):
    print("Pipeline Started")
    
    # Validate Event
    data = cloud_event.data
    bucket_name = data.get("bucket")
    file_name = data.get("name")
    
    if not bucket_name or not file_name:
        raise ValueError("Invalid Eventarc payload: missing bucket or file name.")
        
    # Validate Filename & Target Paths
    if bucket_name != BUCKET_NAME:
        print(f"Skipping event: Bucket {bucket_name} does not match target pipeline bucket.")
        return "Skipped", 200

    if not file_name.startswith(LANDING_FOLDER):
        print(f"Skipping event: File {file_name} is outside landing directory.")
        return "Skipped", 200

    if ARCHIVE_FOLDER in file_name or file_name.endswith("/"):
        print(f"Skipping event: Ignore directory placeholders or archive paths to prevent loop.")
        return "Skipped", 200

    print("File Validated")

    # Initialize clients
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Read File
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Race condition protection
    if not blob.exists():
        print("Landing file already processed and removed by a parallel thread execution. Exiting safely.")
        return "Clean Inbound Exit", 200

    file_bytes = blob.download_as_bytes()
    
    # Process Excel: skiprows=5 skips rows 1-5. Row 6 becomes the header.
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=WORKSHEET_NAME, skiprows=5)
    print("Worksheet Loaded")
    
    # Erase hidden trailing/leading spaces from incoming file headers instantly
    df.columns = df.columns.str.strip()
    
    # Ignore row 7 (which is index 0 of the dataframe after skiprows=5)
    if len(df) > 0:
        df = df.iloc[1:].reset_index(drop=True)
    
    print(f"Rows Read: {len(df)}")

    # Validate Structure
    current_headers = list(df.columns)
    if len(current_headers) != len(set(current_headers)):
        raise RuntimeError("Data contains duplicate headers. Rejecting file processing.")

    for raw_header in HEADER_MAP.keys():
        if raw_header not in current_headers:
            raise RuntimeError(f"Missing mandatory raw schema header in Excel file: '{raw_header}'")

    print("Columns Validated")

    # Validate Live BigQuery Schema Contract dynamically removing spaces for matching safety
    table = bq_client.get_table(TABLE_ID)
    live_schema_fields = {field.name.strip(): field.field_type for field in table.schema}
    
    for bq_name, bq_type in BQ_SCHEMA:
        clean_bq_name = bq_name.strip()
        if clean_bq_name not in live_schema_fields:
            raise RuntimeError(f"Live BigQuery table schema is missing expected column: '{clean_bq_name}'")
        
        live_type = live_schema_fields[clean_bq_name]
        norm_live = "INTEGER" if live_type in ["INTEGER", "INT64"] else ("FLOAT" if live_type in ["FLOAT", "FLOAT64"] else live_type)
        norm_expected = "INTEGER" if bq_type in ["INTEGER", "INT64"] else ("FLOAT" if bq_type in ["FLOAT", "FLOAT64"] else bq_type)
        
        if norm_live != norm_expected:
            raise RuntimeError(f"Type mismatch on column '{bq_name}'. Live: {live_type}, Expected: {bq_type}")

    # Transform: Translate raw stripped excel headers to destination clean headers
    df = df.rename(columns=HEADER_MAP)

    # Apply strict datatype conversions per Engineering Standard Section 11
    for col, data_type in BQ_SCHEMA:
        if data_type == "INTEGER":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif data_type == "FLOAT":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif data_type == "STRING":
            df[col] = df[col].astype(str).replace(["<NA>", "nan", "None", ""], pd.NA).astype("string")
        elif data_type == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col], errors="coerce")
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize("UTC")
            else:
                df[col] = df[col].dt.tz_convert("UTC")
        elif data_type == "DATE":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    print("Datatypes Converted")

    # Enforce precise column ordering matching the BigQuery table contract
    df = df[[name for name, _ in BQ_SCHEMA]]

    # Append BigQuery
    print("BigQuery Started")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    job = bq_client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    
    if job.errors:
        raise RuntimeError(f"BigQuery load failed with errors: {job.errors}")
        
    print("BigQuery Completed")

    # Archive File Execution with absolute isolated path evaluation
    if not blob.exists():
        print("Landing file already processed and removed by a parallel execution. Skipping archive routing.")
        return "Success - Handled by Parallel Run", 200

    base_filename = os.path.basename(file_name)
    archive_filename = f"{ARCHIVE_FOLDER}{base_filename}"
    
    bucket.copy_blob(blob, bucket, archive_filename)
    
    # Verify Archive
    archive_blob = bucket.blob(archive_filename)
    if not archive_blob.exists():
        raise RuntimeError("Archive validation verification failed. Target copy does not exist.")
        
    print("Archive Completed")

    # Delete Landing File Execution
    blob.delete()
    
    # Verify Delete
    if blob.exists():
        raise RuntimeError("Landing file cleanup failed. File still exists in staging area.")
        
    print("Landing Deleted")
    print("Pipeline Completed")
    
    return "Success", 200
