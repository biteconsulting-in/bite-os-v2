import functions_framework
import pandas as pd
import re
from datetime import datetime
from google.cloud import storage, bigquery
import io

# Initialize Google Cloud clients globally
storage_client = storage.Client()
bq_client = bigquery.Client()

PROJECT_ID = "o2-data-s-z"
TABLE_ID = f"{PROJECT_ID}.swiggy_daily_powerbi.s_ops_metrics_daily"
LANDING_FOLDER = "swiggy_o2/swiggy_powerbi_data/swiggy_operational_metrics_overview/"

HEADER_MAP = {
    "Restaurant Name": "res_name",
    "Restaurant ID": "res_id",
    "City": "city",
    "Area": "area",
    "Availability (%)": "availability",
    "Unavailability (%) - Operational Stress": "unavailability_op_stress",
    "Unavailability (%) - Restaurant-driven": "unavailability_res",
    "Unavailability (%) - Swiggy-driven": "unavailability_platform", # <- Fixed Header
    "No. of Orders Accepted post 3 minutes": "orders_accepted_post_3min",
    "Imperfect Orders (per 1,000 orders)": "imperfect_orders_per_1k",
    "% Orders with Customer Complaints": "orders_w_complaints",
    "Restaurant-Driven Cancellations (%)": "cancellations_by_res",
    "Preperation Time": "prep_time",
    "MFR Compliance": "mfr_compliance"
}

@functions_framework.cloud_event
def run_pipeline(cloud_event):
    print("Pipeline Started")
    
    # 1. Validate Event
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    # 2. Validate Filename & Path
    if not file_name.startswith(LANDING_FOLDER):
        print(f"File {file_name} is outside the landing folder. Ignoring.")
        return "Ignored", 200
        
    if "archive/" in file_name:
        print(f"File {file_name} is in the archive folder. Ignoring.")
        return "Ignored", 200

    if not (file_name.endswith(".csv") or file_name.endswith(".xlsx")):
        print(f"File {file_name} is not a valid data file. Ignoring.")
        return "Ignored", 200

    print(f"File Validated: {file_name}")

    # Extract date from filename (e.g., ops_metrics_20260712)
    date_match = re.search(r'(\d{8})', file_name.split('/')[-1])
    if not date_match:
        raise ValueError(f"Invalid filename: Cannot extract yyyymmdd from {file_name}")
    
    extracted_date = datetime.strptime(date_match.group(1), "%Y%m%d").date()

    # 3. Read File
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_bytes()
    
    if file_name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(file_content))
    else:
        df = pd.read_excel(io.BytesIO(file_content))
        print("Worksheet Loaded")
        
    print(f"Rows Read: {len(df)}")

    # 4. Validate Structure (Headers)
    actual_headers = set(df.columns)
    expected_headers = set(HEADER_MAP.keys())
    
    missing_headers = expected_headers - actual_headers
    if missing_headers:
        raise ValueError(f"Missing headers in file: {missing_headers}")
        
    if len(df.columns) != len(set(df.columns)):
        raise ValueError("Duplicate headers found in file.")

    # 5. Transform
    # Rename columns explicitly
    df = df.rename(columns=HEADER_MAP)
    
    # Enforce datatypes strictly as per Bite OS standard
    df["res_name"] = df["res_name"].astype(str).replace("", pd.NA).astype("string")
    df["res_id"] = pd.to_numeric(df["res_id"], errors="coerce").astype("Int64")
    df["city"] = df["city"].astype(str).replace("", pd.NA).astype("string")
    df["area"] = df["area"].astype(str).replace("", pd.NA).astype("string")
    
    float_columns = [
        "availability", "unavailability_op_stress", "unavailability_res", 
        "unavailability_platform", "orders_w_complaints", 
        "cancellations_by_res", "mfr_compliance"
    ]
    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        
    int_columns = ["orders_accepted_post_3min", "imperfect_orders_per_1k", "prep_time"]
    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Append the extracted date column
    df["date"] = extracted_date
    print("Datatypes Converted")

    # 6. Validate Output against Live BigQuery Schema
    table = bq_client.get_table(TABLE_ID)
    bq_schema_names = [field.name for field in table.schema]
    
    # Ensure DataFrame strictly follows BigQuery column order
    try:
        df = df[bq_schema_names]
    except KeyError as e:
        raise ValueError(f"Schema mismatch. BigQuery expects columns not present in transformed data: {e}")
    
    print("Columns Validated")

    # 7. Append BigQuery
    print("BigQuery Started")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    job = bq_client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()  # Wait for the job to complete
    
    # 8. Verify Load
    if job.errors:
        raise RuntimeError(f"BigQuery load failed: {job.errors}")
    print("BigQuery Completed")

    # 9. Archive Logic
    archive_folder_path = f"{LANDING_FOLDER}archive/"
    file_basename = file_name.split('/')[-1]
    archive_file_name = f"{archive_folder_path}{file_basename}"
    
    archive_blob = bucket.blob(archive_file_name)
    bucket.copy_blob(blob, bucket, archive_file_name)
    
    # 10. Verify Archive
    if not archive_blob.exists():
        raise RuntimeError("Archive failure: Copied file does not exist in archive folder.")
    print("Archive Completed")
    
    # 11. Delete Landing File
    blob.delete()
    
    # 12. Verify Delete
    if blob.exists():
        raise RuntimeError("Delete failure: Landing file still exists after deletion attempt.")
    print("Landing Deleted")
    
    print("Pipeline Completed")
    return "Success", 200
