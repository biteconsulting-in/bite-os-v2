import functions_framework
import pandas as pd
import numpy as np
import io
import re
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

# Pipeline Configuration
TARGET_TABLE = "o2-data-s-z.zomato_partnerapp_data.zomato_item_level_data_v3"
LANDING_FOLDER_PREFIX = "zomato_o2/partnerapp_data/zomato_item_sales_report/"

@functions_framework.cloud_event
def run_pipeline(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    # Guard Clause: Ignore files outside the landing zone or already in the archive
    if not file_name.startswith(LANDING_FOLDER_PREFIX) or "archive/" in file_name:
        return

    storage_client = storage.Client()
    bq_client = bigquery.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        # 1. Read & Repair Raw Data
        if file_name.endswith('.csv'):
            raw_text = blob.download_as_text(encoding='utf-8')
            fixed_lines = []
            for line in raw_text.splitlines():
                if line.strip():
                    if not re.match(r'^(\d+|Restaurant ID)', line) and fixed_lines:
                        fixed_lines[-1] = fixed_lines[-1].strip() + " " + line.strip()
                    else:
                        fixed_lines.append(line)
            reconstructed_csv = "\n".join(fixed_lines)
            df_raw = pd.read_csv(io.StringIO(reconstructed_csv))
        elif file_name.endswith(('.xlsx', '.xls')):
            df_raw = pd.read_excel(blob.open("rb"))
        else:
            return

        # 2. Transform & Reshape
        identity_headers = ['Restaurant ID', 'Restaurant name', 'Subzone', 'City', 'Item name', 'Item category', 'Item subcategory', 'Metric']
        df_raw.columns = [str(x).strip() for x in df_raw.columns]
        date_headers = [col for col in df_raw.columns if col not in identity_headers]
        
        df_melted = df_raw.melt(id_vars=identity_headers, value_vars=date_headers, var_name="raw_date", value_name="raw_value")
        df_melted.columns = ['restaurant_id', 'restaurant_name', 'subzone', 'city', 'item_name', 'item_category', 'item_subcategory', 'metric', 'raw_date', 'raw_value']

        df_pivoted = df_melted.pivot_table(
            index=['restaurant_id', 'restaurant_name', 'subzone', 'city', 'item_name', 'item_category', 'item_subcategory', 'raw_date'],
            columns='metric', values='raw_value', aggfunc='first'
        ).reset_index()
        
        df_pivoted.columns = [str(x).strip() for x in df_pivoted.columns]

        def safe_float(val):
            if pd.isna(val) or str(val).strip() in ('', '-', 'None'): return 0.0
            try: return float(str(val).replace('₹', '').replace(',', '').strip())
            except ValueError: return 0.0

        def parse_dates_iso(date_str):
            try: dt = datetime.strptime(str(date_str).strip(), "%d %b %Y")
            except Exception: dt = pd.to_datetime(date_str)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%A"), dt.strftime("%Y-%m")

        date_transforms = df_pivoted['raw_date'].apply(parse_dates_iso)
        
        # 3. Assemble Final Table (with fail-safes for missing columns)
        df_final = pd.DataFrame()
        df_final['restaurant_id'] = df_pivoted['restaurant_id'].astype(str).str.strip()
        df_final['restaurant_name'] = df_pivoted['restaurant_name'].astype(str).str.strip()
        df_final['subzone'] = df_pivoted['subzone'].astype(str).str.strip()
        df_final['city'] = df_pivoted['city'].astype(str).str.strip()
        df_final['item_name'] = df_pivoted['item_name'].astype(str).str.strip()
        df_final['item_category'] = df_pivoted['item_category'].astype(str).str.strip()
        df_final['item_subcategory'] = df_pivoted['item_subcategory'].astype(str).str.strip()
        df_final['date'] = pd.to_datetime([x[0] for x in date_transforms])
        
        df_final['item_quantity_sold'] = df_pivoted.get('Item quantity sold', pd.Series(0, index=df_pivoted.index)).apply(safe_float).astype('int64')
        df_final['unit_cost_item_rs'] = df_pivoted.get('Unit cost of item (₹)', pd.Series(0, index=df_pivoted.index)).apply(safe_float)
        df_final['orders_with_item'] = df_pivoted.get('Orders with item', pd.Series(0, index=df_pivoted.index)).apply(safe_float).astype('int64')
        df_final['item_quantity_per_order'] = np.where(df_final['orders_with_item'] > 0, round(df_final['item_quantity_sold'] / df_final['orders_with_item'], 4), 0.0)
        df_final['item_rating'] = df_pivoted.get('Item rating', pd.Series(0, index=df_pivoted.index)).apply(safe_float)
        df_final['day'] = [x[1] for x in date_transforms]
        df_final['month'] = [x[2] for x in date_transforms]

        # 4. Execute BigQuery Load
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION]
        )
        load_job = bq_client.load_table_from_dataframe(df_final, TARGET_TABLE, job_config=job_config)
        load_job.result()

        # 5. Execute Storage Archive & Cleanup
        first_date = pd.to_datetime(df_final['date'].iloc[0]) if not df_final.empty else datetime.now()
        base_filename = file_name.split('/')[-1]
        
        # Cleanly construct the exact target path without duplication
        archive_path = f"{LANDING_FOLDER_PREFIX}archive/year={first_date.strftime('%Y')}/month={first_date.strftime('%m')}/day={first_date.strftime('%d')}/{base_filename}"
        
        bucket.copy_blob(blob, bucket, archive_path)
        blob.delete()
        
        print(f"✅ Success: Data loaded to BQ and file archived to {archive_path}")

    except Exception as e:
        print(f"❌ Pipeline Failure: {str(e)}")
        raise e
