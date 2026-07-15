import functions_framework
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

TARGET_TABLE = "o2-data-s-z.zomato_partnerapp_data.zomato_bizmetrics_dashboard"
LANDING_FOLDER_PREFIX = "zomato_o2/partnerapp_data/zomato_bizmetrics/"

@functions_framework.cloud_event
def run_pipeline(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    if not file_name.startswith(LANDING_FOLDER_PREFIX) or "archive/" in file_name:
        print(f"⏭️ Skipping file: {file_name} (Outside landing zone or already archived).")
        return

    print(f"📥 Trigger Active: Found raw file 'gs://{bucket_name}/{file_name}'")
    
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        if file_name.endswith('.csv'):
            df_raw = pd.read_csv(blob.open("r"))
        elif file_name.endswith(('.xlsx', '.xls')):
            df_raw = pd.read_excel(blob.open("rb"))
        else:
            print(f"❌ Unsupported format: {file_name}")
            return
    except Exception as e:
        print(f"❌ Ingestion Error: {str(e)}")
        return

    print("⚙️ Executing wide-to-long dynamic matrix unpivot...")
    meta_columns = list(df_raw.columns[:6])
    date_columns = list(df_raw.columns[6:])

    df_melted = df_raw.melt(
        id_vars=meta_columns,
        value_vars=date_columns,
        var_name="raw_date",
        value_name="raw_value"
    )

    df_melted.columns = ['res_id', 'res_name', 'subzone', 'city', 'blank_col', 'metric', 'raw_date', 'raw_value']
    df_melted['location'] = df_melted['subzone'].fillna('') + ', ' + df_melted['city'].fillna('')
    df_melted['location'] = df_melted['location'].str.strip(', ')

    df_pivoted = df_melted.pivot_table(
        index=['res_name', 'res_id', 'location', 'raw_date'],
        columns='metric',
        values='raw_value',
        aggfunc='first'
    ).reset_index()

    expected_metrics = [
        'Delivered orders', 'Sales (Rs)', 'Market share (%)', 'Average rating', 'Rated orders',
        'Bad orders', 'Rejected orders', 'KPT (in minutes)', 'KPT+10 delayed orders', 'Poor rated orders',
        'Total complaints', 'Non-refunded complaints', 'Total complaints - Poor packaging',
        'Total complaints - Poor quality', 'Total complaints - Wrong order', 'Total complaints - Missing items',
        'Self logs other ors', 'Lost sales (Rs)', 'Online %', 'Offline time (in hours)', 'FOR accuracy (%)',
        'Impressions', 'Impressions to menu (%)', 'Menu opens', 'Menu to cart (%)', 'Cart builds',
        'Cart to orders (%)', 'Placed Orders', 'New user orders', 'Repeat user orders', 'Lapsed user orders',
        'Breakfast orders', 'Lunch orders', 'Snacks orders', 'Dinner orders', 'Late night orders',
        'Sales from ads (Rs)', 'Ads CTR (%)', 'Ads impressions', 'Ads menu opens', 'Ads orders',
        'Ads spend (Rs)', 'Ads ROI', 'Orders with offers', 'Gross sales from offers (Rs)',
        'Discount given (Rs)', 'Effective discount (%)'
    ]
    for col in expected_metrics:
        if col not in df_pivoted.columns:
            df_pivoted[col] = np.nan

    def clean_number(val):
        if pd.isna(val) or str(val).strip() in ('', '-', 'None'):
            return 0.0
        cleaned = str(val).replace('%', '').replace(',', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def clean_percentage(val):
        if pd.isna(val) or str(val).strip() in ('', '-', 'None'):
            return 0.0
        cleaned = str(val).strip().rstrip('%')
        try:
            return round(float(cleaned) / 100.0, 4)
        except ValueError:
            return 0.0

    def process_date_elements(date_str):
        try:
            cleaned_date = str(date_str).replace(',', '').strip()
            parsed_dt = datetime.strptime(cleaned_date, "%d %b %Y")
        except Exception:
            parsed_dt = pd.to_datetime(date_str)
        return parsed_dt.strftime("%Y-%m-%d"), parsed_dt.strftime("%A"), parsed_dt.strftime("%Y-%m"), parsed_dt

    df_final = pd.DataFrame()
    df_final['res_name'] = df_pivoted['res_name'].astype(str).str.strip()
    df_final['res_id'] = pd.to_numeric(df_pivoted['res_id'], errors='coerce').fillna(0).astype('int64')
    df_final['location'] = df_pivoted['location'].astype(str).str.strip()

    date_deconstruction = df_pivoted['raw_date'].apply(process_date_elements)
    
    # FIX: Explicitly converts date text list into true datetime objects for BigQuery compatibility
    df_final['date'] = pd.to_datetime([x[0] for x in date_deconstruction])
    
    df_final['day'] = [x[1] for x in date_deconstruction]
    df_final['month'] = [x[2] for x in date_deconstruction]
    
    sample_dt = date_deconstruction.iloc[0][3] if not date_deconstruction.empty else datetime.now()

    df_final['delivered_orders'] = df_pivoted['Delivered orders'].apply(clean_number)
    df_final['sales_rs'] = df_pivoted['Sales (Rs)'].apply(clean_number)
    df_final['market_share_pct'] = df_pivoted['Market share (%)'].apply(clean_number)
    df_final['average_rating'] = df_pivoted['Average rating'].apply(clean_number)
    df_final['rated_orders'] = df_pivoted['Rated orders'].apply(clean_number)
    df_final['bad_orders'] = df_pivoted['Bad orders'].apply(clean_number)
    df_final['rejected_orders'] = df_pivoted['Rejected orders'].apply(clean_number)

    kpt_raw = df_pivoted['KPT (in minutes)'].apply(clean_number)
    df_final['kpt_minutes'] = np.where(kpt_raw < 0, 0.0, kpt_raw)
    df_final['kpt_plus_10_delayed_orders'] = df_pivoted['KPT+10 delayed orders'].apply(clean_number)

    df_final['poor_rated_orders'] = df_pivoted['Poor rated orders'].apply(clean_number)
    df_final['total_complaints'] = df_pivoted['Total complaints'].apply(clean_number)
    df_final['non_refunded_complaints'] = df_pivoted['Non-refunded complaints'].apply(clean_number)
    df_final['total_complaints_poor_packaging'] = df_pivoted['Total complaints - Poor packaging'].apply(clean_number)
    df_final['total_complaints_poor_quality'] = df_pivoted['Total complaints - Poor quality'].apply(clean_number)
    df_final['total_complaints_wrong_order'] = df_pivoted['Total complaints - Wrong order'].apply(clean_number)
    df_final['total_complaints_missing_items'] = df_pivoted['Total complaints - Missing items'].apply(clean_number)
    df_final['self_logs_other_ors'] = df_pivoted['Self logs other ors'].apply(clean_number)
    df_final['lost_sales_rs'] = df_pivoted['Lost sales (Rs)'].apply(clean_number)
    df_final['online_pct'] = df_pivoted['Online %'].apply(clean_number)
    df_final['offline_time_hours'] = df_pivoted['Offline time (in hours)'].apply(clean_number)
    df_final['for_accuracy_pct'] = df_pivoted['FOR accuracy (%)'].apply(clean_number)
    df_final['impressions'] = df_pivoted['Impressions'].apply(clean_number)

    df_final['impressions_to_menu_pct'] = df_pivoted['Impressions to menu (%)'].apply(clean_percentage)
    df_final['menu_opens'] = df_pivoted['Menu opens'].apply(clean_number)
    df_final['menu_to_cart_pct'] = df_pivoted['Menu to cart (%)'].apply(clean_percentage)
    df_final['cart_builds'] = df_pivoted['Cart builds'].apply(clean_number)
    df_final['cart_to_orders_pct'] = df_pivoted['Cart to orders (%)'].apply(clean_percentage)
    df_final['placed_orders'] = df_pivoted['Placed Orders'].apply(clean_number)

    df_final['new_user_orders'] = df_pivoted['New user orders'].apply(clean_number)
    df_final['repeat_user_orders'] = df_pivoted['Repeat user orders'].apply(clean_number)
    df_final['lapsed_user_orders'] = df_pivoted['Lapsed user orders'].apply(clean_number)

    df_final['breakfast_orders'] = df_pivoted['Breakfast orders'].apply(clean_number)
    df_final['lunch_orders'] = df_pivoted['Lunch orders'].apply(clean_number)
    df_final['snacks_orders'] = df_pivoted['Snacks orders'].apply(clean_number)
    df_final['dinner_orders'] = df_pivoted['Dinner orders'].apply(clean_number)
    df_final['late_night_orders'] = df_pivoted['Late night orders'].apply(clean_number)

    df_final['sales_from_ads_rs'] = df_pivoted['Sales from ads (Rs)'].apply(clean_number)
    df_final['ads_ctr_pct'] = df_pivoted['Ads CTR (%)'].apply(clean_percentage)
    df_final['ads_impressions'] = df_pivoted['Ads impressions'].apply(clean_number)
    df_final['ads_menu_opens'] = df_pivoted['Ads menu opens'].apply(clean_number)
    df_final['ads_orders'] = df_pivoted['Ads orders'].apply(clean_number)
    df_final['ads_spend_rs'] = df_pivoted['Ads spend (Rs)'].apply(clean_number)
    df_final['ads_roi'] = df_pivoted['Ads ROI'].apply(clean_number)

    df_final['orders_with_offers'] = df_pivoted['Orders with offers'].apply(clean_number)
    df_final['gross_sales_from_offers_rs'] = df_pivoted['Gross sales from offers (Rs)'].apply(clean_number)
    df_final['discount_given_rs'] = df_pivoted['Discount given (Rs)'].apply(clean_number)
    df_final['effective_discount_pct'] = df_pivoted['Effective discount (%)'].apply(clean_percentage)

    m_opens = df_final['menu_opens']
    p_orders = df_final['placed_orders']
    df_final['m2o'] = np.where(m_opens == 0, 0.0, round(p_orders / m_opens, 4))

    print(f"🚀 Appending records down into production table: {TARGET_TABLE}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = bq_client.load_table_from_dataframe(df_final, TARGET_TABLE, job_config=job_config)
    load_job.result()
    print("✅ Destination commit confirmed.")

    year_str, month_str, day_str = sample_dt.strftime("%Y"), sample_dt.strftime("%m"), sample_dt.strftime("%d")
    pure_filename = file_name.split("/")[-1]
    archive_path = f"{LANDING_FOLDER_PREFIX}archive/year={year_str}/month={month_str}/day={day_str}/{pure_filename}"
    
    print(f"📦 Commencing Hive storage transfer: {archive_path}")
    bucket.copy_blob(blob, bucket, archive_path)
    blob.delete()
    print("🧹 Landing layer clear. Automation cycle complete.")
