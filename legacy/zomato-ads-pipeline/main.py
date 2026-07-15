from __future__ import annotations

import logging
import math
import re
from datetime import date, datetime, timezone
from io import BytesIO
from typing import Any
from zipfile import BadZipFile, ZipFile

import functions_framework
import pandas as pd
from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# config.py
# =============================================================================

PROJECT_ID = "o2-data-s-z"
BUCKET_NAME = "o2-data-raw-ingestion"
LANDING_PREFIX = "zomato_o2/partnerapp_data/zomato_ad_reports/"
ARCHIVE_PREFIX = "zomato_o2/partnerapp_data/zomato_ad_reports/archive/"

BIGQUERY_DATASET = "zomato_partnerapp_data"
BIGQUERY_TABLE = "zomato_ads_partnerapp_combined"
BIGQUERY_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
RESTAURANT_LOOKUP_TABLE = (
    "o2-data-s-z.zomato_partnerapp_data.admin_zomato_res_id"
)
LOOKUP_COLUMNS = [
    "res_id",
    "res_name",
    "subzone",
]

EXPECTED_REPORTS = ("overall", "nrl", "veg_nonveg")
EXPECTED_REPORT_PATTERNS = {
    "overall": r"^OVERALL.*\.csv$",
    "nrl": r"^NRL.*\.csv$",
    "veg_nonveg": r"^(VEG|veg_non_veg).*\.csv$",
}

COLUMN_ORDER = [
    "res_id",
    "date",
    "campaign_id",
    "ad_group",
    "product_type",
    "targeting",
    "segments",
    "report_type",
    "user_type",
    "res_name",
    "city",
    "subzone",
    "ad_impressions",
    "ad_clicks",
    "ad_orders",
    "ad_carts",
    "ad_spend_rs",
    "ad_sales_rs",
    "ads_m2o_pct",
    "ads_ctr_pct",
    "ads_otr_pct",
    "roi",
    "cpx",
    "overall_m2o_pct",
    "overall_ctr_pct",
    "overall_otr_pct",
    "total_impressions",
    "total_clicks",
    "total_orders",
    "delivery_pct",
    "daily_booked_budget_rs",
    "start_date",
    "end_date",
    "bos_keyword",
    "month",
]

INTEGER_COLUMNS = [
    "res_id",
    "campaign_id",
    "ad_group",
    "ad_impressions",
    "ad_clicks",
    "ad_orders",
    "ad_carts",
    "total_impressions",
    "total_clicks",
    "total_orders",
]

FLOAT_COLUMNS = [
    "ad_spend_rs",
    "ad_sales_rs",
    "ads_m2o_pct",
    "ads_ctr_pct",
    "ads_otr_pct",
    "roi",
    "cpx",
    "overall_m2o_pct",
    "overall_ctr_pct",
    "overall_otr_pct",
    "delivery_pct",
    "daily_booked_budget_rs",
]

DATE_COLUMNS = ["date", "start_date", "end_date"]

STRING_COLUMNS = [
    "product_type",
    "targeting",
    "segments",
    "report_type",
    "user_type",
    "res_name",
    "city",
    "subzone",
    "bos_keyword",
    "month",
]

BIGQUERY_SCHEMA_FIELDS = [
    ("res_id", "INTEGER"),
    ("date", "DATE"),
    ("campaign_id", "INTEGER"),
    ("ad_group", "INTEGER"),
    ("product_type", "STRING"),
    ("targeting", "STRING"),
    ("segments", "STRING"),
    ("report_type", "STRING"),
    ("user_type", "STRING"),
    ("res_name", "STRING"),
    ("city", "STRING"),
    ("subzone", "STRING"),
    ("ad_impressions", "INTEGER"),
    ("ad_clicks", "INTEGER"),
    ("ad_orders", "INTEGER"),
    ("ad_carts", "INTEGER"),
    ("ad_spend_rs", "FLOAT"),
    ("ad_sales_rs", "FLOAT"),
    ("ads_m2o_pct", "FLOAT"),
    ("ads_ctr_pct", "FLOAT"),
    ("ads_otr_pct", "FLOAT"),
    ("roi", "FLOAT"),
    ("cpx", "FLOAT"),
    ("overall_m2o_pct", "FLOAT"),
    ("overall_ctr_pct", "FLOAT"),
    ("overall_otr_pct", "FLOAT"),
    ("total_impressions", "INTEGER"),
    ("total_clicks", "INTEGER"),
    ("total_orders", "INTEGER"),
    ("delivery_pct", "FLOAT"),
    ("daily_booked_budget_rs", "FLOAT"),
    ("start_date", "DATE"),
    ("end_date", "DATE"),
    ("bos_keyword", "STRING"),
    ("month", "STRING"),
]

OVERALL_REQUIRED_COLUMNS = [
    "date",
    "res_id",
    "ad_group",
    "city",
    "subzone",
    "product_type",
    "campaign_id",
    "targeting",
    "segments",
    "start_date",
    "end_date",
    "bos_keyword",
    "cpx",
    "ad_impressions",
    "ad_clicks",
    "ad_orders",
    "ad_carts",
    "ad_spend_rs",
    "ad_sales_rs",
    "total_impressions",
    "total_clicks",
    "total_orders",
    "daily_booked_budget_rs",
    "ads_m2o",
    "ads_ctr",
    "ads_otr",
    "roi",
    "delivery",
    "overall_m2o",
    "overall_ctr",
    "overall_otr",
]

BREAKDOWN_REQUIRED_COLUMNS = [
    "date",
    "res_id",
    "ad_group",
    "product_type",
    "campaign_id",
    "targeting",
    "bos_keyword",
    "segments",
    "start_date",
    "end_date",
    "user_type",
    "ad_impressions",
    "ad_clicks",
    "ad_orders",
    "ad_spend_rs",
    "ad_sales_rs",
    "ads_m2o",
    "ads_ctr",
    "ads_otr",
]

# =============================================================================
# gcs_ops.py
# =============================================================================


def get_storage_client() -> storage.Client:
    return storage.Client()


def is_landing_object_valid(bucket_name: str, object_name: str) -> bool:
    if bucket_name != BUCKET_NAME:
        logger.info("Ignoring event from unexpected bucket: %s", bucket_name)
        return False
    if not object_name.startswith(LANDING_PREFIX):
        logger.info("Ignoring object outside targeting landing folder: %s", object_name)
        return False
    if is_archive_object(object_name) or "/archive/" in object_name:
        logger.info("Ignoring archive object: %s", object_name)
        return False
    if not object_name.lower().endswith(".zip"):
        logger.info("Ignoring non-ZIP object inside landing folder: %s", object_name)
        return False
    return True


def is_archive_object(object_name: str) -> bool:
    return object_name.startswith(ARCHIVE_PREFIX)


def read_zip_from_gcs(client: storage.Client, bucket_name: str, object_name: str) -> tuple[bytes, int]:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not blob.exists():
        raise FileNotFoundError(f"Landing object does not exist: gs://{bucket_name}/{object_name}")

    blob.reload()
    generation = int(blob.generation)
    data = blob.download_as_bytes(if_generation_match=generation)
    return data, generation


def archive_zip(
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    source_generation: int,
    expected_size: int,
) -> str:
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(object_name, generation=source_generation)
    archive_name = build_archive_object_name(object_name, source_generation)

    copied_blob = bucket.copy_blob(source_blob, bucket, archive_name)
    copied_blob.reload()

    if not copied_blob.exists() or copied_blob.size != expected_size:
        raise RuntimeError(f"Archive verification failed: gs://{bucket_name}/{archive_name}")

    return archive_name


def delete_landing_zip(
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    source_generation: int,
) -> None:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name, generation=source_generation)
    blob.delete(if_generation_match=source_generation)


def build_archive_object_name(object_name: str, source_generation: int) -> str:
    filename = object_name.rsplit("/", 1)[-1]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_name = filename.replace("/", "_")
    return f"{ARCHIVE_PREFIX}{timestamp}_{source_generation}_{safe_name}"

# =============================================================================
# bq_ops.py
# =============================================================================


def get_bigquery_client() -> bigquery.Client:
    return bigquery.Client()


def build_bigquery_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField(name, field_type, mode="NULLABLE")
        for name, field_type in BIGQUERY_SCHEMA_FIELDS
    ]


def append_dataframe_to_bigquery(client: bigquery.Client, dataframe: pd.DataFrame) -> int:
    if dataframe.empty:
        raise ValueError("No rows to append to BigQuery")

    job_config = bigquery.LoadJobConfig(
        schema=build_bigquery_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(dataframe, BIGQUERY_TABLE_ID, job_config=job_config)
    result = job.result()

    if job.errors:
        raise RuntimeError(f"BigQuery load failed: {job.errors}")

    output_rows = int(result.output_rows or 0)
    expected_rows = int(len(dataframe))
    if output_rows != expected_rows:
        raise RuntimeError(f"BigQuery load row mismatch: expected {expected_rows}, loaded {output_rows}")

    return output_rows


def read_restaurant_lookup(client: bigquery.Client) -> dict[str, dict[str, str]]:
    selected_columns = ",\n          ".join(
        f"CAST({column} AS STRING) AS {column}" for column in LOOKUP_COLUMNS
    )
    query = f"""
        SELECT
          {selected_columns}
        FROM `{RESTAURANT_LOOKUP_TABLE}`
        WHERE res_id IS NOT NULL
    """
    rows = client.query(query).result()

    lookup: dict[str, dict[str, str]] = {}
    for row in rows:
        row_values = dict(row.items())
        res_id = bq_lookup_string_value(row_values.get("res_id"))
        if res_id:
            lookup[res_id] = {
                "res_name": bq_lookup_string_value(row_values.get("res_name")) or "Unknown",
                "subzone": bq_lookup_string_value(row_values.get("subzone")) or "Unknown",
            }
            if "city" in row_values:
                lookup[res_id]["city"] = bq_lookup_string_value(row_values.get("city")) or "Unknown"

    if not lookup:
        raise ValueError("Restaurant lookup table returned zero rows")

    return lookup


def bq_lookup_string_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()

# =============================================================================
# transform.py
# =============================================================================


class PipelineValidationError(ValueError):
    pass


def transform_zip(zip_bytes: bytes, restaurant_lookup: dict[str, dict[str, str]]) -> pd.DataFrame:
    report_bytes = extract_report_csvs(zip_bytes)
    overall_df = read_report_csv(report_bytes["overall"], "overall")
    nrl_df = read_report_csv(report_bytes["nrl"], "nrl")
    veg_df = read_report_csv(report_bytes["veg_nonveg"], "veg_nonveg")

    validate_required_columns(overall_df, OVERALL_REQUIRED_COLUMNS, "overall")
    validate_required_columns(nrl_df, BREAKDOWN_REQUIRED_COLUMNS, "nrl")
    validate_required_columns(veg_df, BREAKDOWN_REQUIRED_COLUMNS, "veg_nonveg")

    restaurant_lookup = enrich_restaurant_lookup_with_overall_city(restaurant_lookup, overall_df)

    frames = [
        process_overall_rows(overall_df, restaurant_lookup),
        process_breakdown_rows(nrl_df, "nrl", restaurant_lookup),
        process_breakdown_rows(veg_df, "veg_nonveg", restaurant_lookup),
    ]
    merged = pd.concat(frames, ignore_index=True)

    if merged.empty:
        raise PipelineValidationError("Merged dataframe contains zero rows")

    merged = merged[COLUMN_ORDER]
    return coerce_bigquery_types(merged)


def extract_report_csvs(zip_bytes: bytes) -> dict[str, bytes]:
    try:
        with ZipFile(BytesIO(zip_bytes)) as archive:
            files = [info for info in archive.infolist() if not info.is_dir()]
            if len(files) != 3:
                raise PipelineValidationError(f"ZIP must contain exactly 3 files, found {len(files)}")

            reports: dict[str, bytes] = {}
            for info in files:
                if not info.filename.lower().endswith(".csv"):
                    raise PipelineValidationError(f"Unexpected non-CSV file in ZIP: {info.filename}")

                report_type = classify_report_filename(info.filename)
                if report_type is None:
                    raise PipelineValidationError(f"Unexpected CSV file in ZIP: {info.filename}")
                if report_type in reports:
                    raise PipelineValidationError(f"Duplicate {report_type} report in ZIP")

                reports[report_type] = archive.read(info)
    except BadZipFile as exc:
        raise PipelineValidationError("Uploaded object is not a valid ZIP file") from exc

    missing = set(EXPECTED_REPORTS) - set(reports)
    if missing:
        raise PipelineValidationError(f"Missing required report(s): {', '.join(sorted(missing))}")

    return reports


def classify_report_filename(filename: str) -> str | None:
    base_name = filename.rsplit("/", 1)[-1]
    for report_type, pattern in EXPECTED_REPORT_PATTERNS.items():
        if re.match(pattern, base_name, flags=re.IGNORECASE):
            return report_type
    return None


def read_report_csv(csv_bytes: bytes, report_type: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(BytesIO(csv_bytes), dtype=str, keep_default_na=False)
    except UnicodeDecodeError:
        df = pd.read_csv(BytesIO(csv_bytes), dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except Exception as exc:
        raise PipelineValidationError(f"Failed to read {report_type} CSV: {exc}") from exc

    if df.empty:
        raise PipelineValidationError(f"{report_type} CSV contains no data rows")

    df.columns = [normalize_header(column) for column in df.columns]
    duplicate_headers = sorted({column for column in df.columns if list(df.columns).count(column) > 1})
    if duplicate_headers:
        raise PipelineValidationError(
            f"{report_type} CSV has duplicate normalized header(s): {', '.join(duplicate_headers)}"
        )

    df = df.map(lambda value: value.strip() if isinstance(value, str) else value)
    df = df.loc[~df.apply(lambda row: all(str(value).strip() == "" for value in row), axis=1)].copy()
    if df.empty:
        raise PipelineValidationError(f"{report_type} CSV contains only empty rows")

    return df


def normalize_header(header: Any) -> str:
    normalized = str(header).strip().lower()
    normalized = re.sub(r"[^\w\s]", "", normalized)
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"^_+|_+$", "", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized


def validate_required_columns(df: pd.DataFrame, required_columns: list[str], report_type: str) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise PipelineValidationError(
            f"{report_type} CSV is missing required column(s): {', '.join(missing)}"
        )


def enrich_restaurant_lookup_with_overall_city(
    restaurant_lookup: dict[str, dict[str, str]],
    overall_df: pd.DataFrame,
) -> dict[str, dict[str, str]]:
    lookup = {res_id: dict(values) for res_id, values in restaurant_lookup.items()}
    for _, row in overall_df.iterrows():
        res_id = str(row.get("res_id", "") or row.get("resid", "")).strip()
        city = str(row.get("city", "")).strip()
        if res_id:
            lookup.setdefault(res_id, {"res_name": "Unknown", "subzone": "Unknown"})
            lookup[res_id].setdefault("res_name", "Unknown")
            lookup[res_id].setdefault("subzone", "Unknown")
            if city and not lookup[res_id].get("city"):
                lookup[res_id]["city"] = city
    return lookup


def process_overall_rows(
    df: pd.DataFrame,
    restaurant_lookup: dict[str, dict[str, str]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for index, row in df.iterrows():
        res_id = string_value(row.get("res_id") or row.get("resid"))
        iso_date = convert_to_iso_date(row.get("date"), "overall", index)
        res_info = restaurant_lookup.get(
            res_id,
            {"res_name": "Unknown", "city": "Unknown", "subzone": "Unknown"},
        )
        rows.append(
            {
                "res_id": format_as_integer(row.get("res_id")),
                "date": iso_date,
                "campaign_id": format_as_integer(row.get("campaign_id")),
                "ad_group": format_as_integer(row.get("ad_group")),
                "product_type": string_value(row.get("product_type")),
                "targeting": string_value(row.get("targeting")),
                "segments": string_value(row.get("segments")),
                "report_type": "overall",
                "user_type": "",
                "res_name": res_info.get("res_name", "Unknown"),
                "city": res_info.get("city", "Unknown"),
                "subzone": res_info.get("subzone", "Unknown"),
                "ad_impressions": format_as_integer(row.get("ad_impressions")),
                "ad_clicks": format_as_integer(row.get("ad_clicks")),
                "ad_orders": format_as_integer(row.get("ad_orders")),
                "ad_carts": format_as_integer(row.get("ad_carts")),
                "ad_spend_rs": format_as_float(row.get("ad_spend_rs")),
                "ad_sales_rs": format_as_float(row.get("ad_sales_rs")),
                "ads_m2o_pct": format_as_percentage_decimal(row.get("ads_m2o")),
                "ads_ctr_pct": format_as_percentage_decimal(row.get("ads_ctr")),
                "ads_otr_pct": format_as_percentage_decimal(row.get("ads_otr")),
                "roi": format_as_float(row.get("roi")),
                "cpx": format_as_float(row.get("cpx")),
                "overall_m2o_pct": format_as_percentage_decimal(row.get("overall_m2o")),
                "overall_ctr_pct": format_as_percentage_decimal(row.get("overall_ctr")),
                "overall_otr_pct": format_as_percentage_decimal(row.get("overall_otr")),
                "total_impressions": format_as_integer(row.get("total_impressions")),
                "total_clicks": format_as_integer(row.get("total_clicks")),
                "total_orders": format_as_integer(row.get("total_orders")),
                "delivery_pct": format_as_percentage_decimal(row.get("delivery")),
                "daily_booked_budget_rs": format_as_float(row.get("daily_booked_budget_rs")),
                "start_date": format_start_end_date(row.get("start_date"), "overall", index),
                "end_date": format_start_end_date(row.get("end_date"), "overall", index),
                "bos_keyword": string_value(row.get("bos_keyword")),
                "month": iso_date[:7],
            }
        )
    return pd.DataFrame(rows)


def process_breakdown_rows(
    df: pd.DataFrame,
    report_type: str,
    restaurant_lookup: dict[str, dict[str, str]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for index, row in df.iterrows():
        res_id = string_value(row.get("res_id") or row.get("resid"))
        iso_date = convert_to_iso_date(row.get("date"), report_type, index)
        res_info = restaurant_lookup.get(
            res_id,
            {"res_name": "Unknown", "city": "Unknown", "subzone": "Unknown"},
        )
        rows.append(
            {
                "res_id": format_as_integer(row.get("res_id")),
                "date": iso_date,
                "campaign_id": format_as_integer(row.get("campaign_id")),
                "ad_group": format_as_integer(row.get("ad_group")),
                "product_type": string_value(row.get("product_type")),
                "targeting": string_value(row.get("targeting")),
                "segments": string_value(row.get("segments")),
                "report_type": report_type,
                "user_type": string_value(row.get("user_type")),
                "res_name": res_info.get("res_name", "Unknown"),
                "city": res_info.get("city", "Unknown"),
                "subzone": res_info.get("subzone", "Unknown"),
                "ad_impressions": format_as_integer(row.get("ad_impressions")),
                "ad_clicks": format_as_integer(row.get("ad_clicks")),
                "ad_orders": format_as_integer(row.get("ad_orders")),
                "ad_carts": 0,
                "ad_spend_rs": format_as_float(row.get("ad_spend_rs")),
                "ad_sales_rs": format_as_float(row.get("ad_sales_rs")),
                "ads_m2o_pct": format_as_percentage_decimal(row.get("ads_m2o")),
                "ads_ctr_pct": format_as_percentage_decimal(row.get("ads_ctr")),
                "ads_otr_pct": format_as_percentage_decimal(row.get("ads_otr")),
                "roi": 0,
                "cpx": 0,
                "overall_m2o_pct": 0,
                "overall_ctr_pct": 0,
                "overall_otr_pct": 0,
                "total_impressions": 0,
                "total_clicks": 0,
                "total_orders": 0,
                "delivery_pct": 0,
                "daily_booked_budget_rs": 0,
                "start_date": format_start_end_date(row.get("start_date"), report_type, index),
                "end_date": format_start_end_date(row.get("end_date"), report_type, index),
                "bos_keyword": string_value(row.get("bos_keyword")),
                "month": iso_date[:7],
            }
        )
    return pd.DataFrame(rows)


def string_value(value: Any) -> str:
    if value is None or is_nan(value):
        return ""
    return str(value).strip()


def format_as_integer(value: Any) -> int:
    if value is None or is_nan(value) or str(value).strip() == "":
        return 0
    try:
        return math.floor(float(str(value).strip().replace(",", "")))
    except ValueError as exc:
        raise PipelineValidationError(f"Invalid integer value: {value}") from exc


def format_as_float(value: Any) -> float:
    if value is None or is_nan(value) or str(value).strip() == "":
        return 0.0
    try:
        return round(float(str(value).strip().replace(",", "")), 2)
    except ValueError as exc:
        raise PipelineValidationError(f"Invalid float value: {value}") from exc


def format_as_percentage_decimal(value: Any) -> float:
    if value is None or is_nan(value) or str(value).strip() == "":
        return 0.0
    try:
        return round(float(str(value).strip().replace("%", "").replace(",", "")) / 100, 4)
    except ValueError as exc:
        raise PipelineValidationError(f"Invalid percentage value: {value}") from exc


def convert_to_iso_date(value: Any, report_type: str, row_index: int) -> str:
    text = string_value(value)
    if re.fullmatch(r"\d{8}", text):
        iso_value = f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        iso_value = text
    else:
        raise PipelineValidationError(f"{report_type} row {row_index + 2} has invalid date: {text}")

    validate_iso_date(iso_value, report_type, row_index, "date")
    return iso_value


def format_start_end_date(value: Any, report_type: str, row_index: int) -> str:
    text = string_value(value)
    if text == "":
        return ""

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        validate_iso_date(text, report_type, row_index, "start/end date")
        return text

    match = re.search(r"([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})", text)
    if not match:
        raise PipelineValidationError(f"{report_type} row {row_index + 2} has invalid start/end date: {text}")

    month_name = match.group(1).title()
    months = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12",
    }
    if month_name not in months:
        raise PipelineValidationError(f"{report_type} row {row_index + 2} has invalid month: {text}")

    iso_value = f"{match.group(3)}-{months[month_name]}-{match.group(2).zfill(2)}"
    validate_iso_date(iso_value, report_type, row_index, "start/end date")
    return iso_value


def validate_iso_date(value: str, report_type: str, row_index: int, field_name: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise PipelineValidationError(
            f"{report_type} row {row_index + 2} has invalid {field_name}: {value}"
        ) from exc


def coerce_bigquery_types(df: pd.DataFrame) -> pd.DataFrame:
    coerced = df.copy()
    for column in INTEGER_COLUMNS:
        coerced[column] = pd.to_numeric(coerced[column], errors="raise").astype("Int64")
    for column in FLOAT_COLUMNS:
        coerced[column] = pd.to_numeric(coerced[column], errors="raise").astype(float)
    for column in DATE_COLUMNS:
        coerced[column] = coerced[column].apply(to_date_or_none)
    for column in STRING_COLUMNS:
        coerced[column] = coerced[column].fillna("").astype(str)
    return coerced


def to_date_or_none(value: Any) -> date | None:
    text = string_value(value)
    if text == "":
        return None
    return datetime.strptime(text, "%Y-%m-%d").date()


def is_nan(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False

# =============================================================================
# main.py (entry point)
# =============================================================================


@functions_framework.cloud_event
def process_zomato_ads_zip(cloud_event: Any) -> None:
    data = cloud_event.data or {}
    bucket_name = data.get("bucket")
    object_name = data.get("name")

    if not bucket_name or not object_name:
        raise ValueError("CloudEvent missing bucket or object name")

    logger.info("Received GCS event for gs://%s/%s", bucket_name, object_name)
    
    if not is_landing_object_valid(bucket_name, object_name):
        return

    storage_client = get_storage_client()
    bigquery_client = get_bigquery_client()

    zip_bytes, source_generation = read_zip_from_gcs(storage_client, bucket_name, object_name)
    logger.info("Read ZIP from GCS: %s bytes", len(zip_bytes))

    restaurant_lookup = read_restaurant_lookup(bigquery_client)
    logger.info("Loaded %s restaurants from BigQuery lookup", len(restaurant_lookup))

    dataframe = transform_zip(zip_bytes, restaurant_lookup)
    logger.info("Transformed ZIP into %s merged rows", len(dataframe))

    loaded_rows = append_dataframe_to_bigquery(bigquery_client, dataframe)
    logger.info("BigQuery append verified: %s rows", loaded_rows)

    archive_name = archive_zip(storage_client, bucket_name, object_name, source_generation, len(zip_bytes))
    logger.info("Archive verified: gs://%s/%s", bucket_name, archive_name)

    delete_landing_zip(storage_client, bucket_name, object_name, source_generation)
    logger.info("Deleted landing ZIP: gs://%s/%s", bucket_name, object_name)
