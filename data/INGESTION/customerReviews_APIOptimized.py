import datetime
import json
import requests
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession

# 1. INITIALIZATION
spark = SparkSession.builder \
    .appName("CustomerReviewsAPI_Optimized") \
    .getOrCreate()

# --- CONFIGURATION ---
GCS_BUCKET = "retailer-datalake-project-06042026"
API_URL = "https://67e51d5418194932a5849592.mockapi.io/retailer/reviews"

# BigQuery Logging Config (Consistent with other scripts)
BQ_PROJECT = "dataengineering-project-492505"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

storage_client = storage.Client()
bq_client = bigquery.Client()
log_entries = []

# 2. LOGGING & AUDIT FUNCTIONS

def log_event(event_type, message, table="customer_reviews"):
    """Standardized logging for GCS and BigQuery"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def save_logs_to_gcs():
    log_filename = f"api_review_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"
    json_data = json.dumps(log_entries, indent=4)
    bucket = storage_client.bucket(GCS_BUCKET)
    bucket.blob(log_filepath).upload_from_string(json_data, content_type="application/json")
    print(f"✅ Logs saved to GCS.")

def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery") \
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append").save()

# 3. HELPER FOR SINGLE FILE PARQUET
def write_as_single_parquet(df, today):
    """Writes Spark output as a single clean .parquet file instead of a folder"""
    bucket = storage_client.bucket(GCS_BUCKET)
    temp_dir = "temp_api_output"
    final_name = f"landing/customer_reviews/customer_reviews_{today}.parquet"

    # Write to temp folder (coalesce(1) ensures one part file)
    df.coalesce(1).write.mode("overwrite").parquet(f"gs://{GCS_BUCKET}/{temp_dir}")

    # Find the parquet part file and rename it
    blobs = list(bucket.list_blobs(prefix=f"{temp_dir}/part-"))
    if blobs:
        bucket.copy_blob(blobs[0], bucket, final_name)
        log_event("SUCCESS", f"Parquet file written: gs://{GCS_BUCKET}/{final_name}")

    # Cleanup temp
    for b in bucket.list_blobs(prefix=f"{temp_dir}/"):
        b.delete()

# 4. MAIN EXECUTION
if __name__ == "__main__":
    try:
        # Step 1: Fetch data from API
        log_event("INFO", f"Fetching data from API: {API_URL}")
        response = requests.get(API_URL)
        
        if response.status_code == 200:
            data = response.json()
            record_count = len(data)
            log_event("INFO", f"Successfully fetched {record_count} records.")
        else:
            raise Exception(f"API Failure. Status: {response.status_code}")

        # Step 2: Convert to Spark DataFrame (Parallel Processing starts here)
        # We convert the list of dicts directly to a Spark DF
        df = spark.createDataFrame(data)

        # Step 3: Write as Single Parquet File to GCS
        today = datetime.datetime.today().strftime('%Y%m%d')
        write_as_single_parquet(df, today)

        # Step 4: Audit Entry
        audit_df = spark.createDataFrame([
            ("customer_reviews", "Full Load", record_count, datetime.datetime.now(), "SUCCESS")], 
            ["tablename", "load_type", "record_count", "load_timestamp", "status"])

        audit_df.write.format("bigquery") \
            .option("table", BQ_AUDIT_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append").save()
        
        log_event("SUCCESS", "Audit log updated.")

    except Exception as e:
        log_event("ERROR", f"Pipeline failed: {str(e)}")

    finally:
        save_logs_to_gcs()
        save_logs_to_bigquery()
        spark.stop()
        print("✅ API Pipeline Finished.")