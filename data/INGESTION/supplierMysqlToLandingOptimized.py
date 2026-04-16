import datetime
import json
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession

# 1. INITIALIZATION
# We use a single SparkSession for all operations
spark = SparkSession.builder \
    .appName("SupplierMySQLToLanding_Optimized") \
    .getOrCreate()

# --- CONFIGURATION ---
GCS_BUCKET = "retailer-datalake-project-06042026"
LANDING_BASE = "landing/supplier-db"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/supplier_config.csv"

# BigQuery Config
BQ_PROJECT = "dataengineering-project-492505"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

# MySQL Config (Specific to Supplier DB)
MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.131.133.150:3306/supplierdb?useSSL=true&trustServerCertificate=true&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Password@123"
}

storage_client = storage.Client()
bq_client = bigquery.Client()

# Original Logging list
log_entries = []

# 2. INTEGRATED LOGGING FUNCTIONS

def log_event(event_type, message, table=None):
    """Captures logs in memory for GCS and BigQuery export"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def save_logs_to_gcs():
    """Saves accumulated logs as a JSON file in GCS"""
    log_filename = f"supplier_pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"  
    
    json_data = json.dumps(log_entries, indent=4)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")
    print(f"✅ Logs saved to GCS: gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    """Saves log entries to BigQuery via Spark"""
    if log_entries:
        try:
            log_df = spark.createDataFrame(log_entries)
            log_df.write.format("bigquery") \
                .option("table", BQ_LOG_TABLE) \
                .option("temporaryGcsBucket", BQ_TEMP_PATH) \
                .mode("append") \
                .save()
            print("✅ Logs synced to BigQuery")
        except Exception as e:
            print(f"❌ BigQuery logging failed: {e}")

# 3. ETL HELPER FUNCTIONS

def get_latest_watermark(table_name):
    """Lookup the last successful load time for incremental loads"""
    query = f"SELECT MAX(load_timestamp) as ts FROM `{BQ_AUDIT_TABLE}` WHERE tablename = '{table_name}' AND status = 'SUCCESS'"
    try:
        query_job = bq_client.query(query)
        result = list(query_job.result())
        if result and result[0].ts:
            return result[0].ts.strftime('%Y-%m-%d %H:%M:%S')
    except Exception: pass
    return "1900-01-01 00:00:00"

def archive_existing_data(table):
    """Moves clean .json files to partitioned archive folders"""
    bucket = storage_client.bucket(GCS_BUCKET)
    prefix = f"{LANDING_BASE}/{table}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    for blob in blobs:
        if blob.name.endswith(".json") and "/archive/" not in blob.name:
            filename = blob.name.split("/")[-1]
            try:
                # Format: table_ddmmYYYY.json
                date_part = filename.split("_")[-1].split(".")[0]
                day, month, year = date_part[:2], date_part[2:4], date_part[4:]
                archive_path = f"{LANDING_BASE}/archive/{table}/{year}/{month}/{day}/{filename}"
                
                bucket.copy_blob(blob, bucket, archive_path)
                blob.delete()
                log_event("INFO", f"Archived: {filename}", table)
            except Exception as e:
                log_event("WARN", f"Archive failed for {filename}: {e}", table)

def write_as_single_json(df, table, today):
    """Fix for 'folders': Writes Spark output then renames to a clean file"""
    bucket = storage_client.bucket(GCS_BUCKET)
    temp_dir = f"temp_supplier_output/{table}"
    final_file_name = f"{LANDING_BASE}/{table}/{table}_{today}.json"

    # Coalesce(1) ensures a single part file is created
    df.coalesce(1).write.mode("overwrite").json(f"gs://{GCS_BUCKET}/{temp_dir}")

    # Identify and rename the Spark output part file
    blobs = list(bucket.list_blobs(prefix=f"{temp_dir}/part-"))
    if blobs:
        bucket.copy_blob(blobs[0], bucket, final_file_name)
        log_event("SUCCESS", f"File written: gs://{GCS_BUCKET}/{final_file_name}", table)

    # Cleanup Spark temp metadata
    temp_blobs = bucket.list_blobs(prefix=f"{temp_dir}/")
    for b in temp_blobs:
        b.delete()

# 4. CORE PROCESSING
def process_supplier_table(config):
    # Mapping based on typical supplier_config.csv headers
    table = config.get("tablename") 
    load_type = (config.get("loadtype") or "full load").lower()
    watermark_col = config.get("watermark")
    
    if not table: return

    try:
        # Step A: Archive old files
        archive_existing_data(table)
        
        # Step B: Incremental vs Full Load Logic
        if load_type == "incremental" and watermark_col:
            last_ts = get_latest_watermark(table)
            log_event("INFO", f"Starting Incremental Load from {last_ts}", table)
            query = f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_ts}') AS tmp"
        else:
            log_event("INFO", "Starting Full Load", table)
            query = f"(SELECT * FROM {table}) AS tmp"

        # Step C: Parallel Read from MySQL
        df = spark.read.format("jdbc").options(**MYSQL_CONFIG).option("dbtable", query).load()
        
        # Step D: Write Single Clean JSON (Memory Safe)
        today = datetime.datetime.now().strftime('%d%m%Y')
        write_as_single_json(df, table, today)
        
        # Step E: Audit Entry
        row_count = df.count()
        audit_df = spark.createDataFrame([
            (table, load_type, row_count, datetime.datetime.now(), "SUCCESS")], 
            ["tablename", "load_type", "record_count", "load_timestamp", "status"])

        audit_df.write.format("bigquery") \
            .option("table", BQ_AUDIT_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()

        log_event("SUCCESS", f"Audit log updated. Rows processed: {row_count}", table)

    except Exception as e:
        log_event("ERROR", f"Failed processing {table}: {str(e)}", table)

# 5. MAIN EXECUTION
if __name__ == "__main__":
    try:
        # Load and Clean Configuration Headers
        raw_config_df = spark.read.csv(CONFIG_FILE_PATH, header=True)
        for col in raw_config_df.columns:
            raw_config_df = raw_config_df.withColumnRenamed(col, col.lower().strip())
        
        active_tables = raw_config_df.filter("is_active = '1'").collect()
        log_event("INFO", f"Read {len(active_tables)} active tables from config.")

        for row in active_tables:
            process_supplier_table(row.asDict())

    except Exception as e:
        log_event("CRITICAL", f"Supplier pipeline failed: {str(e)}")

    finally:
        # Integrated log saving logic
        save_logs_to_gcs()
        save_logs_to_bigquery()
        spark.stop()
        print("✅ Supplier Pipeline Finished.")