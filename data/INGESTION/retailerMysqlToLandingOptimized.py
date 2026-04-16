import datetime
import json
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession

# 1. INITIALIZATION
spark = SparkSession.builder \
    .appName("Retailer_MySQL_to_GCS_Integrated") \
    .getOrCreate()

# --- CONFIGURATION ---
GCS_BUCKET = "retailer-datalake-project-06042026"
BQ_PROJECT = "dataengineering-project-492505"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/" # Required for Spark-BigQuery connector
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/retailer_config.csv"

MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.131.205.144:3306/retailerdb?useSSL=true&trustServerCertificate=true&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Password@123"
}

storage_client = storage.Client()
bq_client = bigquery.Client()

# Global list to store logs (same as your original script)
log_entries = []

# 2. LOGGING FUNCTIONS (Integrated from original)

def log_event(event_type, message, table=None):
    """Log an event and store it in the log list"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def save_logs_to_gcs():
    """Save log_entries list to a JSON file in GCS"""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"  
    
    json_data = json.dumps(log_entries, indent=4)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")
    
    print(f"✅ Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    """Save log_entries to BigQuery using Spark (original logic)"""
    if log_entries:
        try:
            log_df = spark.createDataFrame(log_entries)
            log_df.write.format("bigquery") \
                .option("table", BQ_LOG_TABLE) \
                .option("temporaryGcsBucket", BQ_TEMP_PATH) \
                .mode("append") \
                .save()
            print("✅ Logs stored in BigQuery via Spark")
        except Exception as e:
            print(f"❌ Failed to save logs to BigQuery: {e}")

# 3. ETL HELPER FUNCTIONS

def get_latest_watermark(table_name):
    query = f"SELECT MAX(load_timestamp) as ts FROM `{BQ_AUDIT_TABLE}` WHERE tablename = '{table_name}' AND status = 'SUCCESS'"
    try:
        query_job = bq_client.query(query)
        result = list(query_job.result())
        if result and result[0].ts:
            return result[0].ts.strftime('%Y-%m-%d %H:%M:%S')
    except Exception: pass
    return "1900-01-01 00:00:00"

def archive_existing_data(table):
    """Moves existing .json files to archive."""
    bucket = storage_client.bucket(GCS_BUCKET)
    prefix = f"landing/retailer-db/{table}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    for blob in blobs:
        if blob.name.endswith(".json") and "/archive/" not in blob.name:
            filename = blob.name.split("/")[-1]
            try:
                date_part = filename.split("_")[-1].split(".")[0]
                day, month, year = date_part[:2], date_part[2:4], date_part[4:]
                archive_path = f"landing/retailer-db/archive/{table}/{year}/{month}/{day}/{filename}"
                
                bucket.copy_blob(blob, bucket, archive_path)
                blob.delete()
                log_event("INFO", f"Archived: {filename}", table)
            except Exception as e:
                log_event("WARN", f"Archive error for {filename}: {e}", table)

def write_as_single_json(df, table, today):
    """Writes Spark DF as a single clean .json file (prevents folder creation)."""
    bucket = storage_client.bucket(GCS_BUCKET)
    temp_path = f"temp_spark_output/{table}"
    final_name = f"landing/retailer-db/{table}/{table}_{today}.json"

    # Coalesce(1) ensures one part-file
    df.coalesce(1).write.mode("overwrite").json(f"gs://{GCS_BUCKET}/{temp_path}")

    blobs = list(bucket.list_blobs(prefix=f"{temp_path}/part-"))
    if blobs:
        bucket.copy_blob(blobs[0], bucket, final_name)
        log_event("SUCCESS", f"JSON file written to gs://{GCS_BUCKET}/{final_name}", table)

    # Cleanup temp folder
    temp_blobs = bucket.list_blobs(prefix=f"{temp_path}/")
    for b in temp_blobs:
        b.delete()

# 4. CORE PROCESSING
def process_table(config):
    table = config.get("tablename") 
    load_type = (config.get("loadtype") or "full load").lower()
    watermark_col = config.get("watermark")
    
    if not table: return

    try:
        archive_existing_data(table)
        
        last_watermark = None
        if load_type == "incremental" and watermark_col:
            last_watermark = get_latest_watermark(table)
            log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table)
            query = f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS tmp"
        else:
            query = f"(SELECT * FROM {table}) AS tmp"

        # Read from MySQL
        df = spark.read.format("jdbc").options(**MYSQL_CONFIG).option("dbtable", query).load()
        log_event("SUCCESS", f"Successfully extracted data from {table}", table)

        # Write Single File
        today = datetime.datetime.now().strftime('%d%m%Y')
        write_as_single_json(df, table, today)
        
        # Insert Audit Entry (original BigQuery write style)
        row_count = df.count()
        audit_df = spark.createDataFrame([
            (table, load_type, row_count, datetime.datetime.now(), "SUCCESS")], 
            ["tablename", "load_type", "record_count", "load_timestamp", "status"])

        audit_df.write.format("bigquery") \
            .option("table", BQ_AUDIT_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()

        log_event("SUCCESS", f"Audit log updated for {table}", table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table)

# 5. MAIN EXECUTION
if __name__ == "__main__":
    try:
        # Load and Normalize Config Column Names
        raw_config_df = spark.read.csv(CONFIG_FILE_PATH, header=True)
        for col_name in raw_config_df.columns:
            raw_config_df = raw_config_df.withColumnRenamed(col_name, col_name.lower().strip())
        
        active_tables = raw_config_df.filter("is_active = '1'").collect()
        log_event("INFO", "Successfully read the config file")

        for row in active_tables:
            process_table(row.asDict())

    except Exception as e:
        log_event("CRITICAL", f"Pipeline failure: {str(e)}")

    finally:
        # Final integrated log save actions
        save_logs_to_gcs()
        save_logs_to_bigquery()
        spark.stop()
        print("✅ Pipeline completed successfully!")