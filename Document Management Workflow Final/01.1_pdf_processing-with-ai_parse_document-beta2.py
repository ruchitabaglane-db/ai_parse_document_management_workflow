# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Purpose
# MAGIC
# MAGIC * Ingest PDF Documents using databricks `ai_parse_document` UDF ([Document](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document?language=Python))
# MAGIC * Create markdown text for PDF content
# MAGIC * Convert table struction to dataframe and save as tables
# MAGIC * **NEW: Support for incremental processing using file modification timestamps**

# COMMAND ----------

# MAGIC %md
# MAGIC use base env v3 here for serverless !

# COMMAND ----------

# MAGIC %pip install databricks-sdk -U -q
# MAGIC %pip install tiktoken markdown_frames[pyspark] mdpd -q
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Env

# COMMAND ----------

dbutils.widgets.text("catalog", "databricks_sandbox")
dbutils.widgets.text("schema", "development")
dbutils.widgets.text("volume", "document-intake")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

print(f"Use Unit Catalog: {catalog}")
print(f"Use Schema: {schema}")
print(f"Use Volume: {volume}")

# COMMAND ----------

ai_parse_pdf_pipeline_config = {
    "source_path": f"/Volumes/{catalog}/{schema}/{volume}/",
    "document_intake_table_name": f"{catalog}.{schema}.document_intake",
    "raw_pdf_files_table_name": f"{catalog}.{schema}.raw_pdf_files",
    "parsed_pdf_pages_table_name": f"{catalog}.{schema}.parsed_pdf_pages",
    "parsed_pdf_tables_table_name": f"{catalog}.{schema}.parsed_pdf_tables",
    "enriched_parsed_pdf_pages_table_name": f"{catalog}.{schema}.enriched_parsed_pages"
}
print(ai_parse_pdf_pipeline_config)

# COMMAND ----------

# Configuration tables setup - tables will be created if they don't exist
print("Pipeline configuration loaded successfully")
print("Tables will be created automatically if they don't exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: perform `ai_parse_document` UDF to ingest PDF documents
# MAGIC
# MAGIC ### Step 1: Document Intake (Staging)
# MAGIC ### Step 2: AI Processing (Bronze Layer)

# COMMAND ----------

## Incremental Processing Configuration

from datetime import datetime, timedelta

# Set the baseline cutoff date (Sep 1, 2025)
baseline_cutoff_date = "2025-09-01T00:00:00Z"

# Check if tables exist for incremental processing
intake_table_exists = spark.catalog.tableExists(ai_parse_pdf_pipeline_config['document_intake_table_name'])
raw_table_exists = spark.catalog.tableExists(ai_parse_pdf_pipeline_config['raw_pdf_files_table_name'])

if intake_table_exists:
    print("=== Incremental Mode ===")
    print("Document intake table exists - will add only NEW files")
    
    # Get the latest ingestion timestamp to find new files
    latest_ingestion = spark.sql(f"""
        SELECT COALESCE(MAX(ingestion_timestamp), timestamp('{baseline_cutoff_date}')) as latest_ingestion
        FROM {ai_parse_pdf_pipeline_config['document_intake_table_name']}
    """).collect()[0]['latest_ingestion']
    
    print(f"Latest ingestion timestamp: {latest_ingestion}")
    incremental_mode = True
    cutoff_for_intake = latest_ingestion.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
else:
    print("=== Initial Load Mode ===") 
    print("Document intake table doesn't exist - processing from baseline")
    print(f"Baseline cutoff date: {baseline_cutoff_date}")
    incremental_mode = False
    cutoff_for_intake = baseline_cutoff_date

print(f"Will use cutoff for file intake: {cutoff_for_intake}")
print(f"Intake table exists: {intake_table_exists}")
print(f"Raw table exists: {raw_table_exists}")

# COMMAND ----------

# Step 1: Create document intake table with NEW files only
print("=== Step 1: Document Intake - Add New Files ===")

# Use dynamic cutoff - baseline for initial load, latest ingestion for incremental
read_files_call = f"READ_FILES('{ai_parse_pdf_pipeline_config['source_path']}', format => 'binaryFile', modifiedAfter => '{cutoff_for_intake}')"

# First check if there are any new files
check_files_sql = f"SELECT COUNT(*) as file_count FROM {read_files_call}"
try:
    file_count = spark.sql(check_files_sql).collect()[0]['file_count']
except:
    file_count = 0

print(f"Using cutoff: {cutoff_for_intake}")
print(f"Processing {file_count} new files")

if file_count == 0:
    print("No new files found to process")
    if not incremental_mode:
        # For initial load, still create the table structure
        spark.sql(f"""
        CREATE OR REPLACE TABLE {ai_parse_pdf_pipeline_config['document_intake_table_name']} (
            path STRING,
            content BINARY,
            modificationTime TIMESTAMP,
            length BIGINT,
            ingestion_timestamp TIMESTAMP
        )
        """)
        print("Created empty intake table structure")
else:
    intake_sql_script = f"""
    {'CREATE OR REPLACE TABLE' if not incremental_mode else 'INSERT INTO'} {ai_parse_pdf_pipeline_config['document_intake_table_name']} 
    {'AS' if not incremental_mode else ''}
    SELECT
      path,
      content,
      modificationTime,
      length,
      current_timestamp() as ingestion_timestamp
    FROM
      {read_files_call}
    ORDER BY
      path ASC
    """

    print(f"Intake mode: {'CREATE OR REPLACE' if not incremental_mode else 'INSERT INTO'}")
    print(f"Using cutoff: {cutoff_for_intake}")
    print(f"Processing {file_count} new files")

    result = spark.sql(intake_sql_script)
    print(f"Document intake table updated successfully")

# Show intake summary
if spark.catalog.tableExists(ai_parse_pdf_pipeline_config['document_intake_table_name']):
    intake_count = spark.table(ai_parse_pdf_pipeline_config['document_intake_table_name']).count()
    print(f"Total files now in intake table: {intake_count}")

    # Check for new files added in this run
    if incremental_mode:
        new_files_count = spark.sql(f"""
            SELECT COUNT(*) as new_count 
            FROM {ai_parse_pdf_pipeline_config['document_intake_table_name']}
            WHERE ingestion_timestamp >= current_timestamp() - INTERVAL 1 MINUTE
        """).collect()[0]['new_count']
        print(f"New files added in this run: {new_files_count}")
    else:
        print("Initial load - all files are new")
else:
    print("Intake table does not exist")

# COMMAND ----------

# Step 2: Process NEW documents from intake table - ONE DAY AT A TIME
print("=== Step 2: Daily Processing of New Documents ===")

from pyspark.sql import functions as F

# Find unprocessed files in intake table
# Identify files that are in intake but not yet in raw_pdf_files
if raw_table_exists and incremental_mode:
    unprocessed_files_sql = f"""
    SELECT DISTINCT DATE(i.modificationTime) as process_date
    FROM {ai_parse_pdf_pipeline_config['document_intake_table_name']} i
    LEFT JOIN {ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']} r 
        ON i.path = r.path AND i.modificationTime = r.modificationTime
    WHERE r.path IS NULL
    ORDER BY process_date
    """
    
    unprocessed_dates = [row['process_date'] for row in spark.sql(unprocessed_files_sql).collect()]
    print(f"Found {len(unprocessed_dates)} dates with unprocessed files: {unprocessed_dates}")
else:
    # Initial load - get all dates from intake table
    all_dates_sql = f"""
    SELECT DISTINCT DATE(modificationTime) as process_date
    FROM {ai_parse_pdf_pipeline_config['document_intake_table_name']}
    ORDER BY process_date
    """
    unprocessed_dates = [row['process_date'] for row in spark.sql(all_dates_sql).collect()]
    print(f"Initial load - processing {len(unprocessed_dates)} dates: {unprocessed_dates}")


# COMMAND ----------

# fetching task parameter values(five day unprocessed dates) for testing
 
import datetime
import json

# unprocessed_dates = [
#     datetime.date(2025, 9, 12),
#     datetime.date(2025, 9, 13)
# ]

# Get the parameter string
param = dbutils.widgets.get("unprocessed_dates")
print("Raw param:", param)
# Raw param: ["2025-09-20", "2025-09-21", "2025-09-22"]

# Convert from string → list
unprocessed_dates = json.loads(param)

# Convert to datetime.date
unprocessed_dates = [datetime.date.fromisoformat(d) for d in unprocessed_dates]

#print(unprocessed_dates)

# COMMAND ----------

parse_extensions = ['.pdf', '.jpg', '.jpeg', '.png']
parse_extensions_sql = ', '.join([f"'{ext}'" for ext in parse_extensions])

# Determine if we need to create table for the first time
should_create_table = not raw_table_exists

# Process each day one at a time
total_processed = 0
for i, process_date in enumerate(unprocessed_dates, 1):
    print(f"\\n--- Processing Day {i}/{len(unprocessed_dates)}: {process_date} ---")
    
    # Get files for this specific date - ONLY PDF FILES
    daily_files_condition = f"DATE(modificationTime) = '{process_date}' AND lower(path) LIKE '%.pdf'"
    
    if raw_table_exists and incremental_mode:
        # Only process files not already in raw_pdf_files
        files_to_process_sql = f"""
        SELECT path, content, modificationTime, ingestion_timestamp
        FROM {ai_parse_pdf_pipeline_config['document_intake_table_name']} i
        WHERE {daily_files_condition}
          AND NOT EXISTS (
              SELECT 1 FROM {ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']} r 
              WHERE i.path = r.path AND i.modificationTime = r.modificationTime
          )
        """
    else:
        # Initial load - process all files for the date
        files_to_process_sql = f"""
        SELECT path, content, modificationTime, ingestion_timestamp
        FROM {ai_parse_pdf_pipeline_config['document_intake_table_name']}
        WHERE {daily_files_condition}
        """
    
    # Create temporary view for this day's files
    spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW daily_files AS 
    {files_to_process_sql}
    """)
    
    daily_count = spark.sql("SELECT COUNT(*) as cnt FROM daily_files").collect()[0]['cnt']
    
    if daily_count == 0:
        print(f"No files to process for {process_date}")
        continue
        
    print(f"Processing {daily_count} files for {process_date}")
    
    # Simple logic: CREATE TABLE only for the very first day, then INSERT INTO for all subsequent days
    if should_create_table:
        operation = "CREATE OR REPLACE TABLE"
        as_clause = "AS"
        should_create_table = False  # Only create once
        print("Creating new table")
    else:
        operation = "INSERT INTO"
        as_clause = ""
        print("Inserting into existing table")
    
    # Process this day's files - UPDATED WITH AI FUNCTIONS
    daily_parse_sql = f"""
    {operation} {ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']} 
    {as_clause}
    WITH all_files AS (
        SELECT path, content, modificationTime, ingestion_timestamp
        FROM daily_files
        ORDER BY path ASC
    ),
    parsed_documents AS (
        SELECT
          path, modificationTime, ingestion_timestamp,
          ai_parse_document(
              content,
              map('version', '2.0') --changed as per beta 2                                                                    
            ) as parsed
        FROM all_files
        WHERE lower(regexp_extract(path, '(\\\\.[^.]+)$', 1)) IN ({parse_extensions_sql})
    ),
    raw_documents AS (
        SELECT
          path, modificationTime, ingestion_timestamp,
          null as raw_parsed,
          decode(content, "utf-8") as text
        FROM all_files
        WHERE lower(regexp_extract(path, '(\\\\.[^.]+)$', 1)) NOT IN ({parse_extensions_sql})
    ),
    sorted_page_contents AS (
      SELECT
        path, modificationTime, ingestion_timestamp,
        page:content AS content
      FROM (
          SELECT path, modificationTime, ingestion_timestamp,
                 posexplode(try_cast(parsed:document:elements AS ARRAY<VARIANT>)) AS (page_idx, page) -- changed as per beta 2
          FROM parsed_documents
          WHERE parsed:document:elements IS NOT NULL                                                  -- changed as per beta 2
            AND CAST(parsed:error_status AS STRING) IS NULL
      ) ORDER BY page_idx
    ),
    concatenated AS (
        SELECT path, modificationTime, ingestion_timestamp,
               concat_ws('\\\\n\\\\n', collect_list(content)) AS full_content
        FROM sorted_page_contents
        GROUP BY path, modificationTime, ingestion_timestamp
    ),
    with_raw AS (
        SELECT a.path, a.modificationTime, a.ingestion_timestamp,
               b.parsed as raw_parsed, a.full_content as text
        FROM concatenated a
        JOIN parsed_documents b ON a.path = b.path AND a.modificationTime = b.modificationTime
    ),
    all_processed AS (
        SELECT * FROM with_raw
        UNION ALL 
        SELECT * FROM raw_documents
    ),
    with_ai_analysis AS (
        SELECT 
            path,
            modificationTime, 
            ingestion_timestamp,
            raw_parsed,
            text,
            -- Add AI-generated summary
            CASE 
                WHEN text IS NOT NULL AND length(text) > 100 THEN
                    ai_query('databricks-claude-3-7-sonnet', 
                             CONCAT('Summarize this document in 2-3 sentences: ', LEFT(text, 2000)))
                ELSE 'No summary available'
            END AS summary,
            -- Add AI-generated document type classification
            CASE 
                WHEN text IS NOT NULL AND length(text) > 100 THEN
                    ai_classify(text, ARRAY('invoice', 'receipt', 'purchase_order', 'contract', 'other'))
                ELSE 'unknown'
            END AS document_type
        FROM all_processed
    )
    SELECT * FROM with_ai_analysis;
    """
    
    spark.sql(daily_parse_sql)
    total_processed += daily_count
    print(f"✓ Processed {daily_count} files for {process_date}")

print(f"\\n=== Daily Processing Complete ===")
print(f"Total files processed across all days: {total_processed}")

# COMMAND ----------

# Load the processed data for silver layer processing
# Separate invoice and non-invoice documents with proper incremental handling

print("=== Step 3: Organizing Documents by Type ===")

# Check if other table exists for incremental processing
other_table_exists = spark.catalog.tableExists(f"{ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']}_other")

# Handle non-invoice documents incrementally
spark.sql(f"""
{'CREATE OR REPLACE TABLE' if not other_table_exists else 'INSERT INTO'} {ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']}_other 
{'AS' if not other_table_exists else ''}
SELECT * FROM {ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']}
WHERE document_type != 'invoice'
""")

# Now remove non-invoice documents from main table to keep only invoices
spark.sql(f"""      
DELETE FROM {ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']}
WHERE document_type != 'invoice'
""")

# Count documents after separation
df_raw = spark.table(ai_parse_pdf_pipeline_config['raw_pdf_files_table_name'])
invoice_count = df_raw.count()
other_docs_count = spark.table(f"{ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']}_other").count()

print(f"Document separation complete:")
print(f"  - Invoice documents (main table): {invoice_count}")
print(f"  - Other documents (separate table): {other_docs_count}")
print(f"  - Other table mode: {'CREATE OR REPLACE' if not other_table_exists else 'INSERT INTO'}")

# Show document type breakdown in main table (should only be invoices now)
print(f"\nMain table document types:")
df_raw.groupBy("document_type").count().orderBy("count", ascending=False).display()

print(f"\nMain table ({ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']}) now contains ONLY invoice documents")
print(f"Other table ({ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']}_other) contains all non-invoice documents")

display(df_raw)

# COMMAND ----------

# Show processing summary
print("=== AI Parse Processing Summary ===")
print(f"Mode: {'Incremental' if incremental_mode else 'Full'}")
print(f"Baseline Cutoff Date: {baseline_cutoff_date}")

# Intake table stats
intake_count = spark.table(ai_parse_pdf_pipeline_config['document_intake_table_name']).count()
print(f"Total files in intake table: {intake_count}")

# Processing stats - only if raw table exists
if spark.catalog.tableExists(ai_parse_pdf_pipeline_config['raw_pdf_files_table_name']):
    df_raw_final = spark.table(ai_parse_pdf_pipeline_config['raw_pdf_files_table_name'])
    print(f"Raw files processed: {df_raw_final.count()}")
else:
    print("Raw files processed: 0 (table not created yet)")

# Show latest modification times processed from intake table
print("\\n=== Latest File Modification Times (from Intake) ===")
spark.table(ai_parse_pdf_pipeline_config['document_intake_table_name']) \
    .select("path", "modificationTime", "ingestion_timestamp") \
    .orderBy(F.desc("modificationTime")) \
    .display(5, truncate=False)

# Show file extension breakdown
print("\\n=== File Extension Breakdown ===")
spark.table(ai_parse_pdf_pipeline_config['document_intake_table_name']) \
    .select(F.regexp_extract("path", r'\\.([^.]+)$', 1).alias("extension")) \
    .groupBy("extension") \
    .count() \
    .orderBy(F.desc("count")) \
    .display(10)

print("\\n=== AI Parse Pipeline Completed Successfully ===")

# COMMAND ----------

