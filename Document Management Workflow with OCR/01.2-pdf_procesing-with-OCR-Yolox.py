# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Purpose
# MAGIC
# MAGIC * Process PDF Documents from shared document intake table
# MAGIC   * Using OSS [Unstructured API](https://docs.unstructured.io/open-source/introduction/overview) for OCR + yolox
# MAGIC * Convert content into Markdown text
# MAGIC * Save to a table

# COMMAND ----------

# MAGIC %pip install -qqq markdownify==0.12.1 "unstructured[local-inference, all-docs]==0.14.4" unstructured-client==0.22.0 nltk==3.8.1 "pdfminer.six==20221105"
# MAGIC %pip install databricks-sdk -q
# MAGIC %pip install numpy==1.26.4 pandas==2.2.2

# COMMAND ----------

from typing import List

def install_apt_get_packages(package_list: List[str]):
    """
    Installs apt-get packages required by the parser.

    Parameters:
        package_list (str): A space-separated list of apt-get packages.
    """
    import subprocess

    num_workers = max(
        1, int(spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers"))
    )
    print(f"number of works: {num_workers}")

    packages_str = " ".join(package_list)
    command = f"sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/* && sudo apt-get clean && sudo apt-get update && sudo apt-get install {packages_str} -y"
    subprocess.check_output(command, shell=True)

    def run_command(iterator):
        for x in iterator:
            yield subprocess.check_output(command, shell=True)

    data = spark.sparkContext.parallelize(range(num_workers), num_workers)
    # Use mapPartitions to run command in each partition (worker)
    output = data.mapPartitions(run_command)
    try:
        output.collect()
        print(f"{package_list} libraries installed")
    except Exception as e:
        print(f"Couldn't install {package_list} on all nodes: {e}")
        raise e

# COMMAND ----------

# install OCR model to each worker
install_apt_get_packages(["poppler-utils", "tesseract-ocr"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Env

# COMMAND ----------

dbutils.widgets.text("catalog", "databricks_sandbox")
dbutils.widgets.text("schema", "development")
dbutils.widgets.text("volume", "raw_pdf_files")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

print(f"Use Unit Catalog: {catalog}")
print(f"Use Schema: {schema}")
print(f"Use Volume: {volume}")

# COMMAND ----------

ocr_pdf_pipeline_config = {
    "source_path": f"/Volumes/{catalog}/{schema}/{volume}/",
    "document_intake_table_name": f"{catalog}.{schema}.document_intake",  # Same as AI parse notebook
    "landing_pdf_files_table_name": f"{catalog}.{schema}.landing_ocr_pdf_files",
    "raw_pdf_pages_table_name": f"{catalog}.{schema}.raw_ocr_pdf_files",
    "checkpoint_path": f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints"
}
print(ocr_pdf_pipeline_config)

# COMMAND ----------

# Configuration setup - OCR pipeline uses shared document intake
print("OCR pipeline configuration loaded successfully")
print("Using shared document intake table from AI parse pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## OCR Processing Configuration

# COMMAND ----------

# OCR pipeline reads from shared document_intake table
# No direct file ingestion needed - files are managed by AI parse notebook

# COMMAND ----------

## Incremental Processing Configuration - Uses Same Document Intake Table

from datetime import datetime, timedelta

# Set the baseline cutoff date (Aug 22, 2025) - same as AI parse notebook
baseline_cutoff_date = "2025-09-01T00:00:00Z"

# Check if shared document intake table exists
intake_table_exists = spark.catalog.tableExists(ocr_pdf_pipeline_config['document_intake_table_name'])
ocr_table_exists = spark.catalog.tableExists(ocr_pdf_pipeline_config['raw_pdf_pages_table_name'])

print("=== OCR Pipeline - Using Shared Document Intake ===")
print(f"Shared document intake table: {ocr_pdf_pipeline_config['document_intake_table_name']}")

if not intake_table_exists:
    print("WARNING: Shared document intake table doesn't exist!")
    print("Please run 01.1-pdf_processing-with-ai_parse_document.ipynb first to create the intake table")
    raise Exception("Shared document intake table not found")

print(f"✓ Shared document intake table exists")
print(f"OCR processed table exists: {ocr_table_exists}")

# Get total files in shared intake table
intake_count = spark.table(ocr_pdf_pipeline_config['document_intake_table_name']).count()
print(f"Total files in shared intake table: {intake_count}")

# The intake table is managed by the AI parse notebook
# This OCR notebook will only process from the existing intake table
print("\\nNote: File intake is managed by 01.1-pdf_processing-with-ai_parse_document.ipynb")
print("This notebook processes from the existing shared document_intake table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Process PDFs from Shared Intake - ONE DAY AT A TIME
# MAGIC - Using unstructured IO OSS API `hi-res` option with OCR for bounding box and yolox for object detection
# MAGIC - Processing only files not yet processed by OCR pipeline

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col
import pandas as pd

PARSED_IMG_DIR = f"/Volumes/{catalog}/{schema}/{volume}/parsed_images"

@pandas_udf("string")
def process_pdf_bytes(contents: pd.Series) -> pd.Series:
    from unstructured.partition.pdf import partition_pdf
    from unstructured.partition.image import partition_image
    import pandas as pd
    import re
    import io
    from markdownify import markdownify as md

    def perform_partition(raw_doc_contents_bytes):
        pdf = io.BytesIO(raw_doc_contents_bytes)
        raw_elements = partition_pdf(
            file=pdf,
            infer_table_structure=True,
            lenguages=["eng"],
            strategy="hi_res",                                   
            extract_image_block_types=["Table"],
            extract_image_block_output_dir=PARSED_IMG_DIR,
        )

        text_content = ""
        for section in raw_elements:
            # Tables are parsed seperatly, add a \n to give the chunker a hint to split well.
            if section.category == "Table":
                if section.metadata is not None:
                    if section.metadata.text_as_html is not None:
                        # convert table to markdown
                        text_content += "\n" + md(section.metadata.text_as_html) + "\n"
                    else:
                        text_content += " " + section.text
                else:
                    text_content += " " + section.text
            # Other content often has too-aggresive splitting, merge the content
            else:
                text_content += " " + section.text        

        return text_content
    return pd.Series([perform_partition(content) for content in contents])

# COMMAND ----------

# Daily OCR Processing from Shared Document Intake
print("=== OCR Processing: ONE DAY AT A TIME ===")

from pyspark.sql import functions as F

# Find unprocessed files in shared intake table
# Identify PDF files that are in intake but not yet processed by OCR
if ocr_table_exists:
    unprocessed_files_sql = f"""
    SELECT DISTINCT DATE(i.modificationTime) as process_date
    FROM {ocr_pdf_pipeline_config['document_intake_table_name']} i
    LEFT JOIN {ocr_pdf_pipeline_config['raw_pdf_pages_table_name']} o 
        ON i.path = o.path AND i.modificationTime = o.modificationTime
    WHERE o.path IS NULL 
      AND lower(i.path) LIKE '%.pdf'
    ORDER BY process_date
    """
    
    unprocessed_dates = [row['process_date'] for row in spark.sql(unprocessed_files_sql).collect()]
    print(f"Found {len(unprocessed_dates)} dates with unprocessed PDF files for OCR: {unprocessed_dates}")
else:
    # Initial load - get all PDF dates from intake table
    all_pdf_dates_sql = f"""
    SELECT DISTINCT DATE(modificationTime) as process_date
    FROM {ocr_pdf_pipeline_config['document_intake_table_name']}
    WHERE lower(path) LIKE '%.pdf'
    ORDER BY process_date
    """
    unprocessed_dates = [row['process_date'] for row in spark.sql(all_pdf_dates_sql).collect()]
    print(f"Initial OCR load - processing {len(unprocessed_dates)} dates with PDFs: {unprocessed_dates}")

# COMMAND ----------

# hardcoded to one day for testing
 
import datetime
import json

# unprocessed_dates = [
#     datetime.date(2025, 9, 12)
# ]

# Get the parameter string
param = dbutils.widgets.get("unprocessed_dates")
print("Raw param:", param)

# Convert from string → list
unprocessed_dates = json.loads(param)

# Convert to datetime.date
unprocessed_dates = [datetime.date.fromisoformat(d) for d in unprocessed_dates]

# COMMAND ----------



# Determine if we need to create table for the first time
should_create_table = not ocr_table_exists

# Process each day one at a time
total_processed = 0
for i, process_date in enumerate(unprocessed_dates, 1):
    print(f"\\n--- OCR Processing Day {i}/{len(unprocessed_dates)}: {process_date} ---")
    
    # Get PDF files for this specific date that haven't been OCR processed
    daily_files_condition = f"DATE(i.modificationTime) = '{process_date}' AND lower(i.path) LIKE '%.pdf'"
    
    if ocr_table_exists:
        # Only process PDFs not already in OCR table
        files_to_process_sql = f"""
        SELECT i.path, i.content, i.modificationTime, i.ingestion_timestamp,
               r.summary, r.document_type
        FROM {ocr_pdf_pipeline_config['document_intake_table_name']} i
        LEFT JOIN databricks_sandbox.development.raw_pdf_files r ON i.path = r.path
        WHERE {daily_files_condition}
          AND NOT EXISTS (
              SELECT 1 FROM {ocr_pdf_pipeline_config['raw_pdf_pages_table_name']} o 
              WHERE i.path = o.path AND i.modificationTime = o.modificationTime
          )
        """
    else:
        # Initial load - process all PDFs for the date
        files_to_process_sql = f"""
        SELECT i.path, i.content, i.modificationTime, i.ingestion_timestamp,
               r.summary, r.document_type
        FROM {ocr_pdf_pipeline_config['document_intake_table_name']} i
        LEFT JOIN databricks_sandbox.development.raw_pdf_files r ON i.path = r.path
        WHERE {daily_files_condition}
        """
    
    # Get count of files to process for this date
    daily_count = spark.sql(f"SELECT COUNT(*) as cnt FROM ({files_to_process_sql}) t").collect()[0]['cnt']
    
    if daily_count == 0:
        print(f"No PDF files to OCR process for {process_date}")
        continue
        
    print(f"OCR processing {daily_count} PDF files for {process_date}")
    
    # Create DataFrame for this day's files
    df_daily = spark.sql(files_to_process_sql)
    
    # Apply OCR processing to this day's files
    df_daily_processed = df_daily.withColumn("text", process_pdf_bytes("content"))
    
    # Simple logic: CREATE TABLE only for the very first day, then APPEND for all subsequent days
    if should_create_table:
        write_mode = "overwrite"
        should_create_table = False  # Only create once
        print("Creating new OCR table")
    else:
        write_mode = "append"
        print("Appending to existing OCR table")
    
    df_daily_processed.write.format("delta").mode(write_mode).option(
        "overwriteSchema", "true" if write_mode == "overwrite" else "false"
    ).saveAsTable(ocr_pdf_pipeline_config['raw_pdf_pages_table_name'])
    
    total_processed += daily_count
    print(f"✓ OCR processed {daily_count} PDF files for {process_date}")
    
    # Update ocr_table_exists for subsequent iterations
    if not ocr_table_exists:
        ocr_table_exists = True

print(f"\\n=== OCR Daily Processing Complete ===")
print(f"Total PDF files OCR processed across all days: {total_processed}")

# COMMAND ----------

# Show OCR Processing Summary
print("=== OCR Processing Summary ===")
print(f"Shared document intake table: {ocr_pdf_pipeline_config['document_intake_table_name']}")
print(f"OCR output table: {ocr_pdf_pipeline_config['raw_pdf_pages_table_name']}")

# Get final counts
if spark.catalog.tableExists(ocr_pdf_pipeline_config['raw_pdf_pages_table_name']):
    df_ocr_final = spark.table(ocr_pdf_pipeline_config['raw_pdf_pages_table_name'])
    ocr_final_count = df_ocr_final.count()
    print(f"Total PDF files OCR processed: {ocr_final_count}")
    
    # Show latest processed files
    print("\\n=== Latest OCR Processed Files ===")
    df_ocr_final.select("path", "modificationTime", "ingestion_timestamp", "summary", "document_type") \
        .orderBy(F.desc("modificationTime")) \
        .show(5, truncate=False)
    
    print("\\n=== OCR Processed Data Sample ===")
    display(df_ocr_final.select("path", "text", "modificationTime", "summary", "document_type").limit(3))
else:
    print("No OCR processed files found")

print("\\n=== OCR Pipeline Completed Successfully ===")