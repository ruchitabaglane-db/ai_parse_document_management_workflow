# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.table(
    name="KIE_ai_parse_endpoint_responses",
    table_properties={"delta.feature.variantType-preview": "supported"}
)
def kie_ai_parse_endpoint_responses():
    # Read from the source streaming table
    df = (
        spark.readStream.option("skipChangeCommits", "true").table("databricks_sandbox.development.raw_pdf_files")
    )
    # Rename columns and select relevant fields
    df = df.withColumnRenamed("text", "input")
    # Call the AI endpoint using ai_query function
    df = df.withColumn(
        "response",
        expr("ai_query('kie-a4ba368d-endpoint', input, failOnError => false)")
    )
    # Select and structure the output columns
    return (
        df.select(
            "input",
            col("path").alias("source_file_path"),
            "modificationTime",
            "ingestion_timestamp",
            # "raw_parsed",  # Uncomment if needed
            "summary",
            "document_type",
            expr("response.result").alias("response"),
            expr("response.errorMessage").alias("error")
        )
    )

# COMMAND ----------

# import dlt
# from pyspark.sql.functions import col, split, size, regexp_extract, current_timestamp, struct, when, to_json, from_json

# @dlt.table(
#     name="canonical_invoices",
#     comment="Canonical invoices table",
#     table_properties={
#         "delta.feature.variantType-preview": "supported"
#     }
# )
# def canonical_invoices():
#     df = (
#         spark.readStream.table("databricks_sandbox.development.KIE_ai_parse_endpoint_responses")
#         .filter((col("error").isNull()) & (col("document_type") == "invoice"))
#         .withColumn("source_file_name", when(
#             col("source_file_path").isNotNull(),
#             split(col("source_file_path"), "/")[size(split(col("source_file_path"), "/")) - 1]
#         ))
#         .withColumn("source_volume_path", when(
#             col("source_file_path").isNotNull(),
#             regexp_extract(col("source_file_path"), r"^(.*/)([^/]+)$", 1)
#         ))
#         .withColumnRenamed("response", "parsed_response")
#     )

#     # Define schemas for nested fields
#     address_schema = StructType([
#         StructField("address", StringType()),
#         StructField("city", StringType()),
#         StructField("state", StringType()),
#         StructField("zip", StringType())
#     ])
#     payment_terms_schema = StructType([
#         StructField("termsDiscountPercentage", StringType()),
#         StructField("termsDiscountDueDays", StringType()),
#         StructField("termsNetDuedays", StringType())
#     ])
#     invoice_header_schema = StructType([
#         StructField("invoiceNumber", StringType()),
#         StructField("vendorName", StringType()),
#         StructField("orderNumber", StringType()),
#         StructField("purchaseOrderNumber", StringType()),
#         StructField("bolNumber", StringType()),
#         StructField("vendorNumber", StringType()),
#         StructField("branch", StringType()),
#         StructField("warehouse", StringType()),
#         StructField("invoiceDate", StringType()),
#         StructField("discountDate", StringType()),
#         StructField("discountAmount", StringType()),
#         StructField("freightAmount", StringType()),
#         StructField("subtotalAmount", StringType()),
#         StructField("totalAmount", StringType()),
#         StructField("shippingAddress", address_schema),
#         StructField("remittanceAddress", address_schema),
#         StructField("paymentTerms", payment_terms_schema)
#     ])

#     # Parse invoiceHeader as struct
#     df = df.withColumn(
#         "invoice_header",
#         from_json(
#             to_json(col("parsed_response.invoiceHeader")),
#             invoice_header_schema
#         )
#     )

#     # Parse lineItems as array of struct
#     line_items_schema = "array<struct<lineNumber:int,productCode:string,uom:string,quantityShipped:double,weightShipped:double,unitPrice:double,lineTotal:double>>"
#     df = df.withColumn(
#         "line_items",
#         when(
#             col("parsed_response.lineItems").isNotNull(),
#             from_json(to_json(col("parsed_response.lineItems")), line_items_schema)
#         )
#     )

#     # Parse barcodes as array of string
#     df = df.withColumn(
#         "barcodes",
#         when(
#             col("parsed_response.barcodes").isNotNull(),
#             from_json(to_json(col("parsed_response.barcodes")), "array<string>")
#         )
#     )

#     return df.select(
#         "input",
#         "source_file_path",
#         "source_file_name",
#         "source_volume_path",
#         "modificationTime",
#         "ingestion_timestamp",
#         current_timestamp().alias("processing_timestamp"),
#         "summary",
#         "document_type",
#         "invoice_header",
#         "line_items",
#         col("parsed_response.flags").alias("flags"),
#         "barcodes",
#         "error"
#     )