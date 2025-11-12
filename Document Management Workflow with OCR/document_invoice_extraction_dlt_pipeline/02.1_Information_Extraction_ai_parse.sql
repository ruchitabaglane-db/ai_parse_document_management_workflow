-- Databricks notebook source
-- CREATE  STREAMING TABLE `databricks_sandbox`.`development`.`KIE_ai_parse_endpoint_responses` 
--   TBLPROPERTIES (
--     'delta.feature.variantType-preview' = 'supported'
--   )
--   AS
--   WITH query_results AS (
--     SELECT
--       `text` AS input,
--       path AS source_file_path,
--       modificationTime,
--       ingestion_timestamp,
--     --  raw_parsed,
--       summary,
--       document_type,
--       ai_query(
--         'kie-a4ba368d-endpoint',
--         input,
--         failOnError => false
--       ) AS response
--     FROM STREAM(`databricks_sandbox`.`development`.`raw_pdf_files`)
--   )
--   SELECT
--     input,
--     source_file_path,
--     modificationTime,
--     ingestion_timestamp,
--   --  raw_parsed,
--     summary,
--     document_type,
--     response.result AS response,
--     response.errorMessage AS error
--   FROM query_results;

-- COMMAND ----------

-- Create canonical_invoices table first (matches the structure expected by the view)
CREATE  STREAMING TABLE databricks_sandbox.development.canonical_invoices
  (
    input STRING,
    source_file_path STRING, 
    source_file_name STRING,
    source_volume_path STRING,
    modificationTime TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP,
    
    -- AI-generated metadata from upstream processing
    summary STRING,
    document_type STRING,
    
    -- Main invoice structure matching the current view expectations
    invoice_header STRUCT<
      invoiceNumber: STRING,
      vendorName: STRING,
      orderNumber: STRING,
      purchaseOrderNumber: STRING,
      bolNumber: STRING,
      vendorNumber: STRING,
      branch: STRING,
      warehouse: STRING,
      invoiceDate: STRING,
      discountDate: STRING,
      discountAmount: DOUBLE,
      freightAmount: DOUBLE,
      subtotalAmount: DOUBLE,
      totalAmount: DOUBLE,
      shippingAddress: STRUCT<address: STRING, city: STRING, state: STRING, zip: STRING>,
      remittanceAddress: STRUCT<address: STRING, city: STRING, state: STRING, zip: STRING>,
      paymentTerms: STRUCT<termsDiscountPercentage: DOUBLE, termsDiscountDueDays: INT, termsNetDuedays: INT>
    >,
    
    line_items ARRAY<STRUCT<
      lineNumber: INT,
      productCode: STRING,
      uom: STRING,
      quantityShipped: DOUBLE,
      weightShipped: DOUBLE,
      unitPrice: DOUBLE,
      lineTotal: DOUBLE
    >>,
    
    flags STRING,
    barcodes ARRAY<STRING>,
    error STRING
  )
  TBLPROPERTIES ('delta.feature.variantType-preview' = 'supported') AS

WITH parsed_responses AS (
  SELECT
    input,
    source_file_path,
    modificationTime,
    ingestion_timestamp,
    summary,
    document_type,
    
    -- Extract file name and volume path (matching original logic)
    CASE WHEN source_file_path IS NOT NULL THEN split(source_file_path, '/')[size(split(source_file_path, '/')) - 1] END AS source_file_name,
    CASE WHEN source_file_path IS NOT NULL THEN regexp_extract(source_file_path, '^(.*/)([^/]+)$', 1) END AS source_volume_path,
    
    -- Use response as parsed_response (matching original variable name)
    response AS parsed_response,
    error
    
  FROM STREAM(`databricks_sandbox`.`development`.`KIE_ai_parse_endpoint_responses`)
  WHERE error IS NULL  -- Only process successful extractions
    AND document_type = 'invoice'  -- Only process invoice documents
)

SELECT
  input,
  source_file_path,
  source_file_name,
  source_volume_path,
  modificationTime,
  ingestion_timestamp,
  current_timestamp() AS processing_timestamp,
  
  -- AI-generated metadata from upstream processing
  summary,
  document_type,

  /* -------- invoiceHeader (matches updated schema) -------- */
  struct(
    parsed_response:invoiceHeader:orderNumber::string           AS orderNumber,
    parsed_response:invoiceHeader:bolNumber::string             AS bolNumber,
    parsed_response:invoiceHeader:vendorNumber::string          AS vendorNumber,
    parsed_response:invoiceHeader:vendorName::string            AS vendorName,
    parsed_response:invoiceHeader:invoiceNumber::string         AS invoiceNumber,
    parsed_response:invoiceHeader:invoiceDate::string           AS invoiceDate,
    parsed_response:invoiceHeader:purchaseOrderNumber::string   AS purchaseOrderNumber,
    parsed_response:invoiceHeader:branch::string                AS branch,
    parsed_response:invoiceHeader:warehouse::string             AS warehouse,
    parsed_response:invoiceHeader:discountAmount::double        AS discountAmount,
    parsed_response:invoiceHeader:discountDate::string          AS discountDate,
    parsed_response:invoiceHeader:freightAmount::double         AS freightAmount,
    parsed_response:invoiceHeader:subtotalAmount::double        AS subtotalAmount,
    parsed_response:invoiceHeader:totalAmount::double           AS totalAmount,

    CASE WHEN parsed_response:invoiceHeader:shippingAddress IS NOT NULL THEN
      struct(
        parsed_response:invoiceHeader:shippingAddress:address::string AS address,
        parsed_response:invoiceHeader:shippingAddress:city::string    AS city,
        parsed_response:invoiceHeader:shippingAddress:state::string   AS state,
        parsed_response:invoiceHeader:shippingAddress:zip::string     AS zip
      )
    ELSE NULL END AS shippingAddress,

    CASE WHEN parsed_response:invoiceHeader:remittanceAddress IS NOT NULL THEN
      struct(
        parsed_response:invoiceHeader:remittanceAddress:address::string AS address,
        parsed_response:invoiceHeader:remittanceAddress:city::string    AS city,
        parsed_response:invoiceHeader:remittanceAddress:state::string   AS state,
        parsed_response:invoiceHeader:remittanceAddress:zip::string     AS zip
      )
    ELSE NULL END AS remittanceAddress,

    /* NOTE: schema uses termsNetDuedays (spelling preserved) */
    CASE WHEN parsed_response:invoiceHeader:paymentTerms IS NOT NULL THEN
      struct(
        parsed_response:invoiceHeader:paymentTerms:termsDiscountPercentage::double AS termsDiscountPercentage,
        parsed_response:invoiceHeader:paymentTerms:termsDiscountDueDays::int       AS termsDiscountDueDays,
        parsed_response:invoiceHeader:paymentTerms:termsNetDuedays::int            AS termsNetDuedays
      )
    ELSE NULL END AS paymentTerms
  ) AS invoice_header,

  /* -------- lineItems (typed to new schema) -------- */
  CASE WHEN parsed_response:lineItems IS NOT NULL THEN
    from_json(
      to_json(parsed_response:lineItems),
      'array<struct<
         lineNumber:int,
         productCode:string,
         uom:string,
         quantityShipped:double,
         weightShipped:double,
         unitPrice:double,
         lineTotal:double
       >>'
    )
  ELSE NULL END AS line_items,

  /* -------- flags & barcodes (top-level) -------- */
  parsed_response:flags::string AS flags,
  CASE WHEN parsed_response:barcodes IS NOT NULL THEN
    from_json(to_json(parsed_response:barcodes), 'array<string>')
  ELSE NULL END AS barcodes,

  /* keep error for pipeline observability */
  error

FROM parsed_responses;

-- COMMAND ----------

