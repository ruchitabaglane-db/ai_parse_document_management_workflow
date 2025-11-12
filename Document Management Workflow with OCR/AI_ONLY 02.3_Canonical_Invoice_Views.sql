-- Databricks notebook source
use databricks_sandbox.development;

-- COMMAND ----------

-- =============================================================================
-- CREATE VIEW DEFINITIONS FOR AI PARSE AND OCR PROCESSING
-- =============================================================================

-- AI Parse View: canonical_invoices_view
-- Header-level flattened view aligned to the updated schema (no legacy fields)
CREATE OR REPLACE VIEW canonical_invoices_view AS
SELECT
  -- Audit
  input,
  source_file_path,
  source_file_name,
  source_volume_path,
  ingestion_timestamp,
  processing_timestamp,
  error,
  flags,

  -- AI-generated metadata from upstream processing
  summary,
  document_type,

  -- Header (simple scalars)
  invoice_header.invoiceNumber              AS invoiceNumber,
  invoice_header.vendorName                 AS vendorName,
  invoice_header.orderNumber                AS orderNumber,
  invoice_header.purchaseOrderNumber        AS purchaseOrderNumber,
  invoice_header.bolNumber                  AS bolNumber,
  invoice_header.vendorNumber               AS vendorNumber,
  invoice_header.branch                     AS branch,
  invoice_header.warehouse                  AS warehouse,

  -- Dates: prefer ISO, fallback to common US format
  CASE
    WHEN invoice_header.invoiceDate RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      THEN to_date(invoice_header.invoiceDate, 'yyyy-MM-dd')
    WHEN REGEXP_REPLACE(invoice_header.invoiceDate, '[^0-9/\\-\\s]', '') RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}$'
      THEN to_date(invoice_header.invoiceDate, 'MM/dd/yyyy')
    ELSE NULL
  END                                          AS invoiceDate,

  CASE
    WHEN invoice_header.discountDate RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      THEN to_date(invoice_header.discountDate, 'yyyy-MM-dd')
    WHEN REGEXP_REPLACE(invoice_header.discountDate, '[^0-9/\\-\\s]', '') RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}$'
      THEN to_date(invoice_header.discountDate, 'MM/dd/yyyy')
    ELSE NULL
  END                                          AS discountDate,

  -- Money
  invoice_header.discountAmount               AS discountAmount,
  invoice_header.freightAmount                AS freightAmount,
  invoice_header.subtotalAmount               AS subtotalAmount,
  invoice_header.totalAmount                  AS totalAmount,

  -- Addresses
  invoice_header.shippingAddress.address      AS shipping_address,
  invoice_header.shippingAddress.city         AS shipping_city,
  invoice_header.shippingAddress.state        AS shipping_state,
  invoice_header.shippingAddress.zip          AS shipping_zip,

  invoice_header.remittanceAddress.address    AS remittance_address,
  invoice_header.remittanceAddress.city       AS remittance_city,
  invoice_header.remittanceAddress.state      AS remittance_state,
  invoice_header.remittanceAddress.zip        AS remittance_zip,

  -- Terms (note the exact key names)
  invoice_header.paymentTerms.termsDiscountPercentage AS termsDiscountPercentage,
  invoice_header.paymentTerms.termsDiscountDueDays    AS termsDiscountDueDays,
  invoice_header.paymentTerms.termsNetDuedays         AS termsNetDuedays,

  -- Line items passthrough & counts
  to_json(line_items) as line_items_json,     -- Complete line items array as JSON string
  CASE WHEN line_items IS NOT NULL THEN size(line_items) ELSE 0 END as line_items_count, -- Number of line items

  -- Barcodes
  barcodes
FROM canonical_invoices;

-- COMMAND ----------

-- =========================================================================================
-- Merge OCR (baseline) + AI (fallback) into a canonical merged table
-- =========================================================================================

CREATE OR REPLACE VIEW canonical_invoices_flatten_view AS 
WITH merged AS (
    SELECT
      input, source_file_path, source_file_name, source_volume_path,

      -- AI-generated metadata: prefer AI summary, ensure document_type consistency
      summary,
      document_type,

      -- Strings: prefer OCR; fallback to AI only when OCR is NULL/blank
      invoiceNumber,
      vendorName,
      orderNumber,
      purchaseOrderNumber,
      bolNumber,
      vendorNumber,
      branch,
      warehouse,
      invoiceDate,
      discountDate,
      ingestion_timestamp,
      processing_timestamp,

      -- Numerics: fallback only when NULL
      discountAmount,
      freightAmount,
      subtotalAmount,
      totalAmount,

      -- shippingAddress: field-wise fallback; NULL struct if every part is NULL/blank
      named_struct(
        'address', shipping_address,
        'city',    shipping_city,
        'state',   shipping_state,
        'zip',     shipping_zip
      ) AS shippingAddress,

      -- remittanceAddress
      named_struct(
        'address', remittance_address,
        'city',    remittance_city,
        'state',   remittance_state,
        'zip',     remittance_zip
      ) AS remittanceAddress,

      -- paymentTerms: NULL the struct when all parts are NULL
      named_struct(
        'termsDiscountPercentage', termsDiscountPercentage,
        'termsDiscountDueDays',    termsDiscountDueDays,
        'termsNetDuedays',         termsNetDuedays
      ) AS paymentTerms,

      -- line_items: choose OCR if non-empty; else AI; else NULL (parse from JSON)
      from_json(line_items_json,
            'array<struct<lineNumber:int,productCode:string,uom:string,quantityShipped:double,weightShipped:double,unitPrice:double,lineTotal:double>>') AS line_items,

      -- barcodes: choose OCR if non-empty; else AI; else NULL
      barcodes,

      flags,
      error
    FROM canonical_invoices_view
  )

SELECT
  input,
  source_file_path,
  source_file_name,
  source_volume_path,
  ingestion_timestamp,
  processing_timestamp,
  -- AI-generated metadata from upstream processing
  summary,
  document_type,
  
  named_struct(
    'orderNumber',           orderNumber,
    'bolNumber',             bolNumber,
    'vendorNumber',          vendorNumber,
    'vendorName',            vendorName,
    'invoiceNumber',         invoiceNumber,
    'invoiceDate',           invoiceDate,
    'purchaseOrderNumber',   purchaseOrderNumber,
    'branch',                branch,
    'warehouse',             warehouse,
    'discountAmount',        discountAmount,
    'discountDate',          discountDate,
    'freightAmount',         freightAmount,
    'subtotalAmount',        subtotalAmount,
    'totalAmount',           totalAmount,
    'shippingAddress',       shippingAddress,
    'remittanceAddress',     remittanceAddress,
    'paymentTerms',          paymentTerms
  ) AS invoice_header,
  line_items,
  flags,
  barcodes,
  error
FROM merged;


-- COMMAND ----------

-- =========================================================================================
-- Flattened header view (dates normalized; same updated columns)
-- =========================================================================================
CREATE OR REPLACE view canonical_invoices_final AS
SELECT
  input,
  source_file_path,
  source_file_name,
  source_volume_path,
  ingestion_timestamp,
  processing_timestamp,
  error,
  flags,

  -- AI-generated metadata from upstream processing
  summary,
  document_type,

  invoice_header.invoiceNumber       AS invoiceNumber,
  invoice_header.vendorName          AS vendorName,
  invoice_header.orderNumber         AS orderNumber,
  invoice_header.purchaseOrderNumber AS purchaseOrderNumber,
  invoice_header.bolNumber           AS bolNumber,
  invoice_header.vendorNumber        AS vendorNumber,
  invoice_header.branch              AS branch,
  invoice_header.warehouse           AS warehouse,

  /* Normalize dates if they’re ISO or common US */
  CASE
    WHEN invoice_header.invoiceDate RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      THEN to_date(invoice_header.invoiceDate, 'yyyy-MM-dd')
    WHEN REGEXP_REPLACE(invoice_header.invoiceDate, '[^0-9/\\-\\s]', '') RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}$'
      THEN to_date(invoice_header.invoiceDate, 'MM/dd/yyyy')
    ELSE NULL
  END AS invoiceDate,

  CASE
    WHEN invoice_header.discountDate RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      THEN to_date(invoice_header.discountDate, 'yyyy-MM-dd')
    WHEN REGEXP_REPLACE(invoice_header.discountDate, '[^0-9/\\-\\s]', '') RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}$'
      THEN to_date(invoice_header.discountDate, 'MM/dd/yyyy')
    ELSE NULL
  END AS discountDate,

  invoice_header.discountAmount      AS discountAmount,
  invoice_header.freightAmount       AS freightAmount,
  invoice_header.subtotalAmount      AS subtotalAmount,
  invoice_header.totalAmount         AS totalAmount,

  invoice_header.shippingAddress.address   AS shipping_address,
  invoice_header.shippingAddress.city      AS shipping_city,
  invoice_header.shippingAddress.state     AS shipping_state,
  invoice_header.shippingAddress.zip       AS shipping_zip,

  invoice_header.remittanceAddress.address AS remittance_address,
  invoice_header.remittanceAddress.city    AS remittance_city,
  invoice_header.remittanceAddress.state   AS remittance_state,
  invoice_header.remittanceAddress.zip     AS remittance_zip,

  invoice_header.paymentTerms.termsDiscountPercentage AS termsDiscountPercentage,
  invoice_header.paymentTerms.termsDiscountDueDays    AS termsDiscountDueDays,
  invoice_header.paymentTerms.termsNetDuedays         AS termsNetDuedays,

  CASE WHEN line_items IS NOT NULL THEN size(line_items) ELSE 0 END AS line_items_count,
  CAST(
    line_items AS ARRAY<STRUCT<
      lineNumber:INT,
      productCode:STRING,
      uom:STRING,
      quantityShipped:DOUBLE,
      weightShipped:DOUBLE,
      unitPrice:DOUBLE,
      lineTotal:DOUBLE
    >>
  ) AS line_items_json,
  barcodes
FROM canonical_invoices_flatten_view


-- COMMAND ----------

select * from canonical_invoices_final 

-- COMMAND ----------

