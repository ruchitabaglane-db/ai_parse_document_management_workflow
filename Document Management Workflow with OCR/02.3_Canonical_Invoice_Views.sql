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

-- OCR Parse View: ocr_canonical_invoices_view
-- Header-level flattened view aligned to the updated schema (no legacy fields)
CREATE OR REPLACE VIEW ocr_canonical_invoices_view AS
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
FROM ocr_canonical_invoices;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION nz(s STRING)
RETURNS STRING
RETURN CASE
  WHEN s IS NULL OR length(trim(s)) = 0 OR lower(trim(s)) IN ('n/a','na') THEN NULL
  ELSE s
END;

-- COMMAND ----------

-- =========================================================================================
-- Merge OCR (baseline) + AI (fallback) into a canonical merged table
-- =========================================================================================
-- 1) Helper: treat blanks as NULL so AI can fill only when OCR is empty/missing
CREATE OR REPLACE FUNCTION nz(s STRING)
RETURNS STRING
RETURN CASE
  WHEN s IS NULL OR length(trim(s)) = 0 OR lower(trim(s)) IN ('n/a','na') THEN NULL
  ELSE s
END;

-- 2) Merge OCR (baseline) + AI (fallback) from FLATTENED inputs
CREATE OR REPLACE VIEW canonical_invoices_merged_table AS
WITH
  ocr AS (SELECT * FROM ocr_canonical_invoices_view),
  ai  AS (SELECT * FROM canonical_invoices_view),

  j AS (
    SELECT
      COALESCE(ocr.source_file_path, ai.source_file_path)     AS source_file_path,
      COALESCE(ocr.source_file_name, ai.source_file_name)     AS source_file_name,
      COALESCE(ocr.source_volume_path, ai.source_volume_path) AS source_volume_path,
      COALESCE(ocr.input, ai.input)                           AS input,

      -- AI-generated metadata (prefer AI summary, ensure document_type consistency)
      ocr.summary                  AS ocr_summary,             ai.summary                  AS ai_summary,
      ocr.document_type            AS ocr_document_type,       ai.document_type            AS ai_document_type,

      ocr.ingestion_timestamp    AS ocr_ingestion_timestamp,
      ocr.processing_timestamp   AS ocr_processing_timestamp,
      ai.ingestion_timestamp    AS ai_ingestion_timestamp,
      ai.processing_timestamp   AS ai_processing_timestamp,

      -- header scalars (flattened)
      ocr.invoiceNumber        AS ocr_invoiceNumber,        ai.invoiceNumber        AS ai_invoiceNumber,
      ocr.vendorName           AS ocr_vendorName,           ai.vendorName           AS ai_vendorName,
      ocr.orderNumber          AS ocr_orderNumber,          ai.orderNumber          AS ai_orderNumber,
      ocr.purchaseOrderNumber  AS ocr_purchaseOrderNumber,  ai.purchaseOrderNumber  AS ai_purchaseOrderNumber,
      ocr.bolNumber            AS ocr_bolNumber,            ai.bolNumber            AS ai_bolNumber,
      ocr.vendorNumber         AS ocr_vendorNumber,         ai.vendorNumber         AS ai_vendorNumber,
      ocr.branch               AS ocr_branch,               ai.branch               AS ai_branch,
      ocr.warehouse            AS ocr_warehouse,            ai.warehouse            AS ai_warehouse,
      ocr.invoiceDate          AS ocr_invoiceDate,          ai.invoiceDate          AS ai_invoiceDate,
      ocr.discountDate         AS ocr_discountDate,         ai.discountDate         AS ai_discountDate,

      -- money (fallback only when NULL)
      ocr.discountAmount       AS ocr_discountAmount,       ai.discountAmount       AS ai_discountAmount,
      ocr.freightAmount        AS ocr_freightAmount,        ai.freightAmount        AS ai_freightAmount,
      ocr.subtotalAmount       AS ocr_subtotalAmount,       ai.subtotalAmount       AS ai_subtotalAmount,
      ocr.totalAmount          AS ocr_totalAmount,          ai.totalAmount          AS ai_totalAmount,

      -- addresses (flattened pieces)
      ocr.shipping_address     AS ocr_ship_addr,            ai.shipping_address     AS ai_ship_addr,
      ocr.shipping_city        AS ocr_ship_city,            ai.shipping_city        AS ai_ship_city,
      ocr.shipping_state       AS ocr_ship_state,           ai.shipping_state       AS ai_ship_state,
      ocr.shipping_zip         AS ocr_ship_zip,             ai.shipping_zip         AS ai_ship_zip,

      ocr.remittance_address   AS ocr_remit_addr,           ai.remittance_address   AS ai_remit_addr,
      ocr.remittance_city      AS ocr_remit_city,           ai.remittance_city      AS ai_remit_city,
      ocr.remittance_state     AS ocr_remit_state,          ai.remittance_state     AS ai_remit_state,
      ocr.remittance_zip       AS ocr_remit_zip,            ai.remittance_zip       AS ai_remit_zip,

      -- terms (flattened)
      ocr.termsDiscountPercentage AS ocr_terms_disc_pct,    ai.termsDiscountPercentage AS ai_terms_disc_pct,
      ocr.termsDiscountDueDays    AS ocr_terms_disc_days,   ai.termsDiscountDueDays    AS ai_terms_disc_days,
      ocr.termsNetDuedays         AS ocr_terms_net_days,    ai.termsNetDuedays         AS ai_terms_net_days,

      -- arrays/misc in flattened views
      ocr.line_items_json      AS ocr_items_json,           ai.line_items_json      AS ai_items_json,
      ocr.line_items_count     AS ocr_items_count,          ai.line_items_count     AS ai_items_count,
      ocr.barcodes             AS ocr_barcodes,             ai.barcodes             AS ai_barcodes,

      ocr.flags                AS ocr_flags,                ai.flags                AS ai_flags,
      ocr.error                AS ocr_error,                ai.error                AS ai_error
    FROM ocr
    FULL OUTER JOIN ai
      ON (ocr.source_file_path <=> ai.source_file_path)
         OR (ocr.source_file_path IS NULL AND ai.source_file_path IS NULL
             AND ocr.source_file_name <=> ai.source_file_name)
  ),

  merged AS (
    SELECT
      input, source_file_path, source_file_name, source_volume_path,

      -- AI-generated metadata: prefer AI summary, ensure document_type consistency
      COALESCE(ai_summary, ocr_summary) AS summary,
      COALESCE(ai_document_type, ocr_document_type) AS document_type,

      -- Strings: prefer OCR; fallback to AI only when OCR is NULL/blank
      COALESCE(nz(ocr_invoiceNumber),       nz(ai_invoiceNumber))       AS invoiceNumber,
      COALESCE(nz(ocr_vendorName),          nz(ai_vendorName))          AS vendorName,
      COALESCE(nz(ocr_orderNumber),         nz(ai_orderNumber))         AS orderNumber,
      COALESCE(nz(ocr_purchaseOrderNumber), nz(ai_purchaseOrderNumber)) AS purchaseOrderNumber,
      COALESCE(nz(ocr_bolNumber),           nz(ai_bolNumber))           AS bolNumber,
      COALESCE(nz(ocr_vendorNumber),        nz(ai_vendorNumber))        AS vendorNumber,
      COALESCE(nz(ocr_branch),              nz(ai_branch))              AS branch,
      COALESCE(nz(ocr_warehouse),           nz(ai_warehouse))           AS warehouse,
      COALESCE(nz(ocr_invoiceDate),         nz(ai_invoiceDate))         AS invoiceDate,
      COALESCE(nz(ocr_discountDate),        nz(ai_discountDate))        AS discountDate,
      coalesce(nz(ocr_ingestion_timestamp), nz(ai_ingestion_timestamp)) AS ingestion_temistamp,
      coalesce(nz(ocr_processing_timestamp), nz(ai_processing_timestamp)) AS processing_temistamp,

      -- Numerics: fallback only when NULL
      CASE WHEN ocr_discountAmount IS NOT NULL THEN ocr_discountAmount ELSE ai_discountAmount END AS discountAmount,
      CASE WHEN ocr_freightAmount  IS NOT NULL THEN ocr_freightAmount  ELSE ai_freightAmount  END AS freightAmount,
      CASE WHEN ocr_subtotalAmount IS NOT NULL THEN ocr_subtotalAmount ELSE ai_subtotalAmount END AS subtotalAmount,
      CASE WHEN ocr_totalAmount    IS NOT NULL THEN ocr_totalAmount    ELSE ai_totalAmount    END AS totalAmount,

      -- shippingAddress: field-wise fallback; NULL struct if every part is NULL/blank
      CASE WHEN
        COALESCE(nz(ocr_ship_addr), nz(ai_ship_addr)) IS NOT NULL OR
        COALESCE(nz(ocr_ship_city), nz(ai_ship_city)) IS NOT NULL OR
        COALESCE(nz(ocr_ship_state),nz(ai_ship_state)) IS NOT NULL OR
        COALESCE(nz(ocr_ship_zip),  nz(ai_ship_zip))  IS NOT NULL
      THEN named_struct(
        'address', COALESCE(nz(ocr_ship_addr), nz(ai_ship_addr)),
        'city',    COALESCE(nz(ocr_ship_city), nz(ai_ship_city)),
        'state',   COALESCE(nz(ocr_ship_state),nz(ai_ship_state)),
        'zip',     COALESCE(nz(ocr_ship_zip),  nz(ai_ship_zip))
      ) ELSE NULL END AS shippingAddress,

      -- remittanceAddress
      CASE WHEN
        COALESCE(nz(ocr_remit_addr), nz(ai_remit_addr)) IS NOT NULL OR
        COALESCE(nz(ocr_remit_city), nz(ai_remit_city)) IS NOT NULL OR
        COALESCE(nz(ocr_remit_state),nz(ai_remit_state)) IS NOT NULL OR
        COALESCE(nz(ocr_remit_zip),  nz(ai_remit_zip))  IS NOT NULL
      THEN named_struct(
        'address', COALESCE(nz(ocr_remit_addr), nz(ai_remit_addr)),
        'city',    COALESCE(nz(ocr_remit_city), nz(ai_remit_city)),
        'state',   COALESCE(nz(ocr_remit_state),nz(ai_remit_state)),
        'zip',     COALESCE(nz(ocr_remit_zip),  nz(ai_remit_zip))
      ) ELSE NULL END AS remittanceAddress,

      -- paymentTerms: NULL the struct when all parts are NULL
      CASE WHEN
        COALESCE(ocr_terms_disc_pct,  ai_terms_disc_pct)  IS NOT NULL OR
        COALESCE(ocr_terms_disc_days, ai_terms_disc_days) IS NOT NULL OR
        COALESCE(ocr_terms_net_days,  ai_terms_net_days)  IS NOT NULL
      THEN named_struct(
        'termsDiscountPercentage', COALESCE(ocr_terms_disc_pct,  ai_terms_disc_pct),
        'termsDiscountDueDays',    COALESCE(ocr_terms_disc_days, ai_terms_disc_days),
        'termsNetDuedays',         COALESCE(ocr_terms_net_days,  ai_terms_net_days)
      ) ELSE NULL END AS paymentTerms,

      -- line_items: choose OCR if non-empty; else AI; else NULL (parse from JSON)
      CASE
        WHEN ocr_items_count IS NOT NULL AND ocr_items_count > 0 THEN
          from_json(ocr_items_json,
            'array<struct<lineNumber:int,productCode:string,uom:string,quantityShipped:double,weightShipped:double,unitPrice:double,lineTotal:double>>')
        WHEN ai_items_count IS NOT NULL AND ai_items_count > 0 THEN
          from_json(ai_items_json,
            'array<struct<lineNumber:int,productCode:string,uom:string,quantityShipped:double,weightShipped:double,unitPrice:double,lineTotal:double>>')
        ELSE NULL
      END AS line_items,

      -- barcodes: choose OCR if non-empty; else AI; else NULL
      CASE
        WHEN ocr_barcodes IS NOT NULL AND size(ocr_barcodes) > 0 THEN ocr_barcodes
        WHEN ai_barcodes  IS NOT NULL AND size(ai_barcodes)  > 0 THEN ai_barcodes
        ELSE NULL
      END AS barcodes,

      COALESCE(ocr_flags, ai_flags) AS flags,
      COALESCE(ocr_error, ai_error) AS error
    FROM j
  )

SELECT
  input,
  source_file_path,
  source_file_name,
  source_volume_path,
  ingestion_temistamp,
  processing_temistamp,
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
  ingestion_temistamp,
  processing_temistamp,
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
FROM canonical_invoices_merged_table;


-- COMMAND ----------

select * from canonical_invoices_final 

-- COMMAND ----------

