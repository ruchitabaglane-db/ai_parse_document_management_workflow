-- Databricks notebook source
use databricks_sandbox.development;

-- COMMAND ----------

-- AI Parse View: canonical_invoices_final
-- Complete analytics-ready view with flattened fields, normalized dates, and properly typed line items
CREATE OR REPLACE TEMPORARY VIEW canonical_invoices_final AS
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

  -- Line items: count and properly typed array
  CASE WHEN line_items IS NOT NULL THEN size(line_items) ELSE 0 END as line_items_count,
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

  -- Barcodes
  barcodes
FROM canonical_invoices;

-- COMMAND ----------

select * from canonical_invoices_final 

-- COMMAND ----------

