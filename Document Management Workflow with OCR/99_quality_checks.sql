-- Databricks notebook source
-- MAGIC %md
-- MAGIC Dashboard link : https://e2-demo-field-eng.cloud.databricks.com/sql/dashboardsv3/01f068383dac14a3bb81fa5d9b84c4b0/pages/9715fbdb?o=1444828305810485

-- COMMAND ----------

use databricks_sandbox.development;

-- COMMAND ----------

-- ===============================================================================
-- COMPREHENSIVE UPDATED COMPARISON - ALL COLUMNS SYSTEMATICALLY (NEW SCHEMA)
-- Updated to work with new schema_sep.json structure and include AI metadata
-- ===============================================================================

WITH full_comparison AS (
  SELECT 
    COALESCE(ocr.source_file_name, ai.source_file_name) as file_name,
    COALESCE(ocr.ingestion_timestamp, ai.ingestion_timestamp) as ingestion_timestamp,
    COALESCE(ocr.processing_timestamp, ai.processing_timestamp) as processing_timestamp,
    
    -- =========================================================================
    -- AUDIT & TRACKING FIELDS COMPARISON
    -- =========================================================================
    CASE WHEN COALESCE(ocr.input, '') != COALESCE(ai.input, '') THEN 'DIFF' ELSE 'SAME' END as input_status,
    CASE WHEN COALESCE(ocr.source_file_path, '') != COALESCE(ai.source_file_path, '') THEN 'DIFF' ELSE 'SAME' END as source_file_path_status,
    CASE WHEN COALESCE(ocr.source_volume_path, '') != COALESCE(ai.source_volume_path, '') THEN 'DIFF' ELSE 'SAME' END as source_volume_path_status,
    CASE WHEN COALESCE(ocr.error, '') != COALESCE(ai.error, '') THEN 'DIFF' ELSE 'SAME' END as error_status,
--     CASE WHEN coalesce(ocr.ingestion_timestamp, '') != coalesce(ai.ingestion_timestamp) THEN 'DIFF' ELSE 'SAME' END as ingestion_timestamp_status,
--     CASE WHEN coalesce(ocr.processing_timestamp, '') != coalesce(ai.processing_timestamp) THEN 'DIFF' ELSE 'SAME' END as processing_timestamp_status,
    
    -- =========================================================================
    -- AI-GENERATED METADATA FIELDS COMPARISON (NEW)
    -- =========================================================================
    CASE WHEN COALESCE(ocr.summary, '') != COALESCE(ai.summary, '') THEN 'DIFF' ELSE 'SAME' END as summary_status,
    CASE WHEN COALESCE(ocr.document_type, '') != COALESCE(ai.document_type, '') THEN 'DIFF' ELSE 'SAME' END as document_type_status,
    
    -- =========================================================================
    -- NEW SCHEMA INVOICE HEADER FIELDS COMPARISON
    -- =========================================================================
    CASE WHEN COALESCE(ocr.orderNumber, '') != COALESCE(ai.orderNumber, '') THEN 'DIFF' ELSE 'SAME' END as orderNumber_status,
    CASE WHEN COALESCE(ocr.bolNumber, '') != COALESCE(ai.bolNumber, '') THEN 'DIFF' ELSE 'SAME' END as bolNumber_status,
    CASE WHEN COALESCE(ocr.vendorNumber, '') != COALESCE(ai.vendorNumber, '') THEN 'DIFF' ELSE 'SAME' END as vendorNumber_status,
    CASE WHEN COALESCE(ocr.vendorName, '') != COALESCE(ai.vendorName, '') THEN 'DIFF' ELSE 'SAME' END as vendorName_status,
    CASE WHEN COALESCE(ocr.invoiceNumber, '') != COALESCE(ai.invoiceNumber, '') THEN 'DIFF' ELSE 'SAME' END as invoiceNumber_status,
    CASE WHEN COALESCE(ocr.invoiceDate, DATE('1900-01-01')) != COALESCE(ai.invoiceDate, DATE('1900-01-01')) THEN 'DIFF' ELSE 'SAME' END as invoiceDate_status,
    CASE WHEN COALESCE(ocr.purchaseOrderNumber, '') != COALESCE(ai.purchaseOrderNumber, '') THEN 'DIFF' ELSE 'SAME' END as purchaseOrderNumber_status,
    CASE WHEN COALESCE(ocr.branch, '') != COALESCE(ai.branch, '') THEN 'DIFF' ELSE 'SAME' END as branch_status,
    CASE WHEN COALESCE(ocr.warehouse, '') != COALESCE(ai.warehouse, '') THEN 'DIFF' ELSE 'SAME' END as warehouse_status,
    CASE WHEN COALESCE(ocr.discountAmount, 0.0) != COALESCE(ai.discountAmount, 0.0) THEN 'DIFF' ELSE 'SAME' END as discountAmount_status,
    CASE WHEN COALESCE(ocr.discountDate, DATE('1900-01-01')) != COALESCE(ai.discountDate, DATE('1900-01-01')) THEN 'DIFF' ELSE 'SAME' END as discountDate_status,
    CASE WHEN COALESCE(ocr.freightAmount, 0.0) != COALESCE(ai.freightAmount, 0.0) THEN 'DIFF' ELSE 'SAME' END as freightAmount_status,
    CASE WHEN COALESCE(ocr.totalAmount, 0.0) != COALESCE(ai.totalAmount, 0.0) THEN 'DIFF' ELSE 'SAME' END as totalAmount_status,
    CASE WHEN COALESCE(ocr.subtotalAmount, 0.0) != COALESCE(ai.subtotalAmount, 0.0) THEN 'DIFF' ELSE 'SAME' END as subtotalAmount_status,
    
    -- =========================================================================
    -- SHIPPING & REMITTANCE ADDRESS COMPARISON (NEW SCHEMA)
    -- =========================================================================
    CASE WHEN COALESCE(ocr.shipping_address, '') != COALESCE(ai.shipping_address, '') THEN 'DIFF' ELSE 'SAME' END as shipping_address_status,
    CASE WHEN COALESCE(ocr.shipping_city, '') != COALESCE(ai.shipping_city, '') THEN 'DIFF' ELSE 'SAME' END as shipping_city_status,
    CASE WHEN COALESCE(ocr.shipping_state, '') != COALESCE(ai.shipping_state, '') THEN 'DIFF' ELSE 'SAME' END as shipping_state_status,
    CASE WHEN COALESCE(ocr.shipping_zip, '') != COALESCE(ai.shipping_zip, '') THEN 'DIFF' ELSE 'SAME' END as shipping_zip_status,
    CASE WHEN COALESCE(ocr.remittance_address, '') != COALESCE(ai.remittance_address, '') THEN 'DIFF' ELSE 'SAME' END as remittance_address_status,
    CASE WHEN COALESCE(ocr.remittance_city, '') != COALESCE(ai.remittance_city, '') THEN 'DIFF' ELSE 'SAME' END as remittance_city_status,
    CASE WHEN COALESCE(ocr.remittance_state, '') != COALESCE(ai.remittance_state, '') THEN 'DIFF' ELSE 'SAME' END as remittance_state_status,
    CASE WHEN COALESCE(ocr.remittance_zip, '') != COALESCE(ai.remittance_zip, '') THEN 'DIFF' ELSE 'SAME' END as remittance_zip_status,
    
    -- =========================================================================
    -- PAYMENT TERMS FIELDS COMPARISON (NEW SCHEMA)
    -- =========================================================================
    CASE WHEN COALESCE(ocr.termsDiscountPercentage, 0.0) != COALESCE(ai.termsDiscountPercentage, 0.0) THEN 'DIFF' ELSE 'SAME' END as termsDiscountPercentage_status,
    CASE WHEN COALESCE(ocr.termsDiscountDueDays, 0) != COALESCE(ai.termsDiscountDueDays, 0) THEN 'DIFF' ELSE 'SAME' END as termsDiscountDueDays_status,
    CASE WHEN COALESCE(ocr.termsNetDuedays, 0) != COALESCE(ai.termsNetDuedays, 0) THEN 'DIFF' ELSE 'SAME' END as termsNetDuedays_status,
    
    -- =========================================================================
    -- FLAGS AND BARCODES COMPARISON (NEW SCHEMA) - Fixed for Databricks
    -- =========================================================================
    CASE WHEN COALESCE(ocr.flags, '') != COALESCE(ai.flags, '') THEN 'DIFF' ELSE 'SAME' END as flags_status,
    CASE WHEN 
      CASE 
        WHEN ocr.barcodes IS NULL AND ai.barcodes IS NULL THEN 'SAME'
        WHEN ocr.barcodes IS NULL OR ai.barcodes IS NULL THEN 'DIFF'
        WHEN size(ocr.barcodes) != size(ai.barcodes) THEN 'DIFF'
        WHEN CAST(ocr.barcodes AS STRING) != CAST(ai.barcodes AS STRING) THEN 'DIFF'
        ELSE 'SAME'
      END = 'SAME' THEN 'SAME' ELSE 'DIFF' 
    END as barcodes_status,
    
    -- =========================================================================
    -- LINE ITEMS COMPARISON (NEW SCHEMA) - Fixed field name to line_items_json
    -- =========================================================================
    CASE WHEN COALESCE(ocr.line_items_json, '') != COALESCE(ai.line_items_json, '') THEN 'DIFF' ELSE 'SAME' END as line_items_json_status,
    CASE WHEN COALESCE(ocr.line_items_count, 0) != COALESCE(ai.line_items_count, 0) THEN 'DIFF' ELSE 'SAME' END as line_items_count_status,
    
    -- =========================================================================
    -- STORE ALL ACTUAL VALUES FOR DETAILED DIFFERENCE ANALYSIS
    -- =========================================================================
    -- Store OCR and AI values for all fields to enable detailed comparison
    ocr.input as ocr_input, ai.input as ai_input,
    ocr.source_file_path as ocr_source_file_path, ai.source_file_path as ai_source_file_path,
    ocr.source_volume_path as ocr_source_volume_path, ai.source_volume_path as ai_source_volume_path,
--     ocr.ingestion_timestamp as ocr_ingestion_timestamp,
--     ai.ingestion_timestamp as ai_ingestion_timestamp,
--     ocr.processing_timestamp as ocr_processing_timestamp,
--     ai.processing_timestamp as ai_processing_timestamp,
    ocr.error as ocr_error, ai.error as ai_error,
    
    -- AI-GENERATED METADATA FIELDS (NEW)
    ocr.summary as ocr_summary, ai.summary as ai_summary,
    ocr.document_type as ocr_document_type, ai.document_type as ai_document_type,
    
    -- NEW SCHEMA FIELDS
    ocr.orderNumber as ocr_orderNumber, ai.orderNumber as ai_orderNumber,
    ocr.bolNumber as ocr_bolNumber, ai.bolNumber as ai_bolNumber,
    ocr.vendorNumber as ocr_vendorNumber, ai.vendorNumber as ai_vendorNumber,
    ocr.vendorName as ocr_vendorName, ai.vendorName as ai_vendorName,
    ocr.invoiceNumber as ocr_invoiceNumber, ai.invoiceNumber as ai_invoiceNumber,
    ocr.invoiceDate as ocr_invoiceDate, ai.invoiceDate as ai_invoiceDate,
    ocr.purchaseOrderNumber as ocr_purchaseOrderNumber, ai.purchaseOrderNumber as ai_purchaseOrderNumber,
    ocr.branch as ocr_branch, ai.branch as ai_branch,
    ocr.warehouse as ocr_warehouse, ai.warehouse as ai_warehouse,
    ocr.discountAmount as ocr_discountAmount, ai.discountAmount as ai_discountAmount,
    ocr.discountDate as ocr_discountDate, ai.discountDate as ai_discountDate,
    ocr.freightAmount as ocr_freightAmount, ai.freightAmount as ai_freightAmount,
    ocr.totalAmount as ocr_totalAmount, ai.totalAmount as ai_totalAmount,
    ocr.subtotalAmount as ocr_subtotalAmount, ai.subtotalAmount as ai_subtotalAmount,
    
    ocr.shipping_address as ocr_shipping_address, ai.shipping_address as ai_shipping_address,
    ocr.shipping_city as ocr_shipping_city, ai.shipping_city as ai_shipping_city,
    ocr.shipping_state as ocr_shipping_state, ai.shipping_state as ai_shipping_state,
    ocr.shipping_zip as ocr_shipping_zip, ai.shipping_zip as ai_shipping_zip,
    ocr.remittance_address as ocr_remittance_address, ai.remittance_address as ai_remittance_address,
    ocr.remittance_city as ocr_remittance_city, ai.remittance_city as ai_remittance_city,
    ocr.remittance_state as ocr_remittance_state, ai.remittance_state as ai_remittance_state,
    ocr.remittance_zip as ocr_remittance_zip, ai.remittance_zip as ai_remittance_zip,
    
    ocr.termsDiscountPercentage as ocr_termsDiscountPercentage, ai.termsDiscountPercentage as ai_termsDiscountPercentage,
    ocr.termsDiscountDueDays as ocr_termsDiscountDueDays, ai.termsDiscountDueDays as ai_termsDiscountDueDays,
    ocr.termsNetDuedays as ocr_termsNetDuedays, ai.termsNetDuedays as ai_termsNetDuedays,
    
    ocr.flags as ocr_flags, ai.flags as ai_flags,
    ocr.barcodes as ocr_barcodes, ai.barcodes as ai_barcodes,
    ocr.line_items_json as ocr_line_items_json, ai.line_items_json as ai_line_items_json,
    ocr.line_items_count as ocr_line_items_count, ai.line_items_count as ai_line_items_count
    
  FROM ocr_canonical_invoices_view ocr
  FULL OUTER JOIN canonical_invoices_view ai 
    ON ocr.source_file_name = ai.source_file_name
),
differences_summary AS (
  SELECT 
    file_name,
    ingestion_timestamp,
    processing_timestamp,
    -- Count ALL differences across ALL NEW SCHEMA columns (including AI metadata)
    (CASE WHEN input_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN source_file_path_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN source_volume_path_status = 'DIFF' THEN 1 ELSE 0 END +
     -- CASE WHEN ingestion_timestamp_status = 'DIFF' THEN 1 ELSE 0 END +
     -- CASE WHEN processing_timestamp_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN error_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN summary_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN document_type_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN orderNumber_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN bolNumber_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN vendorNumber_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN vendorName_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN invoiceNumber_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN invoiceDate_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN purchaseOrderNumber_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN branch_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN warehouse_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN discountAmount_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN discountDate_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN freightAmount_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN totalAmount_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN subtotalAmount_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN shipping_address_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN shipping_city_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN shipping_state_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN shipping_zip_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN remittance_address_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN remittance_city_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN remittance_state_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN remittance_zip_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN termsDiscountPercentage_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN termsDiscountDueDays_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN termsNetDuedays_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN flags_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN barcodes_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN line_items_json_status = 'DIFF' THEN 1 ELSE 0 END +
     CASE WHEN line_items_count_status = 'DIFF' THEN 1 ELSE 0 END) as total_differences,
    
    -- Create comprehensive comma-separated list of ALL different columns (NEW SCHEMA + AI metadata)
    CONCAT_WS(', ',
      CASE WHEN input_status = 'DIFF' THEN 'input' END,
      CASE WHEN source_file_path_status = 'DIFF' THEN 'source_file_path' END,
      CASE WHEN source_volume_path_status = 'DIFF' THEN 'source_volume_path' END,
     --  CASE WHEN ingestion_timestamp_status = 'DIFF' THEN 'ingestion_timestamp' END +
     --  CASE WHEN processing_timestamp_status = 'DIFF' THEN 'processing_timestamp' END +
      CASE WHEN error_status = 'DIFF' THEN 'error' END,
      CASE WHEN summary_status = 'DIFF' THEN 'summary' END,
      CASE WHEN document_type_status = 'DIFF' THEN 'document_type' END,
      CASE WHEN orderNumber_status = 'DIFF' THEN 'orderNumber' END,
      CASE WHEN bolNumber_status = 'DIFF' THEN 'bolNumber' END,
      CASE WHEN vendorNumber_status = 'DIFF' THEN 'vendorNumber' END,
      CASE WHEN vendorName_status = 'DIFF' THEN 'vendorName' END,
      CASE WHEN invoiceNumber_status = 'DIFF' THEN 'invoiceNumber' END,
      CASE WHEN invoiceDate_status = 'DIFF' THEN 'invoiceDate' END,
      CASE WHEN purchaseOrderNumber_status = 'DIFF' THEN 'purchaseOrderNumber' END,
      CASE WHEN branch_status = 'DIFF' THEN 'branch' END,
      CASE WHEN warehouse_status = 'DIFF' THEN 'warehouse' END,
      CASE WHEN discountAmount_status = 'DIFF' THEN 'discountAmount' END,
      CASE WHEN discountDate_status = 'DIFF' THEN 'discountDate' END,
      CASE WHEN freightAmount_status = 'DIFF' THEN 'freightAmount' END,
      CASE WHEN totalAmount_status = 'DIFF' THEN 'totalAmount' END,
      CASE WHEN subtotalAmount_status = 'DIFF' THEN 'subtotalAmount' END,
      CASE WHEN shipping_address_status = 'DIFF' THEN 'shipping_address' END,
      CASE WHEN shipping_city_status = 'DIFF' THEN 'shipping_city' END,
      CASE WHEN shipping_state_status = 'DIFF' THEN 'shipping_state' END,
      CASE WHEN shipping_zip_status = 'DIFF' THEN 'shipping_zip' END,
      CASE WHEN remittance_address_status = 'DIFF' THEN 'remittance_address' END,
      CASE WHEN remittance_city_status = 'DIFF' THEN 'remittance_city' END,
      CASE WHEN remittance_state_status = 'DIFF' THEN 'remittance_state' END,
      CASE WHEN remittance_zip_status = 'DIFF' THEN 'remittance_zip' END,
      CASE WHEN termsDiscountPercentage_status = 'DIFF' THEN 'termsDiscountPercentage' END,
      CASE WHEN termsDiscountDueDays_status = 'DIFF' THEN 'termsDiscountDueDays' END,
      CASE WHEN termsNetDuedays_status = 'DIFF' THEN 'termsNetDuedays' END,
      CASE WHEN flags_status = 'DIFF' THEN 'flags' END,
      CASE WHEN barcodes_status = 'DIFF' THEN 'barcodes' END,
      CASE WHEN line_items_json_status = 'DIFF' THEN 'line_items_json' END,
      CASE WHEN line_items_count_status = 'DIFF' THEN 'line_items_count' END
    ) as different_columns,
    
    -- Show sample value differences for ALL FIELDS with differences (NEW SCHEMA + AI metadata)
    -- =================================================================
    -- AUDIT & TRACKING FIELD DIFFERENCES
    -- =================================================================
    CASE WHEN input_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(LEFT(ocr_input, 100), 'NULL'), '..." | AI: "', COALESCE(LEFT(ai_input, 100), 'NULL'), '..."')
         END as input_diff,
    CASE WHEN source_file_path_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_source_file_path, 'NULL'), '" | AI: "', COALESCE(ai_source_file_path, 'NULL'), '"')
         END as source_file_path_diff,
    CASE WHEN error_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_error, 'NULL'), '" | AI: "', COALESCE(ai_error, 'NULL'), '"')
         END as error_diff,

     -- CASE WHEN ingestion_timestamp_status = 'DIFF' 
     --     THEN CONCAT('OCR: "', COALESCE(ocr_ingestion_timestamp, 'NULL'), '" | AI: "', COALESCE(ai_ingestion_timestamp, 'NULL'), '"')
     --     END as ingestion_timestamp_diff,

     -- CASE WHEN processing_timestamp_status = 'DIFF' 
     --     THEN CONCAT('OCR: "', COALESCE(ocr_processing_timestamp, 'NULL'), '" | AI: "', COALESCE(ai_processing_timestamp, 'NULL'), '"')
     --     END as processing_timestamp_diff,
    
    -- =================================================================
    -- AI-GENERATED METADATA FIELD DIFFERENCES (NEW)
    -- =================================================================
    CASE WHEN summary_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(LEFT(ocr_summary, 100), 'NULL'), '..." | AI: "', COALESCE(LEFT(ai_summary, 100), 'NULL'), '..."')
         END as summary_diff,
    CASE WHEN document_type_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_document_type, 'NULL'), '" | AI: "', COALESCE(ai_document_type, 'NULL'), '"')
         END as document_type_diff,
    
    -- =================================================================
    -- NEW SCHEMA INVOICE HEADER FIELD DIFFERENCES
    -- =================================================================
    CASE WHEN orderNumber_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_orderNumber, 'NULL'), '" | AI: "', COALESCE(ai_orderNumber, 'NULL'), '"')
         END as orderNumber_diff,
    CASE WHEN bolNumber_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_bolNumber, 'NULL'), '" | AI: "', COALESCE(ai_bolNumber, 'NULL'), '"')
         END as bolNumber_diff,
    CASE WHEN vendorNumber_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_vendorNumber, 'NULL'), '" | AI: "', COALESCE(ai_vendorNumber, 'NULL'), '"')
         END as vendorNumber_diff,
    CASE WHEN vendorName_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_vendorName, 'NULL'), '" | AI: "', COALESCE(ai_vendorName, 'NULL'), '"')
         END as vendorName_diff,
    CASE WHEN invoiceNumber_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_invoiceNumber, 'NULL'), '" | AI: "', COALESCE(ai_invoiceNumber, 'NULL'), '"')
         END as invoiceNumber_diff,
    CASE WHEN invoiceDate_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(CAST(ocr_invoiceDate AS STRING), 'NULL'), '" | AI: "', COALESCE(CAST(ai_invoiceDate AS STRING), 'NULL'), '"')
         END as invoiceDate_diff,
    CASE WHEN purchaseOrderNumber_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_purchaseOrderNumber, 'NULL'), '" | AI: "', COALESCE(ai_purchaseOrderNumber, 'NULL'), '"')
         END as purchaseOrderNumber_diff,
    CASE WHEN branch_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_branch, 'NULL'), '" | AI: "', COALESCE(ai_branch, 'NULL'), '"')
         END as branch_diff,
    CASE WHEN warehouse_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_warehouse, 'NULL'), '" | AI: "', COALESCE(ai_warehouse, 'NULL'), '"')
         END as warehouse_diff,
    CASE WHEN discountAmount_status = 'DIFF' 
         THEN CONCAT('OCR: ', COALESCE(CAST(ocr_discountAmount AS STRING), 'NULL'), ' | AI: ', COALESCE(CAST(ai_discountAmount AS STRING), 'NULL'))
         END as discountAmount_diff,
    CASE WHEN discountDate_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(CAST(ocr_discountDate AS STRING), 'NULL'), '" | AI: "', COALESCE(CAST(ai_discountDate AS STRING), 'NULL'), '"')
         END as discountDate_diff,
    CASE WHEN freightAmount_status = 'DIFF' 
         THEN CONCAT('OCR: ', COALESCE(CAST(ocr_freightAmount AS STRING), 'NULL'), ' | AI: ', COALESCE(CAST(ai_freightAmount AS STRING), 'NULL'))
         END as freightAmount_diff,
    CASE WHEN totalAmount_status = 'DIFF' 
         THEN CONCAT('OCR: ', COALESCE(CAST(ocr_totalAmount AS STRING), 'NULL'), ' | AI: ', COALESCE(CAST(ai_totalAmount AS STRING), 'NULL'))
         END as totalAmount_diff,
    CASE WHEN subtotalAmount_status = 'DIFF' 
         THEN CONCAT('OCR: ', COALESCE(CAST(ocr_subtotalAmount AS STRING), 'NULL'), ' | AI: ', COALESCE(CAST(ai_subtotalAmount AS STRING), 'NULL'))
         END as subtotalAmount_diff,
    
    -- =================================================================
    -- ADDRESS FIELD DIFFERENCES
    -- =================================================================
    CASE WHEN shipping_address_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(LEFT(ocr_shipping_address, 100), 'NULL'), '" | AI: "', COALESCE(LEFT(ai_shipping_address, 100), 'NULL'), '"')
         END as shipping_address_diff,
    CASE WHEN shipping_city_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_shipping_city, 'NULL'), '" | AI: "', COALESCE(ai_shipping_city, 'NULL'), '"')
         END as shipping_city_diff,
    CASE WHEN shipping_state_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_shipping_state, 'NULL'), '" | AI: "', COALESCE(ai_shipping_state, 'NULL'), '"')
         END as shipping_state_diff,
    CASE WHEN shipping_zip_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_shipping_zip, 'NULL'), '" | AI: "', COALESCE(ai_shipping_zip, 'NULL'), '"')
         END as shipping_zip_diff,
    CASE WHEN remittance_address_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(LEFT(ocr_remittance_address, 100), 'NULL'), '" | AI: "', COALESCE(LEFT(ai_remittance_address, 100), 'NULL'), '"')
         END as remittance_address_diff,
    CASE WHEN remittance_city_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_remittance_city, 'NULL'), '" | AI: "', COALESCE(ai_remittance_city, 'NULL'), '"')
         END as remittance_city_diff,
    CASE WHEN remittance_state_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_remittance_state, 'NULL'), '" | AI: "', COALESCE(ai_remittance_state, 'NULL'), '"')
         END as remittance_state_diff,
    CASE WHEN remittance_zip_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_remittance_zip, 'NULL'), '" | AI: "', COALESCE(ai_remittance_zip, 'NULL'), '"')
         END as remittance_zip_diff,
    
    -- =================================================================
    -- PAYMENT TERMS FIELD DIFFERENCES
    -- =================================================================
    CASE WHEN termsDiscountPercentage_status = 'DIFF' 
         THEN CONCAT('OCR: ', COALESCE(CAST(ocr_termsDiscountPercentage AS STRING), 'NULL'), ' | AI: ', COALESCE(CAST(ai_termsDiscountPercentage AS STRING), 'NULL'))
         END as termsDiscountPercentage_diff,
    CASE WHEN termsDiscountDueDays_status = 'DIFF' 
         THEN CONCAT('OCR: ', COALESCE(CAST(ocr_termsDiscountDueDays AS STRING), 'NULL'), ' | AI: ', COALESCE(CAST(ai_termsDiscountDueDays AS STRING), 'NULL'))
         END as termsDiscountDueDays_diff,
    CASE WHEN termsNetDuedays_status = 'DIFF' 
         THEN CONCAT('OCR: ', COALESCE(CAST(ocr_termsNetDuedays AS STRING), 'NULL'), ' | AI: ', COALESCE(CAST(ai_termsNetDuedays AS STRING), 'NULL'))
         END as termsNetDuedays_diff,
    
    -- =================================================================
    -- FLAGS AND ARRAY FIELD DIFFERENCES - Fixed for Databricks
    -- =================================================================
    CASE WHEN flags_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(ocr_flags, 'NULL'), '" | AI: "', COALESCE(ai_flags, 'NULL'), '"')
         END as flags_diff,
    CASE WHEN barcodes_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(LEFT(CAST(ocr_barcodes AS STRING), 200), 'NULL'), '..." | AI: "', COALESCE(LEFT(CAST(ai_barcodes AS STRING), 200), 'NULL'), '..."')
         END as barcodes_diff,
    CASE WHEN line_items_json_status = 'DIFF' 
         THEN CONCAT('OCR: "', COALESCE(LEFT(ocr_line_items_json, 200), 'NULL'), '..." | AI: "', COALESCE(LEFT(ai_line_items_json, 200), 'NULL'), '..."')
         END as line_items_json_diff,
    CASE WHEN line_items_count_status = 'DIFF' 
         THEN CONCAT('OCR: ', COALESCE(CAST(ocr_line_items_count AS STRING), 'NULL'), ' | AI: ', COALESCE(CAST(ai_line_items_count AS STRING), 'NULL'))
         END as line_items_count_diff
         
  FROM full_comparison
)
SELECT 
  file_name,
  ingestion_timestamp,
  processing_timestamp,
  total_differences,
  different_columns,
--   ingestion_timestamp_diff,
--   processing_timestamp_diff,
  -- =================================================================
  -- ALL FIELD DIFFERENCES - Show actual values for every different field (NEW SCHEMA + AI metadata)
  -- =================================================================
  -- Audit & Tracking Fields
  input_diff,
  source_file_path_diff,
  error_diff,
  
  -- AI-Generated Metadata Fields (NEW)
  summary_diff,
  document_type_diff,
  
  -- New Schema Invoice Header Fields
  orderNumber_diff,
  bolNumber_diff,
  vendorNumber_diff,
  vendorName_diff,
  invoiceNumber_diff,
  invoiceDate_diff,
  purchaseOrderNumber_diff,
  branch_diff,
  warehouse_diff,
  discountAmount_diff,
  discountDate_diff,
  freightAmount_diff,
  totalAmount_diff,
  subtotalAmount_diff,
  
  -- Address Fields
  shipping_address_diff,
  shipping_city_diff,
  shipping_state_diff,
  shipping_zip_diff,
  remittance_address_diff,
  remittance_city_diff,
  remittance_state_diff,
  remittance_zip_diff,
  
  -- Payment Terms Fields
  termsDiscountPercentage_diff,
  termsDiscountDueDays_diff,
  termsNetDuedays_diff,
  
  -- Flags and Array Fields
  flags_diff,
  barcodes_diff,
  line_items_json_diff,
  line_items_count_diff
  
FROM differences_summary 
WHERE total_differences > 0
ORDER BY total_differences DESC, file_name;

-- COMMAND ----------

select *  from canonical_invoices_view

-- COMMAND ----------

select count(1) from ocr_canonical_invoices_view

-- COMMAND ----------

