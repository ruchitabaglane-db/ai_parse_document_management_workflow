-- Databricks notebook source
use databricks_sandbox.development;

-- COMMAND ----------

-- =============================================================================
-- SIMPLE COUNT VALIDATION QUERIES
-- =============================================================================

-- 1. Check record counts across all tables/views
SELECT 
  'raw_pdf_files' as source_table,
  COUNT(*) as record_count
FROM raw_pdf_files

UNION ALL

SELECT 
  'raw_ocr_pdf_files' as source_table,
  COUNT(*) as record_count
FROM raw_ocr_pdf_files

UNION ALL

SELECT 
  'canonical_invoices_view' as view_name,
  COUNT(*) as record_count
FROM canonical_invoices_view

UNION ALL

SELECT 
  'ocr_canonical_invoices_view' as view_name,
  COUNT(*) as record_count
FROM ocr_canonical_invoices_view

UNION ALL

SELECT 
  'canonical_invoices_final' as merged_view,
  COUNT(*) as record_count
FROM canonical_invoices_final

ORDER BY record_count DESC;

-- COMMAND ----------

-- 2. Find PDFs missing between AI and OCR processing
WITH missing_analysis AS (
  SELECT 
    COALESCE(ai.source_file_name, ocr.source_file_name) as file_name,
    CASE WHEN ai.source_file_name IS NOT NULL THEN 'YES' ELSE 'NO' END as in_ai_processing,
    CASE WHEN ocr.source_file_name IS NOT NULL THEN 'YES' ELSE 'NO' END as in_ocr_processing,
    CASE 
      WHEN ai.source_file_name IS NOT NULL AND ocr.source_file_name IS NOT NULL THEN 'BOTH'
      WHEN ai.source_file_name IS NOT NULL AND ocr.source_file_name IS NULL THEN 'AI_ONLY' 
      WHEN ai.source_file_name IS NULL AND ocr.source_file_name IS NOT NULL THEN 'OCR_ONLY'
    END as processing_status
  FROM canonical_invoices_view ai
  FULL OUTER JOIN ocr_canonical_invoices_view ocr
    ON ai.source_file_name = ocr.source_file_name
)
SELECT 
  processing_status,
  COUNT(*) as file_count
FROM missing_analysis
GROUP BY processing_status
ORDER BY file_count DESC;

-- COMMAND ----------

-- 3. Sample of files that are missing from either AI or OCR processing
WITH missing_files AS (
  SELECT 
    COALESCE(ai.source_file_name, ocr.source_file_name) as file_name,
    CASE 
      WHEN ai.source_file_name IS NOT NULL AND ocr.source_file_name IS NOT NULL THEN 'BOTH'
      WHEN ai.source_file_name IS NOT NULL AND ocr.source_file_name IS NULL THEN 'AI_ONLY' 
      WHEN ai.source_file_name IS NULL AND ocr.source_file_name IS NOT NULL THEN 'OCR_ONLY'
    END as processing_status
  FROM canonical_invoices_view ai
  FULL OUTER JOIN ocr_canonical_invoices_view ocr
    ON ai.source_file_name = ocr.source_file_name
)
SELECT 
  processing_status,
  file_name
FROM missing_files
WHERE processing_status != 'BOTH'
ORDER BY processing_status, file_name
LIMIT 20;