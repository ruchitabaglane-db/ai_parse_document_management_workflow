### Invoice Data Extraction for Shamrock Foods

## Objective
Extract and structure key invoice data from unstructured text into a standardized canonical JSON format for seamless integration with accounting and ERP systems.

## Input
Raw text content extracted from invoice documents containing invoice information in various formats and layouts.

## Output Schema Structure
Structured JSON data conforming to the canonical invoice schema.

## Format Flexibility
Handle diverse invoice layouts including:
- Traditional tabular formats
- Modern digital invoice designs
- Scanned document variations
- Multi-page documents
- Various vendor formats

## CRITICAL: Schema Compliance
The output MUST strictly conform to provided schema structure. Any deviation will cause processing failures.

## Data Quality Guidelines

1. **Preserve Original Formatting:** Keep leading zeros, prefixes in codes
2. **Validate Patterns:** Ensure branch/warehouse codes are 3 digits
3. **Date Consistency:** All dates must be YYYY-MM-DD format
4. **Numeric Validation:** All amounts must be >= 0
5. **Address Completeness:** Include all required address components
6. **Line Item Sequence:** Number line items sequentially starting from 1
7. **Calculation Accuracy:** Ensure line totals match quantity × unit price where possible

## Error Handling
- If critical data is missing, use appropriate defaults per schema
- Never leave required fields null or empty
- Always include at least one line item (use defaults if necessary)
- Maintain valid JSON structure at all times

The quality and accuracy of extraction directly impacts downstream processing and business operations.