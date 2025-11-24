# Document Management Workflow with AI Parse - Comprehensive Solution

A implementation for automated **invoice document processing** using Databricks' advanced AI capabilities. This solution demonstrates a complete end-to-end workflow from document ingestion to structured data extraction using `ai_parse_document()` and Agent Bricks.

## 🏗️ Solution Architecture

![Architecture Diagram](architecture_diagram.png)

The solution implements a modern data architecture with the following layers:

### **Bronze Layer (Ingestion)**
- **Document Intake**: Automated file discovery and ingestion from cloud storage volumes
- **Raw Processing**: PDF parsing using Databricks `ai_parse_document()` function
- **Incremental Processing**: Smart processing that handles only new/modified files

### **Silver Layer (Processing & Parsing)**  
- **AI Classification**: Automatic document type detection using `ai_classify()`
- **Content Extraction**: Advanced parsing with custom AI models through endpoints
- **Data Validation**: Schema compliance and quality checks

### **Gold Layer (GenAI Tasks)**
- **Information Extraction**: Structured data extraction using Agent Bricks Information Extraction endpoint following canonical schema
- **Quality Control**: Quality assurance through AI agents
- **Canonical Views**: Business-ready invoice data with proper typing

---

## 🚀 Key Features

✅ **Incremental Processing** - Processes only new/modified documents  
✅ **AI-Powered Classification** - Automatically categorizes document types  
✅ **Schema Validation** - Ensures data quality and consistency  
✅ **Scalable Architecture** - Handles high-volume document processing  

---

## 📂 Solution Components

This repository contains the **Document Management Workflow Final** folder with the complete solution:

### Core Processing Notebooks

| Notebook | Purpose | Databricks Features Used |
|----------|---------|-------------------------|
| `01.1_pdf_processing-with-ai_parse_document-beta2.py` | **Bronze Layer**: Document ingestion and AI parsing | `ai_parse_document()`, `ai_query()`, `ai_classify()`, Volumes, Delta Tables |
| `02.1_Information_Extraction_ai_parse.py` | **Silver Layer**: DLT pipeline for structured extraction using Agent Bricks endpoint | Delta Live Tables (DLT), Streaming Tables, AI Endpoints |
| `02.3_Canonical_Invoice_Views.sql` | **Gold Layer**: Business-ready canonical views | Advanced SQL, Complex Data Types, Date Parsing |

### Configuration & Schema Files

| File | Purpose |
|------|---------|
| `schema_sep.json` | **Canonical Schema**: Defines the target structure for invoice data extraction |
| `instructions.md` | **AI Instructions**: Guidelines for the AI model to ensure consistent extraction |

### Pipeline Components

| Directory | Purpose |
|-----------|----------|
| `document_invoice_extraction_dlt_pipeline/` | **DLT Pipeline**: Delta Live Tables implementation for real-time processing |

---

## 🔄 Solution Flow

### Phase 1: Document Ingestion (Bronze Layer)
1. **File Discovery**: Scans configured volume paths for new PDF documents
2. **Incremental Processing**: Uses modification timestamps to process only new files
3. **AI Parsing**: Applies `ai_parse_document()` to extract text and structure
4. **Document Classification**: Uses `ai_classify()` to identify document types (invoice, receipt, etc.)
5. **Content Summary**: Generates AI-powered summaries for each document
6. **Storage**: Saves results to Delta tables with full audit trail

### Phase 2: Information Extraction (Silver Layer)
1. **Streaming Processing**: DLT pipeline processes documents in real-time
2. **AI Endpoint Integration**: Calls custom trained models for structured extraction
3. **Schema Transformation**: Converts unstructured text to canonical JSON format
4. **Data Validation**: Ensures extracted data meets quality standards
5. **Error Handling**: Captures and routes failed extractions for review

### Phase 3: Canonical Views (Gold Layer)
1. **Data Normalization**: Standardizes dates, addresses, and numeric values
2. **Business Logic**: Applies invoice-specific transformations and calculations
3. **Final Views**: Creates analytics-ready tables for consumption

---

## 🛠️ Databricks Features Utilized

### **Core AI Functions**
- **`ai_parse_document()`**: Advanced PDF parsing with layout awareness
- **`ai_query()`**: Custom AI model integration for structured extraction  
- **`ai_classify()`**: Automatic document type classification

### **Data Platform Capabilities**
- **Unity Catalog**: Centralized governance and data lineage
- **Delta Lake**: ACID transactions and time travel capabilities
- **Volumes**: Managed cloud storage for document files
- **Delta Live Tables (DLT)**: Real-time data pipeline orchestration
- **Agent Bricks Information Extraction**: Point-and-click agent deployment for structured data extraction  

### **Advanced SQL Features**
- **VARIANT Data Type**: Flexible JSON processing and schema evolution
- **Complex Data Types**: Arrays, structs, and nested data handling
- **Streaming Tables**: Real-time data processing capabilities
- **Advanced Analytics**: Window functions and complex aggregations

### **Infrastructure & Monitoring**
- **Serverless Computing**: Auto-scaling compute resources
- **Job Orchestration**: Automated pipeline scheduling and monitoring

---

## 📖 Navigation Guide

### **Getting Started - Execution Order**

1. **Start Here**: `01.1_pdf_processing-with-ai_parse_document-beta2.py`
   - Configure your catalog, schema, and volume settings
   - Run the complete document ingestion process
   - Monitor processing results and document classification

2. **Pipeline Setup**: `document_invoice_extraction_dlt_pipeline/`
   - Deploy the DLT pipeline using `02.1_Information_Extraction_ai_parse.py`
   - Configure your AI endpoint for structured extraction(Agent Bricks)
   - Monitor streaming processing and data quality

3. **Business Views**: `02.3_Canonical_Invoice_Views.sql`
   - Create canonical invoice views for business consumption
   - Apply data transformations and quality rules
   - Generate analytics-ready datasets

### **Configuration Requirements**

Before running the solution, ensure you have:

```python
# Unity Catalog Configuration
catalog = "your_catalog_name"           # e.g., "databricks_sandbox"
schema = "your_schema_name"             # e.g., "development" 
volume = "your_volume_name"             # e.g., "document-intake"

# AI Endpoint Configuration  
ai_endpoint_name = "your-endpoint-name" # Custom trained model endpoint
```

### **Key Parameters**

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| `baseline_cutoff_date` | Starting date for document processing | `2025-09-01T00:00:00Z` |
| `incremental_mode` | Enable incremental processing | `True` (auto-detected) |
| `parse_extensions` | Supported file formats | `['.pdf', '.jpg', '.jpeg', '.png']` |

---

## 📊 Data Schema & Structure

### **Canonical Invoice Schema**

The solution extracts invoices into a structured format with the following components:

#### **Invoice Header**
```json
{
  "invoiceNumber": "string",
  "vendorName": "string", 
  "invoiceDate": "YYYY-MM-DD",
  "totalAmount": "number",
  "purchaseOrderNumber": "string",
  "branch": "3-digit code",
  "warehouse": "3-digit code"
}
```

#### **Line Items Array**
```json
{
  "lineItems": [
    {
      "lineNumber": "integer",
      "productCode": "string",
      "quantityShipped": "number",
      "unitPrice": "number", 
      "lineTotal": "number"
    }
  ]
}
```

#### **Address Information**
```json
{
  "shippingAddress": {
    "address": "string",
    "city": "string", 
    "state": "2-letter code",
    "zip": "string"
  }
}
```



## 🔧 Advanced Configuration

### **Incremental Processing**
The solution automatically detects new files uploaded to UC Volumes and processes only what's needed:
- Uses file modification timestamps for change detection
- Maintains processing history and audit trails
- Supports both full refresh and incremental modes
- Handles schema evolution gracefully

### **AI Model Integration**
- Custom endpoint configuration for specialized extraction models
- Fallback mechanisms for endpoint failures
- Model versioning and A/B testing capabilities
- Performance monitoring and optimization

### **Data Governance**
- Unity Catalog integration for data lineage
- Column-level security and access controls
- Data classification and sensitivity marking
- Compliance reporting and audit trails

---

## 🎯 Business Value

This solution delivers immediate business value through:

- **Automation**: reduction in manual document processing time
- **Accuracy**: AI-powered extraction with built-in validation
- **Scalability**: Processes thousands of documents per hour
- **Integration**: Ready-to-use APIs for downstream applications
- **Governance**: Complete audit trail and data lineage
- **Cost Efficiency**: Serverless architecture scales with demand

---

## 📋 Prerequisites

- **Databricks Runtime**: 17.1+ (for ai_parse_document support)
- **Environment**: Serverless v3+ (for VARIANT data type support)
- **Unity Catalog**: Enabled with proper permissions
- **AI Functions**: Access to Databricks AI functions
- **Custom Endpoint**: Trained model for invoice extraction

---

*This solution demonstrates best practices for building production-ready document processing workflows on Databricks, combining the power of AI with robust data engineering practices.*

