# AI Parse Document – Document Management Workflow

A reference implementation for automating **document ingestion → parsing → validation → normalization → storage** on Databricks using **`ai_parse_document()`** and (optionally) OCR. The repo includes two runnable notebook flows:

- **`Document Management Workflow Final/`** – baseline workflow for parsing structured/semistructured PDFs and images.
- **`Document Management Workflow with OCR/`** – adds OCR pre-processing for scanned or low-quality images/PDFs.

> Note: `ai_parse_document()` is available in Databricks SQL/Python and can run from notebooks, Jobs, Workflows, or Lakeflow Declarative Pipelines. It requires DBR 17.1+ and, if using Serverless, environment version 3+ (for `VARIANT`):contentReference[oaicite:0]{index=0}. Usage is metered under **AI_FUNCTIONS**:contentReference[oaicite:1]{index=1}.

---

## Architecture (High-Level)

1. **Ingest** documents from cloud storage (e.g., `/Volumes/<catalog>.<schema>.<volume>/inbound/`).
2. **(Optional) OCR** pass to improve text extraction for scans.
3. **Parse** with `ai_parse_document()` → capture **text + layout** as structured output.
4. **Normalize & Validate** to your target schema (e.g., invoices, POs).
5. **Persist** curated results (bronze/silver/gold Delta) and **emit JSON** for downstream apps.
6. **Schedule** via Jobs/Workflows; monitor volumes/DBUs and parsing errors.

