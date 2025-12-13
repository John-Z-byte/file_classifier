# file_classifier

## A deterministic, schema-based file classifier for XLSX and CSV files

This project classifies messy spreadsheet files based on **detected schemas (headers)** rather than filenames or folder structure. It is designed for data engineering workflows where **auditability, reproducibility, and explicit control over data meaning** are required.

---

## Problem Statement

### Solution for Spreadsheet-Based Data Pipeline Inconsistency

Spreadsheet-based data pipelines often fail because:

- Filenames are inconsistent or misleading  
- Folder structures change over time  
- The same dataset is exported repeatedly with different names  
- New file variants appear silently and break downstream logic  

This project addresses these issues by treating a file’s **schema as its true identity**.

---

## Core Principle

- Headers define meaning  
- Formats do not  
- Filenames lie  

A file is classified **only** by the exact set of **normalized headers** it contains.

---

## Key Features

- Schema detection using header heuristics  
- Stable schema identity via hashing  
- Manual semantic labeling (no auto-mapping)  
- Explicit handling of unknown schemas  
- Snapshot-based gold outputs (no accumulation)  
- Full audit trail in Parquet  
- Fast preview-based processing  
- Supports XLSX and CSV formats  

---

## High-Level Architecture

The pipeline is organized into four main stages:

- **`datalake/`**  
  Raw input files (read-only source)

- **`pipeline`**  
  Schema detection, normalization, and hashing logic

- **`data/staging/`**  
  Audit and metadata storage (**Truth Layer**) persisted in Parquet

- **`data/classified/`**  
  Snapshot-based gold output organized by schema label

---

## Folder Structure

```text
file_classifier/
├── datalake/                  # Raw input files (gitignored)
├── data/
│   ├── staging/               # Audit & metadata (Parquet)
│   ├── classified/            # Snapshot gold output
│   └── quarantine/            # Unreadable / low-confidence files
├── src/                       # Pipeline source code
├── config/                    # Runtime configuration
└── README.md
```
## Unknown Schema Handling

Unknown schemas are **never silent**:

- Files are copied to `classified/unknown_schema/`
- Missing schema hashes are printed in the CLI
- Users must explicitly label schemas before they are classified

---

## Design Guarantees

- Same headers always produce the same schema hash
- Header order does not matter
- Filenames do not affect classification
- Unknown schemas are always surfaced
- Staging is the single source of truth

---

## How to Run

```bash
python -m src.main



