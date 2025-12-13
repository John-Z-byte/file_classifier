file_classifier

A deterministic, schema-based file classifier for XLSX and CSV files.

This project classifies messy spreadsheet files based on detected schemas (headers) rather than filenames or folder structure. It is designed for data engineering workflows where auditability, reproducibility, and explicit control over data meaning are required.

Problem Statement

In many data pipelines, spreadsheet files are unreliable inputs:

Filenames are inconsistent or misleading

Folder structure changes over time

The same dataset may be exported daily with different names

New file variants appear silently and break downstream logic

This project solves that by treating a file’s schema as its true identity.

Core Idea

Headers define meaning

Formats do not

Filenames lie

A file is classified solely by the exact set of normalized headers it contains.

Key Features

Schema detection using header heuristics

Stable schema identity via hashing

Manual semantic labeling (no auto-mapping)

Explicit handling of unknown schemas

Snapshot-based gold outputs (no accumulation)

Full audit trail in Parquet

Fast preview-based processing

Supports XLSX and CSV

High-Level Architecture
datalake/        # raw input (read-only)
   ↓
pipeline
   ↓
data/staging/    # audit & metadata (truth)
   ↓
data/classified/ # snapshot output (gold)

Folder Structure
file_classifier/
├── datalake/                  # raw input files (gitignored)
│
├── data/
│   ├── staging/               # audit artifacts (Parquet)
│   │   ├── file_catalog.parquet
│   │   ├── schema_registry.parquet
│   │   └── classification_manifest.parquet
│   │
│   ├── classified/            # snapshot output
│   │   ├── <label>/
│   │   └── unknown_schema/
│   │
│   └── quarantine/            # unreadable / low-confidence files
│
├── src/
│   ├── main.py                # orchestrator
│   │
│   ├── io/
│   │   ├── scanner.py
│   │   └── preview_reader.py  # XLSX + CSV preview readers
│   │
│   ├── fingerprint/
│   │   ├── header_detector.py
│   │   └── header_normalizer.py
│   │
│   ├── classify/
│   │   └── file_copier.py
│   │
│   └── labeling/
│       └── schema_labels.py
│
├── config/
│   ├── settings.yaml
│   ├── header_aliases.yaml
│   └── schema_labels.yaml
│
└── README.md

Pipeline Overview
1. Scan

Recursively scan datalake/

Collect file path, size, and modification time

Stable ordering for deterministic runs

2. Preview Read

Read only the first N rows

XLSX: pandas.read_excel(nrows=N)

CSV: pandas.read_csv(nrows=N, sep=None)

Avoids full file reads for performance

3. Header Detection

Candidate rows are scored using:

Non-empty density

Text vs numeric ratio

Short-string dominance

Uniqueness

Coherence with following rows

Low-confidence detections are flagged explicitly.

4. Header Normalization

Headers are normalized by:

Lowercasing

Trimming whitespace

Removing accents

Replacing separators with _

Removing invalid characters

Deduplicating

Optional alias mapping

5. Schema Identity

Schema = exact set of normalized headers (order ignored)

schema_key = sorted join of headers

schema_hash = sha1(schema_key)[:12]

This hash is the stable identity of the schema.

6. Manual Semantic Labeling

Schema hashes are mapped to business labels in schema_labels.yaml

Unknown schemas are never auto-labeled

Unknowns are surfaced explicitly

7. Staging Outputs (Truth Layer)
file_catalog.parquet

One row per file:

Path, size, modified timestamp

Status (ok, unreadable, low_confidence)

Header metadata

Schema identity

Assigned label

schema_registry.parquet

One row per schema:

Schema hash and key

Canonical headers

File count

Example files

classification_manifest.parquet

One row per copy attempt:

Source path

Destination path

Copy status

Errors (if any)

8. Classification Output (Snapshot)

Files are copied to:

data/classified/<label>/<schema_hash>__original_filename.ext


Snapshot semantics:

Gold folders are wiped per label per run

No accumulation across runs

Unreadable or low-confidence files go to data/quarantine/

CLI Output

The CLI prints a compact summary table:

schema                     files  headers  rows
wellsky_clients                6       18      6
ringcentral_calls              8       10      8
...


files: number of files in the snapshot

headers: schema complexity

rows: number of files (cheap metric, not data rows)

Unknown Schema Handling

Unknown schemas are copied to classified/unknown_schema/

Missing schema hashes are printed in the console

User must manually update schema_labels.yaml

Next run classifies them automatically

This behavior is intentional and required.

How to Run

From the project root:

python -m src.main


Dry run:

python -m src.main --dry-run

Design Guarantees

Same headers always produce the same schema hash

Header order does not matter

Filenames do not affect classification

Unknown schemas are never silent

Staging is always the source of truth

Non-Goals

Automatic semantic inference

Fuzzy schema matching

Silent schema drift handling

These are explicitly avoided to preserve auditability and trust.

Possible Extensions

Incremental processing using file signatures

Persisted real row counts (computed once)

Schema drift detection

Historical snapshot archiving

CLI filters and inspection tools

License

MIT