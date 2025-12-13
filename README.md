Staging Outputs (Truth Layer)

All decisions and metadata are persisted in Parquet files under data/staging/.
This layer is the source of truth for auditability and reproducibility.

file_catalog.parquet

One row per file:

File path, size, modified timestamp

Status: ok, unreadable, low_confidence

Detected header metadata

Schema identity (schema_key, schema_hash)

Assigned semantic label

schema_registry.parquet

One row per schema:

Schema hash and canonical key

Canonical normalized headers

Number of files using the schema

Example file paths

classification_manifest.parquet

One row per copy attempt:

Source and destination paths

Copy status

Error message (if any)

Schema identity and label

Physical Classification (Gold Output)

Files are copied into snapshot-style gold folders:

data/classified/<label>/<schema_hash>__original_filename.ext


Rules:

Snapshot semantics (no accumulation across runs)

Gold folders are wiped per label per run

Unreadable or low-confidence files go to data/quarantine/

Folder Structure
file_classifier/
├── datalake/                  # Raw input files (gitignored)
│
├── data/
│   ├── staging/               # Audit & metadata (Parquet)
│   │   ├── file_catalog.parquet
│   │   ├── schema_registry.parquet
│   │   └── classification_manifest.parquet
│   │
│   ├── classified/            # Snapshot gold output
│   │   ├── <label>/
│   │   └── unknown_schema/
│   │
│   └── quarantine/            # Unreadable / low-confidence files
│
├── src/
│   ├── main.py                # Orchestrator
│   ├── io/                    # File discovery & preview readers
│   ├── fingerprint/           # Header detection & normalization
│   ├── classify/              # Snapshot-safe copy logic
│   └── labeling/              # Schema hash → label mapping
│
├── config/                    # Runtime configuration
└── README.md

Unknown Schema Handling

Unknown schemas are never silent.

Files are copied to classified/unknown_schema/

Missing schema hashes are printed in the CLI

User must explicitly add them to schema_labels.yaml

Next run classifies them automatically

This behavior is intentional and enforced.

Design Guarantees

Same headers always produce the same schema hash

Header order does not matter

Filenames do not affect classification

Unknown schemas are always surfaced

Staging is always the source of truth
