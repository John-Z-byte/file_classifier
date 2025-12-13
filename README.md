Project Context Prompt — file_classifier

You are helping me maintain and extend a Python data-engineering project called file_classifier.

The project is already implemented, stable, and working end-to-end.
Your job is to extend or refactor it without breaking its guarantees, performance, or mental model.

🎯 Core Goal

Automatically classify messy spreadsheet files (XLSX, CSV) into business-meaningful categories using schema detection, not filenames or folders.

The system must remain:

deterministic

auditable

fast

explicit about unknown schemas

manually controlled for semantic meaning

🧠 Core Philosophy

Headers define meaning

Formats do not

Filenames lie

A file’s schema (exact normalized header set) is its identity.

📥 Input Assumptions

Input files live in datalake/

Input is read-only

Supported formats:

.xlsx

.csv

Each file:

Has a single logical table

May include metadata rows above the real header

May have broken Excel metadata (UsedRange issues)

🗂️ High-Level Architecture
datalake/              # raw input (never touched)
   ↓
pipeline
   ↓
data/staging/          # metadata, audits, decisions (truth)
   ↓
data/classified/       # business snapshot (gold)

🗃️ Folder Structure
file_classifier/
├── datalake/                  # raw input files (gitignored)
│
├── data/
│   ├── staging/               # audit & metadata (Parquet)
│   │   ├── file_catalog.parquet
│   │   ├── schema_registry.parquet
│   │   └── classification_manifest.parquet
│   │
│   ├── classified/            # snapshot output (gold)
│   │   ├── <label>/
│   │   └── unknown_schema/
│   │
│   └── quarantine/            # unreadable / low-confidence files
│
├── src/
│   ├── main.py                # orchestrator
│   │
│   ├── io/
│   │   ├── scanner.py         # recursive file discovery
│   │   └── preview_reader.py  # XLSX + CSV preview readers
│   │
│   ├── fingerprint/
│   │   ├── header_detector.py # robust header detection
│   │   └── header_normalizer.py
│   │
│   ├── classify/
│   │   └── file_copier.py     # snapshot-safe copy logic
│   │
│   └── labeling/
│       └── schema_labels.py   # schema_hash → label loader
│
├── config/
│   ├── settings.yaml
│   ├── header_aliases.yaml
│   └── schema_labels.yaml     # manual schema → label mapping
│
└── README.md

🔍 Pipeline (Step by Step)
1️⃣ Scan

Recursively scan datalake/

Collect:

path

size

modified time

Sorted for deterministic runs

2️⃣ Preview Read (cheap)

Read only first N rows

XLSX: pandas.read_excel(nrows=N)

CSV: pandas.read_csv(nrows=N, sep=None)

Avoid full reads

3️⃣ Header Detection (Heuristic)

Candidate rows scored by:

non-empty density

text vs numeric ratio

short-string dominance

uniqueness

coherence with following rows

If confidence < threshold → low_confidence

4️⃣ Header Normalization

Rules:

lowercase

trim

remove accents

spaces/dots/dashes → _

collapse _

remove non [a-z0-9_]

deduplicate (col, col__2, …)

optional alias mapping (header_aliases.yaml)

5️⃣ Schema Identity

Schema = set of normalized headers (order ignored)

schema_key = sorted join (audit)

schema_hash = sha1(schema_key)[:12] (stable identity)

6️⃣ Manual Semantic Labeling

Load config/schema_labels.yaml

Map:

schema_hash → business label


Missing → unknown_schema

No auto-labeling ever

7️⃣ Persist Staging Artifacts (Truth Layer)
file_catalog.parquet

One row per file:

path

size

modified_ts

status (ok | unreadable | low_confidence)

header_row_index

header_confidence

raw_headers_json

normalized_headers_json

schema_key

schema_hash

schema_id

label

schema_registry.parquet

One row per schema:

schema_id

schema_hash

schema_key

canonical_headers_json

file_count

example_files_json

classification_manifest.parquet

One row per copy attempt:

src_path

dst_path

copy_status

error_message

schema_id

schema_hash

label

8️⃣ Physical Classification (Gold Snapshot)

Snapshot mode:

Gold folders are wiped per label per run

No accumulation

Output:

data/classified/<label>/<schema_hash>__original_filename.ext


Unreadable / low confidence → data/quarantine/

🧠 Key Mental Model
Layer	Purpose
datalake/	raw input
staging/	truth & audit
classified/	business snapshot
schema_hash	identity
label	meaning

Staging answers “why”.
Classified answers “where”.

🧾 CLI Output (psql-style)

The CLI shows a compact table:

schema                     files  headers  rows
wellsky_clients                6       18      6
ringcentral_calls              8       10      8
...


files = number of files in snapshot

rows = number of files (cheap, not data rows)

🚨 Unknown Schema Handling

Unknown schemas:

go to classified/unknown_schema/

printed in console

User manually updates schema_labels.yaml

Next run → auto-classified

This is intentional and required.

▶️ How to Run
python -m src.main


Dry run:

python -m src.main --dry-run

🧱 Current State (Important)

End-to-end stable

XLSX + CSV supported

Snapshot logic working

CLI clean and fast

Performance optimized

No accumulation

No silent behavior

🚫 Hard Constraints for All Future Work

❌ Do not bypass staging

❌ Do not auto-label schemas

❌ Do not break snapshot semantics

❌ Do not re-read full files unnecessarily

❌ Do not mix audit and presentation layers

🚀 Approved Future Extensions (Optional)

Incremental processing via file_sig

Persisted real row counts (computed once)

Archive historical snapshots

CLI filters (--only unknown, --only label X)

CSV dialect overrides

Final Instruction to Assistant

Preserve the mental model first,
then optimize or extend without regressions.