from __future__ import annotations
from tabulate import tabulate

import warnings
warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style",
    category=UserWarning,
)

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.config_loader import load_config
from src.classify.file_copier import copy_file, prepare_snapshot_folders
from src.fingerprint.header_detector import detect_header_row
from src.fingerprint.header_normalizer import load_header_aliases, normalize_headers
from src.io.preview_reader import read_excel_preview, read_csv_preview
from src.io.scanner import scan_files
from src.labeling.schema_labels import load_schema_labels

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="file_classifier")
    p.add_argument("--config", default="config/settings.yaml", help="Path to YAML config")
    p.add_argument("--input-root", default=None, help="Override input_root")
    p.add_argument("--output-root", default=None, help="Override output_root")
    p.add_argument("--header-search-rows", type=int, default=None, help="Override excel.header_search_rows")
    p.add_argument("--min-confidence", type=float, default=None, help="Override header_detection.min_header_confidence")
    p.add_argument("--overwrite", action="store_true", help="Allow overwriting destination files")
    p.add_argument("--dry-run", action="store_true", help="Do not copy files, only write parquet artifacts")
    return p.parse_args()

def _build_overrides(args: argparse.Namespace) -> Dict[str, Any]:
    o: Dict[str, Any] = {}

    if args.input_root is not None:
        o["input_root"] = args.input_root

    if args.output_root is not None:
        o["output_root"] = args.output_root
        out = Path(args.output_root)
        o.setdefault("paths", {})
        o["paths"]["staging_dir"] = str(out / "staging")
        o["paths"]["classified_dir"] = str(out / "classified")
        o["paths"]["quarantine_dir"] = str(out / "quarantine")

    if args.header_search_rows is not None:
        o.setdefault("excel", {})
        o["excel"]["header_search_rows"] = args.header_search_rows

    if args.min_confidence is not None:
        o.setdefault("header_detection", {})
        o["header_detection"]["min_header_confidence"] = args.min_confidence

    if args.overwrite:
        o.setdefault("copy", {})
        o["copy"]["overwrite"] = True

    if args.dry_run:
        o.setdefault("copy", {})
        o["copy"]["dry_run"] = True

    return o

def count_excel_rows(path: Path) -> int:
    # Lee solo la primera columna para contar filas
    df = pd.read_excel(path, usecols=[0])
    return max(len(df), 0)

def count_csv_rows(path: Path) -> int:
    with open(path, "rb") as f:
        return sum(1 for _ in f) - 1  # menos header

def main() -> None:
    args = _parse_args()
    cfg = load_config(args.config, overrides=_build_overrides(args))

    # Config
    input_root = cfg["input_root"]
    header_search_rows = int(cfg["excel"]["header_search_rows"])
    min_header_confidence = float(cfg["header_detection"]["min_header_confidence"])
    extensions = cfg.get("extensions", [".xlsx", ".csv"])  # CSV added
    overwrite = bool(cfg["copy"]["overwrite"])
    dry_run = bool(cfg["copy"]["dry_run"])

    # Output dirs
    staging_dir = Path(cfg["paths"]["staging_dir"])
    classified_dir = Path(cfg["paths"]["classified_dir"])
    quarantine_dir = Path(cfg["paths"]["quarantine_dir"])
    _ensure_dir(staging_dir)
    _ensure_dir(classified_dir)
    _ensure_dir(quarantine_dir)

    # Aliases + schema labels
    aliases = load_header_aliases("./config/header_aliases.yaml")
    schema_labels = load_schema_labels("./config/schema_labels.yaml")  # {schema_hash: label}

    # Scan
    files = scan_files(input_root, extensions)

    status_counts = Counter()
    schema_to_files = defaultdict(list)  # schema_key -> list[path]
    schema_to_headers = {}               # schema_key -> canonical headers
    schema_to_hash = {}                  # schema_key -> schema_hash
    catalog_rows = []
    run_ts = datetime.now().isoformat(timespec="seconds")

    # --- Build catalog rows + schema grouping ---
    for f in files:
        if f.path.suffix.lower() == ".csv":
            prev = read_csv_preview(f.path, header_search_rows)
        else:
            prev = read_excel_preview(f.path, header_search_rows)

        row = {
            "run_ts": run_ts,
            "path": str(f.path),
            "size_bytes": f.size_bytes,
            "modified_ts": f.modified_ts,
            "sheet_name": prev.sheet_name,
            "status": None,
            "error_message": prev.error_message,
            "header_row_index": None,
            "header_confidence": None,
            "raw_headers_json": None,
            "normalized_headers_json": None,
            "schema_key": None,
            "schema_hash": None,
            "schema_id": None,
            "label": None,
        }

        if prev.status != "ok":
            row["status"] = "unreadable"
            status_counts["unreadable"] += 1
            catalog_rows.append(row)
            continue

        det = detect_header_row(prev.rows, min_header_confidence)
        row["header_row_index"] = det.header_row_index
        row["header_confidence"] = float(det.confidence)

        if det.header_row_index is None:
            row["status"] = "low_confidence"
            status_counts["low_confidence"] += 1
            catalog_rows.append(row)
            continue

        # Normalize + aliases
        norm = normalize_headers(det.raw_headers, aliases=aliases)
        raw_headers = list(det.raw_headers)
        normalized_headers = list(norm.normalized_headers)

        row["raw_headers_json"] = json.dumps(raw_headers, ensure_ascii=False)
        row["normalized_headers_json"] = json.dumps(normalized_headers, ensure_ascii=False)

        # Schema = exact set of normalized headers (order ignored)
        canonical = tuple(sorted(set(normalized_headers)))
        schema_key = "|".join(canonical)  # stable string key for audit
        schema_hash = hashlib.sha1(schema_key.encode("utf-8")).hexdigest()[:12]

        row["schema_key"] = schema_key
        row["schema_hash"] = schema_hash

        # label by hash (stable identity)
        row["label"] = schema_labels.get(schema_hash, "unknown_schema")

        schema_to_files[schema_key].append(str(f.path))
        schema_to_headers[schema_key] = list(canonical)
        schema_to_hash[schema_key] = schema_hash

        row["status"] = "ok"
        status_counts["ok"] += 1
        catalog_rows.append(row)

    # --- Assign schema_ids deterministically (by schema_key) ---
    schema_keys_sorted = sorted(schema_to_files.keys())
    schema_id_map = {
        k: f"schema_{i:03d}__{schema_to_hash[k]}"
        for i, k in enumerate(schema_keys_sorted, start=1)
    }

    # Update catalog with schema_id
    for r in catalog_rows:
        k = r.get("schema_key")
        if k in schema_id_map:
            r["schema_id"] = schema_id_map[k]

    # --- Build schema registry ---
    registry_rows = []
    for k in schema_keys_sorted:
        files_list = schema_to_files[k]
        registry_rows.append(
            {
                "run_ts": run_ts,
                "schema_id": schema_id_map[k],
                "schema_key": k,
                "schema_hash": schema_to_hash[k],
                "canonical_headers_json": json.dumps(schema_to_headers[k], ensure_ascii=False),
                "file_count": len(files_list),
                "example_files_json": json.dumps(files_list[:5], ensure_ascii=False),
            }
        )

    # --- Write staging parquet outputs ---
    catalog_df = pd.DataFrame(catalog_rows)
    registry_df = pd.DataFrame(registry_rows)

    catalog_path = staging_dir / "file_catalog.parquet"
    registry_path = staging_dir / "schema_registry.parquet"

    catalog_df.to_parquet(catalog_path, index=False)
    registry_df.to_parquet(registry_path, index=False)

    # --- Unknown schemas report (after catalog is built) ---
    unknown_ok = [r for r in catalog_rows if r.get("status") == "ok" and r.get("label") == "unknown_schema"]
    if unknown_ok:
        seen = set()
        print("\nUNKNOWN schemas detected (add these to config/schema_labels.yaml):")
        for r in unknown_ok:
            h = r["schema_hash"]
            if h in seen:
                continue
            seen.add(h)
            print(f"- {h}")

    # --- Classification (copy files) + manifest ---
    manifest_rows = []
    copy_counts = Counter()

    if not dry_run:
        # SNAPSHOT MODE: wipe gold folders for labels that appear in THIS run (latest snapshot)
        labels_in_run = sorted(
            {r["label"] for r in catalog_rows if r.get("status") == "ok" and r.get("label")}
        )
        prepare_snapshot_folders(classified_dir, labels_in_run)

        for r in catalog_rows:
            src_path = Path(r["path"])
            src_status = r["status"]

            if src_status == "ok":
                label = r["label"] or "unknown_schema"
                schema_hash = r["schema_hash"] or "nohash"
                dest_dir = classified_dir / label  # FLAT: no schema subfolder
                dest_name = f"{schema_hash}__{src_path.name}"  # keep identity, avoid collisions
                dest_path = dest_dir / dest_name
            else:
                dest_path = quarantine_dir / src_path.name

            res = copy_file(src_path, dest_path, overwrite=overwrite)

            manifest_rows.append(
                {
                    "run_ts": run_ts,
                    "src_path": str(src_path),
                    "dst_path": str(res.dst),
                    "src_status": src_status,
                    "copy_status": res.status,
                    "error_message": res.error_message,
                    "schema_id": r.get("schema_id"),
                    "schema_key": r.get("schema_key"),
                    "schema_hash": r.get("schema_hash"),
                    "label": r.get("label"),
                }
            )
            copy_counts[res.status] += 1
    else:
        for r in catalog_rows:
            manifest_rows.append(
                {
                    "run_ts": run_ts,
                    "src_path": r["path"],
                    "dst_path": None,
                    "src_status": r["status"],
                    "copy_status": "skipped_dry_run",
                    "error_message": None,
                    "schema_id": r.get("schema_id"),
                    "schema_key": r.get("schema_key"),
                    "schema_hash": r.get("schema_hash"),
                    "label": r.get("label"),
                }
            )
        copy_counts["skipped_dry_run"] = len(catalog_rows)

    manifest_df = pd.DataFrame(manifest_rows)
    manifest_path = staging_dir / "classification_manifest.parquet"
    manifest_df.to_parquet(manifest_path, index=False)

        # --- Console summary ---
    print(f"Total files: {len(files)}")
    print(f"Total schemas (distinct normalized header sets): {len(schema_id_map)}")
    print(f"Status counts: {dict(status_counts)}")
    print(f"Copy results: {dict(copy_counts)}")
    print(f"Wrote: {catalog_path}")
    print(f"Wrote: {registry_path}")
    print(f"Wrote: {manifest_path}")

    # --- Schema summary (from registry parquet) ---
    reg = pd.read_parquet(registry_path).copy()

    # headers count
    reg["headers_count"] = reg["canonical_headers_json"].apply(
        lambda s: len(json.loads(s)) if isinstance(s, str) else 0
    )

    # rows_count (CHEAP): number of files per schema (not data rows)
    rows_per_schema = (
        catalog_df.groupby("schema_id")
        .size()
        .rename("rows_count")
        .reset_index()
    )
    reg = reg.merge(rows_per_schema, on="schema_id", how="left")

    # attach label from catalog
    cat_labels = (
        catalog_df[["schema_id", "label"]]
        .dropna()
        .drop_duplicates()
    )
    reg = reg.merge(cat_labels, on="schema_id", how="left")

    from tabulate import tabulate

    reg_show = reg.sort_values(
        ["file_count", "headers_count", "label"],
        ascending=[False, False, True],
    )

    view = (
        reg_show[["label", "file_count", "headers_count", "rows_count"]]
        .rename(columns={
            "label": "schema",
            "file_count": "files",
            "headers_count": "headers",
            "rows_count": "rows",
        })
        .reset_index(drop=True)
    )

    print("\nSchema summary:")
    print(tabulate(view, headers="keys", tablefmt="psql", showindex=False))

if __name__ == "__main__":
    main()
