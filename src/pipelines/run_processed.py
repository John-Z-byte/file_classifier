from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.pipelines.consolidate_schema import consolidate_schema_from_classified
from src.pipelines.transforms.wellsky import add_franchise_columns
from src.pipelines.sanitize import sanitize_for_parquet

import warnings
warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style.*",
    category=UserWarning,
    module="openpyxl"
)


def run_processed() -> None:
    staging_dir = Path("data/staging")
    classified_dir = Path("data/classified")
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)

    catalog_path = staging_dir / "file_catalog.parquet"
    if not catalog_path.exists():
        raise FileNotFoundError(f"Missing: {catalog_path}")

    catalog_df = pd.read_parquet(catalog_path).copy()

    # Only OK + labeled schemas. Skip unknown_schema
    ok = catalog_df[catalog_df["status"].eq("ok")].copy()
    ok = ok[ok["label"].notna()]
    ok = ok[ok["label"].ne("unknown_schema")]

    if ok.empty:
        print("No OK labeled schemas found (nothing to process).")
        return

    # Work list: unique (label, schema_hash)
    work = (
        ok[["label", "schema_hash"]]
        .drop_duplicates()
        .sort_values(["label", "schema_hash"])
        .to_dict(orient="records")
    )

    wrote = 0
    skipped = 0
    failed = 0

    for item in work:
        label = str(item["label"])
        schema_hash = str(item["schema_hash"])

        try:
            df = consolidate_schema_from_classified(
                classified_dir=classified_dir,
                catalog_df=catalog_df,
                label=label,
                schema_hash=schema_hash,
            )
        except FileNotFoundError as e:
            print(f"SKIP (no classified files): {label} {schema_hash} -> {e}")
            skipped += 1
            continue
        except Exception as e:
            print(f"FAIL (consolidate): {label} {schema_hash} -> {type(e).__name__}: {e}")
            failed += 1
            continue

        # Source-specific transforms
        try:
            if label.startswith("wellsky"):
                df = add_franchise_columns(df)
        except Exception as e:
            print(f"FAIL (transform): {label} {schema_hash} -> {type(e).__name__}: {e}")
            failed += 1
            continue

        df = sanitize_for_parquet(df)

        # Write output
        try:
            out_path = processed_dir / f"{label}__{schema_hash}.parquet"
            df.to_parquet(out_path, index=False)
            wrote += 1
            print(f"Wrote: {out_path} (rows={len(df):,})")
        except Exception as e:
            print(f"FAIL (write): {label} {schema_hash} -> {type(e).__name__}: {e}")
            failed += 1
            continue

    print("\nProcessed run summary:")
    print(f"- wrote:   {wrote}")
    print(f"- skipped: {skipped}")
    print(f"- failed:  {failed}")


if __name__ == "__main__":
    run_processed()
