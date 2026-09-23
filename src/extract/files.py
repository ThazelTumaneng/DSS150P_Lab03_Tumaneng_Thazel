from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    TODO:
    1. Create data/raw/run_id=<run_id>/.
    2. Copy customers.csv, products.json, and orders.csv from data/source/.
    3. Return the run-specific raw path.
    4. Do not modify source files in place.
    """
    try:
        source_dir = path_for("source_dir")
    except KeyError:
        source_dir = path_for("source")

    try:
        raw_base = path_for("raw_dir")
    except KeyError:
        raw_base = path_for("raw")

    # Construct and create destination: data/raw/run_id=<run_id>/
    raw_dir = raw_base / f"run_id={run_id}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Copy files immutably
    required_files = ["customers.csv", "products.json", "orders.csv"]
    for filename in required_files:
        src_file = source_dir / filename
        dest_file = raw_dir / filename
        shutil.copy2(src_file, dest_file)

    return raw_dir
