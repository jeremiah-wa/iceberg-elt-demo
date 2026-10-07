import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
EXTRACT_LOAD_DIR = REPO_ROOT / "extract_load"
TRANSFORM_DIR = REPO_ROOT / "transform"

# dbt's artifact directory, relative to transform/. The dagster container sets its own, so
# artifacts from a dbt run on the host (e.g. an IDE extension) don't clash with Linux ones.
DBT_TARGET_PATH = os.getenv("DBT_TARGET_PATH", "target")
