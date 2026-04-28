from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "scripts" / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"
FIGURES_DIR = RESULTS_DIR / "figures"
TABLES_DIR = RESULTS_DIR / "tables"

MIN_WORDS = 100

PAGERANK_DAMPING = 0.85
PAGERANK_TOL = 1e-8
PAGERANK_MAX_ITER = 100

GROUP_SIZE = 500
SHINGLE_SIZE = 3
MINHASH_PERMUTATIONS = 128
RANDOM_SEED = 4022

CATEGORY_GROUPS = {
    "category_mathematics": "Mathematics",
    "category_physics": "Physics",
    "category_computer_science": "Computer_science",
    "category_history": "History",
    "category_biology": "Biology",
    "category_philosophy": "Philosophy",
}


def ensure_dirs() -> None:
    for directory in (RAW_DIR, PROCESSED_DIR, RESULTS_DIR, FIGURES_DIR, TABLES_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def raw_path(filename: str, required: bool = True) -> Path:
    path = RAW_DIR / filename
    if path.exists():
        return path
    if required:
        raise FileNotFoundError(f"Could not find {filename} in {RAW_DIR}.")
    return path


def processed_path(filename: str) -> Path:
    ensure_dirs()
    return PROCESSED_DIR / filename


def parquet_input(filename: str) -> Path:
    parts_dir = processed_path(filename).with_suffix(".parquet.parts")
    if (parts_dir / "_SUCCESS").exists():
        return parts_dir
    return processed_path(filename)


def duckdb_parquet_input(filename: str) -> str:
    path = parquet_input(filename)
    if path.is_dir():
        return (path / "*.parquet").as_posix()
    return path.as_posix()


def table_path(filename: str) -> Path:
    ensure_dirs()
    return TABLES_DIR / filename


def figure_path(filename: str) -> Path:
    ensure_dirs()
    return FIGURES_DIR / filename
