from __future__ import annotations

import csv
import gzip
import importlib
import json
import re
import sys
from pathlib import Path
from typing import Iterable, Iterator


WORD_RE = re.compile(r"[A-Za-z0-9]+")


def require(package: str):
    try:
        return importlib.import_module(package)
    except ImportError as exc:
        raise SystemExit(
            f"Missing dependency '{package}'. Install project dependencies with: "
            f"python -m pip install -r requirements.txt"
        ) from exc


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def normalize_title(title: str) -> str:
    return title.replace(" ", "_")


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in WORD_RE.findall(text) if len(token) > 1]


def batched(iterable: Iterable, size: int) -> Iterator[list]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_sql_lines_gz(path: Path) -> Iterator[str]:
    with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as handle:
        for line in handle:
            yield line.rstrip("\n")


def parse_mysql_string(value: str) -> str:
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        value = value[1:-1]
    return (
        value.replace("\\'", "'")
        .replace('\\"', '"')
        .replace("\\\\", "\\")
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
        .replace("\\0", "\0")
    )


def coerce_sql_value(raw: str):
    raw = raw.strip()
    if raw.upper() == "NULL":
        return None
    if raw.startswith("'"):
        return parse_mysql_string(raw)
    try:
        return int(raw)
    except ValueError:
        try:
            return float(raw)
        except ValueError:
            return raw


def split_insert_tuples(values_sql: str) -> Iterator[list]:
    row = []
    token = []
    in_string = False
    escape = False
    in_row = False
    for char in values_sql:
        if not in_row:
            if char == "(":
                in_row = True
                row = []
                token = []
            continue
        if in_string:
            token.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == "'":
                in_string = False
            continue
        if char == "'":
            in_string = True
            token.append(char)
        elif char == ",":
            row.append(coerce_sql_value("".join(token)))
            token = []
        elif char == ")":
            row.append(coerce_sql_value("".join(token)))
            yield row
            in_row = False
            token = []
        else:
            token.append(char)


def extract_create_columns(sql_path: Path, table: str) -> list[str]:
    in_create = False
    columns = []
    for line in read_sql_lines_gz(sql_path):
        if line.startswith(f"CREATE TABLE `{table}`"):
            in_create = True
            continue
        if in_create and line.startswith(")"):
            break
        if in_create:
            match = re.match(r"\s*`([^`]+)`\s+", line)
            if match:
                columns.append(match.group(1))
    if not columns:
        raise ValueError(f"Could not find CREATE TABLE column list for {table} in {sql_path}.")
    return columns


def iter_insert_rows(sql_path: Path, table: str) -> Iterator[list]:
    prefix = f"INSERT INTO `{table}` VALUES "
    for line in read_sql_lines_gz(sql_path):
        if not line.startswith(prefix):
            continue
        values_sql = line[len(prefix) :].rstrip(";")
        yield from split_insert_tuples(values_sql)


def load_config_module():
    scripts_dir = Path(__file__).resolve().parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    return importlib.import_module("00_config")

