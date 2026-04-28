from __future__ import annotations

import argparse
from pathlib import Path

from pipeline_utils import extract_create_columns, iter_insert_rows, require

config = __import__("00_config")


TABLES = {
    "page": {
        "file": "enwiki-latest-page.sql.gz",
        "columns": ["page_id", "page_namespace", "page_title", "page_is_redirect", "page_len"],
        "output": "page.parquet",
    },
    "linktarget": {
        "file": "enwiki-latest-linktarget.sql.gz",
        "columns": ["lt_id", "lt_namespace", "lt_title"],
        "output": "linktarget.parquet",
    },
    "pagelinks": {
        "file": "enwiki-latest-pagelinks.sql.gz",
        "columns": ["pl_from", "pl_from_namespace", "pl_target_id"],
        "output": "pagelinks.parquet",
    },
    "categorylinks": {
        "file": "enwiki-latest-categorylinks.sql.gz",
        "columns": ["cl_from", "cl_target_id"],
        "output": "categorylinks.parquet",
        "optional": True,
    },
}


def convert_table(table: str, chunk_size: int) -> None:
    spec = TABLES[table]
    sql_path = config.raw_path(spec["file"], required=not spec.get("optional", False))
    if not sql_path.exists():
        print(f"Skipping optional table {table}; {sql_path.name} is not present.")
        return

    pd = require("pandas")
    pa = require("pyarrow")
    pq = require("pyarrow.parquet")

    source_columns = extract_create_columns(sql_path, table)
    keep_columns = spec["columns"]
    missing = [column for column in keep_columns if column not in source_columns]
    if missing:
        raise SystemExit(
            f"{table}: dump schema does not contain expected columns {missing}. "
            f"Available columns are: {source_columns}"
        )
    indexes = [source_columns.index(column) for column in keep_columns]
    output_path = config.processed_path(spec["output"])
    temp_output_path = output_path.with_suffix(output_path.suffix + ".tmp")

    writer = None
    buffer = []
    total = 0
    try:
        for row in iter_insert_rows(sql_path, table):
            buffer.append({column: row[index] for column, index in zip(keep_columns, indexes)})
            if len(buffer) >= chunk_size:
                frame = pd.DataFrame.from_records(buffer, columns=keep_columns)
                arrow_table = pa.Table.from_pandas(frame, preserve_index=False)
                writer = writer or pq.ParquetWriter(temp_output_path, arrow_table.schema)
                writer.write_table(arrow_table)
                total += len(buffer)
                print(f"{table}: wrote {total:,} rows")
                buffer.clear()
        if buffer:
            frame = pd.DataFrame.from_records(buffer, columns=keep_columns)
            arrow_table = pa.Table.from_pandas(frame, preserve_index=False)
            writer = writer or pq.ParquetWriter(temp_output_path, arrow_table.schema)
            writer.write_table(arrow_table)
            total += len(buffer)
    finally:
        if writer is not None:
            writer.close()
    Path(temp_output_path).replace(output_path)
    print(f"{table}: complete -> {output_path} ({total:,} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert selected Wikimedia SQL dumps to Parquet.")
    parser.add_argument("--table", choices=sorted(TABLES), required=True)
    parser.add_argument("--chunk-size", type=int, default=250_000)
    args = parser.parse_args()
    config.ensure_dirs()
    convert_table(args.table, args.chunk_size)


if __name__ == "__main__":
    main()
