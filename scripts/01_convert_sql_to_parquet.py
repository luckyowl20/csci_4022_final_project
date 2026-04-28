from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline_utils import extract_create_columns, is_complete, iter_insert_selected_rows, mark_complete, require

config = __import__("00_config")

LEGACY_PART_CHUNK_SIZE = 250_000


TABLES = {
    "page": {
        "file": "enwiki-latest-page.sql.gz",
        "columns": ["page_id", "page_namespace", "page_title", "page_is_redirect", "page_len"],
        "output": "page.parquet",
        "chunk_size": 250_000,
    },
    "linktarget": {
        "file": "enwiki-latest-linktarget.sql.gz",
        "columns": ["lt_id", "lt_namespace", "lt_title"],
        "output": "linktarget.parquet",
        "chunk_size": 250_000,
    },
    "pagelinks": {
        "file": "enwiki-latest-pagelinks.sql.gz",
        "columns": ["pl_from", "pl_from_namespace", "pl_target_id"],
        "output": "pagelinks.parquet",
        "part_files": True,
        "chunk_size": 250_000,
    },
    "categorylinks": {
        "file": "enwiki-latest-categorylinks.sql.gz",
        "columns": ["cl_from", "cl_target_id"],
        "output": "categorylinks.parquet",
        "optional": True,
        "part_files": True,
        "chunk_size": 250_000,
    },
}


def part_manifest_path(output_dir: Path) -> Path:
    return output_dir / "_manifest.json"


def load_part_manifest(output_dir: Path) -> dict[str, int]:
    manifest_path = part_manifest_path(output_dir)
    if not manifest_path.exists():
        return {}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {str(key): int(value) for key, value in payload.get("parts", {}).items()}


def write_part_manifest(output_dir: Path, part_rows: dict[str, int]) -> None:
    manifest_path = part_manifest_path(output_dir)
    temp_path = manifest_path.with_suffix(".json.tmp")
    payload = {"parts": dict(sorted(part_rows.items()))}
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(manifest_path)


def existing_contiguous_parts(output_dir: Path) -> list[Path]:
    parts = sorted(output_dir.glob("part-*.parquet"))
    contiguous = []
    for expected, part_path in enumerate(parts):
        if part_path.name != f"part-{expected:06d}.parquet":
            break
        contiguous.append(part_path)
    return contiguous


def make_arrow_table(pa, columns_data: dict[str, list], columns: list[str]):
    return pa.Table.from_pydict({column: columns_data[column] for column in columns})


def write_part(
    pq,
    pa,
    output_dir: Path,
    table: str,
    part_number: int,
    columns_data: dict[str, list],
    columns: list[str],
    row_count: int,
    compression: str,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    part_path = output_dir / f"part-{part_number:06d}.parquet"
    temp_path = output_dir / f"part-{part_number:06d}.parquet.tmp"
    arrow_table = make_arrow_table(pa, columns_data, columns)
    if arrow_table.num_rows != row_count:
        raise RuntimeError(f"{table}: part {part_number:06d} has {arrow_table.num_rows:,} rows, expected {row_count:,}.")
    pq.write_table(arrow_table, temp_path, compression=compression)
    temp_path.replace(part_path)
    print(f"{table}: wrote part {part_number:06d} ({row_count:,} rows)")


def validate_output(pq, table: str, output_path: Path, output_dir: Path, expected_rows: int, part_files: bool, manifest_rows: int | None = None) -> None:
    if part_files:
        actual_rows = manifest_rows
    else:
        actual_rows = pq.ParquetFile(output_path).metadata.num_rows
    if actual_rows is None:
        raise RuntimeError(f"{table}: missing part manifest; cannot validate output.")
    if actual_rows != expected_rows:
        raise RuntimeError(
            f"{table}: validation found {actual_rows:,} rows, expected {expected_rows:,}."
        )


def finalize_existing_parts(table: str, chunk_size: int) -> None:
    spec = TABLES[table]
    output_path = config.processed_path(spec["output"])
    output_dir = output_path.with_suffix(".parquet.parts")
    existing_parts = existing_contiguous_parts(output_dir)
    if not existing_parts:
        raise SystemExit(f"{table}: no existing part files found in {output_dir}.")

    pq = require("pyarrow.parquet")
    part_rows = load_part_manifest(output_dir)
    if not part_rows:
        existing_chunk_size = LEGACY_PART_CHUNK_SIZE if chunk_size != LEGACY_PART_CHUNK_SIZE else chunk_size
        for part_path in existing_parts[:-1]:
            part_rows[part_path.stem.removeprefix("part-")] = existing_chunk_size
        last_part = existing_parts[-1]
        part_rows[last_part.stem.removeprefix("part-")] = pq.ParquetFile(last_part).metadata.num_rows

    total = sum(part_rows.values())
    write_part_manifest(output_dir, part_rows)
    (output_dir / "_SUCCESS").write_text(f"{total}\n", encoding="utf-8")
    print(f"{table}: finalized existing parts -> {output_dir} ({total:,} rows)")


def existing_single_file_is_valid(pq, output_path: Path) -> int | None:
    if not output_path.exists():
        return None
    try:
        return int(pq.ParquetFile(output_path).metadata.num_rows)
    except Exception:
        return None


def convert_table(table: str, chunk_size: int, force: bool = False, compression: str = "snappy") -> None:
    spec = TABLES[table]
    sql_path = config.raw_path(spec["file"], required=not spec.get("optional", False))
    if not sql_path.exists():
        print(f"Skipping optional table {table}; {sql_path.name} is not present.")
        return

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
    part_files = spec.get("part_files", False)
    output_dir = output_path.with_suffix(".parquet.parts")
    if not force:
        if part_files and (output_dir / "_SUCCESS").exists():
            print(f"{table}: complete output already exists -> {output_dir}")
            return
        if not part_files:
            existing_rows = existing_single_file_is_valid(pq, output_path)
            if existing_rows is not None:
                if not is_complete(output_path):
                    mark_complete(output_path, {"rows": existing_rows, "finalized_existing": True})
                print(f"{table}: complete output already exists -> {output_path} ({existing_rows:,} rows)")
                return
    part_rows = load_part_manifest(output_dir) if part_files else {}
    existing_parts = existing_contiguous_parts(output_dir) if part_files else []
    resume_part_number = len(existing_parts)
    if part_files and existing_parts:
        print(f"{table}: resuming after {resume_part_number:,} existing part files")
        existing_chunk_size = LEGACY_PART_CHUNK_SIZE if not part_rows else chunk_size
        for part_path in existing_parts:
            part_rows.setdefault(part_path.stem.removeprefix("part-"), existing_chunk_size)

    writer = None
    columns_data = {column: [] for column in keep_columns}
    buffered_rows = 0
    total = 0
    part_number = 0
    skipped_rows = 0
    resume_rows = sum(part_rows.get(f"{part_number:06d}", chunk_size) for part_number in range(resume_part_number))
    try:
        for row in iter_insert_selected_rows(sql_path, table, indexes):
            if skipped_rows < resume_rows:
                skipped_rows += 1
                total += 1
                continue
            for column, value in zip(keep_columns, row):
                columns_data[column].append(value)
            buffered_rows += 1
            if buffered_rows >= chunk_size:
                if part_files:
                    write_part(
                        pq,
                        pa,
                        output_dir,
                        table,
                        part_number + resume_part_number,
                        columns_data,
                        keep_columns,
                        buffered_rows,
                        compression,
                    )
                    part_rows[f"{part_number + resume_part_number:06d}"] = buffered_rows
                    write_part_manifest(output_dir, part_rows)
                    part_number += 1
                else:
                    arrow_table = make_arrow_table(pa, columns_data, keep_columns)
                    writer = writer or pq.ParquetWriter(temp_output_path, arrow_table.schema, compression=compression)
                    writer.write_table(arrow_table)
                total += buffered_rows
                print(f"{table}: wrote {total:,} rows")
                columns_data = {column: [] for column in keep_columns}
                buffered_rows = 0
        if buffered_rows:
            if part_files:
                write_part(
                    pq,
                    pa,
                    output_dir,
                    table,
                    part_number + resume_part_number,
                    columns_data,
                    keep_columns,
                    buffered_rows,
                    compression,
                )
                part_rows[f"{part_number + resume_part_number:06d}"] = buffered_rows
                write_part_manifest(output_dir, part_rows)
                part_number += 1
            else:
                arrow_table = make_arrow_table(pa, columns_data, keep_columns)
                writer = writer or pq.ParquetWriter(temp_output_path, arrow_table.schema, compression=compression)
                writer.write_table(arrow_table)
            total += buffered_rows
    finally:
        if writer is not None:
            writer.close()
    if part_files:
        manifest_rows = sum(part_rows.values())
        if skipped_rows and skipped_rows < resume_rows and part_rows:
            last_part = f"{resume_part_number - 1:06d}"
            inferred_last_rows = part_rows[last_part] - (resume_rows - skipped_rows)
            if inferred_last_rows <= 0:
                raise RuntimeError(
                    f"{table}: existing parts imply {resume_rows:,} rows, but source only had {skipped_rows:,} rows."
                )
            part_rows[last_part] = inferred_last_rows
            write_part_manifest(output_dir, part_rows)
            manifest_rows = sum(part_rows.values())
            total = skipped_rows
            print(f"{table}: finalized existing partial part {last_part} with {inferred_last_rows:,} inferred rows")
        elif skipped_rows and not part_number and manifest_rows == total:
            print(f"{table}: no new rows found after existing parts")
        elif skipped_rows:
            total = skipped_rows + sum(
                rows for part_id, rows in part_rows.items() if int(part_id) >= resume_part_number
            )
            manifest_rows = sum(part_rows.values())
        validate_output(pq, table, output_path, output_dir, total, part_files, manifest_rows=manifest_rows)
        (output_dir / "_SUCCESS").write_text(f"{total}\n", encoding="utf-8")
    else:
        Path(temp_output_path).replace(output_path)
        validate_output(pq, table, output_path, output_dir, total, part_files)
        mark_complete(output_path, {"rows": total})
    final_path = output_dir if part_files else output_path
    print(f"{table}: complete -> {final_path} ({total:,} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert selected Wikimedia SQL dumps to Parquet.")
    parser.add_argument("--table", choices=sorted(TABLES), required=True)
    parser.add_argument("--chunk-size", type=int, default=None)
    parser.add_argument(
        "--finalize-existing",
        action="store_true",
        help="Trust existing contiguous part files, write their manifest and _SUCCESS marker, and exit.",
    )
    parser.add_argument("--force", action="store_true", help="Regenerate output even when a complete output already exists.")
    parser.add_argument("--compression", default="snappy", choices=["snappy", "zstd", "none"])
    args = parser.parse_args()
    config.ensure_dirs()
    if args.chunk_size is None:
        args.chunk_size = TABLES[args.table].get("chunk_size", 250_000)
    if args.finalize_existing:
        finalize_existing_parts(args.table, args.chunk_size)
        return
    compression = None if args.compression == "none" else args.compression
    convert_table(args.table, args.chunk_size, force=args.force, compression=compression)


if __name__ == "__main__":
    main()
