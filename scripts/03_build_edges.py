from __future__ import annotations

from pipeline_utils import mark_complete, replace_temp_output, require, require_complete

config = __import__("00_config")


def main() -> None:
    duckdb = require("duckdb")
    require_complete(config.processed_path("pages_clean.parquet"), "pages_clean.parquet")
    pages = config.duckdb_parquet_input("pages_clean.parquet")
    linktarget = config.duckdb_parquet_input("linktarget.parquet")
    pagelinks = config.duckdb_parquet_input("pagelinks.parquet")
    output = config.processed_path("edges.parquet")
    temp_output = output.with_suffix(output.suffix + ".tmp")

    query = f"""
    COPY (
      WITH clean_pages AS (
        SELECT page_id, title FROM read_parquet('{pages}')
      ),
      source_links AS (
        SELECT pl_from, pl_target_id
        FROM read_parquet('{pagelinks}')
        WHERE pl_from_namespace = 0
      ),
      targets AS (
        SELECT lt_id, lt_title
        FROM read_parquet('{linktarget}')
        WHERE lt_namespace = 0
      )
      SELECT DISTINCT
        source.page_id AS source_page_id,
        target.page_id AS target_page_id
      FROM source_links links
      JOIN clean_pages source ON links.pl_from = source.page_id
      JOIN targets lt ON links.pl_target_id = lt.lt_id
      JOIN clean_pages target ON lt.lt_title = target.title
      WHERE source.page_id <> target.page_id
    ) TO '{temp_output.as_posix()}' (FORMAT PARQUET)
    """
    con = duckdb.connect()
    con.execute(query)
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{temp_output.as_posix()}')").fetchone()[0]
    if count == 0:
        raise SystemExit("No graph edges were produced.")
    replace_temp_output(temp_output, output)
    mark_complete(output, {"rows": count})
    print(f"Wrote {count:,} directed edges to {output}")


if __name__ == "__main__":
    main()
