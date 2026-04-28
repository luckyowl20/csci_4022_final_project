from __future__ import annotations

from pipeline_utils import require

config = __import__("00_config")


def main() -> None:
    duckdb = require("duckdb")
    pages = config.processed_path("pages_clean.parquet")
    linktarget = config.processed_path("linktarget.parquet")
    pagelinks = config.processed_path("pagelinks.parquet")
    output = config.processed_path("edges.parquet")

    query = f"""
    COPY (
      WITH clean_pages AS (
        SELECT page_id, title FROM read_parquet('{pages.as_posix()}')
      ),
      source_links AS (
        SELECT pl_from, pl_target_id
        FROM read_parquet('{pagelinks.as_posix()}')
        WHERE pl_from_namespace = 0
      ),
      targets AS (
        SELECT lt_id, lt_title
        FROM read_parquet('{linktarget.as_posix()}')
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
    ) TO '{output.as_posix()}' (FORMAT PARQUET)
    """
    con = duckdb.connect()
    con.execute(query)
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output.as_posix()}')").fetchone()[0]
    print(f"Wrote {count:,} directed edges to {output}")


if __name__ == "__main__":
    main()

