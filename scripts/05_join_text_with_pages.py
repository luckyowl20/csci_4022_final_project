from __future__ import annotations

from pipeline_utils import require

config = __import__("00_config")


def main() -> None:
    duckdb = require("duckdb")
    pages = config.processed_path("pages_clean.parquet")
    articles = config.processed_path("articles_raw.parquet")
    output = config.processed_path("articles_clean.parquet")
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
          SELECT p.page_id, p.title, a.clean_text, a.word_count, p.page_len
          FROM read_parquet('{pages.as_posix()}') p
          JOIN read_parquet('{articles.as_posix()}') a USING (page_id)
          WHERE a.word_count >= {config.MIN_WORDS}
            AND a.clean_text IS NOT NULL
            AND length(a.clean_text) > 0
        ) TO '{output.as_posix()}' (FORMAT PARQUET)
        """
    )
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output.as_posix()}')").fetchone()[0]
    print(f"Wrote {count:,} clean text articles to {output}")


if __name__ == "__main__":
    main()

