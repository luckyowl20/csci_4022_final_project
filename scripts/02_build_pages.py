from __future__ import annotations

from pipeline_utils import require

config = __import__("00_config")


def main() -> None:
    pd = require("pandas")
    page_path = config.processed_path("page.parquet")
    output_path = config.processed_path("pages_clean.parquet")
    pages = pd.read_parquet(page_path, columns=["page_id", "page_namespace", "page_title", "page_is_redirect", "page_len"])
    pages = pages[
        (pages["page_namespace"] == 0)
        & (pages["page_is_redirect"] == 0)
        & (pages["page_len"] > config.MIN_WORDS)
    ].copy()
    pages = pages.rename(columns={"page_title": "title"})
    pages = pages[["page_id", "title", "page_len"]].drop_duplicates("page_id")
    pages.to_parquet(output_path, index=False)
    print(f"Wrote {len(pages):,} clean article pages to {output_path}")


if __name__ == "__main__":
    main()

