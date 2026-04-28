from __future__ import annotations

from pipeline_utils import mark_complete, replace_temp_output, require

config = __import__("00_config")


def main() -> None:
    pd = require("pandas")
    page_path = config.parquet_input("page.parquet")
    output_path = config.processed_path("pages_clean.parquet")
    pages = pd.read_parquet(page_path, columns=["page_id", "page_namespace", "page_title", "page_is_redirect", "page_len"])
    pages = pages[
        (pages["page_namespace"] == 0)
        & (pages["page_is_redirect"] == 0)
        & (pages["page_len"] > config.MIN_WORDS)
    ].copy()
    pages = pages.rename(columns={"page_title": "title"})
    pages = pages[["page_id", "title", "page_len"]].drop_duplicates("page_id")
    temp_output = output_path.with_suffix(output_path.suffix + ".tmp")
    pages.to_parquet(temp_output, index=False)
    replace_temp_output(temp_output, output_path)
    mark_complete(output_path, {"rows": len(pages)})
    print(f"Wrote {len(pages):,} clean article pages to {output_path}")


if __name__ == "__main__":
    main()
