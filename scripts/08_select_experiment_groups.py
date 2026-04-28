from __future__ import annotations

import argparse

from pipeline_utils import require

config = __import__("00_config")


def take_group(frame, name: str, sample_size: int, random_state: int):
    if len(frame) == 0:
        return frame.assign(group_name=name)
    size = min(sample_size, len(frame))
    return frame.sample(n=size, random_state=random_state).assign(group_name=name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Select PageRank, random, and optional category groups.")
    parser.add_argument("--group-size", type=int, default=config.GROUP_SIZE)
    args = parser.parse_args()

    pd = require("pandas")
    pagerank = pd.read_parquet(config.processed_path("pagerank.parquet"))
    articles = pd.read_parquet(config.processed_path("articles_clean.parquet"), columns=["page_id", "word_count"])
    base = pagerank.merge(articles, on="page_id", how="inner")
    if len(base) == 0:
        raise SystemExit("No pages with clean article text are available.")

    groups = [
        base.nsmallest(args.group_size, "rank").assign(group_name="top_pagerank"),
        take_group(
            base[(base["percentile"] >= 0.45) & (base["percentile"] <= 0.55)],
            "median_pagerank",
            args.group_size,
            config.RANDOM_SEED,
        ),
        base.nlargest(args.group_size, "rank").assign(group_name="bottom_pagerank"),
        take_group(base, "random", args.group_size, config.RANDOM_SEED),
    ]

    category_path = config.processed_path("categorylinks.parquet")
    if category_path.exists():
        categories = pd.read_parquet(category_path)
        linktarget_path = config.processed_path("linktarget.parquet")
        if "cl_target_id" in categories.columns and linktarget_path.exists():
            linktargets = pd.read_parquet(linktarget_path, columns=["lt_id", "lt_namespace", "lt_title"])
            categories = categories.merge(
                linktargets[linktargets["lt_namespace"] == 14][["lt_id", "lt_title"]],
                left_on="cl_target_id",
                right_on="lt_id",
                how="inner",
            )
            categories = categories.rename(columns={"lt_title": "cl_to"})
        for group_name, category in config.CATEGORY_GROUPS.items():
            if "cl_to" not in categories.columns:
                continue
            ids = categories.loc[categories["cl_to"] == category, "cl_from"].drop_duplicates()
            category_pages = base[base["page_id"].isin(ids)]
            if len(category_pages) > 0:
                groups.append(take_group(category_pages, group_name, args.group_size, config.RANDOM_SEED))

    output = pd.concat(groups, ignore_index=True)
    output = output[["page_id", "title", "group_name", "pagerank", "rank", "percentile", "word_count"]]
    output.to_parquet(config.processed_path("experiment_groups.parquet"), index=False)
    print(output.groupby("group_name").size().to_string())


if __name__ == "__main__":
    main()
