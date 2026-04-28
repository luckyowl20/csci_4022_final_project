from __future__ import annotations

import itertools
import json

from pipeline_utils import require, require_complete

config = __import__("00_config")


def jaccard(a: set[str], b: set[str]) -> float:
    union = len(a | b)
    return len(a & b) / union if union else 0.0


def main() -> None:
    pd = require("pandas")
    np = require("numpy")
    summary = pd.read_parquet(require_complete(config.processed_path("similarity_summary.parquet"), "similarity_summary.parquet"))
    pairs = pd.read_parquet(require_complete(config.processed_path("pairwise_similarity.parquet"), "pairwise_similarity.parquet"))
    groups = pd.read_parquet(require_complete(config.processed_path("experiment_groups.parquet"), "experiment_groups.parquet"))
    shingles = pd.read_parquet(require_complete(config.processed_path("shingles.parquet"), "shingles.parquet"))

    summary.to_csv(config.table_path("similarity_summary.csv"), index=False)
    lookup = summary.set_index("group_name")["mean_similarity"].to_dict()
    rows = [
        {
            "question": "Top PageRank diversity vs random",
            "metric": "mean_similarity_difference_top_minus_random",
            "value": lookup.get("top_pagerank", np.nan) - lookup.get("random", np.nan),
            "interpretation": "Negative values mean top PageRank pages are more diverse than random pages.",
        },
        {
            "question": "Top vs bottom PageRank similarity",
            "metric": "mean_similarity_difference_top_minus_bottom",
            "value": lookup.get("top_pagerank", np.nan) - lookup.get("bottom_pagerank", np.nan),
            "interpretation": "Negative values mean top PageRank pages are less similar than bottom PageRank pages.",
        },
        {
            "question": "MinHash validation",
            "metric": "mean_absolute_error_minhash_vs_exact",
            "value": float((pairs["jaccard_minhash"] - pairs["jaccard_exact"]).abs().mean()),
            "interpretation": "Lower values mean MinHash closely approximates exact Jaccard on selected pairs.",
        },
    ]
    pd.DataFrame(rows).to_csv(config.table_path("research_question_results.csv"), index=False)

    percentile_data = groups.merge(shingles, on=["page_id", "group_name"], how="inner")
    percentile_data["percentile_bin"] = pd.cut(
        percentile_data["percentile"],
        bins=np.linspace(0, 1, 11),
        include_lowest=True,
        labels=[f"{i * 10}-{(i + 1) * 10}%" for i in range(10)],
    )
    percentile_rows = []
    for bin_name, frame in percentile_data.groupby("percentile_bin", observed=True):
        records = [set(json.loads(value)) for value in frame["shingles"].tolist()]
        similarities = [jaccard(a, b) for a, b in itertools.combinations(records, 2)]
        if similarities:
            percentile_rows.append(
                {
                    "percentile_bin": str(bin_name),
                    "n_pages": len(records),
                    "n_pairs": len(similarities),
                    "mean_similarity": float(np.mean(similarities)),
                    "median_similarity": float(np.median(similarities)),
                    "std_similarity": float(np.std(similarities)),
                }
            )
    pd.DataFrame(percentile_rows).to_csv(config.table_path("pagerank_percentile_similarity.csv"), index=False)
    print("Wrote research question and percentile summary tables")


if __name__ == "__main__":
    main()
