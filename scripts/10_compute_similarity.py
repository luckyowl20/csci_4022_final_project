from __future__ import annotations

import itertools
import json

from pipeline_utils import mark_complete, replace_temp_output, require, require_complete

config = __import__("00_config")


def exact_jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    union = len(a | b)
    return len(a & b) / union if union else 0.0


def minhash_jaccard(a: list[int], b: list[int]) -> float:
    if not a or not b:
        return 0.0
    length = min(len(a), len(b))
    return sum(1 for i in range(length) if a[i] == b[i]) / length


def main() -> None:
    pd = require("pandas")
    shingles = pd.read_parquet(require_complete(config.processed_path("shingles.parquet"), "shingles.parquet"))
    signatures = pd.read_parquet(
        require_complete(config.processed_path("minhash_signatures.parquet"), "minhash_signatures.parquet")
    )
    groups = pd.read_parquet(require_complete(config.processed_path("experiment_groups.parquet"), "experiment_groups.parquet"))
    data = groups.merge(shingles, on=["page_id", "group_name"]).merge(signatures, on=["page_id", "group_name"])

    pair_rows = []
    for group_name, frame in data.groupby("group_name"):
        records = []
        for row in frame.itertuples(index=False):
            records.append(
                {
                    "page_id": row.page_id,
                    "title": row.title,
                    "shingles": set(json.loads(row.shingles)),
                    "signature": json.loads(row.signature),
                }
            )
        for a, b in itertools.combinations(records, 2):
            pair_rows.append(
                {
                    "group_name": group_name,
                    "page_id_a": a["page_id"],
                    "page_id_b": b["page_id"],
                    "title_a": a["title"],
                    "title_b": b["title"],
                    "jaccard_exact": exact_jaccard(a["shingles"], b["shingles"]),
                    "jaccard_minhash": minhash_jaccard(a["signature"], b["signature"]),
                }
            )

    pairs = pd.DataFrame(pair_rows)
    pairs_path = config.processed_path("pairwise_similarity.parquet")
    pairs_temp = pairs_path.with_suffix(pairs_path.suffix + ".tmp")
    pairs.to_parquet(pairs_temp, index=False)
    replace_temp_output(pairs_temp, pairs_path)
    mark_complete(pairs_path, {"rows": len(pairs)})
    summary = (
        pairs.groupby("group_name")["jaccard_exact"]
        .agg(
            n_pairs="count",
            mean_similarity="mean",
            median_similarity="median",
            std_similarity="std",
            min_similarity="min",
            max_similarity="max",
            q25_similarity=lambda s: s.quantile(0.25),
            q75_similarity=lambda s: s.quantile(0.75),
        )
        .reset_index()
    )
    counts = data.groupby("group_name")["page_id"].nunique().rename("n_pages")
    summary = summary.merge(counts, on="group_name")
    summary = summary[["group_name", "n_pages", "n_pairs", "mean_similarity", "median_similarity", "std_similarity", "min_similarity", "max_similarity", "q25_similarity", "q75_similarity"]]
    summary_path = config.processed_path("similarity_summary.parquet")
    summary_temp = summary_path.with_suffix(summary_path.suffix + ".tmp")
    summary.to_parquet(summary_temp, index=False)
    replace_temp_output(summary_temp, summary_path)
    mark_complete(summary_path, {"rows": len(summary)})
    summary.to_csv(config.table_path("similarity_summary.csv"), index=False)
    print(f"Wrote {len(pairs):,} pairwise similarity rows")


if __name__ == "__main__":
    main()
