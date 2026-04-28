from __future__ import annotations

import time

from pipeline_utils import require, write_json

config = __import__("00_config")


def main() -> None:
    np = require("numpy")
    pd = require("pandas")

    start = time.time()
    pages = pd.read_parquet(config.processed_path("pages_clean.parquet"))
    edges = pd.read_parquet(config.processed_path("edges.parquet"))
    pages = pages.reset_index(drop=True)
    n = len(pages)
    if n == 0:
        raise SystemExit("No pages found. Run 02_build_pages.py first.")

    page_to_idx = {int(page_id): idx for idx, page_id in enumerate(pages["page_id"].to_numpy())}
    indexed_edges = edges.assign(
        source_idx=edges["source_page_id"].map(page_to_idx),
        target_idx=edges["target_page_id"].map(page_to_idx),
    ).dropna(subset=["source_idx", "target_idx"])
    src = indexed_edges["source_idx"].astype("int64").to_numpy()
    dst = indexed_edges["target_idx"].astype("int64").to_numpy()

    out_degree = np.bincount(src, minlength=n).astype(float)
    in_degree = np.bincount(dst, minlength=n).astype(int)
    dangling = out_degree == 0
    rank = np.full(n, 1.0 / n)
    damping = config.PAGERANK_DAMPING

    final_error = float("inf")
    iterations = 0
    for iteration in range(1, config.PAGERANK_MAX_ITER + 1):
        new_rank = np.full(n, (1.0 - damping) / n)
        contribution = rank[src] / out_degree[src]
        np.add.at(new_rank, dst, damping * contribution)
        new_rank += damping * rank[dangling].sum() / n
        final_error = float(np.abs(new_rank - rank).sum())
        rank = new_rank
        iterations = iteration
        if final_error < config.PAGERANK_TOL:
            break

    result = pages[["page_id", "title"]].copy()
    result["pagerank"] = rank
    result["in_degree"] = in_degree
    result["out_degree"] = out_degree.astype(int)
    result = result.sort_values("pagerank", ascending=False).reset_index(drop=True)
    result["rank"] = result.index + 1
    result["percentile"] = 1.0 - (result["rank"] - 1) / max(len(result) - 1, 1)
    result = result[["page_id", "title", "pagerank", "rank", "percentile", "in_degree", "out_degree"]]
    result.to_parquet(config.processed_path("pagerank.parquet"), index=False)
    result.head(100).to_csv(config.table_path("top_100_pagerank.csv"), index=False)
    write_json(
        config.processed_path("pagerank_run_stats.json"),
        {"iterations": iterations, "final_error": final_error, "runtime_seconds": time.time() - start},
    )
    print(f"PageRank complete in {iterations} iterations; final L1 error={final_error:.3e}")


if __name__ == "__main__":
    main()
