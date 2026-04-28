from __future__ import annotations

from pipeline_utils import require

config = __import__("00_config")


def main() -> None:
    pd = require("pandas")
    plt = require("matplotlib.pyplot")

    pagerank = pd.read_parquet(config.processed_path("pagerank.parquet"))
    summary = pd.read_parquet(config.processed_path("similarity_summary.parquet"))
    pairs = pd.read_parquet(config.processed_path("pairwise_similarity.parquet"))
    percentile = pd.read_csv(config.table_path("pagerank_percentile_similarity.csv"))

    plt.figure(figsize=(8, 5))
    pagerank["pagerank"].plot(kind="hist", bins=100, logy=True)
    plt.xlabel("PageRank")
    plt.ylabel("Pages (log scale)")
    plt.tight_layout()
    plt.savefig(config.figure_path("pagerank_distribution.png"), dpi=200)
    plt.close()

    top20 = pagerank.nsmallest(20, "rank").sort_values("pagerank")
    plt.figure(figsize=(10, 6))
    plt.barh(top20["title"], top20["pagerank"])
    plt.xlabel("PageRank")
    plt.tight_layout()
    plt.savefig(config.figure_path("top_20_pagerank.png"), dpi=200)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.bar(summary["group_name"], summary["mean_similarity"])
    plt.ylabel("Mean exact Jaccard similarity")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(config.figure_path("similarity_by_group_bar.png"), dpi=200)
    plt.close()

    plt.figure(figsize=(10, 5))
    order = summary["group_name"].tolist()
    pairs.boxplot(column="jaccard_exact", by="group_name", grid=False, rot=30)
    plt.suptitle("")
    plt.title("Pairwise similarity by group")
    plt.ylabel("Exact Jaccard similarity")
    plt.tight_layout()
    plt.savefig(config.figure_path("similarity_by_group_boxplot.png"), dpi=200)
    plt.close()

    if not percentile.empty:
        plt.figure(figsize=(8, 5))
        plt.plot(percentile["percentile_bin"], percentile["mean_similarity"], marker="o")
        plt.ylabel("Mean exact Jaccard similarity")
        plt.xlabel("PageRank percentile bin")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        plt.savefig(config.figure_path("pagerank_percentile_similarity.png"), dpi=200)
        plt.close()

    plt.figure(figsize=(6, 6))
    plt.scatter(pairs["jaccard_exact"], pairs["jaccard_minhash"], s=6, alpha=0.35)
    plt.xlabel("Exact Jaccard")
    plt.ylabel("MinHash estimate")
    plt.tight_layout()
    plt.savefig(config.figure_path("minhash_vs_exact_jaccard.png"), dpi=200)
    plt.close()

    print(f"Wrote figures to {config.FIGURES_DIR}")


if __name__ == "__main__":
    main()

