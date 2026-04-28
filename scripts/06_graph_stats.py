from __future__ import annotations

from pipeline_utils import require, require_complete, write_json

config = __import__("00_config")


def main() -> None:
    pd = require("pandas")
    pages_path = require_complete(config.processed_path("pages_clean.parquet"), "pages_clean.parquet")
    edges_path = require_complete(config.processed_path("edges.parquet"), "edges.parquet")
    pages = pd.read_parquet(pages_path, columns=["page_id"])
    edges = pd.read_parquet(edges_path)

    nodes = pages["page_id"]
    in_degree = edges.groupby("target_page_id").size()
    out_degree = edges.groupby("source_page_id").size()
    degree = pd.DataFrame({"page_id": nodes})
    degree["in_degree"] = degree["page_id"].map(in_degree).fillna(0).astype("int64")
    degree["out_degree"] = degree["page_id"].map(out_degree).fillna(0).astype("int64")

    stats = {
        "number_of_nodes": int(len(degree)),
        "number_of_edges": int(len(edges)),
        "average_in_degree": float(degree["in_degree"].mean()),
        "average_out_degree": float(degree["out_degree"].mean()),
        "median_in_degree": float(degree["in_degree"].median()),
        "median_out_degree": float(degree["out_degree"].median()),
        "max_in_degree": int(degree["in_degree"].max()),
        "max_out_degree": int(degree["out_degree"].max()),
        "number_of_dangling_nodes": int((degree["out_degree"] == 0).sum()),
        "percent_of_dangling_nodes": float((degree["out_degree"] == 0).mean() * 100),
        "number_of_isolated_pages": int(((degree["in_degree"] == 0) & (degree["out_degree"] == 0)).sum()),
    }
    write_json(config.processed_path("graph_stats.json"), stats)
    degree.describe().to_csv(config.table_path("degree_summary.csv"))
    print(f"Wrote graph stats for {stats['number_of_nodes']:,} nodes and {stats['number_of_edges']:,} edges")


if __name__ == "__main__":
    main()
