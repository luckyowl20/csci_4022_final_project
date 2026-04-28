$ErrorActionPreference = "Stop"

python scripts\01_convert_sql_to_parquet.py --table page
python scripts\01_convert_sql_to_parquet.py --table linktarget
python scripts\01_convert_sql_to_parquet.py --table pagelinks
python scripts\01_convert_sql_to_parquet.py --table categorylinks

python scripts\02_build_pages.py
python scripts\03_build_edges.py
python scripts\04_extract_article_text.py
python scripts\05_join_text_with_pages.py

python scripts\06_graph_stats.py
python scripts\07_pagerank.py
python scripts\08_select_experiment_groups.py
python scripts\09_prepare_text_shingles.py
python scripts\10_compute_similarity.py
python scripts\11_analyze_results.py
python scripts\12_make_figures.py

