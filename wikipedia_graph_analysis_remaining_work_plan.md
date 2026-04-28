# Wikipedia Graph Analysis Project: Remaining Work Plan

This document outlines the remaining data preparation and analysis scripts needed for the project:

**Project goal:** Analyze Wikipedia as a directed graph where pages are nodes and hyperlinks are directed edges, compute PageRank using power iteration, then compare semantic similarity across PageRank-based groups using MinHash/Jaccard similarity.

The project should answer:

1. Does selecting top-ranked pages by PageRank produce a more semantically diverse set than selecting pages by category or random sampling?
2. Are highly ranked pages more or less similar to each other than lower-ranked pages?
3. How does similarity vary across PageRank percentiles?

---

## 1. Expected Project Structure

```text
wiki-graph-project/
  README.md
  requirements.txt
  .gitignore

  data/
    raw/
      enwiki-latest-pages-articles-multistream.xml.bz2
      enwiki-latest-pages-articles-multistream-index.txt.bz2
      enwiki-latest-page.sql.gz
      enwiki-latest-linktarget.sql.gz
      enwiki-latest-pagelinks.sql.gz
      enwiki-latest-redirect.sql.gz
      enwiki-latest-categorylinks.sql.gz

    processed/
      page.parquet
      linktarget.parquet
      pagelinks.parquet
      categorylinks.parquet
      pages_clean.parquet
      edges.parquet
      articles_raw.parquet
      articles_clean.parquet
      graph_stats.json
      pagerank.parquet
      experiment_groups.parquet
      shingles.parquet
      minhash_signatures.parquet
      pairwise_similarity.parquet
      similarity_summary.parquet

    results/
      figures/
        pagerank_distribution.png
        top_20_pagerank.png
        similarity_by_group_bar.png
        similarity_by_group_boxplot.png
        pagerank_percentile_similarity.png
        minhash_vs_exact_jaccard.png

      tables/
        degree_summary.csv
        top_100_pagerank.csv
        research_question_results.csv
        similarity_summary.csv
        pagerank_percentile_similarity.csv

  scripts/
    00_config.py
    01_convert_sql_to_parquet.py
    02_build_pages.py
    03_build_edges.py
    04_extract_article_text.py
    05_join_text_with_pages.py
    06_graph_stats.py
    07_pagerank.py
    08_select_experiment_groups.py
    09_prepare_text_shingles.py
    10_compute_similarity.py
    11_analyze_results.py
    12_make_figures.py
    run_pipeline.ps1

  notebooks/
    01_data_exploration.ipynb
    02_pagerank_analysis.ipynb
    03_similarity_analysis.ipynb
```

---

## 2. Raw Data Inputs

The raw data should already be downloading into:

```text
data/raw/
```

Required files:

```text
enwiki-latest-pages-articles-multistream.xml.bz2
enwiki-latest-page.sql.gz
enwiki-latest-linktarget.sql.gz
enwiki-latest-pagelinks.sql.gz
```

Strongly recommended:

```text
enwiki-latest-redirect.sql.gz
```

Optional but useful for category experiments:

```text
enwiki-latest-categorylinks.sql.gz
```

---

## 3. Script-by-Script Plan

---

# `00_config.py`

## Goal

Store shared paths and experiment settings in one place.

## Purpose

Avoid hardcoding paths, PageRank settings, group sizes, shingle sizes, and random seeds across multiple scripts.

## Key settings

```python
PROJECT_ROOT
RAW_DIR
PROCESSED_DIR
RESULTS_DIR
FIGURES_DIR
TABLES_DIR

MIN_WORDS = 100

PAGERANK_DAMPING = 0.85
PAGERANK_TOL = 1e-8
PAGERANK_MAX_ITER = 100

GROUP_SIZE = 500
SHINGLE_SIZE = 3
MINHASH_PERMUTATIONS = 128
RANDOM_SEED = 4022
```

## Inputs

None.

## Outputs

None.

---

# `01_convert_sql_to_parquet.py`

## Goal

Convert large Wikimedia SQL dump files into smaller, easier-to-query Parquet files.

## Why this script is needed

The raw `.sql.gz` files are huge and difficult to query directly. Converting them to Parquet makes later analysis much faster and avoids requiring MySQL, MariaDB, or WSL.

## Inputs

```text
data/raw/enwiki-latest-page.sql.gz
data/raw/enwiki-latest-linktarget.sql.gz
data/raw/enwiki-latest-pagelinks.sql.gz
data/raw/enwiki-latest-categorylinks.sql.gz
```

## Outputs

```text
data/processed/page.parquet
data/processed/linktarget.parquet
data/processed/pagelinks.parquet
data/processed/categorylinks.parquet
```

## Columns to keep

From `page.sql.gz`:

```text
page_id
page_namespace
page_title
page_is_redirect
page_len
```

From `linktarget.sql.gz`:

```text
lt_id
lt_namespace
lt_title
```

From `pagelinks.sql.gz`:

```text
pl_from
pl_from_namespace
pl_target_id
```

From `categorylinks.sql.gz`:

```text
cl_from
cl_to
```

## Commands

```powershell
python scripts\01_convert_sql_to_parquet.py --table page
python scripts\01_convert_sql_to_parquet.py --table linktarget
python scripts\01_convert_sql_to_parquet.py --table pagelinks
python scripts\01_convert_sql_to_parquet.py --table categorylinks
```

## Notes

This script should only convert data. It should not run PageRank or similarity analysis.

---

# `02_build_pages.py`

## Goal

Create the final list of valid Wikipedia article pages.

## Why this script is needed

Wikipedia dumps include non-article pages, redirects, templates, talk pages, categories, and other namespaces. For this project, use only normal article pages.

## Input

```text
data/processed/page.parquet
```

## Output

```text
data/processed/pages_clean.parquet
```

## Cleaning rules

Keep pages where:

```text
page_namespace == 0
page_is_redirect == 0
page_len > 100
```

## Output columns

```text
page_id
page_title
page_len
```

## Notes

This cleaned page table defines the allowed nodes for the graph.

---

# `03_build_edges.py`

## Goal

Build the directed Wikipedia graph edge list.

Each row should represent:

```text
source_page_id -> target_page_id
```

## Why this script is needed

PageRank requires a directed graph. The graph is built from Wikipedia internal hyperlinks.

## Inputs

```text
data/processed/pages_clean.parquet
data/processed/linktarget.parquet
data/processed/pagelinks.parquet
```

## Output

```text
data/processed/edges.parquet
```

## Main join logic

Join:

```text
pagelinks.pl_target_id = linktarget.lt_id
linktarget.lt_title = pages_clean.page_title
```

Then keep only edges where:

```text
source page is in pages_clean
target page is in pages_clean
source_page_id != target_page_id
```

## Output columns

```text
source_page_id
target_page_id
```

## Important details

Use `DISTINCT` or another deduplication method so repeated links from one article to another count as one edge.

---

# `04_extract_article_text.py`

## Goal

Extract article text from the compressed Wikipedia XML dump.

## Why this script is needed

PageRank uses links, but the similarity analysis requires article content.

## Input

```text
data/raw/enwiki-latest-pages-articles-multistream.xml.bz2
```

## Output

```text
data/processed/articles_raw.parquet
```

## Output columns

```text
page_id
title
raw_wikitext
clean_text
word_count
```

## Cleaning steps

At minimum:

```text
remove wiki markup
remove templates if possible
remove references if possible
normalize whitespace
count words
```

## Text cleaning approach

Use the pure-Python cleaner in `scripts/04_extract_article_text.py`.

Avoid `mwparserfromhell` for this project because it may try to compile a C
extension on Windows/Python versions without a prebuilt wheel. The in-repo
cleaner removes common templates, references, tables, HTML tags, file/category
links, headings, and basic wiki markup well enough for the shingle-based
similarity analysis.

## Useful debug option

Add support for:

```powershell
python scripts\04_extract_article_text.py --limit 10000
```

This lets the group test extraction without processing the whole dump.

---

# `05_join_text_with_pages.py`

## Goal

Create the final clean article-text dataset.

## Why this script is needed

Not every article in the XML should be used. This script joins text with the clean page table and removes unusable articles.

## Inputs

```text
data/processed/pages_clean.parquet
data/processed/articles_raw.parquet
```

## Output

```text
data/processed/articles_clean.parquet
```

## Cleaning rules

Keep rows where:

```text
page_id exists in pages_clean
word_count >= 100
clean_text is not null
```

## Output columns

```text
page_id
title
clean_text
word_count
page_len
```

## Notes

This is the main input for MinHash/Jaccard document similarity.

---

# `06_graph_stats.py`

## Goal

Compute basic graph statistics before PageRank.

## Why this script is needed

Graph statistics help verify that the graph was constructed correctly and provide useful report material.

## Inputs

```text
data/processed/pages_clean.parquet
data/processed/edges.parquet
```

## Outputs

```text
data/processed/graph_stats.json
data/results/tables/degree_summary.csv
```

## Metrics to compute

```text
number of nodes
number of edges
average in-degree
average out-degree
median in-degree
median out-degree
max in-degree
max out-degree
number of dangling nodes
percent of dangling nodes
number of isolated pages
```

## Optional figure outputs

```text
data/results/figures/indegree_distribution.png
data/results/figures/outdegree_distribution.png
```

## Notes

If the edge count is suspiciously small, the graph-building script likely has a join problem.

---

# `07_pagerank.py`

## Goal

Compute PageRank using power iteration.

## Why this script is needed

PageRank is one of the main graph algorithms in the proposal. It provides a structural importance score for each page.

## Inputs

```text
data/processed/pages_clean.parquet
data/processed/edges.parquet
```

## Output

```text
data/processed/pagerank.parquet
```

## Output columns

```text
page_id
title
pagerank
rank
percentile
in_degree
out_degree
```

## Algorithm details

Use power iteration with:

```text
damping factor = 0.85
tolerance = 1e-8
maximum iterations = 100
```

Handle dangling nodes carefully.

Dangling nodes are pages with no outgoing links. Their PageRank mass should be redistributed uniformly across all nodes each iteration.

## Additional outputs

```text
data/results/tables/top_100_pagerank.csv
```

Also record:

```text
number of iterations
final convergence error
runtime
```

---

# `08_select_experiment_groups.py`

## Goal

Create the page groups used in similarity experiments.

## Why this script is needed

The project compares semantic similarity across PageRank-ranked groups, random pages, and possibly category-selected pages.

## Inputs

```text
data/processed/pagerank.parquet
data/processed/articles_clean.parquet
data/processed/categorylinks.parquet
```

## Output

```text
data/processed/experiment_groups.parquet
```

## Required groups

Use equal-sized groups.

Recommended:

```text
top_pagerank
median_pagerank
bottom_pagerank
random
```

## Optional category groups

If category data is manageable, add:

```text
category_mathematics
category_physics
category_computer_science
category_history
category_biology
category_philosophy
```

## Output columns

```text
page_id
title
group_name
pagerank
rank
percentile
word_count
```

## Important rule

Only select pages that have usable article text.

This means the script should join PageRank results with `articles_clean.parquet` before sampling.

---

# `09_prepare_text_shingles.py`

## Goal

Convert article text into shingles and MinHash signatures.

## Why this script is needed

Jaccard similarity and MinHash both operate on sets. This script turns article text into sets of word shingles.

## Inputs

```text
data/processed/articles_clean.parquet
data/processed/experiment_groups.parquet
```

## Outputs

```text
data/processed/shingles.parquet
data/processed/minhash_signatures.parquet
```

## Text preprocessing

For each selected article:

```text
lowercase
remove punctuation
remove extra whitespace
tokenize by words
remove very short tokens
optional: remove stopwords
```

## Shingling method

Recommended:

```text
3-word shingles
```

Example:

```text
"graph analysis of wikipedia pages"
```

3-word shingles:

```text
graph analysis of
analysis of wikipedia
of wikipedia pages
```

## Output columns for shingles

```text
page_id
group_name
shingles
num_shingles
```

## Output columns for MinHash signatures

```text
page_id
group_name
signature
```

## Recommended MinHash settings

```text
num_perm = 128
```

---

# `10_compute_similarity.py`

## Goal

Compute document similarity within each experimental group.

## Why this script is needed

This script produces the core result used to answer whether top PageRank pages are more semantically diverse.

## Inputs

```text
data/processed/shingles.parquet
data/processed/minhash_signatures.parquet
data/processed/experiment_groups.parquet
```

## Outputs

```text
data/processed/pairwise_similarity.parquet
data/processed/similarity_summary.parquet
```

## Pairwise output columns

```text
group_name
page_id_a
page_id_b
title_a
title_b
jaccard_exact
jaccard_minhash
```

## Summary output columns

```text
group_name
n_pages
n_pairs
mean_similarity
median_similarity
std_similarity
min_similarity
max_similarity
q25_similarity
q75_similarity
```

## Exact Jaccard

For smaller groups, compute exact Jaccard:

```text
J(A, B) = |A ∩ B| / |A ∪ B|
```

If `GROUP_SIZE = 500`, each group has:

```text
500 * 499 / 2 = 124,750 pairwise comparisons
```

This is usually manageable.

If too slow, use:

```text
GROUP_SIZE = 100
```

or:

```text
GROUP_SIZE = 250
```

## MinHash comparison

Also compute approximate similarity using MinHash.

Use exact Jaccard on a smaller validation subset if the full set is too slow.

---

# `11_analyze_results.py`

## Goal

Answer the project research questions using the similarity outputs.

## Why this script is needed

This turns raw PageRank and similarity numbers into project conclusions.

## Inputs

```text
data/processed/pagerank.parquet
data/processed/experiment_groups.parquet
data/processed/pairwise_similarity.parquet
data/processed/similarity_summary.parquet
```

## Outputs

```text
data/results/tables/research_question_results.csv
data/results/tables/similarity_summary.csv
data/results/tables/pagerank_percentile_similarity.csv
```

---

## Research Question 1

**Does selecting top-ranked pages by PageRank produce a more semantically diverse set than category or random sampling?**

Analysis:

```text
Compare mean similarity of top_pagerank vs random.
Compare mean similarity of top_pagerank vs category groups.
Lower mean similarity means more semantic diversity.
```

Expected support for hypothesis:

```text
top_pagerank mean similarity < category mean similarity
```

---

## Research Question 2

**Are highly ranked pages more or less similar to each other than lower-ranked pages?**

Analysis:

```text
Compare top_pagerank, median_pagerank, and bottom_pagerank.
```

Expected support for hypothesis:

```text
bottom_pagerank similarity > top_pagerank similarity
```

---

## Research Question 3

**How does similarity vary across PageRank percentiles?**

Analysis:

Create percentile bins:

```text
0-10%
10-20%
20-30%
...
90-100%
```

For each bin:

```text
sample N pages
compute pairwise similarity
calculate mean and median similarity
```

Output columns:

```text
percentile_bin
mean_similarity
median_similarity
std_similarity
```

## Optional statistical tests

If time allows:

```text
bootstrap confidence intervals
Mann-Whitney U test
permutation test
```

These are optional. Clear tables and plots are enough for a strong class project.

---

# `12_make_figures.py`

## Goal

Create final report and presentation figures.

## Why this script is needed

The project should include visual evidence for PageRank distribution and similarity comparisons.

## Inputs

```text
data/processed/pagerank.parquet
data/processed/similarity_summary.parquet
data/processed/pairwise_similarity.parquet
data/results/tables/pagerank_percentile_similarity.csv
```

## Outputs

```text
data/results/figures/pagerank_distribution.png
data/results/figures/top_20_pagerank.png
data/results/figures/similarity_by_group_bar.png
data/results/figures/similarity_by_group_boxplot.png
data/results/figures/pagerank_percentile_similarity.png
data/results/figures/minhash_vs_exact_jaccard.png
```

## Required figures

### 1. PageRank distribution

Shows whether PageRank scores are heavily skewed.

### 2. Top 20 pages by PageRank

Useful for sanity checking and presentation.

### 3. Mean similarity by group

Directly answers the main semantic diversity question.

### 4. Boxplot of pairwise similarity by group

Shows whether differences are consistent or caused by outliers.

### 5. PageRank percentile vs average similarity

Answers the percentile-based research question.

### 6. MinHash vs exact Jaccard scatterplot

Shows whether MinHash approximates exact Jaccard well.

---

# `run_pipeline.ps1`

## Goal

Run all scripts in the correct order.

## Purpose

This makes the pipeline easier to reproduce.

## Contents

```powershell
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
```

## Run command

```powershell
.\scripts\run_pipeline.ps1
```

---

## 4. Full Run Order

After the raw downloads finish, run:

```powershell
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
```

---

## 5. Minimum Viable Project

If time becomes limited, focus on these scripts:

```text
01_convert_sql_to_parquet.py
02_build_pages.py
03_build_edges.py
04_extract_article_text.py
05_join_text_with_pages.py
07_pagerank.py
08_select_experiment_groups.py
09_prepare_text_shingles.py
10_compute_similarity.py
12_make_figures.py
```

Minimum analysis:

```text
1. Compute PageRank.
2. Select top, median, bottom, and random page groups.
3. Compute pairwise Jaccard/MinHash similarity within each group.
4. Compare mean and median similarity.
5. Make three plots:
   - PageRank distribution
   - Similarity by group
   - PageRank percentile vs similarity
```

This is enough to directly answer the project’s main questions.

---

## 6. Suggested Division of Work

For a two-person group:

## Person 1: Graph and PageRank

Responsibilities:

```text
01_convert_sql_to_parquet.py
02_build_pages.py
03_build_edges.py
06_graph_stats.py
07_pagerank.py
PageRank visualizations
```

## Person 2: Text Similarity and Experiments

Responsibilities:

```text
04_extract_article_text.py
05_join_text_with_pages.py
08_select_experiment_groups.py
09_prepare_text_shingles.py
10_compute_similarity.py
similarity visualizations
```

## Shared responsibilities

```text
11_analyze_results.py
12_make_figures.py
README.md
final report
presentation
```

---

## 7. Key Final Outputs for the Report

The final report should include:

```text
data/results/tables/top_100_pagerank.csv
data/results/tables/degree_summary.csv
data/results/tables/similarity_summary.csv
data/results/tables/research_question_results.csv
data/results/tables/pagerank_percentile_similarity.csv
```

And figures:

```text
data/results/figures/pagerank_distribution.png
data/results/figures/top_20_pagerank.png
data/results/figures/similarity_by_group_bar.png
data/results/figures/similarity_by_group_boxplot.png
data/results/figures/pagerank_percentile_similarity.png
data/results/figures/minhash_vs_exact_jaccard.png
```

---

## 8. Success Criteria

The project is complete when you can answer the following clearly:

1. **Top PageRank diversity:** Are top-ranked pages more semantically diverse than random or category-selected pages?
2. **Rank-level comparison:** Are top PageRank pages less similar to each other than median or bottom PageRank pages?
3. **Percentile trend:** Does average similarity change as PageRank percentile changes?
4. **Method validation:** Does MinHash approximate exact Jaccard well enough for your samples?
5. **Interpretation:** Do the results support the original hypothesis?

---

## 9. Notes for GitHub

Do not commit raw or processed Wikipedia data.

Add to `.gitignore`:

```text
data/raw/
data/processed/
data/results/
.venv/
__pycache__/
*.pyc
.ipynb_checkpoints/
```

Commit:

```text
scripts/
notebooks/
README.md
requirements.txt
.gitignore
```

Do not commit:

```text
*.bz2
*.gz
*.parquet
large CSV files
large PNG outputs if not needed
```
