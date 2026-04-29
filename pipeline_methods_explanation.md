# Wikipedia Graph Analysis Pipeline Explanation

## Introduction

This project analyzes English Wikipedia as a directed graph where article pages are nodes and internal hyperlinks are directed edges. The goal is to compute structural importance with PageRank, then test whether highly ranked pages are semantically more or less similar than random, category-based, median-ranked, and low-ranked pages. The implementation uses current Wikimedia dump files, converts SQL dumps to Parquet, computes PageRank with power iteration, and compares article text with 3-word shingle Jaccard similarity plus deterministic MinHash estimates. The main Python tools are `pandas`, `pyarrow`, `duckdb`, `numpy`, and `matplotlib`; the repository is https://github.com/luckyowl20/csci_4022_final_project.

## Research Questions

The proposal asks how Wikipedia's link structure relates to document similarity. In the current implementation, that broad goal is translated into three measurable research questions:

1. **Does selecting top-ranked pages by PageRank produce a more semantically diverse set than selecting pages by category or random sampling?**

   The metric used is the average pairwise exact Jaccard similarity among pages in each group. Every selected page is represented as a set of word shingles. If a group has lower average Jaccard similarity, then the articles in that group share less text vocabulary/context and are treated as more semantically diverse. The implementation compares `top_pagerank` against `random` and the available category groups.

2. **Are highly ranked pages more or less similar to each other than lower-ranked pages?**

   This is measured by comparing mean and median pairwise Jaccard similarity for `top_pagerank`, `median_pagerank`, and `bottom_pagerank`. The proposal hypothesized that high PageRank pages would be broad connector pages and therefore more diverse, while low PageRank pages would be more niche and more similar. In the current output, the top PageRank group has higher mean similarity than both the random and bottom groups, so the current results do not support that specific hypothesis.

3. **How does similarity vary across PageRank percentiles?**

   The implementation bins selected articles into 10 PageRank percentile ranges, such as `0-10%` through `90-100%`, and computes pairwise exact Jaccard similarity within each bin. This gives a rank-continuum view rather than only comparing top, middle, and bottom samples.

The generated results also include a fourth validation-style measurement: **how close MinHash estimates are to exact Jaccard similarity**. This is not a separate proposal question, but it is important because the proposal planned to use MinHash as an approximate similarity algorithm. The current run reports a mean absolute error of about `0.00219` between MinHash and exact Jaccard over the selected pairs.

## Methods

### Data Description

The project uses Wikimedia English Wikipedia dump files stored under `scripts/data/raw/`. The raw files currently present in the repository workspace are:

| File | Purpose | Last modified/accessed locally |
| --- | --- | --- |
| `enwiki-latest-page.sql.gz` | Page metadata: page id, namespace, title, redirect flag, page length | April 27, 2026 10:38:33 PM |
| `enwiki-latest-linktarget.sql.gz` | Link target ids and target titles/namespaces | April 27, 2026 10:43:31 PM |
| `enwiki-latest-pagelinks.sql.gz` | Internal hyperlink records from source pages to target ids | April 27, 2026 11:08:50 PM |
| `enwiki-latest-categorylinks.sql.gz` | Page-to-category assignments for category comparison groups | April 27, 2026 11:19:19 PM |
| `enwiki-latest-pages-articles-multistream.xml.bz2` | Article wikitext used for document similarity | April 27, 2026 10:27:52 PM |
| `enwiki-latest-pages-articles-multistream-index.txt.bz2` | Index for the article XML dump; downloaded but not used by the current extraction script | April 27, 2026 10:28:52 PM |
| `enwiki-latest-redirect.sql.gz` | Redirect metadata; downloaded but not used directly because redirects are filtered from `page.sql.gz` | April 27, 2026 11:09:46 PM |
| `enwiki-latest-sha1sums.txt` | Wikimedia checksums for downloaded dump validation | April 27, 2026 8:35:27 PM |

The download script, `scripts/download-wiki.ps1`, downloads from `https://dumps.wikimedia.org/enwiki/latest`. That means the raw files are the English Wikipedia "latest" snapshot available from Wikimedia at download time, not a hard-coded historical dump date. The local timestamps above are the best evidence in this repository for when this copy of the data was obtained.

The beginning of the data preparation pipeline converts raw SQL dumps into Parquet so later steps can query only the columns needed for graph construction. This avoids loading the dumps into MySQL and makes the later joins practical in Python/DuckDB. The conversion step is `scripts/01_convert_sql_to_parquet.py`.

The converted SQL-derived tables are:

| Output | Rows in current run | Important columns |
| --- | ---: | --- |
| `data/processed/page.parquet` | 65,401,140 | `page_id`, `page_namespace`, `page_title`, `page_is_redirect`, `page_len` |
| `data/processed/linktarget.parquet` | 126,570,717 | `lt_id`, `lt_namespace`, `lt_title` |
| `data/processed/pagelinks.parquet.parts/` | 1,005,750,000 | `pl_from`, `pl_from_namespace`, `pl_target_id` |
| `data/processed/categorylinks.parquet.parts/` | 218,853,102 | `cl_from`, `cl_target_id` |

Large tables are stored as partition-like directories ending in `.parquet.parts/`, with numbered `part-*.parquet` files and a `_SUCCESS` marker. Smaller tables are stored as single Parquet files. The `.complete.json` files next to outputs are pipeline markers used to avoid accidentally continuing from missing or partially written outputs.

After SQL conversion, `scripts/02_build_pages.py` creates the graph node set in `data/processed/pages_clean.parquet`. It keeps only normal article pages:

- `page_namespace == 0`, meaning main encyclopedia articles rather than talk pages, categories, templates, etc.
- `page_is_redirect == 0`, meaning redirect pages are excluded as nodes.
- `page_len > 100`, using the project `MIN_WORDS`/minimum-size threshold as a rough filter against tiny or unusable pages.

The current clean page table contains `7,160,764` article nodes.

The article text path starts from the XML dump, not from the SQL dumps. `scripts/04_extract_article_text.py` streams through `enwiki-latest-pages-articles-multistream.xml.bz2`, keeps namespace-0 non-redirect pages, extracts revision text, and applies a lightweight wikitext cleaner. The cleaner removes or simplifies common markup: comments, references, tables, templates, file/category links, internal links, external bracket links, headings, list prefixes, HTML tags/entities, and bold/italic/nowiki markers. It then tokenizes with a simple alphanumeric regex and records word counts.

The raw extraction output, `articles_raw.parquet`, contains `4,670,000` rows and is marked `complete_with_warnings` because it was finalized from an existing extraction. `scripts/05_join_text_with_pages.py` then joins extracted text back to `pages_clean.parquet`, keeps only articles with `word_count >= 100` and nonempty `clean_text`, and writes `articles_clean.parquet`. The current clean article-text table contains `3,302,036` rows. This table is the text universe used for the similarity analysis.

### Algorithms, Implementation, and Software

#### 1. SQL dump conversion

The pipeline starts with:

```powershell
python scripts\01_convert_sql_to_parquet.py --table page
python scripts\01_convert_sql_to_parquet.py --table linktarget
python scripts\01_convert_sql_to_parquet.py --table pagelinks
python scripts\01_convert_sql_to_parquet.py --table categorylinks
```

`01_convert_sql_to_parquet.py` reads compressed MySQL dump files directly with Python's `gzip` module. It parses the `CREATE TABLE` statement to find source column positions, then parses `INSERT INTO ... VALUES ...` rows while retaining only the selected columns. This is why the SQL-to-Parquet step is a preprocessing step rather than analysis: it is changing the physical storage format and reducing columns, but it is not yet computing graph metrics.

The code uses:

- `pyarrow` and `pyarrow.parquet` to write Parquet.
- Custom parsing helpers in `scripts/pipeline_utils.py` to split MySQL insert tuples and decode SQL values.
- Part files for massive tables such as `pagelinks` and `categorylinks`.
- Completion markers to support resumability and validation.

#### 2. Graph node construction

`scripts/02_build_pages.py` defines the allowed graph nodes. This matters because Wikipedia dump tables contain many namespaces and redirect records that would distort a page graph if they were treated like normal encyclopedia articles. Filtering to namespace 0 and non-redirect pages makes each node represent an actual article page.

Current output:

- `data/processed/pages_clean.parquet`
- `7,160,764` rows
- Columns: `page_id`, `title`, `page_len`

#### 3. Directed edge construction

`scripts/03_build_edges.py` creates the graph edge list using DuckDB. Conceptually, each row is:

```text
source_page_id -> target_page_id
```

The join is:

1. Start with `pagelinks`, keeping only links where `pl_from_namespace = 0`.
2. Join `pl_from` to `pages_clean.page_id` so the source is a valid article.
3. Join `pagelinks.pl_target_id` to `linktarget.lt_id`.
4. Keep only link targets where `lt_namespace = 0`.
5. Join `linktarget.lt_title` to `pages_clean.title` so the target is also a valid article.
6. Drop self-links where source and target are the same page.
7. Use `SELECT DISTINCT` so repeated links from one article to the same target count as one directed edge.

This produces `data/processed/edges.parquet` with `642,411,155` directed edges.

This edge construction is the core step that turns Wikipedia from dump tables into a graph suitable for PageRank. PageRank needs a directed adjacency structure because the algorithm models probability mass flowing along outgoing links.

#### 4. Graph statistics

`scripts/06_graph_stats.py` computes basic graph diagnostics before PageRank:

- Number of nodes: `7,160,764`
- Number of edges: `642,411,155`
- Mean in-degree/out-degree: about `89.71`
- Median in-degree: `16`
- Median out-degree: `34`
- Max in-degree: `1,256,079`
- Max out-degree: `11,178`
- Dangling nodes: `8,953` pages with no outgoing graph edges
- Isolated pages: `6,305` pages with no incoming or outgoing graph edges

This analysis is not just descriptive. It checks whether graph construction worked. For example, an unexpectedly tiny edge count would indicate a bad join between `pagelinks`, `linktarget`, and `pages_clean`. It also identifies dangling nodes, which must be handled correctly in PageRank.

#### 5. PageRank with power iteration

`scripts/07_pagerank.py` implements PageRank directly with `numpy`, not with NetworkX. That is appropriate for this graph size because the graph has millions of nodes and hundreds of millions of edges, and a dense graph object would be impractical.

The configuration is in `scripts/00_config.py`:

- Damping factor: `0.85`
- Convergence tolerance: `1e-8`
- Maximum iterations: `100`

Implementation details:

1. The script maps each `page_id` to a zero-based array index.
2. It converts the edge list into source and destination index arrays, `src` and `dst`.
3. It computes out-degree with `np.bincount(src)` and in-degree with `np.bincount(dst)`.
4. It initializes every page with equal PageRank mass, `1 / n`.
5. On each iteration, it starts each page with teleportation mass `(1 - damping) / n`.
6. It sends each source page's current rank across outgoing links as `rank[src] / out_degree[src]`.
7. It accumulates incoming contributions with `np.add.at(new_rank, dst, damping * contribution)`.
8. It redistributes dangling-node mass uniformly across all nodes with `damping * rank[dangling].sum() / n`.
9. It stops when the L1 difference between old and new rank vectors falls below `1e-8` or when 100 iterations have run.

The current PageRank run converged in `63` iterations with final L1 error about `8.58e-09` and runtime about `2446` seconds. The output is `data/processed/pagerank.parquet` with:

- `page_id`
- `title`
- `pagerank`
- `rank`
- `percentile`
- `in_degree`
- `out_degree`

The current top ranked pages include `Geographic_coordinate_system`, `Wayback_Machine`, `United_States`, `Wikidata`, `Time_zone`, `Taxonomy_(biology)`, and `Global_Biodiversity_Information_Facility`. This list is useful as a sanity check: many top pages have very high incoming link counts because they are broadly referenced across Wikipedia.

#### 6. Experiment group selection

`scripts/08_select_experiment_groups.py` builds the page sets used for similarity comparisons. It first joins PageRank results with `articles_clean.parquet`, because similarity can only be computed for pages with usable extracted text.

The main groups are:

- `top_pagerank`: the 500 pages with smallest rank values.
- `median_pagerank`: a random sample of up to 500 pages with PageRank percentile from `0.45` to `0.55`.
- `bottom_pagerank`: the 500 pages with largest rank values.
- `random`: a random sample of 500 pages from all clean articles with PageRank.

The script also attempts category groups using `categorylinks.parquet` and `linktarget.parquet`. It maps category target ids back to category names and samples pages from these configured categories:

- `Mathematics`
- `Physics`
- `Computer_science`
- `History`
- `Biology`
- `Philosophy`

In the current run, category groups are much smaller than the PageRank/random groups: for example, `category_mathematics` has only 2 selected pages, `category_computer_science` has 7, and `category_biology` has 10. That makes category comparisons much less reliable than the main PageRank/random comparisons. The likely reason is that the category matching uses exact category titles only, so it captures pages directly in broad top-level categories but not pages in subcategories.

The output `experiment_groups.parquet` contains `2,040` rows.

#### 7. Text shingling and MinHash signatures

`scripts/09_prepare_text_shingles.py` prepares selected article text for similarity analysis.

Each article's cleaned text is:

1. Tokenized by `pipeline_utils.tokenize`, which keeps lowercase alphanumeric tokens of length greater than 1.
2. Converted into 3-word shingles, because `SHINGLE_SIZE = 3`.
3. Stored as a sorted JSON list in `shingles.parquet`.
4. Converted into a deterministic MinHash signature with 128 hash functions, because `MINHASH_PERMUTATIONS = 128`.

A 3-word shingle preserves a little local word order and phrase context. This is more informative than comparing single words, but much cheaper than comparing full document structure. Jaccard similarity over these shingle sets measures how much phrase-level content two articles share.

The MinHash implementation uses `hashlib.blake2b` with a seed prefix for each permutation. For each seed, the signature value is the minimum 64-bit hash across all shingles. Two articles with more overlapping shingles should have more matching MinHash signature positions. The current pipeline writes:

- `data/processed/shingles.parquet`, `2,040` rows
- `data/processed/minhash_signatures.parquet`, `2,040` rows

#### 8. Pairwise similarity

`scripts/10_compute_similarity.py` computes all pairwise similarities within each experimental group. For each pair of pages in the same group, it writes:

- `group_name`
- `page_id_a`
- `page_id_b`
- `title_a`
- `title_b`
- `jaccard_exact`
- `jaccard_minhash`

Exact Jaccard similarity is:

```text
J(A, B) = |A intersection B| / |A union B|
```

where `A` and `B` are the two articles' shingle sets.

MinHash Jaccard is the fraction of signature positions where the two pages have the same minimum hash. It is an approximation of exact Jaccard and is included because the proposal planned to use MinHash for scalable document similarity.

For a 500-page group, there are:

```text
500 * 499 / 2 = 124,750
```

within-group pairs. The current run produced `499,137` pairwise similarity rows across all groups and `10` group-level summary rows.

Current group-level exact Jaccard means:

| Group | Pages | Pairs | Mean similarity | Median similarity |
| --- | ---: | ---: | ---: | ---: |
| `top_pagerank` | 500 | 124,750 | 0.004164 | 0.003248 |
| `random` | 500 | 124,750 | 0.000769 | 0.000000 |
| `median_pagerank` | 500 | 124,750 | 0.000700 | 0.000000 |
| `bottom_pagerank` | 500 | 124,750 | 0.001269 | 0.000000 |
| `category_history` | 8 | 28 | 0.004617 | 0.004345 |
| `category_mathematics` | 2 | 1 | 0.004313 | 0.004313 |
| `category_philosophy` | 4 | 6 | 0.003725 | 0.001081 |
| `category_computer_science` | 7 | 21 | 0.002507 | 0.000923 |
| `category_biology` | 10 | 45 | 0.001483 | 0.001135 |
| `category_physics` | 9 | 36 | 0.001390 | 0.000748 |

The top PageRank group has higher average similarity than random, median, and bottom groups in this run. This is the opposite of the proposal's stated hypothesis that top PageRank pages would be more semantically diverse. A plausible interpretation is that many top PageRank pages are globally referenced infrastructure or high-level reference topics that share recurring encyclopedia vocabulary, metadata-like phrasing, geographic/time/category terminology, or biological taxonomy language. The code does not currently remove stopwords or domain boilerplate beyond the wikitext cleaner, so the similarity metric may also reflect repeated article structure and common reference language.

#### 9. Research question analysis

`scripts/11_analyze_results.py` converts raw similarity outputs into report tables:

- `data/results/tables/similarity_summary.csv`
- `data/results/tables/research_question_results.csv`
- `data/results/tables/pagerank_percentile_similarity.csv`

The research question table computes:

- `top_pagerank mean - random mean = 0.003395`
- `top_pagerank mean - bottom_pagerank mean = 0.002895`
- `mean absolute error MinHash vs exact Jaccard = 0.002187`

For the first two differences, negative values would support the hypothesis that top PageRank pages are more diverse. The current values are positive, so the current analysis suggests top PageRank pages are more similar to each other than random or bottom-ranked pages under the chosen shingle/Jaccard metric.

The percentile table shows that the `90-100%` percentile bin has mean similarity about `0.003299`, while most middle bins are below `0.001`. The `0-10%` bin has mean similarity about `0.001214`. Because this percentile analysis is computed only from the selected experiment pages, not from a fresh balanced sample from every percentile, the bin counts are uneven. For example, the `90-100%` bin has 605 pages and the `10-20%` bin has only 21 pages. This makes the percentile trend useful but not perfectly balanced.

#### 10. Figures

`scripts/12_make_figures.py` creates final visualizations under `data/results/figures/`:

- `pagerank_distribution.png`: shows the heavy-tailed PageRank score distribution.
- `top_20_pagerank.png`: shows the highest ranked pages for sanity checking and presentation.
- `similarity_by_group_bar.png`: compares group mean exact Jaccard similarity.
- `similarity_by_group_boxplot.png`: shows the spread of pairwise similarities by group.
- `pagerank_percentile_similarity.png`: plots mean similarity by percentile bin.
- `minhash_vs_exact_jaccard.png`: compares approximate MinHash similarity to exact Jaccard.

These plots connect the numerical pipeline back to the project goal: they show which pages are structurally central, how similarity differs across selected groups, and whether MinHash is a reasonable approximation for this dataset.

## Alignment With the Proposal

The current repository mostly aligns with the project proposal.

Aligned parts:

- The proposal says Wikipedia should be represented as a directed graph of pages and hyperlinks. The current code does exactly this with `pages_clean.parquet` as nodes and `edges.parquet` as directed links.
- The proposal says PageRank should be computed with power iteration. The current implementation uses explicit power iteration with damping, convergence tolerance, and dangling-node handling.
- The proposal says document similarity should use MinHash to approximate Jaccard similarity, with true Jaccard if the set is small enough. The current pipeline computes both exact Jaccard and MinHash for all selected pairs.
- The proposal asks whether top PageRank pages are more semantically diverse than category or random selections. The current pipeline creates PageRank, random, and category groups and compares average pairwise similarity.
- The proposal asks how similarity varies across top, median, and bottom PageRank percentiles. The current pipeline includes top, median, bottom, and 10 percentile-bin comparisons.

Differences or limitations:

- The proposal mentions building on the 2018 WikiLinkGraphs tooling. The current code does not use that tool directly. Instead, it independently builds the graph from Wikimedia `page`, `linktarget`, and `pagelinks` SQL dumps.
- The proposal refers generally to "Wikipedia pages and page link documents." The implementation uses a more specific set of raw files: SQL dumps for page/link/category structure and the pages-articles XML dump for text.
- The downloaded `redirect.sql.gz` file is not currently used directly. Redirects are excluded using `page_is_redirect` from `page.sql.gz`.
- The category comparison exists, but the current category groups are tiny because the implementation matches exact broad category titles and does not expand through category subgraphs. This weakens the category-vs-PageRank part of Research Question 1.
- The percentile analysis is based on the already selected experiment groups, so bins are not evenly sampled from the full PageRank distribution. A stronger version would sample a fixed number of pages independently from each percentile bin.
- The text cleaning is intentionally lightweight and pure Python. It removes many common wikitext patterns, but it is not a full MediaWiki parser. Some markup, boilerplate, citation remnants, or template artifacts may remain and influence similarity.
- The current result contradicts the original hypothesis: top PageRank pages are not more semantically diverse under this metric; they have higher average shingle similarity than random, median, and bottom PageRank groups.

Overall, the implemented pipeline answers the proposal's core research questions, but it does so with a self-built Wikimedia dump pipeline rather than external WikiLinkGraphs tooling, and the category/percentile comparisons should be described as exploratory because their current sampling is uneven.
