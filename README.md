# Wikipedia Graph Analysis

Pipeline for the CSCI 4022 final project: build a directed Wikipedia article-link
graph, compute PageRank with power iteration, then compare semantic similarity
across PageRank-based, random, and optional category-based article groups.

## Setup

```powershell
python -m pip install -r requirements.txt
```

Download scripts write to the repo-level `data\raw` directory:

```powershell
.\scripts\download-wiki.ps1
.\scripts\checksum.ps1
```

The analysis scripts also look in the legacy `scripts\data\raw` location so a
partial download can still be used for testing.

## Test A Small Text Extraction

```powershell
python scripts\04_extract_article_text.py --limit 10000
```

## Full Pipeline

Run after the raw dumps finish downloading:

```powershell
.\scripts\run_pipeline.ps1
```

Key outputs are written under `data\processed`, `data\results\tables`, and
`data\results\figures`. These generated data directories are intentionally
ignored by Git.
