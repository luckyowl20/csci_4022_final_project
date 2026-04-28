$ErrorActionPreference = "Stop"

function Invoke-Step {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Command)
    & $Command[0] $Command[1..($Command.Length - 1)]
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed with exit code $LASTEXITCODE`: $($Command -join ' ')"
    }
}

#Invoke-Step python scripts\01_convert_sql_to_parquet.py --table page
#Invoke-Step python scripts\01_convert_sql_to_parquet.py --table linktarget
Invoke-Step python scripts\01_convert_sql_to_parquet.py --table pagelinks
Invoke-Step python scripts\01_convert_sql_to_parquet.py --table categorylinks

Invoke-Step python scripts\02_build_pages.py
Invoke-Step python scripts\03_build_edges.py
Invoke-Step python scripts\04_extract_article_text.py
Invoke-Step python scripts\05_join_text_with_pages.py

Invoke-Step python scripts\06_graph_stats.py
Invoke-Step python scripts\07_pagerank.py
Invoke-Step python scripts\08_select_experiment_groups.py
Invoke-Step python scripts\09_prepare_text_shingles.py
Invoke-Step python scripts\10_compute_similarity.py
Invoke-Step python scripts\11_analyze_results.py
Invoke-Step python scripts\12_make_figures.py
