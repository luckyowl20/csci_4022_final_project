$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Base = "https://dumps.wikimedia.org/enwiki/latest"
$RawDir = Join-Path $ProjectRoot "scripts\data\raw"

$Files = @(
    "enwiki-latest-sha1sums.txt",

    # Page content for document similarity
    "enwiki-latest-pages-articles-multistream.xml.bz2",
    "enwiki-latest-pages-articles-multistream-index.txt.bz2",

    # Link graph data
    "enwiki-latest-page.sql.gz",
    "enwiki-latest-linktarget.sql.gz",
    "enwiki-latest-pagelinks.sql.gz",

    # Useful cleanup/filtering data
    "enwiki-latest-redirect.sql.gz",

    # Optional, but useful for category-based comparison
    "enwiki-latest-categorylinks.sql.gz"
)

New-Item -ItemType Directory -Force -Path $RawDir | Out-Null

foreach ($File in $Files) {
    $Url = "$Base/$File"
    $Out = Join-Path $RawDir $File

    Write-Host "Downloading $File"
    curl.exe -L -C - --retry 10 --retry-delay 10 -o $Out $Url
}

Write-Host "Downloads complete."
