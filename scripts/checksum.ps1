$ProjectRoot = Split-Path -Parent $PSScriptRoot
$RawDir = Join-Path $ProjectRoot "scripts\data\raw"
$ShaFile = Join-Path $RawDir "enwiki-latest-sha1sums.txt"

Get-ChildItem $RawDir -File | Where-Object { $_.Name -ne "enwiki-latest-sha1sums.txt" } | ForEach-Object {
    $file = $_.Name
    $line = Select-String -Path $ShaFile -Pattern " $file$" | Select-Object -First 1

    if ($null -eq $line) {
        Write-Host "No SHA1 entry found for $file"
    }
    else {
        $expected = ($line.Line -split "\s+")[0].ToLower()
        $actual = (Get-FileHash $_.FullName -Algorithm SHA1).Hash.ToLower()

        if ($expected -eq $actual) {
            Write-Host "OK: $file"
        }
        else {
            Write-Host "BAD: $file"
            Write-Host "Expected: $expected"
            Write-Host "Actual:   $actual"
        }
    }
}
