$csvPath = "C:\Quant\src\quant\config\ticker_reference.csv"
$outPath = "C:\Quant\src\tickers_table.md"

$csv = Import-Csv $csvPath

$md = "| ticker | company_name | market_sector |`n"
$md += "| --- | --- | --- |`n"

$csv | ForEach-Object {
    $md += "| $($_.ticker) | $($_.company_name) | $($_.market_sector) |`n"
}

Set-Content -Path $outPath -Value $md