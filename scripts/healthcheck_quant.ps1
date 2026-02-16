$errors = Select-String -Path "C:\Quant\logs\*.log" -Pattern "Traceback","ERROR","Exception" -SimpleMatch -Quiet
if ($errors) { Write-Host "HEALTHCHECK_FAIL"; exit 1 } else { Write-Host "HEALTHCHECK_OK"; exit 0 }
