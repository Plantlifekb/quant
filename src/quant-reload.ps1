Write-Host "=== QUANT: FULL DOCKER RESET + STACK RELOAD ==="

# 1. Kill Docker Desktop + backend
Write-Host "Stopping Docker Desktop processes..."
taskkill /F /IM "Docker Desktop.exe" 2>$null
taskkill /F /IM "com.docker.backend.exe" 2>$null
taskkill /F /IM "com.docker.proxy.exe" 2>$null

# 2. Kill WSL2 backend
Write-Host "Shutting down WSL2..."
wsl --shutdown

# 3. Start Docker Desktop fresh
Write-Host "Starting Docker Desktop..."
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"

# 4. Wait for engine to come online
Write-Host "Waiting for Docker engine..."
$maxWait = 60
$waited = 0
while ($waited -lt $maxWait) {
    $ps = docker ps 2>$null
    if ($ps -and $ps -notmatch "error") {
        Write-Host "Docker engine is up."
        break
    }
    Start-Sleep -Seconds 2
    $waited += 2
}
if ($waited -ge $maxWait) {
    Write-Host "ERROR: Docker engine did not start."
    exit 1
}

# 5. Bring quant stack up
Write-Host "Starting quant stack..."
docker compose up -d

# 6. Verify Postgres container
Write-Host "Checking Postgres container..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host "=== QUANT RELOAD COMPLETE ==="