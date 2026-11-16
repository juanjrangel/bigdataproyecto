# Equivalente en PowerShell (Docker Desktop en Windows). Uso: .\scripts\run_pipeline.ps1 [-Months "1-12"]
param([string]$Months = "1-12")
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d --build
Write-Host ">> Esperando a que los workers se registren..."
Start-Sleep -Seconds 15
docker compose exec -T spark-client python3 -m pytest
docker compose exec -T -e PIPELINE_MONTHS=$Months spark-client spark-submit jobs/run_pipeline.py
Write-Host ">> Listo. Spark Master UI: http://localhost:8080 | History Server: http://localhost:18080"
