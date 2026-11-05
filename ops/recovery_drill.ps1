param(
    [string]$Container = "postgres",
    [string]$SparkService = "stream-processor",
    [string]$Database = $env:POSTGRES_DB,
    [string]$User = $env:POSTGRES_USER,
    [string]$CheckpointPath = (Join-Path $PSScriptRoot "..\checkpoints\spark_pipeline"),
    [string]$EvidenceDirectory = (Join-Path $PSScriptRoot "..\recovery-evidence")
)

$ErrorActionPreference = "Stop"
$started = Get-Date

if ([string]::IsNullOrWhiteSpace($Database)) {
    $Database = "app_db"
}
if ([string]::IsNullOrWhiteSpace($User)) {
    $User = "admin"
}

$resolvedEvidence = [System.IO.Path]::GetFullPath($EvidenceDirectory)
New-Item -ItemType Directory -Force -Path $resolvedEvidence | Out-Null
$started = Get-Date
$beforeSnapshot = Join-Path $resolvedEvidence "before.txt"
$afterSnapshot = Join-Path $resolvedEvidence "after.txt"
$logPath = Join-Path $resolvedEvidence "recovery-$($started.ToString('yyyyMMdd-HHmmss')).log"

Start-Transcript -Path $logPath -Force | Out-Null
try {
    & "$PSScriptRoot\backup_postgres.ps1" -Container $Container -Database $Database -User $User
if ($LASTEXITCODE -ne 0) {
    throw "Backup phase failed."
}

& "$PSScriptRoot\verify_recovery.ps1" -Container $Container -Database $Database -User $User -OutputPath $beforeSnapshot
if ($LASTEXITCODE -ne 0) {
    throw "Pre-recovery integrity check failed."
}

docker compose restart $Container $SparkService
if ($LASTEXITCODE -ne 0) {
    throw "Service restart phase failed."
}

$deadline = (Get-Date).AddMinutes(5)
do {
    Start-Sleep -Seconds 5
    $postgresHealth = docker inspect $Container --format '{{.State.Health.Status}}' 2>$null
    $sparkState = docker inspect spark-processor --format '{{.State.Status}}' 2>$null
    if ($postgresHealth -eq "healthy" -and $sparkState -eq "running") {
        break
    }
} while ((Get-Date) -lt $deadline)

if ($postgresHealth -ne "healthy") {
    throw "PostgreSQL did not become healthy after restart."
}
if ($sparkState -ne "running") {
    throw "Spark processor did not remain running after restart."
}
if (-not (Test-Path -LiteralPath $CheckpointPath)) {
    throw "Checkpoint directory was not found: $CheckpointPath"
}

& "$PSScriptRoot\verify_recovery.ps1" -Container $Container -Database $Database -User $User -OutputPath $afterSnapshot
if ($LASTEXITCODE -ne 0) {
    throw "Post-recovery integrity check failed."
}

$before = (Get-Content -Raw -LiteralPath $beforeSnapshot).Trim()
$after = (Get-Content -Raw -LiteralPath $afterSnapshot).Trim()
$beforeParts = $before -split "\|"
$afterParts = $after -split "\|"
if ($beforeParts.Count -ne 5 -or $afterParts.Count -ne 5) {
    throw "Unexpected integrity snapshot format."
}
if ([int64]$afterParts[0] -lt [int64]$beforeParts[0]) {
    throw "Row count decreased during restart: before='$before'; after='$after'"
}
if ([int64]$afterParts[1] -lt [int64]$beforeParts[1]) {
    throw "Logical row count decreased during restart: before='$before'; after='$after'"
}
if ([int64]$afterParts[2] -ne 0 -or [int64]$afterParts[4] -ne 0) {
    throw "Post-recovery integrity failed: after='$after'"
}

$elapsed = (Get-Date) - $started
Write-Output ("Recovery drill completed in {0:N1} seconds." -f $elapsed.TotalSeconds)
if ($elapsed.TotalMinutes -gt 5) {
    throw "Recovery drill exceeded the five-minute target."
}
}
finally {
    Stop-Transcript | Out-Null
}
