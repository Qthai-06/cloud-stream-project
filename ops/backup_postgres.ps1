param(
    [string]$BackupDirectory = (Join-Path $PSScriptRoot "..\backups\postgres"),
    [string]$Container = "postgres",
    [string]$Database = $env:POSTGRES_DB,
    [string]$User = $env:POSTGRES_USER
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($Database)) {
    $Database = "app_db"
}
if ([string]::IsNullOrWhiteSpace($User)) {
    $User = "admin"
}

$resolvedDirectory = [System.IO.Path]::GetFullPath($BackupDirectory)
New-Item -ItemType Directory -Force -Path $resolvedDirectory | Out-Null
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backupPath = Join-Path $resolvedDirectory "$Database-$timestamp.dump"
$checksumPath = "$backupPath.sha256"

docker inspect $Container *> $null
if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL container '$Container' was not found."
}

docker exec $Container pg_isready -U $User -d $Database
if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL is not ready."
}

docker exec $Container pg_dump -U $User -d $Database -Fc --no-owner --file "/tmp/$Database.dump"
if ($LASTEXITCODE -ne 0) {
    throw "pg_dump failed."
}

docker cp "${Container}:/tmp/$Database.dump" $backupPath
if ($LASTEXITCODE -ne 0) {
    throw "Could not copy the backup out of the container."
}

docker exec $Container rm -f "/tmp/$Database.dump"
if ($LASTEXITCODE -ne 0) {
    throw "Could not remove the temporary backup from the container."
}

$hash = (Get-FileHash -Algorithm SHA256 -Path $backupPath).Hash
"$hash *$(Split-Path -Leaf $backupPath)" | Set-Content -Encoding ascii -Path $checksumPath

Write-Output "Backup: $backupPath"
Write-Output "Checksum: $checksumPath"
Write-Output "SHA256: $hash"
