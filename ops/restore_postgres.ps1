param(
    [Parameter(Mandatory = $true)]
    [string]$BackupFile,
    [string]$Container = "postgres",
    [string]$Database = $env:POSTGRES_DB,
    [string]$User = $env:POSTGRES_USER,
    [switch]$ConfirmRestore
)

$ErrorActionPreference = "Stop"

if (-not $ConfirmRestore) {
    throw "Restore is destructive. Re-run with -ConfirmRestore after taking a current backup."
}
if ([string]::IsNullOrWhiteSpace($Database)) {
    $Database = "app_db"
}
if ([string]::IsNullOrWhiteSpace($User)) {
    $User = "admin"
}

$resolvedBackup = [System.IO.Path]::GetFullPath($BackupFile)
if (-not (Test-Path -LiteralPath $resolvedBackup -PathType Leaf)) {
    throw "Backup file was not found: $resolvedBackup"
}

$checksumFile = "$resolvedBackup.sha256"
if (Test-Path -LiteralPath $checksumFile) {
    $expected = (Get-Content -Raw -LiteralPath $checksumFile).Trim().Split(" ")[0]
    $actual = (Get-FileHash -Algorithm SHA256 -Path $resolvedBackup).Hash
    if ($expected -ne $actual) {
        throw "Backup checksum mismatch."
    }
}

docker exec $Container pg_isready -U $User -d $Database
if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL is not ready."
}

$remotePath = "/tmp/restore-$([guid]::NewGuid().ToString('N')).dump"
docker cp $resolvedBackup "${Container}:$remotePath"
if ($LASTEXITCODE -ne 0) {
    throw "Could not copy the backup into the container."
}

try {
    docker exec $Container pg_restore -U $User -d $Database --clean --if-exists --no-owner $remotePath
    if ($LASTEXITCODE -ne 0) {
        throw "pg_restore failed."
    }
}
finally {
    docker exec $Container rm -f $remotePath
}

Write-Output "Restore completed from: $resolvedBackup"
