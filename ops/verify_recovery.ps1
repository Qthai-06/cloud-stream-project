param(
    [string]$Container = "postgres",
    [string]$Database = $env:POSTGRES_DB,
    [string]$User = $env:POSTGRES_USER,
    [string]$OutputPath
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($Database)) {
    $Database = "app_db"
}
if ([string]::IsNullOrWhiteSpace($User)) {
    $User = "admin"
}

docker exec $Container pg_isready -U $User -d $Database
if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL is not ready."
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $env:TEMP "cloud-stream-recovery-$([guid]::NewGuid().ToString('N')).txt"
}

$snapshot = docker exec $Container psql -U $User -d $Database -At -v ON_ERROR_STOP=1 -c @"
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT (window_start, window_end, device_id)) AS logical_row_count,
    COUNT(*) - COUNT(DISTINCT (window_start, window_end, device_id)) AS duplicate_count,
    COALESCE(
        md5(string_agg(
            concat_ws(
                '|', window_start::text, window_end::text, device_id,
                event_count::text, COALESCE(average_temperature::text, ''),
                COALESCE(average_humidity::text, '')
            ),
            '||' ORDER BY window_start, window_end, device_id
        )),
        md5('')
    ) AS result_checksum
FROM sensor_window_metrics;
"@

if ($LASTEXITCODE -ne 0) {
    throw "Recovery integrity checks failed."
}

$invalidWindows = docker exec $Container psql -U $User -d $Database -At -v ON_ERROR_STOP=1 -c @"
SELECT COUNT(*)
FROM sensor_window_metrics
WHERE window_end <= window_start;
"@
if ($LASTEXITCODE -ne 0) {
    throw "Window validation failed."
}

$snapshot = ($snapshot | Out-String).Trim()
$invalidWindows = ($invalidWindows | Out-String).Trim()
if ([string]::IsNullOrWhiteSpace($snapshot) -or [string]::IsNullOrWhiteSpace($invalidWindows)) {
    throw "Integrity snapshot was empty."
}

$snapshot = "$snapshot|$invalidWindows"
Set-Content -LiteralPath $OutputPath -Value $snapshot -Encoding ascii
Write-Output "Integrity snapshot: $snapshot"
Write-Output "Recovery integrity checks completed."
