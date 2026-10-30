$ErrorActionPreference = "Stop"

$javaPaths = @(
    "C:\Program Files\Java",
    "C:\Program Files\Eclipse Adoptium"
)

$javaHome = $null
foreach ($base in $javaPaths) {
    if (Test-Path $base) {
        $candidates = Get-ChildItem $base -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -like "jdk-17*" } |
            Sort-Object Name -Descending

        if ($candidates.Count -gt 0) {
            $javaHome = $candidates[0].FullName
            break
        }
    }
}

if (-not $javaHome) {
    $javaHome = "C:\Program Files\Java\jdk-17"
}

if (-not (Test-Path (Join-Path $javaHome "bin\java.exe"))) {
    throw "Khong tim thay Java 17. Hay coi OpenJDK 17 truoc khi chay script nay."
}

$hadoopHome = "C:\hadoop"
New-Item -ItemType Directory -Force -Path (Join-Path $hadoopHome "bin") | Out-Null

[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Java\jdk-17", "User")
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")

$env:JAVA_HOME = $javaHome
$env:HADOOP_HOME = $hadoopHome
$env:Path = "$javaHome\bin;$hadoopHome\bin;$env:Path"

Write-Host "JAVA_HOME = $env:JAVA_HOME"
Write-Host "HADOOP_HOME = $env:HADOOP_HOME"
Write-Host ""
Write-Host "Ban  chọn Java 17. Hãyy chạy: java -version"
