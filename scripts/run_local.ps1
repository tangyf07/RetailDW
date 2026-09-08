$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
if (-not $env:SPARK_LOCAL_IP) { $env:SPARK_LOCAL_IP = "127.0.0.1" }

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    Write-Error "Java 17+ is required for PySpark. Install a JDK and retry."
}

$Python = if ($env:PYTHON) { $env:PYTHON } else { "python" }
& $Python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -U pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt

$dtArgs = @()
if ($env:DT) {
    $dtArgs = @("--dt", $env:DT)
} elseif ($args.Count -ge 2 -and $args[0] -eq "--dt") {
    $dtArgs = @("--dt", $args[1])
} elseif ($args.Count -ge 1 -and $args[0] -match '^\d{4}-\d{2}-\d{2}$') {
    $dtArgs = @("--dt", $args[0])
}

# PySpark prints JVM warnings to stderr; do not treat as terminating errors.
$ErrorActionPreference = "Continue"
& .\.venv\Scripts\python.exe jobs\pipeline.py @dtArgs
exit $LASTEXITCODE