param(
  [Parameter(Mandatory=$true)][string]$DataDir
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) { throw "DataDir not found: $DataDir" }
Write-Host "READ-ONLY operator remediation plan. Use a stopped-server data copy when making incident decisions."
$raw = & go run ./cmd/operator-remediation -data $DataDir
if ($LASTEXITCODE -ne 0) { throw "operator-remediation failed" }
$raw
