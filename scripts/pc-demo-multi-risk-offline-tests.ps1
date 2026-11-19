#Requires -Version 5.1
<# v0.4.11-dev.3.138. Offline Go tests only. No API keys, server or orders; test fixtures use temporary data only. #>
[CmdletBinding()]
param([string]$Root = (Split-Path $PSScriptRoot -Parent))
$ErrorActionPreference = 'Stop'
$version = (Get-Content -LiteralPath (Join-Path $Root 'VERSION') -Raw -Encoding UTF8).Trim()
if ($version -cne 'v0.4.11-dev.3.138') { throw "Requires dev.3.138; found $version" }
Push-Location -LiteralPath $Root
try {
    & go test -count=1 -timeout=2m ./internal/trading -run '^TestOfflineMultiModelRiskReview'
    if ($LASTEXITCODE -ne 0) { throw "Offline multi-model risk test failed: $LASTEXITCODE" }
} finally { Pop-Location }
