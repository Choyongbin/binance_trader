param(
    [Parameter(Mandatory=$true)][string]$DataDir
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) { throw "DataDir not found: $DataDir" }
$raw = & go run ./cmd/operator-incident-packets -data $DataDir
if ($LASTEXITCODE -ne 0) { throw "operator-incident-packets failed" }
$packet = $raw | ConvertFrom-Json
if ($packet.trading_authorized -ne $false -or $packet.automatic_repair -ne $false -or $packet.mutation_performed -ne $false) {
    throw "unsafe incident packet invariant"
}
$packet | ConvertTo-Json -Depth 12
