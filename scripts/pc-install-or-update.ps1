#Requires -Version 5.1
# Safe offline installer wrapper. Default: dry-run. Runs the tested Go updater.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Target,
    [switch]$Apply,
    [switch]$NewInstall
)
$ErrorActionPreference = 'Stop'
$source = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw 'Go 1.23 or later is required; verify: go version'
}
$goArgs = @('run', './cmd/safe-update', '-source', $source, '-target', $Target)
if ($NewInstall) { $goArgs += '-new' }
if ($Apply) { $goArgs += '-apply' }
Push-Location $source
try {
    & go @goArgs
    if ($LASTEXITCODE -ne 0) { throw "Safe updater refused installation (exit $LASTEXITCODE). Target was not automatically rolled back; inspect any reported backup path." }
} finally {
    Pop-Location
}
