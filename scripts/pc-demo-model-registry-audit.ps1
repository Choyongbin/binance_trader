#Requires -Version 5.1
<#
v0.4.11-dev.3.138 OFFLINE model-registry inspection.
No Binance credentials, REST/WS, trading stores, orders or account changes.
With no arguments it shows the compiled registry only. Use -SettingsPath to
inspect an existing settings.json without creating or changing the file.
#>
[CmdletBinding()]
param(
    [string]$SettingsPath = '',
    [string]$Selected = ''
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$release = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw -Encoding UTF8).Trim()
if ($release -cne 'v0.4.11-dev.3.138') { throw "Requires dev.3.138; found $release" }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required.' }
if ($SettingsPath -and $Selected) { throw 'Choose -SettingsPath OR -Selected, not both.' }
$arguments = @('run', './cmd/model-registry-audit')
if ($SettingsPath) {
    $file = Get-Item -LiteralPath $SettingsPath -Force -ErrorAction Stop
    if ($file.PSIsContainer -or ($file.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
        throw 'Settings must be a regular JSON file.'
    }
    $arguments += @('-settings', $file.FullName)
} elseif ($Selected) {
    $arguments += @('-selected', $Selected)
}
Push-Location -LiteralPath $root
try {
    & go @arguments
    if ($LASTEXITCODE -ne 0) { throw "Model registry audit BLOCKED (exit=$LASTEXITCODE). No execution authorized." }
} finally { Pop-Location }
