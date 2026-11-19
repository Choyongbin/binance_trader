#Requires -Version 5.1
# Read an independently authenticated ZIP SHA-256 before executing ANY ZIP code.
# The expected hash must come from a trusted channel OUTSIDE the ZIP itself.
[CmdletBinding(DefaultParameterSetName='DryRun')]
param(
    [Parameter(Mandatory=$true)][string]$Package,
    [Parameter(Mandatory=$true)][string]$Target,
    [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-fA-F]{64}$')][string]$ExpectedSHA256,
    [Parameter(ParameterSetName='DryRun')][switch]$DryRun,
    [Parameter(ParameterSetName='Apply')][switch]$Apply
)
$ErrorActionPreference = 'Stop'
$pkg = (Resolve-Path -LiteralPath $Package -ErrorAction Stop).Path
$targetDir = (Resolve-Path -LiteralPath $Target -ErrorAction Stop).Path
if (-not (Test-Path -LiteralPath $pkg -PathType Leaf)) { throw 'Package must be an existing ZIP file.' }
if (-not (Test-Path -LiteralPath $targetDir -PathType Container)) { throw 'Target must be an existing directory.' }
$root = [IO.Path]::GetFullPath($targetDir).TrimEnd([char[]]@('\','/'))
$packageFull = [IO.Path]::GetFullPath($pkg)
if ($packageFull.StartsWith(($root + '\'), [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Put ZIP outside the target installation to preserve full backups.'
}
$stageDir = Join-Path ([IO.Path]::GetTempPath()) ('binance-reviewed-updater-' + [guid]::NewGuid().ToString('N'))
try {
    New-Item -ItemType Directory -Path $stageDir -Force | Out-Null
    # Hash the exact private staging copy that will be expanded and executed.
    $stagedPkg = Join-Path $stageDir 'verified-release.zip'
    Copy-Item -LiteralPath $pkg -Destination $stagedPkg -ErrorAction Stop
    $actual = (Get-FileHash -LiteralPath $stagedPkg -Algorithm SHA256).Hash
    if (-not [string]::Equals($actual, $ExpectedSHA256, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Untrusted ZIP SHA-256: expected=$ExpectedSHA256 actual=$actual. NO code executed."
    }
    Write-Host 'Independent ZIP SHA-256 PASS (before execution).' -ForegroundColor Green
    $sourceDir = Join-Path $stageDir 'release'
    Expand-Archive -LiteralPath $stagedPkg -DestinationPath $sourceDir -ErrorAction Stop
    $reviewedUpdater = Join-Path $sourceDir 'update.ps1'
    if (-not (Test-Path -LiteralPath $reviewedUpdater -PathType Leaf)) {
        throw 'Verified ZIP contains no update.ps1'
    }
    if ($Apply) {
        & $reviewedUpdater -Package $stagedPkg -TargetRoot $root -Apply
    } else {
        & $reviewedUpdater -Package $stagedPkg -TargetRoot $root -DryRun
    }
    if (-not $?) { throw 'Reviewed updater failed' }
} finally {
    if (Test-Path -LiteralPath $stageDir) {
        Remove-Item -LiteralPath $stageDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
