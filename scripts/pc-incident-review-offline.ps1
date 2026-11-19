#Requires -Version 5.1
# Two-phase offline COPY evidence acknowledgement, not stale-lock recovery.
# Requires a preexisting review directory OUTSIDE data and its installed tree.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][ValidateSet('Prepare','Acknowledge','Verify')][string]$Action,
    [string]$DataCopy = '',
    [Parameter(Mandatory=$true)][string]$ReviewDirectory,
    [string]$ReviewID = '',
    [string]$Challenge = '',
    [switch]$StoppedWriters,
    [switch]$ExternalBackup
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$reviews = (Resolve-Path -LiteralPath $ReviewDirectory).Path
if (-not (Test-Path -LiteralPath $reviews -PathType Container)) { throw 'The separate review directory must exist.' }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required to verify an offline data copy.' }
$arguments = @('run','./cmd/incident-review','-action',$Action.ToLowerInvariant(),'-reviews',$reviews)
if ($Action -eq 'Verify') {
    if ($DataCopy -ne '' -or $ReviewID -ne '' -or $Challenge -ne '' -or $StoppedWriters -or $ExternalBackup) {
        throw 'Verify accepts only the external review directory.'
    }
} else {
    if ($DataCopy -eq '') { throw 'STOP: use an OFFLINE COPY made with all server processes stopped; never supply live data.' }
    $data = (Resolve-Path -LiteralPath $DataCopy).Path
    if (-not (Test-Path -LiteralPath $data -PathType Container)) { throw 'DataCopy must be an existing directory.' }
    $live = [System.IO.Path]::GetFullPath((Join-Path $root 'data')).TrimEnd('\')
    $copy = [System.IO.Path]::GetFullPath($data).TrimEnd('\')
    $reviewPath = [System.IO.Path]::GetFullPath($reviews).TrimEnd('\')
    $cmp = [System.StringComparison]::OrdinalIgnoreCase
    if ([string]::Equals($live,$copy,$cmp) -or $copy.StartsWith($live+'\',$cmp)) {
        throw 'STOP: refuse the live installation data directory or any of its children.'
    }
    if ([string]::Equals($reviewPath,$copy,$cmp) -or $reviewPath.StartsWith($copy+'\',$cmp) -or $copy.StartsWith($reviewPath+'\',$cmp)) {
        throw 'STOP: review records must be outside the data copy.'
    }
    $arguments += @('-data',$data)
    if ($Action -eq 'Acknowledge') {
        if ($ReviewID -eq '' -or $Challenge -eq '' -or -not $StoppedWriters -or -not $ExternalBackup) {
            throw 'Acknowledge requires the prepared ID/challenge and separate process-stop/external-backup self-attestations.'
        }
        $arguments += @('-id',$ReviewID,'-challenge',$Challenge,'-stopped-writers','-external-backup')
    } elseif ($ReviewID -ne '' -or $Challenge -ne '' -or $StoppedWriters -or $ExternalBackup) {
        throw 'Prepare accepts only the offline data copy and review directory.'
    }
}
Push-Location $root
try {
    & go @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Incident evidence verification failed; do not delete any locks or retry without investigation.' }
} finally { Pop-Location }
Write-Host 'READ-ONLY TO ORIGINAL DATA. No locks cleared. Trading and automatic restart authorization remain FALSE.'
