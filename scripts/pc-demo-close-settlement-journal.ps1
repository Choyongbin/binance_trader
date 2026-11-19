#Requires -Version 5.1
# OFFLINE append-only settlement barrier ONLY; no financial posting.
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][ValidatePattern('^bt_[0-9a-f]{28}$')][string]$ClientOrderID,
  [Parameter(Mandatory=$true)][ValidatePattern('^package-a-[A-Za-z0-9_.-]+\.json$')][string]$EntryArchiveFile,
  [ValidateSet('inspect','inspect-recovery','inspect-evidence','prepare','fence')][string]$Action='inspect',
  [switch]$LegacyReadOnly
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') { throw 'Require dev.3.138' }
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {throw 'Go required'}
$data=Join-Path $root 'data'
if (-not (Test-Path -LiteralPath $data -PathType Container)) {throw 'Existing data directory required; do not initialize it'}
$results=Join-Path $root 'pc-results'
$resultsItem=Get-Item -LiteralPath $results -ErrorAction Stop
if (-not $resultsItem.PSIsContainer -or ($resultsItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'Unsafe results directory' }
$archive=Join-Path $results $EntryArchiveFile
$archiveItem=Get-Item -LiteralPath $archive -ErrorAction Stop
if ($archiveItem.PSIsContainer -or ($archiveItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) {throw 'Unsafe entry archive file'}
# Stop the app manually. Do not use Test-NetConnection's long default timeout.
$socket=New-Object System.Net.Sockets.TcpClient
try {
  $pending=$socket.BeginConnect('127.0.0.1',8080,$null,$null)
  if ($pending.AsyncWaitHandle.WaitOne(350,$false)) {
    try {$socket.EndConnect($pending);throw 'Port 8080 is listening: stop the server before journal creation'}
    catch [System.Net.Sockets.SocketException] { }
  }
} finally { $socket.Dispose() }
Push-Location $root
try {
  if ($LegacyReadOnly -and $Action -in @('prepare','fence')) {throw 'Legacy journal is READ-ONLY; only inspection is permitted'}
  & go test ./cmd/demo-close-settlement-journal
  if ($LASTEXITCODE -ne 0) { throw 'Journal command compilation failed' }
  $arguments=@('run','./cmd/demo-close-settlement-journal','-data',$data,'-client',$ClientOrderID,'-archive',$archive,'-action',$Action)
  if ($LegacyReadOnly) {$arguments+='-legacy-read-only'}
  if ($Action -in @('prepare','fence')) {
    Write-Host 'Creating immutable NON-POSTED sidecar only. No settlement or trading permission.' -ForegroundColor Yellow
    $arguments+=@('-confirm','JOURNAL_ONLY_NO_SETTLEMENT')
  }
  $old=$ErrorActionPreference
  try {$ErrorActionPreference='Continue'; & go @arguments; $rc=$LASTEXITCODE}
  finally {$ErrorActionPreference=$old}
  if ($rc -ne 0) {throw 'Blocked: preserve immutable originals, do not repair/delete incomplete sidecars'}
} finally {Pop-Location}
