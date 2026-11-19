#Requires -Version 5.1
# OFFLINE ONLY: rehearsal stages are NOT financial accounting or settlement.
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][ValidatePattern('^bt_[0-9a-f]{28}$')][string]$ClientOrderID,
  [Parameter(Mandatory=$true)][ValidatePattern('^package-a-[A-Za-z0-9_.-]+\.json$')][string]$EntryArchiveFile,
  [ValidateSet('inspect','pair','continuity','commit-gate','prepare','lifecycle','ownership','seal')][string]$Action='inspect',
  [ValidatePattern('^[a-f0-9]{64}$')][string]$CommitSHA256='',
  [switch]$LegacyReadOnly,
  [switch]$LinkedEvent
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne 'v0.4.11-dev.3.138') {throw 'Require dev.3.138'}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {throw 'Go required'}
$data=Join-Path $root 'data'
if (-not (Test-Path -LiteralPath $data -PathType Container)) {throw 'Existing data directory required'}
$results=Join-Path $root 'pc-results'
$resultsItem=Get-Item -LiteralPath $results -ErrorAction Stop
if (-not $resultsItem.PSIsContainer -or ($resultsItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) {throw 'Unsafe results directory'}
$archive=Join-Path $results $EntryArchiveFile
$archiveItem=Get-Item -LiteralPath $archive -ErrorAction Stop
if ($archiveItem.PSIsContainer -or ($archiveItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) {throw 'Unsafe entry archive'}
$socket=New-Object System.Net.Sockets.TcpClient
try {
  $pending=$socket.BeginConnect('127.0.0.1',8080,$null,$null)
  if ($pending.AsyncWaitHandle.WaitOne(350,$false)) {
    try {$socket.EndConnect($pending);throw 'Port 8080 listening: stop the server before any rehearsal operation'}
    catch [System.Net.Sockets.SocketException] { }
  }
} finally {$socket.Dispose()}
Push-Location $root
try {
  if ($LegacyReadOnly -and $Action -notin @('inspect','pair')) {throw 'Legacy mode is strictly read-only'}
  if ($LegacyReadOnly -and $LinkedEvent) {throw 'Legacy and linked-event modes cannot be combined'}
  if ($Action -in @('continuity','commit-gate') -and -not $LinkedEvent) {throw 'Continuity or commit-gate requires -LinkedEvent'}
  if ($Action -in @('pair','continuity','commit-gate') -and -not $CommitSHA256) {throw 'Read-only pair/continuity/commit-gate requires a seal commit SHA-256 from inspect'}
  if ($Action -notin @('pair','continuity','commit-gate') -and $CommitSHA256) {throw 'CommitSHA256 is only valid for pair, continuity or commit-gate'}
  $argsList=@('run','./cmd/demo-close-atomic-rehearsal','-data',$data,'-client',$ClientOrderID,'-archive',$archive,'-action',$Action)
  if ($Action -in @('pair','continuity','commit-gate')) {$argsList+=@('-commit',$CommitSHA256)}
  if ($LinkedEvent) {$argsList+=@('-linked-event')}
  if ($LegacyReadOnly) {$argsList+=@('-legacy-read-only')}
  if ($Action -notin @('inspect','pair','continuity','commit-gate')) {
    Write-Warning 'Creates immutable NON-POSTED rehearsal checkpoints only. No settlement authorization.'
    $argsList+=@('-confirm','SHADOW_ONLY_NO_FINANCIAL_POSTING')
  }
  $old=$ErrorActionPreference
  try {$ErrorActionPreference='Continue'; & go @argsList; $rc=$LASTEXITCODE}
  finally {$ErrorActionPreference=$old}
  if ($rc -ne 0) {throw 'Rehearsal blocked. Preserve immutable sidecar evidence for manual inspection.'}
} finally {Pop-Location}
