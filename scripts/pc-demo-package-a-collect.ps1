#Requires -Version 5.1
# One-command, fail-closed, READ-ONLY Binance Futures Demo evidence capture.
# Preserves data/, journals, MAY_HAVE_SENT and HTTP 423 trading lock.
[CmdletBinding()]
param(
  [ValidatePattern('^[A-Za-z0-9_-]{1,80}$')]
  [string]$ClientOrderID = 'bt_06893a0c50b6928dede7365cd2f0'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$results = Join-Path $root 'pc-results'
$expected = 'v0.4.11-dev.3.138'
if ((Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim() -ne $expected) {
  throw "Expected $expected source; do not mix versions."
}
if (-not (Test-Path -LiteralPath (Join-Path $root 'data') -PathType Container)) {
  throw 'Existing data/ is required; do NOT initialize or reset it.'
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go is required.' }
# Never stop a running server automatically: the operator must choose when.
$socket = New-Object System.Net.Sockets.TcpClient
try {
  $pending = $socket.BeginConnect('127.0.0.1', 8080, $null, $null)
  if ($pending.AsyncWaitHandle.WaitOne(350, $false)) {
    try { $socket.EndConnect($pending); throw 'Port 8080 is listening. Stop the server before signed inspection.' }
    catch [System.Net.Sockets.SocketException] { }
  }
} finally { $socket.Dispose() }
if (Test-Path -LiteralPath $results) {
  $resultsItem = Get-Item -LiteralPath $results -Force
  if (-not $resultsItem.PSIsContainer -or ($resultsItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
    throw 'Unsafe pc-results path (symlink/junction or not a directory).'
  }
} else {
  New-Item -Path $results -ItemType Directory -ErrorAction Stop | Out-Null
}
$runId = (Get-Date -Format 'yyyyMMdd_HHmmss_fff') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
$temp = Join-Path ([IO.Path]::GetTempPath()) ('binance-audit-' + [Guid]::NewGuid().ToString('N'))
$bin = Join-Path $temp 'demo-signed-inspect.exe'
$summaryPath = Join-Path $results ('package-a-collection-' + $runId + '.json')
$summary = [ordered]@{
  schema = 1
  release = $expected
  client_order_id = $ClientOrderID
  mode = 'READ_ONLY_SIGNED_GET'
  stages = [ordered]@{}
  archive_file = $null
  entry_recovery_plan_file = $null
  diagnostic_file = $null
  state = 'NOT_STARTED'
  settlement_authorized = $false
  trading_authorized = $false
  lifecycle_changed = $false
  ownership_changed = $false
  original_data_unchanged_by_tool = $true
  limitations = @('The archive is a local copy, not a Binance-signed document.',
                  '14-day bounded trade-history limit can prevent old incident discovery.',
                  'Funding/model attribution and atomic settlement are NOT proven.',
                  'Entry recovery plan is not permission to import fills or settle a lifecycle.')
}
function Invoke-AuditStage([string]$action, [string]$inputValue) {
  $fileName = 'package-a-' + $action + '-' + $runId + '.json'
  $outPath = Join-Path $results $fileName
  $auditArgs = @('-root', $root, '-out', $outPath, '-package-a-action', $action,
                 '-package-a-client', $inputValue)
  # Do not use Invoke-Expression, Start-Transcript or echo credentials.
  $savedErrorAction = $ErrorActionPreference
  try {
    $ErrorActionPreference = 'Continue'
    # Native stdout must not become part of this function's returned filename.
    & $bin @auditArgs | Out-Host
    $exitCode = $LASTEXITCODE
  } finally { $ErrorActionPreference = $savedErrorAction }
  if ($exitCode -ne 0) { throw "Fail-closed $action signed audit (exit $exitCode)." }
  if (-not (Test-Path -LiteralPath $outPath -PathType Leaf)) { throw "No $action JSON generated." }
  $summary.stages[$action] = $fileName
  return $fileName
}
Push-Location $root
try {
  New-Item -ItemType Directory -Path $temp -Force | Out-Null
  & go test -short ./cmd/demo-signed-inspect
  if ($LASTEXITCODE -ne 0) { throw 'Go inspector tests failed; no signed requests issued.' }
  & go build -o $bin ./cmd/demo-signed-inspect
  if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $bin)) { throw 'Inspector build failed.' }
  $summary.state = 'CAPTURING'
  # Stage 1 saves signed entry order and fills even if the exit is unknowable.
  $entryName = Invoke-AuditStage 'entry-archive' $ClientOrderID
  # Stage 1b: OFFLINE candidate-only mapping of missing entry fills, even
  # if the external exit is ambiguous/outside the bounded search window.
  $planName = Invoke-AuditStage 'plan-entry-recovery' $entryName
  $summary.entry_recovery_plan_file = $planName
  # Stage 2 preserves the candidate order IDs and reasons for ambiguity.
  $exitName = Invoke-AuditStage 'exit-discovery' $ClientOrderID
  $exitReport = Get-Content -LiteralPath (Join-Path $results $exitName) -Raw | ConvertFrom-Json
  $discovery = $exitReport.exit_discovery
  if ($null -eq $discovery -or $discovery.status -ne 'UNIQUE_EXIT_ORDER_ID_CANDIDATE_REVIEW_ONLY' -or
      $null -eq $discovery.external_exit_order_candidates -or @($discovery.external_exit_order_candidates).Count -ne 1 -or
      -not $discovery.external_exit_order_candidates[0].signed_order_confirmed) {
    $summary.state = 'AMBIGUOUS_OR_INCOMPLETE_REVIEW_ONLY'
    Write-Warning 'Exit not uniquely evidenced. Keep original data intact; review exit-discovery report.'
  } else {
    # Stage 3 performs an independent, second signed incident audit.
    $archiveName = Invoke-AuditStage 'discovery-archive' $ClientOrderID
    $summary.archive_file = $archiveName
    # Stage 4 verifies the saved archive against original journals and reports blockers.
    $diagnosticName = Invoke-AuditStage 'diagnose-discovery-archive' $archiveName
    $summary.diagnostic_file = $diagnosticName
    $summary.state = 'ARCHIVED_AND_DIAGNOSED_NO_SETTLEMENT'
  }
} catch {
  $summary.state = 'FAILED_CLOSED'
  $summary.error = [string]$_.Exception.Message
  Write-Warning ('Package A stopped safely: ' + $summary.error)
} finally {
  try {
    # A failed stage never deletes its existing forensic JSON.
    $summary | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
    Write-Host ('READ-ONLY collection summary: ' + $summaryPath)
  } finally {
    Pop-Location
    Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue
  }
}
if ($summary.state -eq 'FAILED_CLOSED') { throw 'Signed evidence collection failed closed; inspect summary and preserve original journals.' }
Write-Host ('State: ' + $summary.state + '; ORDERS LOCKED; NO SETTLEMENT.') -ForegroundColor Yellow
