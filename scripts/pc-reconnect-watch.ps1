#Requires -Version 5.1
# Operator-triggered, FINITE Demo reconnect engineering test, NEVER a trade permit.
[CmdletBinding()]
param([ValidateSet(30,90)][int]$DurationSeconds=30, [ValidateSet(24,72,120)][int]$LookbackHours=24)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$base = 'http://127.0.0.1:8080'
$v = Invoke-RestMethod ($base+'/api/v1/version') -TimeoutSec 8
$boot = Invoke-RestMethod ($base+'/api/v1/bootstrap') -TimeoutSec 8
if ($v.trading -ne 'LOCKED' -or $v.release -ne (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()) { throw 'STOP: installed version or trading lock mismatched' }
if ($boot.environment -ne 'demo' -or -not $boot.csrf_token) { throw 'STOP: connected Demo account and localhost CSRF required' }
Write-Host ('Read-only finite reconnection engineering test: '+$DurationSeconds+' seconds. No orders. Max 2 reconnects.')
$secure = Read-Host 'Stored Demo vault passphrase (hidden)' -AsSecureString
$bstr = [IntPtr]::Zero
try {
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    $passphrase = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    if ([string]::IsNullOrWhiteSpace($passphrase)) { throw 'Empty passphrase' }
    $body = @{passphrase=$passphrase;lookback_hours=$LookbackHours;duration_seconds=$DurationSeconds} | ConvertTo-Json -Compress
    $passphrase = $null
    $secure.Dispose()
    $headers = @{'X-CSRF-Token'=$boot.csrf_token;'Origin'=$base}
    try {
        $result = Invoke-RestMethod -Uri ($base+'/api/v1/recovery/reconnect-watch') -Method Post -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec ($DurationSeconds+110)
    } finally { $body = $null }
} finally {
    if ($bstr -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}
$report = Invoke-RestMethod ($base+'/api/v1/recovery/reconnect-watch/status') -TimeoutSec 12
if ($report.trading_authorized -ne $false -or $report.stream_continuity_proven -ne $false -or $report.manual_review_required -ne $true -or $report.historical_only -ne $true) { throw 'STOP: unexpected diagnostic safety fields' }
if ($result.status -ne 'FINITE_RECONNECT_TEST_ONLY' -or $report.status -ne 'OBSERVED' -or $report.final_rest_reviewed -ne $true) { throw 'STOP: reconnect review did not finish' }
$outDir = Join-Path $root 'pc-results'
New-Item -ItemType Directory -Path $outDir -Force | Out-Null
$path = Join-Path $outDir 'reconnect-watch-safe.json'
# Only server-sanitized status; NEVER save a password, listenKey or exchange URL.
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $path -Encoding UTF8
$epochCount = @($report.epochs).Count
$blockerCount = @($report.blockers).Count
Write-Host ("PASS: historical reconnection check; epochs={0}; reconnects={1}. Trading LOCKED." -f $epochCount, $report.reconnects)
Write-Host ("Final signed REST reviewed: {0}; manual blockers: {1}" -f $report.final_rest_reviewed, $blockerCount)
Write-Host ('Sanitized status: '+$path)
Write-Host 'This does NOT prove continuous history; a disconnect may still hide unknown events.'
