#Requires -Version 5.1
# Explicit, finite 30s/90s, signed full-account REST before/after private WS.
# Secret is requested interactively and never written to a file/command line.
[CmdletBinding()]
param([ValidateSet(30,90)][int]$DurationSeconds=30, [ValidateSet(24,72,120)][int]$LookbackHours=24)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=New-Object System.Text.UTF8Encoding $false
$root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$base='http://127.0.0.1:8080'
$v=Invoke-RestMethod ($base+'/api/v1/version') -TimeoutSec 8
$boot=Invoke-RestMethod ($base+'/api/v1/bootstrap') -TimeoutSec 8
if($v.trading -ne 'LOCKED' -or $v.release -ne (Get-Content (Join-Path $root 'VERSION') -Raw).Trim()) {throw 'STOP: unexpected release / order lock'}
if($boot.environment -ne 'demo' -or -not $boot.csrf_token){throw 'STOP: Demo mode and local CSRF token required'}
Write-Host ('Starting signed read-only '+$DurationSeconds+' second observation. This is NOT trade authorization.')
$secure=Read-Host 'Stored Demo vault passphrase (hidden)' -AsSecureString
$bstr=[IntPtr]::Zero
try {
 $bstr=[Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
 $passphrase=[Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
 if([string]::IsNullOrWhiteSpace($passphrase)){throw 'Empty passphrase'}
 $body=@{passphrase=$passphrase;lookback_hours=$LookbackHours;duration_seconds=$DurationSeconds} | ConvertTo-Json -Compress
 $passphrase=$null
 $secure.Dispose()
 $headers=@{'X-CSRF-Token'=$boot.csrf_token;'Origin'=$base}
 try {
  $result=Invoke-RestMethod -Uri ($base+'/api/v1/recovery/soak') -Method Post -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec ($DurationSeconds+110)
 } finally {$body=$null}
} finally {
 if($bstr -ne [IntPtr]::Zero) {[Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)}
}
$report=Invoke-RestMethod ($base+'/api/v1/recovery/soak/status') -TimeoutSec 12
if($report.trading_authorized -ne $false -or $report.stream_continuity_proven -ne $false -or $report.manual_review_required -ne $true -or $report.historical_only -ne $true){throw 'STOP: unexpected trading/continuity flags'}
if($report.status -ne 'REVIEWED' -or $report.soak_completed -ne $true -or $report.second_rest_reviewed -ne $true){throw 'STOP: signed second read or observation completion was not confirmed'}
$outDir=Join-Path $root 'pc-results';New-Item -ItemType Directory -Path $outDir -Force | Out-Null
$path=Join-Path $outDir 'soak-status-safe.json'
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $path -Encoding UTF8
Write-Host 'PASS: two signed REST reads and a finite WS observation completed. Trading remains LOCKED.'
if ($result.first_last_exposure_equal -eq $false) {
 Write-Warning 'REST account exposure changed between reads. Investigate manually; this is NOT a stable account result.'
} else {
 Write-Host 'No exposure-digest drift observed between these two samples (not continuous proof).'
}
Write-Host ('Manual blockers retained: '+@($report.blockers).Count)
Write-Host ('Sanitized historical report: '+$path)
Write-Host 'Observation stopped. Continuous event coverage was NOT proven. Trading LOCKED.'
