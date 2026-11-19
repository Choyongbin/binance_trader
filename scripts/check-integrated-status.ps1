# Windows PowerShell 5.1 / PowerShell 7, historical READ ONLY; no credentials.
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$v = Invoke-RestMethod 'http://127.0.0.1:8080/api/v1/version' -TimeoutSec 5
if ($v.release -ne (Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\VERSION') -Raw).Trim() -or $v.trading -ne 'LOCKED') { throw '서버 버전 또는 거래 잠금 불일치' }
$r = Invoke-RestMethod 'http://127.0.0.1:8080/api/v1/trading/integrated-review/status' -TimeoutSec 5
if ($r.trading_authorized -ne $false -or $r.stream_continuity_proven -ne $false -or $r.manual_review_required -ne $true -or $r.historical_only -ne $true) { throw '진단 결과에서 안전 잠금 위반' }
Write-Host ('PASS: '+$v.release+', 주문 잠금. 과거 통합 진단: '+$r.status+', 보고서 '+$r.report_count+'개')
Write-Host 'NOT_RUN이 정상일 수 있습니다: 이 API는 새 진단을 실행하지 않습니다.'
