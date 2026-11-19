# Windows PowerShell 5.1+/7. GET-only status: no account keys or orders.
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$api = 'http://127.0.0.1:8080'
$v = Invoke-RestMethod ($api + '/api/v1/version') -TimeoutSec 5
$a = Invoke-RestMethod ($api + '/api/v1/trading/accounting/status') -TimeoutSec 5
if ($v.release -ne (Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\VERSION') -Raw).Trim() -or $v.trading -ne 'LOCKED') { throw '배포 버전 또는 주문 잠금 오류' }
if ($a.trading_authorized -ne $false -or $a.actual_net_verified -ne $false -or $a.historical_only -ne $true) {
    throw '읽기 전용 회계 진단이 거래 승인을 반환했습니다.'
}
Write-Host ('PASS: ' + $v.release + '; historical accounting=' + $a.status + '; count=' + $a.report_count)
Write-Host '주의: 실제 Demo 체결/펀딩/비공개 스트림/SQLite 데이터는 검증되지 않았습니다.'
