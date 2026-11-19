# Windows PowerShell 5.1 and PowerShell 7, read-only local verification.
$ErrorActionPreference='Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$api='http://127.0.0.1:8080'
Write-Host '로컬 과거 회계 SQLite 색인 확인 (Binance 주문 없음)'
$status=Invoke-RestMethod ($api + '/api/v1/trading/accounting/index/status') -TimeoutSec 5
if ($status.trading_authorized -ne $false -or $status.actual_net_verified -ne $false -or $status.historical_only -ne $true) { throw '색인이 거래를 승인하거나 최종 순손익으로 표시되고 있습니다.' }
Write-Host ('색인=' + $status.state + ', 원본 보고서=' + $status.report_count + '건')
if ($status.state -eq 'NOT_BUILT' -and $status.report_count -eq 0) {Write-Host 'PASS: 과거 회계 보고서 없음, 가짜 손익 없음';exit 0}
if ($status.state -ne 'CURRENT') {throw '색인이 최신이 아닙니다. 별도 운영자 CLI를 이용해 불변 원본을 검사하고 색인을 생성하세요.'}
$page=Invoke-RestMethod ($api + '/api/v1/trading/accounting/history?limit=10&offset=0') -TimeoutSec 5
if ($page.index.trading_authorized -ne $false -or $page.index.actual_net_verified -ne $false -or $page.index.historical_only -ne $true) {throw '조회 기록의 안전 레이블 불일치'}
Write-Host ('PASS: 읽기 전용 과거 보고서 총 ' + $page.total + '개')
