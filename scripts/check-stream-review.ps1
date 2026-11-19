# Windows PowerShell 5.1/PowerShell 7 — historical read-only status. No keys/orders.
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$r = Invoke-RestMethod 'http://127.0.0.1:8080/api/v1/trading/stream-review/status' -TimeoutSec 5
if ($r.trading_authorized -ne $false -or $r.manual_review_required -ne $true -or $r.stream_continuity_proven -ne $false -or $r.historical_only -ne $true) {
    throw '읽기 전용 사설 계정 이벤트 대조가 거래 권한을 잘못 부여했습니다.'
}
Write-Host ('PASS: 과거 사설 이벤트 진단만 조회 (상태=' + $r.status + ', 저장=' + $r.report_count + ')')
Write-Host '비공개 Binance Demo 스트림 실환경 테스트와 주문 검증은 별도로 필요합니다.'
