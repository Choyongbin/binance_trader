$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
# Historical read-only diagnostics; this never triggers a private stream,
# starts a signed REST diagnostic, or sends any new/cancel order request.
$s = Invoke-RestMethod 'http://127.0.0.1:8080/api/v1/trading/bounded-drift/status' -TimeoutSec 6
if ($s.trading_authorized -ne $false -or $s.continuous_coverage_proven -ne $false -or
    $s.manual_review_required -ne $true -or $s.historical_only -ne $true) {
    throw '이중 관측 진단에서 잘못된 거래 승인 플래그 발견'
}
Write-Host ('PASS: 과거 이중 관측 진단=' + $s.status + ', 기록=' + $s.report_count + ', 거래 잠금 유지')
if ($s.status -eq 'NOT_RUN') { Write-Host '주의: 이 기능은 라이브 Demo 계정의 자동 진단으로 연결되어 있지 않습니다.' }
