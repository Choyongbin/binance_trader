# Windows PowerShell 5.1/PowerShell 7. Read-only validation; no order POST.
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$api = 'http://127.0.0.1:8080'
Write-Host '현재 실행 중인 누적 릴리스를 조회합니다.'
$v = Invoke-RestMethod ($api + '/api/v1/version') -TimeoutSec 5
$h = Invoke-RestMethod ($api + '/api/v1/health') -TimeoutSec 5
if ($v.release -ne (Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\VERSION') -Raw).Trim()) { throw ('예상 버전이 아닙니다: ' + $v.release) }
if ($v.trading -ne 'LOCKED' -or $h.trading -ne 'disabled') { throw '거래 잠금 상태가 예상과 다릅니다.' }
Write-Host ('PASS: 릴리스 ' + $v.release + ', 거래 잠금 유지, Go 서버 정상')
Write-Host '주의: 실제 Demo 사설 계정 스트림 및 주문·장애 복구는 아직 검증하지 않았습니다.'

$r = Invoke-RestMethod ($api + '/api/v1/recovery/status') -TimeoutSec 5
if ($r.trading_authorized -ne $false -or $r.manual_review_required -ne $true) { throw '복구 진단이 거래를 잠금 해제해서는 안 됩니다.' }
Write-Host ('PASS: 복구 진단 읽기 전용 상태: ' + $r.status)

# Locally allocated auto-wallet never authorizes orders by itself.
$boot = Invoke-RestMethod ($api + '/api/v1/bootstrap') -TimeoutSec 5
if ($boot.environment -eq 'demo') {
    $w = Invoke-RestMethod ($api + '/api/v1/wallet/auto') -TimeoutSec 5
    if ($w.wallet.kind -ne 'INTERNAL_ALLOCATION_DEMO') { throw '자동매매 내부 배정 지갑 버전이 예상과 다릅니다.' }
    Write-Host ('PASS: 내부 배정 예산=' + $w.wallet.assigned_usdt + ' USDT. 실제 거래소 이체가 아닙니다.')
} else {
    Write-Host '실거래 화면이 잠겨 있어 Demo 배정 정보 조회는 생략합니다.'
}

Write-Host '수동 시장가·지정가 서버 사전점검은 UI에서 한 번 눌러 확인하세요. 실제 주문은 여전히 잠겨 있습니다.'

$sr = Invoke-RestMethod ($api + '/api/v1/trading/stream-review/status') -TimeoutSec 5
if ($sr.trading_authorized -ne $false -or $sr.manual_review_required -ne $true -or $sr.stream_continuity_proven -ne $false -or $sr.historical_only -ne $true) { throw '저장된 스트림 대조 기록이 거래 권한을 부여해서는 안 됩니다.' }
Write-Host ('PASS: 비공개 스트림 대조 (과거 읽기 전용) ' + $sr.status)

$private = Invoke-RestMethod ($api + '/api/v1/trading/private-probe/status') -TimeoutSec 5
if ($private.trading_authorized -ne $false -or $private.stream_continuity_proven -ne $false -or $private.historical_only -ne $true) { throw '사설 진단이 거래를 허용했습니다.' }
Write-Host ('PASS: 비공개 스트림 단기 진단 과거 기록 (읽기 전용): ' + $private.status)

$w = Invoke-RestMethod ($api + '/api/v1/trading/bounded-drift/status') -TimeoutSec 5
if ($w.trading_authorized -ne $false -or $w.continuous_coverage_proven -ne $false -or $w.historical_only -ne $true) {
    throw '과거 서명 이중 관측 기록이 거래를 승인해서는 안 됩니다.'
}
Write-Host ('PASS: 서명 이중 관측 과거 기록=' + $w.status)

# Historical accounting summaries are deliberately non-executable.
$accounting = Invoke-RestMethod ($api + '/api/v1/trading/accounting/status') -TimeoutSec 5
if ($accounting.trading_authorized -ne $false -or $accounting.actual_net_verified -ne $false -or $accounting.historical_only -ne $true) {
    throw '회계 과거 보고서가 거래를 허용하거나 최종 PnL로 표시했습니다.'
}
Write-Host ('PASS: 과거 회계 진단=' + $accounting.status + ', 실제 주문 잠금')

# The accounting index must NEVER authorize trading.
$index = Invoke-RestMethod ($api + '/api/v1/trading/accounting/index/status') -TimeoutSec 5
if ($index.trading_authorized -ne $false -or $index.actual_net_verified -ne $false -or $index.historical_only -ne $true) { throw '과거 회계 색인이 거래를 허용했습니다.' }
Write-Host ('PASS: 과거 회계 색인=' + $index.state + ', 실거래 잠금 유지')
