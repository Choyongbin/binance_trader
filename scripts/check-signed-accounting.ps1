# Windows PowerShell read-only check. Never submits an accounting generation or trade request.
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$base = 'http://127.0.0.1:8080'
$v = Invoke-RestMethod ($base + '/api/v1/version') -TimeoutSec 5
if ($v.release -ne (Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\VERSION') -Raw).Trim() -or $v.trading -ne 'LOCKED') { throw '버전 또는 거래 잠금 상태가 예상과 다릅니다.' }
$a = Invoke-RestMethod ($base + '/api/v1/trading/accounting/status') -TimeoutSec 5
if ($a.trading_authorized -ne $false -or $a.actual_net_verified -ne $false -or $a.historical_only -ne $true) { throw '과거 회계 진단이 실제 거래/확정 손익으로 표시되었습니다.' }
Write-Host ('PASS: ' + $v.release + ' / 과거 회계=' + $a.status + ' / 실제 주문 잠금')
Write-Host '과거 회계 보고서는 Demo 인증 연결 후 UI 설정에서 사용자가 직접 생성하세요. 생성 전 실제 주문은 실행되지 않습니다.'
Write-Host 'SQLite는 신규 보고서 저장 후 CLI로 수동 색인 재생성이 필요합니다. Windows 실행 검증은 아직 사용자 PC에서 진행해야 합니다.'
