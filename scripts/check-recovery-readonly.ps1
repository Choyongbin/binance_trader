# Read-only Windows check. No API key, secret or trading POST is performed.
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$base = 'http://127.0.0.1:8080'
$v = Invoke-RestMethod ($base + '/api/v1/version') -TimeoutSec 5
$r = Invoke-RestMethod ($base + '/api/v1/recovery/status') -TimeoutSec 5
if ($v.release -ne (Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\VERSION') -Raw).Trim() -or $v.trading -ne 'LOCKED') { throw '버전 또는 잠금 상태가 다릅니다' }
if ($r.trading_authorized -ne $false -or $r.stream_continuity_proven -ne $false -or $r.manual_review_required -ne $true) { throw '복구 기록의 읽기 전용 잠금 상태가 올바르지 않습니다' }
Write-Host ('PASS: ' + $v.release + '; 복구=' + $r.status + '; 과거 기록=' + $r.report_count)
if ($r.diagnostic_running_or_stale_lock) { Write-Warning '복구 진단 실행 중 또는 기존 크래시 잠금 확인 필요' }
Write-Host '추가 관측을 실행하려면 한글 UI 설정에서 사용자가 직접 승인하세요. 주문은 실행하지 않습니다.'
