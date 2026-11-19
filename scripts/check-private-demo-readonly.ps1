$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$api = 'http://127.0.0.1:8080'
Write-Host 'Demo 비공개 계정 WebSocket 과거 진단 확인 (새 연결/주문 없음)'
$version = Invoke-RestMethod ($api + '/api/v1/version') -TimeoutSec 5
if ($version.release -ne (Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\VERSION') -Raw).Trim() -or $version.trading -ne 'LOCKED') { throw '버전 또는 주문 잠금이 예상과 다릅니다' }
$status = Invoke-RestMethod ($api + '/api/v1/trading/private-probe/status') -TimeoutSec 5
if ($status.trading_authorized -ne $false -or $status.stream_continuity_proven -ne $false -or $status.historical_only -ne $true) { throw '안전 플래그가 올바르지 않습니다' }
Write-Host ('진단 상태: ' + $status.status + ', 기록: ' + $status.report_count + ', 현재 실행중: ' + $status.busy)
Write-Host '참고: 새 진단은 한글 UI 설정 화면에서 데모 암호를 직접 입력해 실행하세요. 이 스크립트는 키를 요청/저장하지 않습니다.'
