#Requires -Version 5.1
[CmdletBinding()]
param(
    [int]$Port = 8080,
    [switch]$Apply
)
$ErrorActionPreference = 'Stop'
$base = "http://127.0.0.1:$Port"
if (-not $Apply) {
    throw '설정 변경은 명시적 -Apply가 필요합니다. 주문은 생성하지 않지만 Binance Demo 선물 계정 설정을 변경합니다.'
}
$version = Invoke-RestMethod "$base/api/v1/version" -TimeoutSec 5
if ($version.trading -ne 'LOCKED') { throw '거래 잠금 상태가 예상과 다릅니다.' }
$bootstrap = Invoke-RestMethod "$base/api/v1/bootstrap" -TimeoutSec 5
if ($bootstrap.environment -ne 'demo') { throw 'Demo 환경에서만 실행할 수 있습니다.' }
if (-not $bootstrap.csrf_token) { throw 'CSRF token을 받지 못했습니다.' }
Write-Host '현재 signed Demo 설정:'
Invoke-RestMethod "$base/api/v1/trading/config" -TimeoutSec 15 | ConvertTo-Json -Depth 20 | Write-Host
$headers = @{ 'X-CSRF-Token' = [string]$bootstrap.csrf_token }
$body = @{ confirmation = 'APPLY DEMO BTCUSDT HEDGE ISOLATED 15X' } | ConvertTo-Json -Compress
$result = Invoke-RestMethod -Method Post -Uri "$base/api/v1/trading/config/apply-demo-v1" -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec 35
Write-Host '설정 적용 결과:'
$result | ConvertTo-Json -Depth 20 | Write-Host
$verify = Invoke-RestMethod "$base/api/v1/trading/config" -TimeoutSec 15
if (-not $verify.settings_match_v1) { throw '최종 signed 설정이 안전 계약과 일치하지 않습니다.' }
Write-Host 'PASS: Demo BTCUSDT Hedge + ISOLATED + 15x + auto-add OFF signed 검증 완료. 주문은 여전히 LOCKED.'
