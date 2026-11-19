#Requires -Version 5.1
[CmdletBinding()]
param([int]$Port = 8080)
$ErrorActionPreference = 'Stop'
$base = "http://127.0.0.1:$Port"
$version = Invoke-RestMethod "$base/api/v1/version" -TimeoutSec 5
if ($version.trading -ne 'LOCKED') { throw '거래 잠금 상태가 예상과 다릅니다.' }
$bootstrap = Invoke-RestMethod "$base/api/v1/bootstrap" -TimeoutSec 5
if ($bootstrap.environment -ne 'demo') { throw 'Demo 환경에서만 실행할 수 있습니다.' }
if (-not $bootstrap.csrf_token) { throw 'CSRF token을 받지 못했습니다.' }
$headers = @{ 'X-CSRF-Token' = [string]$bootstrap.csrf_token }
$body = @{ confirmation = 'RUN READ ONLY DEMO EXECUTION PREFLIGHT' } | ConvertTo-Json -Compress
Write-Host 'Signed Demo execution preflight 실행 중 (주문/설정 변경 없음)...'
$result = Invoke-RestMethod -Method Post -Uri "$base/api/v1/trading/execution-preflight" -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec 20
$result | ConvertTo-Json -Depth 30 | Write-Host
if ($result.mutation_performed -ne $false) { throw 'read-only preflight가 mutation을 보고했습니다.' }
if ($result.trading_authorized -ne $false) { throw '진단 결과가 거래 허가로 잘못 표시되었습니다.' }
if ($result.can_trade -ne $true) { throw 'Binance signed account canTrade=true가 아닙니다.' }
if ($result.settings_match_v1 -ne $true) { throw 'Demo 거래 설정이 안전 계약과 일치하지 않습니다.' }
if ([int]$result.nonzero_position_count -ne 0) { throw '비어 있지 않은 포지션이 있습니다.' }
if ([int]$result.open_order_count -ne 0) { throw '미체결 일반 주문이 있습니다.' }
if ([int]$result.open_algo_count -ne 0) { throw '미체결 algo 주문이 있습니다.' }
if ([int64]$result.elapsed_ms -gt 5000) { throw 'execution preflight freshness 한도 5초를 초과했습니다.' }
Write-Host 'PASS: signed canTrade + flat account + no open orders/algo + Hedge/ISOLATED/15x execution preflight 검증 완료. 주문은 여전히 LOCKED.'
