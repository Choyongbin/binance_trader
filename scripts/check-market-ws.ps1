#Requires -Version 5.1
param(
    [int]$WaitSeconds = 45,
    [int]$ObserveSeconds = 8
)
$ErrorActionPreference = 'Stop'
try {
    & chcp.com 65001 > $null
    [Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
    $OutputEncoding = [Console]::OutputEncoding
} catch { }
$uri = 'http://127.0.0.1:8080'
Write-Host "Binance Demo WebSocket 확인 시작 (최대 $WaitSeconds 초 연결 대기)" -ForegroundColor Cyan
$first = $null
for ($i = 0; $i -lt $WaitSeconds; $i++) {
    try {
        $status = Invoke-RestMethod -Uri "$uri/api/v1/market/status" -TimeoutSec 3
        Write-Host ('[{0:HH:mm:ss}] 상태={1} 준비={2} 누적수신={3} 마지막지연={4}ms 재연결={5}' -f (Get-Date),$status.state,$status.ready,$status.message_count,$status.age_ms,$status.reconnect_count)
        if ($status.endpoint) { Write-Host ("  사용 중인 WS 경로: {0}" -f $status.endpoint) }
        if ($status.last_error) { Write-Host ("  최근 오류: {0}" -f $status.last_error) -ForegroundColor Yellow }
        if ($status.ready -eq $true) { $first = $status; break }
        if ($status.state -eq 'locked') { throw '현재 실거래 화면입니다. UI를 데모로 전환하세요.' }
    } catch {
        if ($_.Exception.Message -match '실거래 화면') { throw }
        Write-Warning "상태 조회 실패: $($_.Exception.Message)"
    }
    Start-Sleep -Seconds 1
}
if ($null -eq $first) {
    Write-Host 'WebSocket가 아직 준비되지 않았습니다. 아래 서버 로그 마지막 30줄을 확인하세요.' -ForegroundColor Yellow
    $log = Join-Path (Split-Path -Parent $PSScriptRoot) 'data\logs\server-stderr.log'
    if (Test-Path -LiteralPath $log) { Get-Content -LiteralPath $log -Encoding UTF8 -Tail 30 }
    exit 1
}
$initialCount = [long]$first.message_count
Write-Host "신규 WS 메시지 수신 여부 확인 중 ($ObserveSeconds 초)..." -ForegroundColor Cyan
for ($i = 0; $i -lt $ObserveSeconds; $i++) { Start-Sleep -Seconds 1; Write-Host -NoNewline '.' }
Write-Host '' 
$last = Invoke-RestMethod -Uri "$uri/api/v1/market/status" -TimeoutSec 3
$live = Invoke-RestMethod -Uri "$uri/api/v1/market/live" -TimeoutSec 3
if ($last.ready -ne $true -or $live.source -ne 'BINANCE_FUTURES_DEMO_WEBSOCKET' -or $live.symbol -ne 'BTCUSDT' -or $live.interval -ne '1m' -or @($live.candles).Count -lt 100) {
    throw 'WS 신선도 또는 캔들 데이터 검증 실패'
}
Write-Host ("최종 수신 증가: {0}건, 지연: {1}ms, 현재 캔들: {2}" -f ([long]$last.message_count-$initialCount),$last.age_ms,$live.candles[-1].open_time) -ForegroundColor Green
if ([long]$last.message_count -le $initialCount) { throw '관찰 시간 동안 신규 WS 메시지가 없습니다.' }
Write-Host 'PASS: 실제 Demo 연결 및 신규 메시지 수신 확인' -ForegroundColor Green
