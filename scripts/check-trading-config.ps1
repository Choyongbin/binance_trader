param([int]$Port = 8080)
$ErrorActionPreference = 'Stop'
$uri = "http://127.0.0.1:$Port/api/v1/trading/config"
Write-Host '[Demo] 실제 거래소 설정 읽기 (주문 및 설정 변경 없음)'
try {
    $config = Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 15
    Write-Host ("종목: {0} | Hedge: {1} | 증거금: {2} | 레버리지: {3}배 | 자동 증거금 추가: {4}" -f $config.symbol, $config.hedge_mode, $config.margin_type, $config.leverage, $config.auto_add_margin)
    Write-Host ("설정 기준 일치: {0} | 확인 시각: {1}" -f $config.settings_match_v1, $config.as_of)
    foreach ($warning in @($config.warnings)) { if ($warning) { Write-Warning $warning } }
    if (-not $config.settings_match_v1) { Write-Warning '현재 설정은 모델 기본 요건과 다릅니다. 설정을 자동으로 변경하지 않습니다.' }
    Write-Host '주문 기능은 현재 릴리스에서 잠겨 있습니다.'
} catch {
    Write-Warning '설정 조회 실패: 먼저 UI에서 데모 계정을 암호로 연결했는지 확인하세요.'
    Write-Warning $_.Exception.Message
    exit 1
}
