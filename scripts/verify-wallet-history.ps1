$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))

function Run-Step([string]$Title, [scriptblock]$Command) {
    Write-Host "`n[VERIFY] $Title"
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$Title 실패 (exit=$LASTEXITCODE)" }
}

Run-Step 'Go 버전' { go version }
Run-Step 'Go 소스 포맷 (별도 검토 폴더에서 실행)' { go fmt ./... }
Run-Step '지갑 및 서버 회귀 테스트' { go test -count=1 ./internal/trading ./internal/server ./internal/wallet ./internal/credentials }
Run-Step '핵심 패키지 경쟁 검사' { go test -race -count=1 ./internal/server ./internal/trading ./internal/credentials ./internal/wallet }
Run-Step '전체 Go 테스트' { go test -count=1 ./... }
Run-Step 'Go 정적 검사' { go vet ./... }
Run-Step 'Windows 실행 파일 빌드' { go build -o (Join-Path $env:TEMP 'binance-trader-wallet-review.exe') ./cmd/trader }
if (Get-Command node -ErrorAction SilentlyContinue) {
    Run-Step 'UI JavaScript 문법 검사' { node --check ./internal/server/web/app.js }
    Run-Step '보조지표 검사' { node ./tests/check-indicators.cjs }
}
Write-Host "`n기본 검증 통과. 주문 기능은 계속 잠금 상태입니다. 실제 Demo 계정 및 브라우저 UI는 별도 확인이 필요합니다."
