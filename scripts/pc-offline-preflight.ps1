#Requires -Version 5.1
# Runs ONLY offline checks and a disposable new-install rehearsal.
[CmdletBinding()]
param([switch]$Race)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
& (Join-Path $PSScriptRoot 'pc-parse-scripts.ps1')
# The PowerShell parser script throws on failure; LASTEXITCODE is only for native programs.
foreach ($tool in @('go', 'node')) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "Missing test prerequisite: $tool. Install it before claiming a full PASS."
    }
}
function Run-Command([string]$label, [scriptblock]$block) {
    Write-Host ("[OFFLINE] {0}" -f $label)
    & $block
    if ($LASTEXITCODE -ne 0) { throw ("FAIL: {0} (exit {1})" -f $label, $LASTEXITCODE) }
}
$temp = Join-Path ([System.IO.Path]::GetTempPath()) ("binance-preflight-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null
Push-Location $root
try {
    Run-Command 'Go version' { go version }
    Run-Command 'all Go tests' { go test -count=1 ./... }
    Run-Command 'Go vet' { go vet ./... }
    Run-Command 'JS syntax' { node --check ./internal/server/web/app.js }
    Run-Command 'JS indicators' { node ./tests/check-indicators.cjs }
    if ($Race) {
        Run-Command 'Go race detector (Windows requires CGO-supported C compiler)' {
            go test -race -count=1 ./internal/server ./internal/trading ./internal/wallet ./internal/credentials ./internal/exchange ./internal/releaseupdate
        }
    }
    Run-Command 'native Windows SQLite temporary DB, UTF-8 and read-only reopen' { go run ./cmd/pc-check }
    $exe = Join-Path $temp 'binance-trader-test.exe'
    Run-Command 'native Windows build' { go build -o $exe ./cmd/trader }
    $incidentExe = Join-Path $temp 'incident-review-test.exe'
    Run-Command 'native Windows offline incident-review CLI build' { go build -o $incidentExe ./cmd/incident-review }
    $protectionExe = Join-Path $temp 'protection-audit-test.exe'
    Run-Command 'native Windows read-only protection-audit CLI build' { go build -o $protectionExe ./cmd/protection-audit }
    $protectionData = Join-Path $temp 'protection-empty-data'
    New-Item -ItemType Directory -Path $protectionData | Out-Null
    Write-Host '[OFFLINE] protection plan read-only empty archive rehearsal'
    $protectionRaw = & $protectionExe -data $protectionData
    if ($LASTEXITCODE -ne 0) { throw 'Temporary protection audit failed.' }
    $protection = ($protectionRaw -join "`n") | ConvertFrom-Json
    if ($protection.state -ne 'EMPTY' -or $protection.trading_authorized -ne $false -or
        $protection.per_owner_algo_arming_allowed -ne $false -or $protection.manual_review_required -ne $true) {
        throw 'Temporary protection audit was not fail-closed.'
    }
    if (Test-Path -LiteralPath (Join-Path $protectionData 'trading')) { throw 'Protection audit modified inspected data.' }
    Write-Host 'PASS: protection plan audit is read-only and trading remains locked.'
    $pooledExitExe = Join-Path $temp 'pooled-exit-audit-test.exe'
    Run-Command 'native Windows read-only pooled-exit-audit CLI build' { go build -o $pooledExitExe ./cmd/pooled-exit-audit }
    $pooledExitData = Join-Path $temp 'pooled-exit-empty-data'
    New-Item -ItemType Directory -Path $pooledExitData | Out-Null
    Write-Host '[OFFLINE] pooled exit serialization read-only empty archive rehearsal'
    $pooledExitRaw = & $pooledExitExe -data $pooledExitData
    if ($LASTEXITCODE -ne 0) { throw 'Temporary pooled exit audit failed.' }
    $pooledExit = ($pooledExitRaw -join "`n") | ConvertFrom-Json
    if ($pooledExit.state -ne 'EMPTY' -or $pooledExit.trading_authorized -ne $false -or
        $pooledExit.manual_review_required -ne $true -or $pooledExit.active_reservations -ne 0) {
        throw 'Temporary pooled exit audit was not fail-closed.'
    }
    if (Test-Path -LiteralPath (Join-Path $pooledExitData 'trading')) { throw 'Pooled exit audit modified inspected data.' }
    Write-Host 'PASS: pooled exit serialization audit is read-only and trading remains locked.'
    $lifecycleExe = Join-Path $temp 'order-lifecycle-audit-test.exe'
    Run-Command 'native Windows read-only order-lifecycle-audit CLI build' { go build -o $lifecycleExe ./cmd/order-lifecycle-audit }
    $lifecycleData = Join-Path $temp 'order-lifecycle-empty-data'
    New-Item -ItemType Directory -Path $lifecycleData | Out-Null
    Write-Host '[OFFLINE] durable order lifecycle read-only empty archive rehearsal'
    $lifecycleRaw = & $lifecycleExe -data $lifecycleData
    if ($LASTEXITCODE -ne 0) { throw 'Temporary order lifecycle audit failed.' }
    $lifecycle = ($lifecycleRaw -join "`n") | ConvertFrom-Json
    if ($lifecycle.state -ne 'EMPTY' -or $lifecycle.trading_authorized -ne $false -or
        $lifecycle.manual_review_required -ne $true -or $lifecycle.total -ne 0 -or $lifecycle.pending -ne 0) {
        throw 'Temporary order lifecycle audit was not fail-closed.'
    }
    if (Test-Path -LiteralPath (Join-Path $lifecycleData 'trading')) { throw 'Order lifecycle audit modified inspected data.' }
    Write-Host 'PASS: durable order lifecycle audit is read-only and trading remains locked.'
    $offlineData = Join-Path $temp 'incident-data-copy'
    $offlineReview = Join-Path $temp 'incident-review-outside'
    New-Item -ItemType Directory -Path $offlineData, $offlineReview | Out-Null
    Write-Host '[OFFLINE] incident review two-phase rehearsal ONLY inside temporary folders'
    $preparedRaw = & $incidentExe -action prepare -data $offlineData -reviews $offlineReview
    if ($LASTEXITCODE -ne 0) { throw 'Temporary offline review preparation failed.' }
    $prepared = ($preparedRaw -join "`n") | ConvertFrom-Json
    if ($prepared.trading_authorized -ne $false -or $prepared.auto_rearm_allowed -ne $false -or
        $prepared.stream_continuity_proven -ne $false -or $prepared.manual_review_required -ne $true -or
        $prepared.review_category -ne 'NO_MONITOR_HISTORY') { throw 'Temporary plan was not fail-closed.' }
    $receivedRaw = & $incidentExe -action acknowledge -data $offlineData -reviews $offlineReview -id $prepared.id -challenge $prepared.confirmation -stopped-writers -external-backup
    if ($LASTEXITCODE -ne 0) { throw 'Temporary offline review acknowledgement failed.' }
    $received = ($receivedRaw -join "`n") | ConvertFrom-Json
    if ($received.trading_authorized -ne $false -or $received.auto_rearm_allowed -ne $false -or
        $received.stream_continuity_proven -ne $false -or $received.manual_review_required -ne $true) { throw 'Temporary receipt was not fail-closed.' }
    $verifiedRaw = & $incidentExe -action verify -reviews $offlineReview
    if ($LASTEXITCODE -ne 0) { throw 'Temporary offline ledger verification failed.' }
    $verified = ($verifiedRaw -join "`n") | ConvertFrom-Json
    if ($verified.verified_plans -ne 1 -or $verified.verified_acknowledgements -ne 1 -or $verified.trading_authorized -ne $false) {
        throw 'Temporary incident review ledger was not verified.'
    }
    if (Test-Path -LiteralPath (Join-Path $offlineData 'trading')) { throw 'Two-phase rehearsal modified inspected data.' }
    Write-Host 'PASS: two-phase incident review in disposable directories; source data untouched, no unlock.'

    if (-not (Test-Path -LiteralPath $exe)) { throw 'Windows build output is missing.' }
    $trial = Join-Path $temp 'disposable-install'
    Run-Command 'installer dry-run to nonexistent test folder' { go run ./cmd/safe-update -source $root -target $trial -new }
    if (Test-Path -LiteralPath $trial) { throw 'Dry-run created files unexpectedly.' }
    Run-Command 'installer actual rehearsal ONLY inside temporary folder' { go run ./cmd/safe-update -source $root -target $trial -new -apply }
    $expected = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
    $installed = (Get-Content -LiteralPath (Join-Path $trial 'VERSION') -Raw).Trim()
    if ($installed -ne $expected) { throw 'Temporary install VERSION mismatch.' }
    Run-Command 'installer existing-install dry-run rehearsal' { go run ./cmd/safe-update -source $root -target $trial }
    Run-Command 'installer existing-install actual rehearsal with complete temporary backup' { go run ./cmd/safe-update -source $root -target $trial -apply }
    $installed = (Get-Content -LiteralPath (Join-Path $trial 'VERSION') -Raw).Trim()
    if ($installed -ne $expected) { throw 'Temporary update VERSION mismatch.' }
    Remove-Item -LiteralPath (Join-Path $trial 'VERSION') -Force
    Run-Command 'legacy missing VERSION marker dry-run repair rehearsal' { go run ./cmd/safe-update -source $root -target $trial }
    Run-Command 'legacy missing VERSION marker apply repair rehearsal' { go run ./cmd/safe-update -source $root -target $trial -apply }
    $installed = (Get-Content -LiteralPath (Join-Path $trial 'VERSION') -Raw).Trim()
    if ($installed -ne $expected) { throw 'Missing VERSION repair rehearsal failed.' }
    Write-Host ('PASS: all offline checks, native SQLite, Windows build and safe install rehearsal. Release ' + $expected) -ForegroundColor Green
} finally {
    Pop-Location
    # The only files deleted are under our randomly named OS temporary path.
    Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue
}
