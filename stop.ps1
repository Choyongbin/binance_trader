#Requires -Version 5.1
# Stop only the process started by start.ps1 (or explicit verified legacy Phase 1/2 go-run).
param([switch]$Legacy)
$ErrorActionPreference = 'Stop'
try {
    & chcp.com 65001 > $null
    [Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    $OutputEncoding = [Console]::OutputEncoding
} catch { }
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = (Resolve-Path -LiteralPath $root).Path
$record = Join-Path $root 'data\runtime\server-process.json'

function Get-PortOwner {
    if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
        $owners = @(Get-NetTCPConnection -State Listen -LocalPort 8080 -ErrorAction SilentlyContinue |
            Where-Object { $_.LocalAddress -in @('127.0.0.1', '0.0.0.0', '::', '::1') } |
            Select-Object -ExpandProperty OwningProcess -Unique)
        if ($owners.Count -gt 0) { return [int]$owners[0] }
    }
    foreach ($line in (& netstat.exe -ano -p TCP)) {
        if ($line -match '^\s*TCP\s+\S+:8080\s+\S+\s+LISTENING\s+(\d+)\s*$') { return [int]$Matches[1] }
    }
    return $null
}

if (-not $Legacy) {
    if (-not (Test-Path -LiteralPath $record)) {
        Write-Host '신규 실행 기록이 없습니다. 이전 go run 서버라면 .\stop.ps1 -Legacy를 사용하세요.' -ForegroundColor Yellow
        return
    }
    try { $info = Get-Content -LiteralPath $record -Encoding UTF8 -Raw | ConvertFrom-Json }
    catch { throw 'PID 기록이 손상되었습니다. 자동 종료하지 않습니다. 수동으로 확인하세요.' }

    $pidToStop = [int]$info.pid
    $process = Get-Process -Id $pidToStop -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        Remove-Item -LiteralPath $record -Force
        Write-Host '이미 종료된 프로세스의 기록을 정리했습니다.'
        return
    }
    $actualPath = $null
    $actualStartUtc = $null
    try {
        $actualPath = $process.Path
        $actualStartUtc = $process.StartTime.ToUniversalTime().ToString('o')
    } catch { throw '프로세스 신원을 확인할 수 없습니다. 다른 프로세스는 종료하지 않습니다.' }
    if ($actualPath -ine [string]$info.exe_path -or $actualStartUtc -ne [string]$info.start_utc) {
        throw 'PID가 다른 프로세스를 가리킬 수 있어 종료하지 않았습니다. 직접 확인하세요.'
    }
    Write-Host "프로젝트 서버를 종료합니다: PID=$pidToStop" -ForegroundColor Cyan
    # Phase 2 is read-only. Before enabling orders, replace forced stop with graceful shutdown.
    Stop-Process -Id $pidToStop -Force -ErrorAction Stop
    $process.WaitForExit(5000) | Out-Null
    Remove-Item -LiteralPath $record -Force
    Write-Host '종료 완료.' -ForegroundColor Green
    return
}

# Explicitly requested one-time cleanup of the old go-run process. Never kill an unknown server.
$owner = Get-PortOwner
if ($null -eq $owner) { Write-Host '8080 포트에 실행 중인 서버가 없습니다.'; return }
$bootstrap = $null
try { $bootstrap = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/api/v1/bootstrap' -TimeoutSec 2 }
catch { }
if ($null -eq $bootstrap -or $bootstrap.product -ne '바이낸스 트레이더' -or
    @('1', '2') -notcontains [string]$bootstrap.phase -or $bootstrap.trading_enabled -ne $false) {
    throw "PID=$owner 서비스가 이전 프로젝트의 조회 전용 서버인지 확인되지 않았습니다. 자동 종료하지 않습니다."
}
$details = Get-CimInstance -ClassName Win32_Process -Filter "ProcessId = $owner"
if ($null -eq $details) { throw '8080 포트 소유 프로세스를 확인할 수 없습니다.' }
Write-Host '기존 서버 확인:' -ForegroundColor Cyan
Write-Host "PID: $owner"
Write-Host "프로세스: $($details.Name)"
Write-Host "실행 파일: $($details.ExecutablePath)"
Write-Host "명령줄: $($details.CommandLine)"
# Only Phase 1/2 Go-run temp builds or binaries within this working project qualify.
$looksLikeGoRun = ([string]$details.ExecutablePath -match '(?i)[\\/]go-build\d+[\\/]')
$looksLikeThisProject = ([string]$details.ExecutablePath).StartsWith($root + '\', [StringComparison]::OrdinalIgnoreCase)
if (-not $looksLikeGoRun -and -not $looksLikeThisProject) {
    throw '실행 파일 위치가 기존 go run 또는 현재 프로젝트가 아닙니다. 자동 종료하지 않습니다.'
}
Write-Host '기존 조회 전용 서버를 종료합니다.' -ForegroundColor Yellow
Stop-Process -Id $owner -Force -ErrorAction Stop
Write-Host '종료 완료. 이제 .\start.ps1을 실행하세요.' -ForegroundColor Green
