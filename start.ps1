#Requires -Version 5.1
# Phase 2 launcher: build a native executable and start it as a managed background process.
# No nested PowerShell / go run; startup returns to the prompt and stop.ps1 stops it.
$ErrorActionPreference = 'Stop'

# Windows PowerShell 5.1 needs a UTF-8 BOM when reading Korean literals.
try {
    & chcp.com 65001 > $null
    [Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    $OutputEncoding = [Console]::OutputEncoding
} catch { }

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = (Resolve-Path -LiteralPath $root).Path
$bindPort = 8080
$uri = 'http://127.0.0.1:8080'
$binDir = Join-Path $root 'bin'
$exe = Join-Path $binDir 'binance-trader-server.exe'
$dataDir = Join-Path $root 'data'
$runtimeDir = Join-Path $dataDir 'runtime'
$logDir = Join-Path $dataDir 'logs'
$record = Join-Path $runtimeDir 'server-process.json'
$stdout = Join-Path $logDir 'server-stdout.log'
$stderr = Join-Path $logDir 'server-stderr.log'

function Get-SourceRelease {
    $versionSource = Join-Path $root 'internal\version\version.go'
    if (-not (Test-Path -LiteralPath $versionSource)) { throw 'internal\version\version.go가 없습니다.' }
    $raw = Get-Content -LiteralPath $versionSource -Raw -Encoding UTF8
    $m = [regex]::Match($raw, 'Release\s*=\s*"([^"]+)"')
    if (-not $m.Success) { throw 'internal/version/version.go에서 Release를 찾을 수 없습니다.' }
    return $m.Groups[1].Value
}

function Ensure-VersionMarker([string]$expected) {
    $versionPath = Join-Path $root 'VERSION'
    if (Test-Path -LiteralPath $versionPath) {
        $actual = (Get-Content -LiteralPath $versionPath -Raw).Trim()
        if ($actual -ne $expected) {
            throw "VERSION과 소스 Release가 다릅니다: VERSION=$actual source=$expected"
        }
        return
    }
    # Legacy Windows updater releases could omit only the top-level VERSION.
    # Recreate exactly UTF-8(no BOM) + LF from the manifest-tracked source
    # Release constant; never repair an existing mismatched marker.
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($versionPath, $expected + "`n", $utf8)
    Write-Warning "누락된 VERSION 릴리스 마커를 복구했습니다: $expected"
}

function Get-PortOwner {
    if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
        $owners = @(Get-NetTCPConnection -State Listen -LocalPort $bindPort -ErrorAction SilentlyContinue |
            Where-Object { $_.LocalAddress -in @('127.0.0.1', '0.0.0.0', '::', '::1') } |
            Select-Object -ExpandProperty OwningProcess -Unique)
        if ($owners.Count -gt 0) { return [int]$owners[0] }
    }
    # Fallback for installations where Get-NetTCPConnection is unavailable.
    foreach ($line in (& netstat.exe -ano -p TCP)) {
        if ($line -match '^\s*TCP\s+\S+:8080\s+\S+\s+LISTENING\s+(\d+)\s*$') { return [int]$Matches[1] }
    }
    return $null
}

function Get-LocalBootstrap {
    try { return Invoke-RestMethod -Method Get -Uri "$uri/api/v1/bootstrap" -TimeoutSec 2 }
    catch { return $null }
}

if (-not (Test-Path -LiteralPath (Join-Path $root 'go.mod')) -or
    -not (Test-Path -LiteralPath (Join-Path $root 'cmd\trader\main.go'))) {
    throw 'go.mod 및 cmd\trader\main.go가 필요합니다. 파일을 프로젝트 루트에 넣으세요.'
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw 'Go 실행 파일이 없습니다. go version을 확인하세요.'
}

$expectedRelease = Get-SourceRelease
Ensure-VersionMarker $expectedRelease

# Never start a second server on an occupied port, and never kill unknown processes.
$owner = Get-PortOwner
if ($null -ne $owner) {
    $bootstrap = Get-LocalBootstrap
    if ($null -ne $bootstrap -and $bootstrap.product -eq '바이낸스 트레이더' -and
        @('1', '2') -contains [string]$bootstrap.phase -and $bootstrap.trading_enabled -eq $false) {
        Write-Host "이미 Binance Trader가 실행 중입니다. PID=$owner" -ForegroundColor Yellow
        Write-Host "UI: $uri" -ForegroundColor Green
        Write-Host '이전 go run으로 실행했다면, 별도 PowerShell 창에서 .\stop.ps1 -Legacy를 실행하세요.'
    } else {
        Write-Warning "포트 $bindPort 를 다른 프로세스가 사용 중입니다. PID=$owner"
        Write-Host "확인: Get-CimInstance Win32_Process -Filter 'ProcessId = $owner' | Select ProcessId,Name,ExecutablePath,CommandLine"
    }
    return
}

# Refuse to overwrite an existing runtime record: this could point to another live server.
if (Test-Path -LiteralPath $record) {
    try {
        $previous = Get-Content -LiteralPath $record -Encoding UTF8 -Raw | ConvertFrom-Json
        $existing = Get-Process -Id ([int]$previous.pid) -ErrorAction SilentlyContinue
        if ($null -ne $existing) {
            $path = $null
            try { $path = $existing.Path } catch { }
            if ($null -ne $path -and $path -ieq [string]$previous.exe_path) {
                throw "기존 프로세스(PID=$($previous.pid))가 실행 중이므로 중복 시작을 중단합니다. .\stop.ps1로 확인하세요."
            }
        }
    } catch {
        if ($_.Exception.Message -like '*중복 시작*') { throw }
        Write-Warning '이전 PID 파일을 읽지 못했습니다. 포트 사용 여부는 별도 검사했습니다.'
    }
    # A stale record can only be removed if its process is no longer a match.
    Remove-Item -LiteralPath $record -Force
}

New-Item -ItemType Directory -Path $binDir, $runtimeDir, $logDir -Force | Out-Null
Write-Host 'Go 서버를 Windows 실행 파일로 빌드합니다...' -ForegroundColor Cyan
Push-Location $root
try { & go build -o $exe ./cmd/trader; $buildExit = $LASTEXITCODE }
finally { Pop-Location }
if ($buildExit -ne 0) { throw "Go 빌드 실패: 종료 코드 $buildExit" }

# Old logs are overwritten only on a new successful launch attempt, not when the port is busy.
Write-Host '백그라운드로 서버를 시작합니다...' -ForegroundColor Cyan
# Start-Process launches the .exe itself, not go run or another PowerShell process.
$proc = Start-Process -FilePath $exe `
    -ArgumentList @('-addr', '127.0.0.1:8080', '-data', ('"{0}"' -f $dataDir)) `
    -WorkingDirectory $root -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr

$startUtc = $proc.StartTime.ToUniversalTime().ToString('o')
@{
    pid = [int]$proc.Id
    exe_path = $exe
    start_utc = $startUtc
    bind = '127.0.0.1:8080'
} | ConvertTo-Json | Set-Content -LiteralPath $record -Encoding UTF8

$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    $proc.Refresh()
    if ($proc.HasExited) { break }
    try {
        $health = Invoke-RestMethod -Uri "$uri/api/v1/health" -TimeoutSec 1
        $version = Invoke-RestMethod -Uri "$uri/api/v1/version" -TimeoutSec 1
        $currentOwner = Get-PortOwner
        if ($health.server -eq 'running' -and $currentOwner -eq $proc.Id -and
            [string]$version.release -eq $expectedRelease -and [string]$version.trading -eq 'LOCKED') {
            $ready = $true
            break
        }
    } catch { }
    Start-Sleep -Milliseconds 250
}
if (-not $ready) {
    $proc.Refresh()
    if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue }
    Remove-Item -LiteralPath $record -Force -ErrorAction SilentlyContinue
    Write-Host '서버 시작 확인에 실패했습니다. 오류 로그:' -ForegroundColor Red
    if (Test-Path $stderr) { Get-Content -LiteralPath $stderr -Encoding UTF8 -Tail 25 }
    throw "시작 실패. $stderr 로그를 확인하세요."
}
Write-Host "서버 실행 완료! PID=$($proc.Id)" -ForegroundColor Green
Write-Host "UI: $uri" -ForegroundColor Green
Write-Host '종료 명령: .\stop.ps1' -ForegroundColor Yellow
Write-Host "오류 로그: $stderr"
Write-Host '이제 PowerShell 프롬프트로 돌아갑니다. Ctrl+C가 필요하지 않습니다.'
