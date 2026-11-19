#Requires -Version 5.1
[CmdletBinding(DefaultParameterSetName='DryRun')]
param(
    [Parameter(Mandatory=$true)][string]$Package,
    [string]$TargetRoot,
    [Parameter(ParameterSetName='DryRun')][switch]$DryRun,
    [Parameter(ParameterSetName='Apply')][switch]$Apply
)

$ErrorActionPreference = 'Stop'
try {
    & chcp.com 65001 > $null
    [Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    $OutputEncoding = [Console]::OutputEncoding
} catch { }

# Allow invoking a reviewed ZIP updater from an independent staging folder,
# without first overwriting the old install's manifest-tracked update.ps1.
$defaultRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = if ($TargetRoot) { (Resolve-Path -LiteralPath $TargetRoot).Path } else { (Resolve-Path -LiteralPath $defaultRoot).Path }
$packagePath = (Resolve-Path -LiteralPath $Package).Path
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw 'Go 실행 파일이 필요합니다.' }

$temp = Join-Path ([System.IO.Path]::GetTempPath()) ('binance-update-' + [guid]::NewGuid().ToString('N'))
$source = Join-Path $temp 'release'
$buildProbe = Join-Path $temp 'trader-probe.exe'
New-Item -ItemType Directory -Path $source -Force | Out-Null

function Invoke-GoChecked([string]$Label, [string]$WorkingDirectory, [string[]]$Arguments) {
    Write-Host $Label -ForegroundColor Cyan
    Push-Location $WorkingDirectory
    try {
        & go @Arguments
        $code = $LASTEXITCODE
    } finally { Pop-Location }
    if ($code -ne 0) { throw "$Label 실패 (코드 $code)" }
}

function Stop-ManagedServerIfNeeded {
    $record = Join-Path $root 'data\runtime\server-process.json'
    if (Test-Path -LiteralPath $record) {
        & (Join-Path $root 'stop.ps1')
        return
    }
    try {
        $health = Invoke-RestMethod 'http://127.0.0.1:8080/api/v1/health' -TimeoutSec 1
        if ($null -ne $health) {
            throw '8080에서 서버가 응답하지만 관리 PID 기록이 없습니다. 자동 종료하지 않습니다. 직접 확인하세요.'
        }
    } catch {
        if ($_.Exception.Message -like '*관리 PID 기록*') { throw }
    }
}

function Start-And-Verify([string]$ExpectedRelease) {
    & (Join-Path $root 'start.ps1')
    $v = Invoke-RestMethod 'http://127.0.0.1:8080/api/v1/version' -TimeoutSec 3
    if ([string]$v.release -ne $ExpectedRelease -or [string]$v.trading -ne 'LOCKED') {
        throw "기동 버전 불일치: release=$($v.release) trading=$($v.trading) expected=$ExpectedRelease"
    }
}

# The Go updater may partially install a release before returning a nonzero
# exit code. Recover that case as well, not just post-install test failures.
function Restore-VerifiedBackup([string]$BackupPath) {
    if ([string]::IsNullOrWhiteSpace($BackupPath) -or
        -not $BackupPath.StartsWith(($root + '.backup-'), [StringComparison]::OrdinalIgnoreCase) -or
        -not (Test-Path -LiteralPath $BackupPath -PathType Container)) {
        throw "자동 복구용 complete backup 위치를 검증할 수 없습니다: $BackupPath"
    }
    Write-Warning "complete backup 복구 시도: $BackupPath"
    Push-Location $source
    $pref = $ErrorActionPreference
    try {
        # Windows PowerShell 5.1 can promote a redirected native stderr into
        # a terminating error when ErrorActionPreference is Stop.
        $ErrorActionPreference = 'Continue'
        $restoreRaw = (& go run ./cmd/safe-update -restore-backup $BackupPath -target $root -json) -join "`n"
        $restoreCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $pref
        Pop-Location
    }
    if ($restoreCode -ne 0) { throw "자동 복구 실패: $restoreRaw" }
    try { $restore = $restoreRaw | ConvertFrom-Json }
    catch { throw "복구 응답 JSON 검증 실패: $restoreRaw" }
    if ($restore.restored -ne $true) { throw "복구 완료 증명을 확인할 수 없습니다: $restoreRaw" }
    Write-Warning "기존 전체 소스로 복구 완료. 실패 설치는 격리됨: $($restore.quarantine)"
    try { & (Join-Path $root 'start.ps1') }
    catch { Write-Warning "복구 후 서버 재시작 실패: $($_.Exception.Message)" }
}

try {
    Write-Host '[1/5] ZIP 압축 해제 및 릴리스 무결성 사전 검증...' -ForegroundColor Cyan
    Expand-Archive -LiteralPath $packagePath -DestinationPath $source -Force
    foreach ($required in @('VERSION','go.mod','docs\RELEASE_MANIFEST_SHA256.txt','cmd\safe-update\main.go')) {
        if (-not (Test-Path -LiteralPath (Join-Path $source $required))) { throw "누적 릴리스 필수 파일 없음: $required" }
    }
    $expectedRelease = (Get-Content -LiteralPath (Join-Path $source 'VERSION') -Raw).Trim()
    if ($expectedRelease -notmatch '^v0\.4\.11-dev\.\d+\.\d+$') { throw "예상하지 못한 VERSION 형식: $expectedRelease" }

    Invoke-GoChecked '[2/5] ZIP 소스 bounded short regression...' $source @('test','-short','-count=1','-timeout=3m','./...')
    Invoke-GoChecked '[2/5] ZIP 소스 Windows 서버 빌드...' $source @('build','-o',$buildProbe,'./cmd/trader')

    if (-not $Apply) {
        Write-Host '[3/5] 기존 설치 DRY RUN...' -ForegroundColor Cyan
        Push-Location $source
        $pref = $ErrorActionPreference
        try {
            $ErrorActionPreference = 'Continue'
            $dryRaw = (& go run ./cmd/safe-update -source $source -target $root -json) -join "`n"
            $code = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $pref
            Pop-Location
        }
        if ($code -ne 0) { throw "DRY RUN 실패(code=$code): $dryRaw" }
        try { $dryResult = $dryRaw | ConvertFrom-Json }
        catch { throw "DRY RUN 응답 JSON을 읽을 수 없습니다: $dryRaw" }
        if ($dryResult.ok -ne $true -or $dryResult.plan.Installed -eq $true) { throw "DRY RUN 응답이 올바르지 않습니다: $dryRaw" }
        Write-Host "DRY RUN PASS: $expectedRelease. 파일은 변경하지 않았습니다." -ForegroundColor Green
        return
    }

    Write-Host '[3/5] 관리 서버 종료 확인...' -ForegroundColor Cyan
    Stop-ManagedServerIfNeeded

    Write-Host '[4/5] complete backup 생성 후 누적 릴리스 적용...' -ForegroundColor Cyan
    Push-Location $source
    $pref = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $raw = (& go run ./cmd/safe-update -source $source -target $root -apply -json) -join "`n"
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $pref
        Pop-Location
    }
    $result = $null
    try { $result = $raw | ConvertFrom-Json }
    catch { Write-Warning 'safe-update 응답 JSON 파싱 실패' }
    $backup = ''
    if ($null -ne $result -and $null -ne $result.plan) {
        $backup = [string]$result.plan.Backup
    }
    if ($code -ne 0 -or $null -eq $result -or $result.ok -ne $true -or $result.plan.Installed -ne $true) {
        $applyError = "safe-update APPLY 실패(code=$code): $raw"
        if ($backup) {
            try { Restore-VerifiedBackup $backup }
            catch { throw "$applyError ; 복구도 실패: $($_.Exception.Message) ; 백업=$backup" }
        }
        throw $applyError
    }

    try {
        Write-Host '[5/5] 적용 후 전체 테스트/빌드/버전 검증...' -ForegroundColor Cyan
        Invoke-GoChecked '적용 후 bounded short regression...' $root @('test','-short','-count=1','-timeout=3m','./...')
        $postBuild = Join-Path $temp 'post-update-trader.exe'
        Invoke-GoChecked '적용 후 Windows 서버 빌드...' $root @('build','-o',$postBuild,'./cmd/trader')
        $marker = (Get-Content -LiteralPath (Join-Path $root 'VERSION') -Raw).Trim()
        if ($marker -ne $expectedRelease) { throw "설치 VERSION 불일치: $marker / $expectedRelease" }
        Start-And-Verify $expectedRelease
        Write-Host "업데이트 완료: $expectedRelease" -ForegroundColor Green
        if ($backup) { Write-Host "complete backup: $backup" }
    } catch {
        $postError = $_
        Write-Warning "적용 후 검증 실패: $($postError.Exception.Message)"
        try { Stop-ManagedServerIfNeeded } catch { Write-Warning $_.Exception.Message }
        if ($backup) {
            try { Restore-VerifiedBackup $backup }
            catch { throw "적용 후 검증 실패 + 자동 복구 실패: original=$($postError.Exception.Message) / restore=$($_.Exception.Message) / backup=$backup" }
        }
        throw $postError
    }
} finally {
    if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Recurse -Force -ErrorAction SilentlyContinue }
}
