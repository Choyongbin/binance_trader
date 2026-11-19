#Requires -Version 5.1
# Can run against an unchanged dev.3.4 installation, with server STOPPED.
# Explicitly never reads credentials, user_events, recovery raw bodies or order data.
[CmdletBinding()]
param([string]$Install = 'F:\binance_trader_pc_dev31')
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$root = (Resolve-Path -LiteralPath $Install).Path
$directory = Join-Path $root 'data\trading\stream_discontinuities'
$allowed = @('STARTUP','DISCONNECT','ROTATE','EXPIRED','KEEPALIVE_FAILED',
    'UNKNOWN_EVENT','CANCEL_UNCERTAIN','STREAM_DISCONTINUITY')
if (-not (Test-Path -LiteralPath $directory -PathType Container)) {
    Write-Host 'No stream_discontinuities directory; there is no local marker evidence to inspect.'
    return
}
$all = @(Get-ChildItem -LiteralPath $directory -Force)
if ($all.Count -gt 100000) { throw 'Unusually many marker files; inspect the directory offline.' }
$records = New-Object System.Collections.ArrayList
foreach ($file in $all) {
    if ($file.PSIsContainer -or (($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) -or
        $file.Name -cnotmatch '^[a-f0-9]{32}\.json$' -or $file.Length -gt 4096) {
        throw 'An invalid gap marker filename, link, folder or oversize file was found. No files changed.'
    }
    try { $m = Get-Content -LiteralPath $file.FullName -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop }
    catch { throw 'One gap marker is not valid JSON. Stop and preserve all original records.' }
    $keys = @($m.PSObject.Properties.Name)
    if ($m.schema -ne 1 -or $keys.Count -ne 3 -or @($keys | Where-Object { $_ -notin @('schema','reason','at') }).Count -gt 0 -or
        $m.reason -notin $allowed -or [string]$m.at -notmatch '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z$') {
        throw 'One marker has unknown schema/reason/time. Preserve the original files.'
    }
    # Only allowlisted reason and timestamp; never print raw JSON or random filenames.
    [void]$records.Add([pscustomobject]@{ at_utc = [string]$m.at; reason = [string]$m.reason; bytes = [int]$file.Length })
}
$sorted = @($records | Sort-Object at_utc,reason)
Write-Host ('Historical gap markers found: ' + $sorted.Count)
$sorted | Format-Table at_utc, reason, bytes -AutoSize
Write-Host 'Reason counts:'
$sorted | Group-Object reason | Sort-Object Name | Select-Object Name,Count | Format-Table -AutoSize
Write-Host 'Files were NOT modified. Time proximity alone cannot prove these markers are harmless.'
