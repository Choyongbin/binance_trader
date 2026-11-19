#Requires -Version 5.1
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$files = @((Get-ChildItem -LiteralPath (Join-Path $root 'scripts') -Filter '*.ps1' -File) +
           (Get-Item -LiteralPath (Join-Path $root 'start.ps1')) +
           (Get-Item -LiteralPath (Join-Path $root 'stop.ps1')) +
           (Get-Item -LiteralPath (Join-Path $root 'update.ps1')))
foreach ($file in $files) {
    $tokens = $null
    $parseErrors = $null
    [void][System.Management.Automation.Language.Parser]::ParseFile($file.FullName, [ref]$tokens, [ref]$parseErrors)
    if ($null -ne $parseErrors -and $parseErrors.Count -gt 0) {
        foreach ($e in $parseErrors) { Write-Error ("$($file.Name): $($e.Message)") }
        throw 'PowerShell syntax check FAILED. Do not apply this release.'
    }
}
Write-Host ("PASS: PowerShell parser accepted {0} local scripts." -f $files.Count)
