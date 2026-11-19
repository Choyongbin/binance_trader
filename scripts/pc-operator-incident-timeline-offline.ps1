param([Parameter(Mandatory=$true)][string]$Data)
$ErrorActionPreference='Stop'
go run ./cmd/operator-incident-timeline -data $Data
