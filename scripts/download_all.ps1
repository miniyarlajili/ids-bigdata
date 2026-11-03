# Telecharge le jeu COMPLET CSE-CIC-IDS2018 (10 fichiers CSV, environ 16 millions de flux)
# La liste des fichiers est lue directement dans le bucket public Amazon S3 du CIC.
# Le script reprend la ou il s'est arrete si on le relance (fichiers deja complets ignores).
#
# Usage (depuis la racine du projet) :
#   powershell -ExecutionPolicy Bypass -File scripts\download_all.ps1

$ErrorActionPreference = "Stop"
$bucket = "https://cse-cic-ids2018.s3.ca-central-1.amazonaws.com"
$prefix = "Processed Traffic Data for ML Algorithms/"
$dest   = Join-Path $PSScriptRoot "..\data\raw"
New-Item -ItemType Directory -Force -Path $dest | Out-Null
$dest   = (Resolve-Path $dest).Path

Write-Host "[1/3] Lecture de la liste des fichiers dans le bucket S3..."
$listUrl = "$bucket/?list-type=2&prefix=" + [uri]::EscapeDataString($prefix)
[xml]$xml = (Invoke-WebRequest -Uri $listUrl -UseBasicParsing).Content
$files = @($xml.ListBucketResult.Contents | Where-Object { $_.Key -like "*.csv" })
if ($files.Count -eq 0) { Write-Host "Aucun fichier trouve : verifier la connexion Internet."; exit 1 }

$missing = @(); $toDownload = [int64]0; $all = [int64]0
foreach ($f in $files) {
    $name  = $f.Key.Split('/')[-1]
    $size  = [int64]$f.Size
    $local = Join-Path $dest $name
    $all  += $size
    $ok = (Test-Path $local) -and ((Get-Item $local).Length -eq $size)
    $state = if ($ok) { "deja telecharge" } else { "a telecharger" }
    "{0,-55} {1,8:N0} Mo   {2}" -f $name, ($size / 1MB), $state
    if (-not $ok) { $missing += $f; $toDownload += $size }
}
"`n{0} fichiers, {1:N1} Go au total, {2:N1} Go a telecharger" -f $files.Count, ($all / 1GB), ($toDownload / 1GB)

Write-Host "`n[2/3] Verification de l'espace disque..."
$drive = (Get-Item $dest).PSDrive.Name
$free  = (Get-PSDrive -Name $drive).Free
$margin = 10GB   # images Docker, Parquet, fichiers temporaires de Spark
"Espace libre sur {0}: : {1:N1} Go (besoin : {2:N1} Go + {3:N0} Go de marge)" -f $drive, ($free / 1GB), ($toDownload / 1GB), ($margin / 1GB)
if ($free -lt ($toDownload + $margin)) {
    Write-Host "ESPACE INSUFFISANT : liberer de la place puis relancer le script."
    exit 1
}

Write-Host "`n[3/3] Telechargement..."
$i = 0
foreach ($f in $missing) {
    $i++
    $name = $f.Key.Split('/')[-1]
    $url  = "$bucket/" + ($f.Key -replace ' ', '%20')
    Write-Host "`n($i/$($missing.Count)) $name"
    curl.exe -L --fail --retry 3 -C - -o (Join-Path $dest $name) $url
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ECHEC sur $name : relancer le script, il reprendra la ou il s'est arrete."
        exit 1
    }
}

Write-Host "`nTermine. Contenu de data\raw :"
Get-ChildItem $dest -Filter *.csv | Select-Object Name, @{n="Mo"; e={[math]::Round($_.Length / 1MB)}} | Format-Table -AutoSize
