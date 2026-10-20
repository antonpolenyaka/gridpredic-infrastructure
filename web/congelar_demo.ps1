# Congela la copia de datos de la demo publica (gridpredic.com/demo/): copia los JSON y
# los partes de guardia que los jobs han dejado en dashboard\data a web\demo\data, que si
# se versiona. Despues: revisar "git status", hacer commit y abrir el PR; el workflow de
# GitHub Pages publica la web al mezclar en main.
#
#     .\web\congelar_demo.ps1                       # desde la raiz del repositorio
#     .\web\congelar_demo.ps1 -Origen D:\otra\data  # otra carpeta de datos
#
# Comprueba antes que ningun fichero lleva el nombre real de una distribuidora (los jobs
# escriben "Distribuidora 1, 2 y 3" desde la 0.15.0; _runlogs\anonimizar_dashboard.py
# arregla ficheros antiguos) y se detiene si encuentra alguno.
#
# Los municipios, sus codigos INE y las estaciones de AEMET salen con seudonimos: la copia
# se anonimiza en una carpeta temporal con web\anonimizar_demo.py y solo despues se copia a
# web\demo\data, asi que esa carpeta nunca tiene los nombres reales. Los seudonimos estan en
# _runlogs\demo_seudonimos.json (no se versiona); con el mismo mapa, cada municipio tiene el
# mismo seudonimo en todas las copias. Necesita Python con openpyxl (requirements.txt).

param(
    [string]$Origen = (Join-Path $PSScriptRoot "..\dashboard\data"),
    [string]$Destino = (Join-Path $PSScriptRoot "demo\data"),
    [string]$Nombres = (Join-Path $PSScriptRoot "..\_runlogs\anonimizar_nombres.json"),
    [string]$Mapa = (Join-Path $PSScriptRoot "..\_runlogs\demo_seudonimos.json")
)

$ErrorActionPreference = "Stop"
$Origen = (Resolve-Path $Origen).Path
if (-not (Test-Path $Destino)) { New-Item -ItemType Directory -Path $Destino | Out-Null }
$Destino = (Resolve-Path $Destino).Path

$files = Get-ChildItem -Path $Origen -File | Where-Object { $_.Extension -in ".json", ".xlsx" }
if (-not $files) { throw "No hay ficheros en $Origen" }

# Los nombres reales solo existen en el portatil (el fichero no se versiona); si esta, se buscan.
$reales = @()
if (Test-Path $Nombres) {
    $map = Get-Content $Nombres -Raw -Encoding UTF8 | ConvertFrom-Json
    $reales = $map.PSObject.Properties | ForEach-Object { $_.Name }
}
if ($reales.Count -gt 0) {
    foreach ($f in ($files | Where-Object { $_.Extension -eq ".json" })) {
        $text = Get-Content $f.FullName -Raw -Encoding UTF8
        foreach ($real in $reales) {
            if ($text -match [regex]::Escape($real)) { throw "$($f.Name) lleva un nombre real; pasar _runlogs\anonimizar_dashboard.py antes" }
        }
    }
}

# Seudonimos en una carpeta temporal; si algo falla, web\demo\data no se toca.
$Tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("gridpredic_demo_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    foreach ($f in $files) { Copy-Item $f.FullName (Join-Path $Tmp $f.Name) -Force }
    & python (Join-Path $PSScriptRoot "anonimizar_demo.py") $Tmp --mapa $Mapa
    if ($LASTEXITCODE -ne 0) { throw "anonimizar_demo.py no ha podido quitar los nombres reales; web\demo\data no ha cambiado" }
    # Los ficheros que ya estaban se sobreescriben; los dias archivados que ya no esten en el origen se quedan.
    foreach ($f in $files) { Copy-Item (Join-Path $Tmp $f.Name) (Join-Path $Destino $f.Name) -Force }
}
finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
$riesgo = Get-Content (Join-Path $Destino "riesgo.json") -Raw -Encoding UTF8 | ConvertFrom-Json
Write-Host ("Copiados {0} ficheros a {1}, con seudonimos; riesgo.json generado el {2} para el {3}" -f $files.Count, $Destino, $riesgo.generado, $riesgo.fecha_texto)
Write-Host "Siguiente: git add web/demo/data; git commit; PR a main. El workflow pages.yml publica y comprueba la web."
