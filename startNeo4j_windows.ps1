# Solo Windows (PowerShell 7 y Docker Desktop). Arranca Neo4j con el grafo de la red para la defensa (D30), fuera de
# compose.yaml: un contenedor aparte (gridpredic-neo4j) que no toca la plataforma ni la CI.
#   1. Comprueba que Docker responde y que los puertos 7474 y 7687 estan libres la primera vez.
#   2. Crea el contenedor la primera vez (Neo4j 5.26 Community, sin usuario ni clave, solo en 127.0.0.1, 1 GB de
#      heap y 512 MB de cache) o arranca el que ya existe, y espera a que responda.
#   3. Si la base esta vacia (o con -Recargar), carga los CSV de _runlogs\neo4j\import con
#      experimentos\topologia\neo4j\carga.cypher. Los CSV los deja preparar_csv.py, solo con seudonimos.
#
#   pwsh -File D:\GitHub\gridpredic-infrastructure\startNeo4j_windows.ps1
#   pwsh -File D:\GitHub\gridpredic-infrastructure\startNeo4j_windows.ps1 -Recargar
#
# Despues: http://localhost:7474 en el navegador, "No authentication", y las consultas de
# experimentos\topologia\neo4j\consultas.cypher. Para pararlo: stopNeo4j_windows.ps1.
# Deja un registro en _runlogs\neo4j_start_<fecha>.log.

param([switch]$Recargar, [int]$MinutosMax = 5,
      [string]$Carga = "D:\GitHub\gridpredic-infrastructure\experimentos\topologia\neo4j\carga.cypher")

$ErrorActionPreference = "Continue"
$Repo = "D:\GitHub\gridpredic-infrastructure"
$L = "$Repo\_runlogs"
$Import = "$L\neo4j\import"
$Nombre = "gridpredic-neo4j"
$Imagen = "neo4j:5.26.31-community"
$Volumen = "gridpredic_neo4j_data"
$Out = "$L\neo4j_start_" + (Get-Date -Format "yyyyMMdd_HHmm") + ".log"

function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}

function Cypher([string]$query) {
    $r = docker exec $Nombre cypher-shell --format plain $query 2>&1
    return @($LASTEXITCODE, ($r -join "`n"))
}

if (-not (Test-Path "D:\")) { Write-Host "ERROR: no esta el disco D: (el disco de Docker vive ahi)."; exit 1 }
New-Item -ItemType Directory -Force -Path $L | Out-Null
Say "=== Arranque de Neo4j === Log: $Out"

docker info *> $null
if ($LASTEXITCODE -ne 0) { Say "ERROR: Docker no responde. Arranca antes Docker Desktop (o startGridPredic_windows.ps1)."; exit 1 }

$existe = docker ps -a --filter "name=^/$Nombre$" --format "{{.Names}}"
if (-not $existe) {
    if (-not (Test-Path "$Import\cts.csv")) { Say "ERROR: faltan los CSV en $Import (preparar_csv.py)."; exit 1 }
    foreach ($p in @(7474, 7687)) {
        if (Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue) {
            Say "ERROR: el puerto $p ya esta ocupado. Libera el puerto y vuelve a lanzar."; exit 1
        }
    }
    Say "Creo el contenedor $Nombre ($Imagen)"
    docker run -d --name $Nombre `
        -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 `
        -e NEO4J_AUTH=none `
        -e NEO4J_server_directories_import=/import `
        -e NEO4J_server_memory_heap_initial__size=512m `
        -e NEO4J_server_memory_heap_max__size=1G `
        -e NEO4J_server_memory_pagecache_size=512m `
        -v "${Volumen}:/data" `
        -v "${Import}:/import:ro" `
        $Imagen 2>&1 | ForEach-Object { Say ("    " + $_) }
    if ($LASTEXITCODE -ne 0) { Say "ERROR: no se ha podido crear el contenedor."; exit 1 }
} else {
    Say "Arranco el contenedor $Nombre que ya existe"
    docker start $Nombre 2>&1 | ForEach-Object { Say ("    " + $_) }
}

Say "Espero a que Neo4j responda (hasta $MinutosMax min)"
$limite = (Get-Date).AddMinutes($MinutosMax)
do {
    Start-Sleep -Seconds 5
    $r = Cypher "RETURN 1;"
} while ($r[0] -ne 0 -and (Get-Date) -lt $limite)
if ($r[0] -ne 0) { Say "ERROR: Neo4j no responde. Mira: docker logs $Nombre"; exit 1 }
Say "    Neo4j responde"

$r = Cypher "MATCH (c:CT) RETURN count(c) AS n;"
$hay = 0
if ($r[1] -match "(\d+)\s*$") { $hay = [int]$Matches[1] }
if ($Recargar -and $hay -gt 0) {
    Say "Borro el grafo para recargarlo"
    $r = Cypher "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;"
    Say ("    " + $r[1])
    if ($r[0] -ne 0) { Say "ERROR: el borrado del grafo ha fallado (codigo $($r[0])): no cargo encima de lo que haya."; exit 1 }
    $hay = 0
}
if ($hay -eq 0) {
    Say "Cargo el grafo desde $Import"
    $t0 = Get-Date
    Get-Content -Raw -Path $Carga | docker exec -i $Nombre cypher-shell --format plain 2>&1 | ForEach-Object { Say ("    " + $_) }
    if ($LASTEXITCODE -ne 0) { Say "ERROR: la carga ha fallado (ver arriba)."; exit 1 }
    Say ("    carga en " + [math]::Round(((Get-Date) - $t0).TotalMinutes, 1) + " min")
} else {
    Say "El grafo ya esta cargado ($hay CT). Para cargarlo de nuevo: -Recargar"
}

Say "Listo: http://localhost:7474 (No authentication). Consultas: $Repo\experimentos\topologia\neo4j\consultas.cypher"
exit 0
