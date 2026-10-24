# Solo Windows (PowerShell 7 y Docker Desktop). Para el contenedor de Neo4j de la defensa (gridpredic-neo4j). El grafo
# se queda en el volumen gridpredic_neo4j_data y startNeo4j_windows.ps1 lo vuelve a levantar sin recargar.
# Con -Borrar quita tambien el contenedor y el volumen; el siguiente arranque crea todo y recarga los CSV.
#
#   pwsh -File D:\GitHub\gridpredic-infrastructure\stopNeo4j_windows.ps1
#   pwsh -File D:\GitHub\gridpredic-infrastructure\stopNeo4j_windows.ps1 -Borrar

param([switch]$Borrar)

$Nombre = "gridpredic-neo4j"
$Volumen = "gridpredic_neo4j_data"

docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "Docker no responde: no hay nada que parar."; exit 0 }

$existe = docker ps -a --filter "name=^/$Nombre$" --format "{{.Names}}"
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: docker ps ha devuelto $LASTEXITCODE; no se sabe si $Nombre existe."; exit 1 }
if (-not $existe) { Write-Host "No existe el contenedor $Nombre."; exit 0 }

Write-Host "Paro $Nombre (hasta 60 s para cerrar bien)"
docker stop -t 60 $Nombre | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: docker stop ha devuelto $LASTEXITCODE; $Nombre puede seguir en marcha."; exit 1 }
if ($Borrar) {
    Write-Host "Borro el contenedor $Nombre y el volumen $Volumen"
    docker rm $Nombre | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: docker rm ha devuelto $LASTEXITCODE."; exit 1 }
    docker volume rm $Volumen | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: docker volume rm ha devuelto $LASTEXITCODE."; exit 1 }
}
Write-Host "Hecho."
exit 0
