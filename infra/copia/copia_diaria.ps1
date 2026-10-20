# Copia diaria de lo que no se puede rehacer desde las copias de SQL Server, fuera del disco
# del repositorio (el externo): la Landing en vivo de MinIO (00_landing/vivo: previsiones de
# cada tarde, observaciones de AEMET, rayos, avisos), los JSON y partes de guardia del cuadro
# de mando (dashboard\data) y un volcado del catalogo de Hive (metastore_db en PostgreSQL):
# sin el, las tablas Delta siguen en MinIO pero no aparecen en Spark ni en Trino.
#
#     .\infra\copia\copia_diaria.ps1                                   # desde la raiz del repositorio; destino %USERPROFILE%\gridpredic_copia
#     .\infra\copia\copia_diaria.ps1 -Destino C:\Users\yo\copia_gp     # otra carpeta
#
# Para que corra sola hay que registrar a mano una tarea del Programador de tareas de Windows,
# todos los dias a las 22:30 (despues de la tarde), con "Iniciar en" la raiz del repositorio
# (el comando esta en docs/operacion-diaria.md, apartado 4). Solo lee: mc mirror
# copia sin borrar en el destino, pg_dump no bloquea el catalogo y robocopy no toca el origen.
# Las claves no salen de los contenedores (mc y pg_dump usan sus variables de entorno).

param(
    [string]$Destino = (Join-Path $env:USERPROFILE "gridpredic_copia"),
    [int]$Volcados = 14
)

# "Continue" and not "Stop": in PowerShell 5.1 with the output redirected, a line that docker writes to stderr
# (progress of docker compose cp) would stop the script. Every native command is checked by its exit code.
$ErrorActionPreference = "Continue"
$Raiz = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $Raiz
$hoy = Get-Date -Format "yyyyMMdd"
foreach ($d in @($Destino, (Join-Path $Destino "vivo"), (Join-Path $Destino "dashboard_data"), (Join-Path $Destino "metastore"))) {
    if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d | Out-Null }
}
$Destino = (Resolve-Path $Destino).Path
if ($Destino.Substring(0, 2) -eq $Raiz.Substring(0, 2)) {
    Write-Warning "El destino $Destino esta en el mismo disco que el repositorio: la copia no protege de su fallo"
}

# 1. Landing en vivo: mc mirror dentro del contenedor de MinIO (landing_vivo.sh) a una carpeta
#    suya que se mantiene entre dias, y de ahi al destino. Los .sh se copian al contenedor y se
#    ejecutan alli, sin pasar comillas por la linea de comandos de Windows.
docker compose cp (Join-Path $PSScriptRoot "landing_vivo.sh") minio:/tmp/landing_vivo.sh
if ($LASTEXITCODE -ne 0) { throw "docker compose cp de landing_vivo.sh ha fallado" }
docker compose exec -T minio sh /tmp/landing_vivo.sh
if ($LASTEXITCODE -ne 0) { throw "mc mirror de 00_landing/vivo ha fallado" }
docker compose cp minio:/tmp/copia_vivo/. (Join-Path $Destino "vivo")
if ($LASTEXITCODE -ne 0) { throw "docker compose cp de la Landing en vivo ha fallado" }

# 2. Cuadro de mando: robocopy sin /MIR, para que un fichero borrado en el origen siga en la copia.
robocopy (Join-Path $Raiz "dashboard\data") (Join-Path $Destino "dashboard_data") /E /NP /NFL /NDL /R:2 /W:5 | Out-Null
if ($LASTEXITCODE -ge 8) { throw "robocopy de dashboard\data ha fallado ($LASTEXITCODE)" }

# 3. Catalogo de Hive: pg_dump en formato propio dentro del contenedor (volcado_catalogo.sh),
#    que tambien da el numero de tablas de TBLS para comprobar el volcado restaurado.
$volcado = "metastore_db_$hoy.dump"
docker compose cp (Join-Path $PSScriptRoot "volcado_catalogo.sh") postgres:/tmp/volcado_catalogo.sh
if ($LASTEXITCODE -ne 0) { throw "docker compose cp de volcado_catalogo.sh ha fallado" }
$tablas = docker compose exec -T postgres sh /tmp/volcado_catalogo.sh
if ($LASTEXITCODE -ne 0) { throw "pg_dump de metastore_db ha fallado" }
docker compose cp postgres:/tmp/metastore_db.dump (Join-Path $Destino "metastore\$volcado")
if ($LASTEXITCODE -ne 0) { throw "docker compose cp del volcado ha fallado" }

# Se guardan los ultimos $Volcados volcados.
Get-ChildItem (Join-Path $Destino "metastore") -Filter "metastore_db_*.dump" | Sort-Object Name -Descending |
    Select-Object -Skip $Volcados | Remove-Item -Force

$vivo = (Get-ChildItem (Join-Path $Destino "vivo") -Recurse -File | Measure-Object).Count
$panel = (Get-ChildItem (Join-Path $Destino "dashboard_data") -File | Measure-Object).Count
$tam = [math]::Round((Get-Item (Join-Path $Destino "metastore\$volcado")).Length / 1KB)
Write-Host ("Copia del {0} en {1}: {2} ficheros de la Landing en vivo, {3} del cuadro de mando, volcado {4} ({5} KB) con {6} tablas en TBLS" -f $hoy, $Destino, $vivo, $panel, $volcado, $tam, ($tablas | Out-String).Trim())
