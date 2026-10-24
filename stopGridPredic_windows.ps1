# Solo Windows (PowerShell 7 y Docker Desktop). Para GridPredic en el portatil sin cortar nada a medias:
#   1. Comprueba que la cola no tiene un trabajo en marcha y que Spark no tiene aplicaciones (si las hay, no para, salvo con -Forzar).
#   2. Avisa si es la franja de la tarde (de 20:15 a 21:00), cuando corre el riesgo de manana.
#   3. Para todos los contenedores, tambien los del perfil cdc, dando a cada uno hasta 2 minutos para cerrar bien.
#   4. Con -ApagarDocker cierra tambien Docker Desktop.
# La cola y el vigilante pueden seguir abiertos: sin Docker esperan y no reinician nada.
#
#   pwsh -File D:\GitHub\gridpredic-infrastructure\stopGridPredic_windows.ps1
#   pwsh -File D:\GitHub\gridpredic-infrastructure\stopGridPredic_windows.ps1 -ApagarDocker
#   pwsh -File D:\GitHub\gridpredic-infrastructure\stopGridPredic_windows.ps1 -Forzar
#
# Deja un registro en _runlogs\stop_<fecha>.log.

param([switch]$ApagarDocker, [switch]$Forzar)

$ErrorActionPreference = "Continue"
$Repo = $PSScriptRoot   # la carpeta donde esta clonado el repositorio, sea cual sea
$Compose = "$Repo\compose.yaml"
$L = "$Repo\_runlogs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Out = "$L\stop_$Stamp.log"

function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}

Say "=== Parada de GridPredic === Log: $Out"
docker info *> $null
if ($LASTEXITCODE -ne 0) { Say "Docker no responde: no hay nada que parar."; exit 0 }

$motivos = @()
# Trabajo de la cola en marcha
$hist = "$L\cola\historial.log"
if (Test-Path $hist) {
    $ultima = Get-Content $hist -Tail 50 | Where-Object { $_ -match " (INICIO|FIN) " } | Select-Object -Last 1
    if ($ultima -match " INICIO (.+)$") { $motivos += "la cola esta ejecutando $($Matches[1])" }
}
# Aplicaciones de Spark
$puertoSpark = "8080"
if (Test-Path "$Repo\.env") {
    $linea = Get-Content "$Repo\.env" | Where-Object { $_ -match '^\s*SPARK_MASTER_UI_PORT\s*=\s*(\d+)' } | Select-Object -Last 1
    if ($linea -match '=\s*(\d+)') { $puertoSpark = $Matches[1] }
}
try {
    $j = Invoke-RestMethod -Uri "http://localhost:$puertoSpark/json/" -TimeoutSec 10
    $n = @($j.activeapps).Count
    if ($n -gt 0) { $motivos += "Spark tiene $n aplicaciones en marcha ($((@($j.activeapps) | ForEach-Object { $_.name }) -join ', '))" }
} catch {
    # Si el maestro de Spark ni siquiera esta arrancado, no hay aplicaciones que proteger.
    $maestro = docker compose -f $Compose ps --status running -q spark-master 2>$null
    if ($LASTEXITCODE -ne 0) {
        $motivos += "no he podido consultar Spark ni el estado del contenedor spark-master (codigo $LASTEXITCODE), asi que no se si hay aplicaciones en marcha"
    } elseif ($maestro) { $motivos += "no he podido consultar Spark en el puerto $puertoSpark, asi que no se si hay aplicaciones en marcha" }
}
# Franja de la tarde
$ahora = Get-Date
if ($ahora.TimeOfDay -ge [TimeSpan]"20:15" -and $ahora.TimeOfDay -lt [TimeSpan]"21:00") { $motivos += "es la franja de la tarde (20:15 - 21:00): el riesgo de manana puede estar corriendo" }

if ($motivos.Count -gt 0) {
    $motivos | ForEach-Object { Say "  AVISO: $_" }
    if (-not $Forzar) {
        Say "No paro. Espera a que acabe o crea el fichero $L\cola\parar para que la cola se detenga al terminar el trabajo actual. Para parar igualmente: -Forzar."
        exit 1
    }
    Say "  -Forzar: paro igualmente."
}

Say "--- Parando contenedores (hasta 2 minutos cada uno)"
docker compose -f $Compose --profile cdc stop -t 120 2>&1 | ForEach-Object { Say ("    " + $_) }
$codigoStop = $LASTEXITCODE
docker compose -f $Compose --profile cdc ps -a --format "table {{.Service}}\t{{.Status}}" 2>&1 | ForEach-Object { Say ("    " + $_) }
if ($codigoStop -ne 0) { Say "ERROR: docker compose stop devolvio ${codigoStop}: puede haber contenedores en marcha (mira la lista de arriba). No cierro Docker Desktop."; exit 1 }

if ($ApagarDocker) {
    Say "--- Cerrando Docker Desktop"
    Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue | Stop-Process -ErrorAction SilentlyContinue
    Say "    Docker Desktop cerrado."
}
Say "GridPredic parado. Para volver a arrancarlo: pwsh -File $Repo\startGridPredic_windows.ps1"
exit 0
