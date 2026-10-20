# Solo Windows (PowerShell 7 y Docker Desktop). Arranca todo lo que necesita GridPredic en el portatil, en orden y esperando a que cada parte este sana:
#   1. Docker Desktop (si no esta abierto) y espera a que responda.
#   2. PostgreSQL y MinIO; despues Hive, Trino y Spark master; despues el worker, el History Server, Airflow y el cuadro de mando.
#   3. SQL Server al final y sin esperarlo: tras un reinicio tarda unos 15 minutos en recuperar sus bases y el dia a dia no lo necesita.
#   4. La cola de trabajos (_runlogs\cola.ps1) y el vigilante de Airflow, si no estan corriendo.
# Kafka, Kafka Connect y Kafka UI (perfil cdc) solo arrancan con -Cdc.
#
#   pwsh -File D:\GitHub\gridpredic-infrastructure\startGridPredic_windows.ps1
#   pwsh -File D:\GitHub\gridpredic-infrastructure\startGridPredic_windows.ps1 -Cdc
#
# Deja un registro en _runlogs\start_<fecha>.log.

param([switch]$Cdc, [int]$MinutosMax = 30)

$ErrorActionPreference = "Continue"
$Repo = "D:\GitHub\gridpredic-infrastructure"
$Compose = "$Repo\compose.yaml"
$L = "$Repo\_runlogs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Out = "$L\start_$Stamp.log"
$DockerExe = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
$Pwsh = "C:\Program Files\PowerShell\7\pwsh.exe"
$Fallos = 0

function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}

function Fase($titulo, [string[]]$servicios, [int]$segundos, [switch]$SinEsperar) {
    Say "--- $titulo ($($servicios -join ', '))"
    $t0 = Get-Date
    if ($SinEsperar) {
        docker compose -f $Compose up -d @servicios 2>&1 | ForEach-Object { Say ("    " + $_) }
    } else {
        docker compose -f $Compose up -d --wait --wait-timeout $segundos @servicios 2>&1 | ForEach-Object { Say ("    " + $_) }
    }
    $ok = ($LASTEXITCODE -eq 0)
    $min = [math]::Round(((Get-Date) - $t0).TotalMinutes, 1)
    if ($ok) { Say "    listo en $min min" } else { Say "    AVISO: no todo esta sano tras $min min (ver arriba)"; $script:Fallos++ }
}

if (-not (Test-Path "D:\")) { Write-Host "ERROR: no esta el disco D: (el disco de Docker vive ahi). Conectalo y vuelve a lanzar."; exit 1 }
Say "=== Arranque de GridPredic === Log: $Out"

# 1. Docker Desktop
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    if (-not (Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue)) {
        Say "Abro Docker Desktop"
        Start-Process -FilePath $DockerExe
    }
    Say "Espero a que Docker responda (hasta $MinutosMax min)"
    $limite = (Get-Date).AddMinutes($MinutosMax)
    do {
        Start-Sleep -Seconds 10
        docker info *> $null
    } while ($LASTEXITCODE -ne 0 -and (Get-Date) -lt $limite)
    if ($LASTEXITCODE -ne 0) { Say "ERROR: Docker no responde. Abre Docker Desktop a mano y vuelve a lanzar."; exit 1 }
}
Say "Docker responde"

# 2. El stack por fases, para que el disco externo no lo reparta todo a la vez
Fase "Almacenamiento" @("postgres", "minio") 600
Fase "Catalogo, consultas y Spark" @("hive", "trino", "spark-master") 1500
Fase "Proceso y operacion" @("spark-worker", "spark-history", "airflow", "dashboard") 900

# 3. SQL Server, sin esperarlo
Fase "SQL Server (recupera sus bases en segundo plano)" @("sqlserver") 0 -SinEsperar
if ($Cdc) {
    Say "--- CDC: espero a SQL Server y arranco Kafka, Kafka Connect y Kafka UI"
    docker compose -f $Compose --profile cdc up -d --wait --wait-timeout ($MinutosMax * 60) kafka kafka-connect kafka-ui 2>&1 | ForEach-Object { Say ("    " + $_) }
    if ($LASTEXITCODE -ne 0) { Say "    AVISO: el CDC no ha quedado sano"; $Fallos++ }
}

# 4. Cola y vigilante
foreach ($s in @("cola.ps1", "vigilar_airflow.ps1")) {
    if (Test-Path "$L\$s") {
        & $Pwsh -NoProfile -File "$L\arrancar_si_falta.ps1" -Script $s
        Say "Comprobado $s (arrancado si no estaba)"
    }
}

# Resumen
Say "--- Estado"
docker compose -f $Compose --profile cdc ps -a --format "table {{.Service}}\t{{.Status}}" 2>&1 | ForEach-Object { Say ("    " + $_) }
Say "Cuadro de mando: http://localhost:8086   Airflow: http://localhost:8084   Spark: http://localhost:8090   Trino: http://localhost:8085   MinIO: http://localhost:9001"
if ($Fallos -eq 0) { Say "GridPredic arrancado." ; exit 0 }
Say "GridPredic arrancado con $Fallos avisos: mira las lineas AVISO de arriba."
exit 1
