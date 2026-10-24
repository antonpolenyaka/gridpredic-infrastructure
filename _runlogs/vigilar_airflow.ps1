# Vigila Airflow mientras corre la carga del lakehouse y lo reanima si se muere.
#
# El contenedor airflow va en modo "standalone": api, scheduler, triggerer y
# dag-processor son subprocesos de un mismo proceso. Cuando el scheduler muere
# (en este portatil pasa bajo presion de memoria y de disco, con el disco USB
# cargando), la web sigue viva, el healthcheck de Docker sigue en verde y el
# "restart: unless-stopped" del compose nunca actua. Resultado: el DAG se queda
# con tareas "scheduled" para siempre y nadie las lanza. Ya ha pasado dos veces
# (noche del 07.10 y tarde del 07.10 a las 19:27).
#
# Este script mira cada PollSeconds el endpoint de salud de Airflow:
#   http://localhost:8084/api/v2/monitor/health
# y el master de Spark (http://localhost:8090/json/). Si el scheduler lleva mas
# de MaxStaleMinutes (3) sin latido y Spark no tiene aplicaciones activas (las
# tareas lanzadas ya han acabado y no hay nada que perder), o lleva mas de
# MaxBusyMinutes (45) sin latido en cualquier caso, o la web no responde varias
# veces seguidas, hace:
#   docker compose restart airflow
# espera a que vuelva a estar sano y relanza las tareas fallidas de los DAGs de
# la carga con "airflow tasks clear --only-failed". Las tareas que se quedaron
# en "scheduled" las coge el scheduler nuevo solo. En este portatil el scheduler
# se muere poco despues de lanzar cada pareja de tareas (SQLite en el disco
# lento), asi que el ciclo normal es: reinicio, 2 tablas, reinicio, 2 tablas...
#
# Desde el 14.10 vigila tambien el dag-processor: con SQLite se muere con
# "database is locked" (paso a las 20:42 del 14.10) y, aunque el scheduler
# sigue lanzando los DAG ya serializados, ningun cambio de codigo de los DAG
# vuelve a cargarse. Si lleva mas de MaxProcessorMinutes (20) "unhealthy" y
# Spark esta vacio, se reinicia el contenedor igual.
#
# Se lanza en una consola aparte, con cargar_lakehouse.ps1 corriendo en la otra:
#   & "D:\GitHub\gridpredic-infrastructure\_runlogs\vigilar_airflow.ps1"
# Se para con Ctrl+C. Log en _runlogs\vigilar_<fecha>.log

param(
    [int]$PollSeconds = 60,
    [int]$MaxStaleMinutes = 3,
    [int]$MaxBusyMinutes = 45,
    [int]$MaxProcessorMinutes = 20,
    [int]$MaxRestarts = 200
)

$ErrorActionPreference = "Continue"

$Repo = Split-Path -Parent $PSScriptRoot   # la carpeta donde este clonado el repositorio
$Compose = "$Repo\compose.yaml"
$L = "$Repo\_runlogs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Out = "$L\vigilar_$Stamp.log"
$Health = "http://localhost:8084/api/v2/monitor/health"
$Dags = @("ingest_sqlserver_batch_bronze", "dag_bronze_reference_municipios", "dag_silver", "dag_gold")
$SparkUiPort = "8090"
if (Test-Path "$Repo\.env") {
    $pl = Get-Content "$Repo\.env" | Where-Object { $_ -match '^\s*SPARK_MASTER_UI_PORT\s*=\s*(\d+)' } | Select-Object -First 1
    if ($pl) { $SparkUiPort = $Matches[1] }
}

function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}

function Get-Health {
    try { return (Invoke-RestMethod $Health -TimeoutSec 20) } catch { return $null }
}

# Minutos desde el ultimo latido del scheduler; -1 si no hay dato.
function Get-SchedulerAgeMinutes($h) {
    if (-not $h -or -not $h.scheduler) { return -1 }
    $hb = $h.scheduler.latest_scheduler_heartbeat
    if (-not $hb) { return -1 }
    try {
        $t = [DateTimeOffset]::Parse($hb, [Globalization.CultureInfo]::InvariantCulture)
        return [math]::Round(([DateTimeOffset]::UtcNow - $t).TotalMinutes, 1)
    } catch { return -1 }
}

# Aplicaciones activas en el master de Spark; -1 si no responde (se trata como "ocupado").
function Get-SparkActiveApps {
    try {
        $m = Invoke-RestMethod "http://localhost:$SparkUiPort/json/" -TimeoutSec 20
        return @($m.activeapps).Count
    } catch { return -1 }
}

function Restart-Airflow {
    Say "Reiniciando el contenedor airflow..."
    & docker compose -f $Compose restart airflow 2>&1 | ForEach-Object { Say "    $_" }
    $deadline = (Get-Date).AddMinutes(12)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 20
        $h = Get-Health
        if ($h -and $h.scheduler.status -eq "healthy") {
            Say "Airflow sano otra vez (scheduler con latido)"
            return $true
        }
    }
    Say "AVISO: Airflow sigue sin estar sano 12 minutos despues del reinicio"
    return $false
}

function Clear-Failed {
    foreach ($dag in $Dags) {
        $out = @(& docker compose -f $Compose exec -T airflow airflow tasks clear $dag --only-failed --yes 2>&1 | ForEach-Object { "$_" })
        $txt = ($out -join " ")
        if ($txt -match "No tasks to clear|not found|does not exist") { continue }
        Say "clear --only-failed $dag : $(($out | Select-Object -Last 2) -join ' | ')"
    }
}

if (-not (Test-Path $L)) { New-Item -ItemType Directory -Path $L -Force | Out-Null }
Say "=== Vigilancia de Airflow (cada $PollSeconds s; reinicio si el scheduler lleva mas de $MaxStaleMinutes min sin latido y Spark esta vacio, o mas de $MaxBusyMinutes min en cualquier caso) ==="
Say "Log: $Out"

$restarts = 0
$webDown = 0
$lastOk = ""
$graceUntil = Get-Date
$procSince = $null

while ($true) {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        Say "Docker no responde; espero (no se reinicia nada sin Docker)"
        Start-Sleep -Seconds $PollSeconds
        continue
    }

    $h = Get-Health
    $needRestart = $false
    if (-not $h) {
        $webDown++
        Say "La web de Airflow no responde ($webDown seguidas)"
        if ($webDown -ge 3) { $needRestart = $true }
    } else {
        $webDown = 0
        $age = Get-SchedulerAgeMinutes $h
        $state = "scheduler=$($h.scheduler.status) triggerer=$($h.triggerer.status) dag_processor=$($h.dag_processor.status) ultimo latido hace $age min"
        # Solo cuenta el scheduler: es el que lanza tareas. El triggerer y el
        # dag-processor se retrasan con el disco cargado y no afectan a la carga.
        $spark = Get-SparkActiveApps
        $state = "$state ; Spark apps activas=$spark"
        if ($h.dag_processor -and $h.dag_processor.status -eq "unhealthy") {
            if ($null -eq $procSince) { $procSince = Get-Date }
        } else {
            $procSince = $null
        }
        $procMinutes = 0
        if ($null -ne $procSince) { $procMinutes = [math]::Round(((Get-Date) - $procSince).TotalMinutes, 1) }
        if ((Get-Date) -lt $graceUntil) {
            if ($state -ne $lastOk) { Say "Margen tras reinicio: $state"; $lastOk = $state }
        } elseif ($age -lt 0 -or $age -gt $MaxStaleMinutes) {
            # Solo la edad del latido, no la etiqueta "unhealthy" (Airflow la pone
            # a los 30 s). En este portatil el scheduler se muere poco despues de
            # lanzar cada pareja de tareas; las tareas ya lanzadas terminan solas.
            # Por eso: si Spark esta trabajando no se toca nada (reiniciar mataria
            # esas tareas); se reinicia en cuanto Spark queda vacio, que es cuando
            # hace falta un scheduler vivo para lanzar las siguientes. Si Spark
            # lleva demasiado ocupado con el scheduler muerto, se reinicia igual.
            if ($spark -eq 0 -or $age -gt $MaxBusyMinutes) {
                Say "Airflow enfermo: $state"
                $needRestart = $true
            } elseif ($state -ne $lastOk) {
                Say "Scheduler sin latido pero Spark ocupado, espero a que acabe: $state"
                $lastOk = $state
            }
        } elseif ($procMinutes -gt $MaxProcessorMinutes -and $spark -eq 0) {
            Say "El dag-processor lleva $procMinutes min sin vida (SQLite bloqueado) y Spark esta vacio: $state"
            $needRestart = $true
        } elseif ($state -ne $lastOk) {
            Say "Airflow sano: $state"
            $lastOk = $state
        }
    }

    if ($needRestart) {
        if ($restarts -ge $MaxRestarts) {
            Say "ERROR: ya van $restarts reinicios, no sigo. Revisa memoria de la VM (.wslconfig) y el disco"
            exit 1
        }
        $restarts++
        Say "Reinicio numero $restarts"
        if (Restart-Airflow) {
            Start-Sleep -Seconds 30
            Clear-Failed
        }
        # 3 minutos de margen: el scheduler nuevo da latido en menos de un minuto.
        $graceUntil = (Get-Date).AddMinutes(3)
        $webDown = 0
        $lastOk = ""
        $procSince = $null
    }

    Start-Sleep -Seconds $PollSeconds
}
