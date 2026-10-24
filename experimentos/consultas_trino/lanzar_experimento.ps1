# Experimento de planes de consulta de la memoria (4.7.3) en el portatil: Trino y Spark sobre el lakehouse.
#   pwsh -File D:\GitHub\gridpredic-infrastructure\experimentos\consultas_trino\lanzar_experimento.ps1
# Fases (cada consulta con Trino recien reiniciado: una pasada en frio, cinco en caliente y sus planes):
#   A  linea base y la misma consulta con el filtro de la columna de particion
#   F  el join con otro reparto (PARTITIONED, BROADCAST) y sin filtrado dinamico
#   C  ANALYZE de las tablas del join y las consultas otra vez
#   DE compactacion por particiones y orden Z de 2025 en dataset_train (Spark) y las consultas otra vez
# Solo cambian los datos de sitio (OPTIMIZE no cambia su contenido) y ANALYZE deja estadisticas junto al log.
# Trino usa esas estadisticas en todas las consultas y no las actualiza cuando Spark reescribe la tabla: el
# lanzador las borra (system.drop_extended_stats) antes de la fase A y al acabar.
# Con -SoloEstadisticas solo las borra y mide c2a y c5b sin ellas (unos 10 minutos).
# Deja todo en _runlogs\planes_trino_<fecha>\ (un JSON por consulta y fase); lo resume resumir_planes.py.
# La tarde (docs/operacion-diaria.md 5) tiene reservada la franja de las 20:25 a las 21:00. La regla es una
# reserva con duracion: un trabajo a mano solo empieza si, con la duracion de su ultimo registro por 1,25, acaba
# antes de las 20:25. Este lanzador aun no lee esa duracion y la aplica de forma simplificada: no empieza
# ninguna consulta ni la compactacion entre las 19:00 y las 21:00 (si le toca en esa franja, espera a las
# 21:00), y la compactacion no empieza otra particion despues de la hora de -PararA (18 por defecto).
param([string]$Repo = (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)), [int]$PararA = 18, [switch]$SinCompactar,
      [switch]$SoloEstadisticas)

$Compose = "$Repo\compose.yaml"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Dir = "$Repo\_runlogs\planes_trino_$Stamp"
New-Item -ItemType Directory -Force -Path $Dir | Out-Null
$Out = "$Dir\lanzar.log"
$Script:Fallos = 0

function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}

function FueraDeLaTarde() {
    $h = (Get-Date).Hour
    if ($h -ge 19 -and $h -lt 21) {
        $t = Get-Date -Hour 21 -Minute 0 -Second 0
        Say "Son mas de las 19:00: espero a las 21:00 para no coincidir con la tarde"
        while ((Get-Date) -lt $t) { Start-Sleep -Seconds 60 }
    }
}

function EsperarTrino() {
    for ($i = 0; $i -lt 60; $i++) {
        docker compose -f $Compose exec -T trino trino --execute "SELECT 1" 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) { return $true }
        Start-Sleep -Seconds 5
    }
    return $false
}

function Reiniciar() {
    docker compose -f $Compose restart trino 2>&1 | Out-Null
    if (-not (EsperarTrino)) { Say "ERROR: Trino no responde despues de reiniciar"; exit 1 }
    Start-Sleep -Seconds 5
}

function BorrarEstadisticas() {
    Say "--- Estadisticas extendidas de Trino: se borran (dataset_train, map_ct_celda, meteo_celda_hora)"
    docker compose -f $Compose exec -T spark-master python3 /tmp/gp/planes.py --fase B --borrar-estadisticas --salida /tmp/gp_planes 2>&1 |
        ForEach-Object { Say ("    " + $_) }
    if ($LASTEXITCODE -ne 0) { $Script:Fallos++ }
}

function Copiar() {
    New-Item -ItemType Directory -Force -Path "$Dir\json" | Out-Null
    docker compose -f $Compose cp "spark-master:/tmp/gp_planes/." "$Dir\json" 2>&1 | ForEach-Object { Say "  $_" }
    if ($LASTEXITCODE -ne 0) { $Script:Fallos++; Say "FALLO: no se pudieron copiar los resultados del contenedor" }
    $n = (Get-ChildItem -Path "$Dir\json" -Filter "*.json" -File -ErrorAction SilentlyContinue).Count
    Say "Ficheros de resultados: $n"
    if ($n -eq 0) { $Script:Fallos++; Say "FALLO: ningun fichero de resultados" }
}

function Planes([string]$Fase, [string[]]$Consultas, [string[]]$Sesion = @(), [switch]$SinReinicio) {
    foreach ($c in $Consultas) {
        FueraDeLaTarde
        if (-not $SinReinicio) { Reiniciar }
        $a = @("compose", "-f", $Compose, "exec", "-T", "spark-master", "python3", "/tmp/gp/planes.py",
            "--fase", $Fase, "--consultas", $c, "--salida", "/tmp/gp_planes")
        foreach ($s in $Sesion) { $a += @("--sesion", $s) }
        & docker @a 2>&1 | ForEach-Object { Say ("    " + $_) }
        if ($LASTEXITCODE -ne 0) { $Script:Fallos++ }
    }
}

if ($Repo -like "D:*" -and -not (Test-Path "D:\")) { Write-Host "ERROR: no esta el disco D:"; exit 1 }
Say "=== Planes de consulta (4.7.3) === Salida: $Dir"
Say ("  Commit: " + (git -C $Repo log -1 --format="%h %s"))
FueraDeLaTarde
# Spark ocupado segun la API del master (activeapps), que ve tambien lo que lanza Airflow: la tarde, dag_silver,
# dag_gold y el mantenimiento. Contar procesos spark-submit en spark-master solo veia lo lanzado a mano.
docker compose -f $Compose exec -T spark-master mkdir -p /tmp/gp 2>&1 | Out-Null
docker compose -f $Compose cp "$Repo\experimentos\consultas_trino\apps_spark.py" "spark-master:/tmp/gp/apps_spark.py" 2>&1 | Out-Null
while ($true) {
    $salida = (((docker compose -f $Compose exec -T spark-master python3 /tmp/gp/apps_spark.py) -join "").Trim())
    if ($LASTEXITCODE -ne 0 -or $salida -notmatch '^-?\d+$') { Say "ERROR: no he podido saber si Spark tiene aplicaciones en marcha (codigo $LASTEXITCODE, salida '$salida')"; exit 1 }
    $n = [int]$salida
    if ($n -eq 0) { break }
    if ($n -lt 0) { Say "ERROR: el master de Spark no responde"; exit 1 }
    Say "Spark tiene $n aplicaciones en marcha, espero"; Start-Sleep -Seconds 120
}

$TrinoAntes = (docker compose -f $Compose ps --status running --services 2>$null) -contains "trino"
Say ("Trino arrancado al empezar: " + $TrinoAntes)
if (-not $TrinoAntes) { docker compose -f $Compose up -d trino 2>&1 | ForEach-Object { Say "  $_" } }
if (-not (EsperarTrino)) { Say "ERROR: Trino no responde"; exit 1 }

docker compose -f $Compose exec -T spark-master mkdir -p /tmp/gp /tmp/gp_planes 2>&1 | Out-Null
# Cada ejecucion empieza sin los JSON de la anterior, para que resumir_planes.py no los mezcle.
docker compose -f $Compose exec -T spark-master sh -c "rm -f /tmp/gp_planes/*.json" 2>&1 | Out-Null
foreach ($f in @("planes.py", "compactar_dataset.py")) {
    docker compose -f $Compose cp "$Repo\experimentos\consultas_trino\$f" "spark-master:/tmp/gp/$f" 2>&1 | ForEach-Object { Say "  $_" }
}

Say "--- Tamano de cada zona y de las tablas mas grandes en MinIO"
docker compose -f $Compose exec -T minio sh -c "du -sm /data/datalake/*/ 2>/dev/null; du -sm /data/datalake/*/* 2>/dev/null | sort -n | tail -12" 2>&1 |
    ForEach-Object { Say ("    " + $_) }

BorrarEstadisticas
if ($SoloEstadisticas) {
    Say "--- Sin estadisticas: c2a y c5b"
    Planes "A" @("c2a", "c5b")
    Copiar
    if (-not $TrinoAntes) { docker compose -f $Compose stop trino 2>&1 | ForEach-Object { Say "  $_" } }
    if ($Script:Fallos -eq 0) { Say "Terminado sin errores."; exit 0 }
    Say "Terminado con $($Script:Fallos) pasos con errores (ver el log)."
    exit 1
}

Say "--- Fase A: linea base y filtro de particion"
Planes "A" @("ficheros_dataset", "ficheros_meteo", "c1c") -SinReinicio
Planes "A" @("c1a", "c2a", "c2b", "c2c", "c2d", "c3a", "c3b", "c3c", "c4a", "c5a", "c5b", "c5c")

Say "--- Fase F: reparto del join y filtrado dinamico"
Planes "F" @("c5b") @("join_distribution_type=PARTITIONED")
Planes "F" @("c5b") @("join_distribution_type=BROADCAST")
Planes "F" @("c5c") @("enable_dynamic_filtering=false")

Say "--- Fase C: estadisticas con ANALYZE"
docker compose -f $Compose exec -T spark-master python3 /tmp/gp/planes.py --fase C --analyze --salida /tmp/gp_planes 2>&1 |
    ForEach-Object { Say ("    " + $_) }
if ($LASTEXITCODE -ne 0) { $Script:Fallos++; Say "FALLO: ANALYZE devolvio $LASTEXITCODE; las medidas de la fase C que siguen no cuentan como completadas" }
Planes "C" @("c2a", "c2b", "c2d", "c5b", "c5c")

if (-not $SinCompactar) {
    Say "--- Fase DE: compactacion por particiones y orden Z de 2025 (Spark)"
    FueraDeLaTarde
    $cmd = @("compose", "-f", $Compose, "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit", "--driver-memory", "2g",
        "--conf", "spark.executor.memory=6g", "--conf", "spark.executor.memoryOverhead=1g", "--conf", "spark.cores.max=4",
        "/tmp/gp/compactar_dataset.py", "/tmp/gp_planes/compactacion.json", "--parar-a", "$PararA")
    & docker @cmd 2>&1 | ForEach-Object {
        $t = $_.ToString()
        if ($t -match "INFO compactar_dataset|ERROR|Traceback|Exception") { Say ("    " + $t.Substring(0, [Math]::Min(300, $t.Length))) }
    }
    if ($LASTEXITCODE -ne 0) { $Script:Fallos++ }
    Planes "DE" @("ficheros_dataset") -SinReinicio
    Planes "DE" @("c1a", "c2a", "c2b", "c2c", "c2d", "c5b", "c5c")
}

BorrarEstadisticas
Copiar

if (-not $TrinoAntes) {
    Say "Trino no estaba arrancado: lo paro otra vez."
    docker compose -f $Compose stop trino 2>&1 | ForEach-Object { Say "  $_" }
}
if ($Script:Fallos -eq 0) { Say "Terminado sin errores."; exit 0 }
Say "Terminado con $($Script:Fallos) pasos con errores (ver el log)."
exit 1
