# Escalado de la lectura de Gold (memoria 4.7.3 y 6.1) en el portatil: la carga pesada de escalado.py con
# 1, 2, 3, 4, 6, 8, 10 y 12 nucleos y tres pasadas, y con 8 nucleos sobre la cuarta parte y la mitad de los meses.
#   pwsh -File D:\GitHub\gridpredic-infrastructure\experimentos\consultas_trino\lanzar_escalado.ps1
# Unos 65 - 75 minutos de maquina. No empieza ni sigue entre las 19:00 y las 21:00 (la tarde) y espera mientras el
# master de Spark tenga alguna aplicacion en marcha (tambien las que lanza Airflow). Cada medida usa un ejecutor de
# 6 GB, como compactar_dataset.py. Deja los JSON en _runlogs\escalado_<fecha>\json y escalado.csv y
# escalado_ajuste.txt (ajustar_escalado.py) en experimentos\consultas_trino\salida.
param([string]$Repo = "D:\GitHub\gridpredic-infrastructure", [int[]]$Nucleos = @(1, 2, 3, 4, 6, 8, 10, 12),
      [double[]]$Meses = @(0.25, 0.5), [int]$Pasadas = 3)

$Compose = "$Repo\compose.yaml"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Dir = "$Repo\_runlogs\escalado_$Stamp"
New-Item -ItemType Directory -Force -Path "$Dir\json" | Out-Null
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

function SparkLibre() {
    while ($true) {
        $n = [int](((docker compose -f $Compose exec -T spark-master python3 /tmp/gp/apps_spark.py) -join "").Trim())
        if ($n -eq 0) { return }
        if ($n -lt 0) { Say "ERROR: el master de Spark no responde"; exit 1 }
        Say "Spark tiene $n aplicaciones en marcha, espero"
        Start-Sleep -Seconds 120
    }
}

function Medir([int]$N, [double]$M) {
    FueraDeLaTarde
    SparkLibre
    $ms = $M.ToString([Globalization.CultureInfo]::InvariantCulture)
    $tag = if ($M -lt 1) { "${N}_m$ms" } else { "$N" }
    $a = @("compose", "-f", $Compose, "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit", "--driver-memory", "2g",
        "--conf", "spark.cores.max=$N", "--conf", "spark.executor.memory=6g", "--conf", "spark.executor.memoryOverhead=1g",
        "/tmp/gp/escalado.py", "$N", "/tmp/gp_escalado/escalado_$tag.json", "--pasadas", "$Pasadas",
        "--meses", $ms)
    & docker @a 2>&1 | ForEach-Object {
        $t = $_.ToString()
        if ($t -match "ESCALADO|ERROR|Traceback|Exception") { Say ("    " + $t.Substring(0, [Math]::Min(300, $t.Length))) }
    }
    if ($LASTEXITCODE -ne 0) { $Script:Fallos++ }
}

if (-not (Test-Path "D:\")) { Write-Host "ERROR: no esta el disco D:"; exit 1 }
Say "=== Escalado de la lectura de Gold === Salida: $Dir"
Say ("  Commit: " + (git -C $Repo log -1 --format="%h %s"))
docker compose -f $Compose exec -T spark-master mkdir -p /tmp/gp /tmp/gp_escalado 2>&1 | Out-Null
docker compose -f $Compose exec -T spark-master sh -c "rm -f /tmp/gp_escalado/*.json" 2>&1 | Out-Null
foreach ($f in @("escalado.py", "ajustar_escalado.py", "apps_spark.py")) {
    docker compose -f $Compose cp "$Repo\experimentos\consultas_trino\$f" "spark-master:/tmp/gp/$f" 2>&1 | ForEach-Object { Say "  $_" }
}

Say "--- Nucleos: $($Nucleos -join ', ') con toda la tabla y $Pasadas pasadas"
foreach ($n in $Nucleos) { Medir $n 1.0 }
Say "--- 8 nucleos con parte de los meses: $($Meses -join ', ')"
foreach ($m in $Meses) { Medir 8 $m }

docker compose -f $Compose exec -T spark-master python3 /tmp/gp/ajustar_escalado.py /tmp/gp_escalado /tmp/gp_escalado/salida 2>&1 |
    ForEach-Object { Say ("    " + $_) }
docker compose -f $Compose cp "spark-master:/tmp/gp_escalado/." "$Dir\json" 2>&1 | ForEach-Object { Say "  $_" }
foreach ($f in @("escalado.csv", "escalado_ajuste.txt")) {
    Copy-Item -Force "$Dir\json\salida\$f" "$Repo\experimentos\consultas_trino\salida\$f"
}
if ($Script:Fallos -eq 0) { Say "Terminado sin errores."; exit 0 }
Say "Terminado con $($Script:Fallos) medidas con errores (ver el log)."
exit 1
