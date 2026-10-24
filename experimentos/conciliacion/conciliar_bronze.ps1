# M102 (alta): conciliacion de SQL Server con Bronze. Filas de cada tabla en el origen frente a filas de su tabla en
# l1_bronze, para las 90 cargas por lotes (etl/config/01_bronze/config_bronze_sqlserver.json: 33 tablas de TedisNet
# y 19 por cada una de las tres bases de Calser) y las 29 System* del CDC (infra/kafka-connect/connectors/
# tedisnet-eosa-connector.json). El nombre en Bronze es el de los jobs de Bronze: l1_bronze.<base>_<tabla> en
# snake_case (to_snake). En SQL Server, recuento por metadatos (sys.dm_db_partition_stats, indices 0 y 1), instantaneo
# tambien en HistoricTagIntervalValuesBig (220 GB, que no se recorre), y COUNT_BIG(*) exacto solo en las tablas de
# 2 millones de filas o menos. En Bronze, count(*) con Trino, que solo lee los pies de los Parquet. Imprime una fila
# por carga con la diferencia y lo que se espera de cada tipo de carga (sin filtros de ventana en Bronze; el CDC
# deduplica por clave), y la deja en _runlogs\m102_<fecha>\conciliacion.csv (ignorada por git). Las bases salen como
# TedisNet, Calser D1 (3), Calser D2 (2) y Calser D3 (1366). Solo lee.
# Unos 5 minutos con SQL Server y Trino arrancados. Si SQL Server esta parado lo arranca, espera hasta 25 minutos a que
# tenga sus cuatro bases en linea y lo vuelve a parar al acabar; si Trino esta parado lo arranca y lo deja arrancado.
# No empieza entre las 18:30 y las 21:00 y espera si Spark tiene aplicaciones en marcha.
$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path   # la carpeta donde este clonado el repositorio
$Compose = "$Repo\compose.yaml"
$L = "$Repo\_runlogs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Out = "$L\m102_conciliacion_bronze_$Stamp.log"
$Dir = "$L\m102_$Stamp"
$Tmp = "/tmp/gp_m102"
$Limite = 2000000
$Referencia = 2152292109
$Script:Fallos = 0
$Es = [Globalization.CultureInfo]::GetCultureInfo("es-ES")
# Las bases llevan el nombre de la distribuidora: en el registro salen con el de la memoria y su distribuidora_id.
$Alias = @{ "TedisNet_EOSA" = "TedisNet"; "Calser_EOSA" = "Calser D1 (3)"; "Calser_Pitarch" = "Calser D2 (2)";
    "Calser_ValleSantaAna" = "Calser D3 (1366)" }
function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}
function EnLaTarde() {
    $d = Get-Date
    $m = $d.Hour * 60 + $d.Minute
    return ($m -ge 18 * 60 + 30 -and $m -lt 21 * 60)
}
function AppsSpark() {
    $t = ((docker compose -f $Compose exec -T spark-master python3 /tmp/gp/apps_spark.py 2>$null) -join "").Trim()
    $n = 0
    if ([int]::TryParse($t, [ref]$n)) { return $n }
    return -1
}
function EsperarSpark() {
    docker compose -f $Compose exec -T spark-master mkdir -p /tmp/gp 2>&1 | Out-Null
    docker compose -f $Compose cp "$Repo\experimentos\consultas_trino\apps_spark.py" "spark-master:/tmp/gp/apps_spark.py" 2>&1 | Out-Null
    for ($i = 0; $i -lt 30; $i++) {
        $n = AppsSpark
        if ($n -eq 0) { return $true }
        if ($n -lt 0) { Say "ERROR: el master de Spark no responde"; return $false }
        Say "Spark tiene $n aplicaciones en marcha, espero"
        Start-Sleep -Seconds 120
    }
    Say "ERROR: Spark sigue ocupado despues de una hora"
    return $false
}
function TrinoListo() {
    $activos = @(docker compose -f $Compose ps --status running --services 2>$null)
    if (-not ($activos -contains "trino")) {
        Say "  Trino no esta arrancado: lo arranco y lo dejo arrancado"
        docker compose -f $Compose start trino 2>&1 | ForEach-Object { Say "    $_" }
        if ($LASTEXITCODE -ne 0) { docker compose -f $Compose up -d trino 2>&1 | ForEach-Object { Say "    $_" } }
    }
    for ($i = 0; $i -lt 40; $i++) {
        docker compose -f $Compose exec -T trino trino --execute "SELECT 1" 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) { return $true }
        Start-Sleep -Seconds 15
    }
    return $false
}
function Trino([string]$Nombre, [string]$Sql) {
    $f = Join-Path $Dir "$Nombre.sql"
    [IO.File]::WriteAllText($f, (($Sql -replace "`r", "").Trim() + ";`n"), [Text.Encoding]::ASCII)
    docker compose -f $Compose cp $f "trino:$Tmp/$Nombre.sql" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        # sin la copia, el contenedor podria tener un $Nombre.sql de otra ejecucion: no se ejecuta nada
        Say "  ERROR: no se pudo copiar $Nombre.sql al contenedor de Trino (codigo $LASTEXITCODE)"
        return $null
    }
    $a = @("compose", "-f", $Compose, "exec", "-T", "trino", "trino", "--output-format", "TSV_HEADER",
        "--session", "query_max_execution_time=20m", "--file", "$Tmp/$Nombre.sql")
    $todo = @(& docker @a 2>&1)
    $code = $LASTEXITCODE
    $lineas = @($todo | Where-Object { $_ -is [string] })
    $errores = @($todo | Where-Object { $_ -isnot [string] } | ForEach-Object { $_.ToString() })
    [IO.File]::WriteAllLines((Join-Path $Dir "$Nombre.tsv"), [string[]]$lineas, [Text.Encoding]::ASCII)
    if ($code -ne 0) {
        Say "  ERROR en la consulta $Nombre (codigo $code):"
        foreach ($e in @($errores + $lineas | Select-Object -First 6)) { Say ("    " + $e) }
        return $null
    }
    return ,$lineas
}
function Tabla($Lineas) {
    $Lineas = @($Lineas | Where-Object { $null -ne $_ -and "$_" -ne "" })
    if ($Lineas.Count -lt 2) { return @() }
    $cab = $Lineas[0].Split("`t")
    $filas = @()
    for ($i = 1; $i -lt $Lineas.Count; $i++) {
        $v = $Lineas[$i].Split("`t")
        $o = [ordered]@{}
        for ($j = 0; $j -lt $cab.Count; $j++) { $o[$cab[$j]] = $(if ($j -lt $v.Count) { $v[$j] } else { "" }) }
        $filas += [pscustomobject]$o
    }
    return $filas
}
function Escribir([string]$Ruta, [string]$Texto) {
    [IO.File]::WriteAllText($Ruta, ($Texto -replace "`r", ""), [Text.Encoding]::ASCII)
}
function SqlServer([string]$Base, [string]$Fichero) {
    $todo = @(docker compose -f $Compose exec -T sqlserver sh "$Tmp/sqlcmd.sh" $Base "$Tmp/$Fichero" 2>&1 | ForEach-Object { "$_" })
    $Script:SqlCodigo = $LASTEXITCODE
    return $todo
}
# Igual que to_snake de job_bronze_sqlserver_batch.py y job_bronze_sqlserver_streaming.py.
function ToSnake([string]$v) {
    $v = [regex]::Replace($v, '([A-Z]+)([A-Z][a-z])', '${1}_${2}')
    $v = [regex]::Replace($v, '([a-z0-9])([A-Z])', '${1}_${2}')
    return $v.ToLower()
}
function AliasDe([string]$Base) {
    if ($Alias.ContainsKey($Base)) { return $Alias[$Base] }
    return "otra base"
}
function Num($v) {
    if ($null -eq $v) { return "-" }
    return ([double]$v).ToString("N0", $Es)
}

if ($Repo -like "D:*" -and -not (Test-Path "D:\")) { Write-Host "ERROR: no esta el disco D:"; exit 1 }
docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Docker Desktop no esta arrancado"; exit 1 }
if (EnLaTarde) { Say "ERROR: franja de la tarde (18:30 - 21:00), no se lanza"; exit 1 }
New-Item -ItemType Directory -Force -Path $Dir | Out-Null
Say "=== M102: conciliacion de SQL Server con Bronze (90 cargas por lotes y 29 tablas del CDC) === Log: $Out"
Say ("  Commit del portatil: " + (git -C $Repo log -1 --format="%h %s"))

# --- Lo que se carga, de la configuracion del repositorio del portatil
$cfg = Get-Content -Raw "$Repo\etl\config\01_bronze\config_bronze_sqlserver.json" | ConvertFrom-Json
$con = Get-Content -Raw "$Repo\infra\kafka-connect\connectors\tedisnet-eosa-connector.json" | ConvertFrom-Json
$Esperadas = @()
foreach ($src in $cfg.sources) {
    foreach ($db in $src.databases) {
        foreach ($t in $src.tables) {
            $Esperadas += [pscustomobject]@{ Origen = "lote"; Db = $db; Tabla = $t.name;
                Modo = "$($t.extract_mode)/$($t.write_mode)"; Bronze = (ToSnake $db) + "_" + (ToSnake $t.name) }
        }
    }
}
$cdcDb = $con.'database.names'
foreach ($t in ($con.'table.include.list' -split ",")) {
    $n = ($t.Trim() -replace '^dbo\.', '')
    if ($n) {
        $Esperadas += [pscustomobject]@{ Origen = "cdc"; Db = $cdcDb; Tabla = $n; Modo = "CDC/MERGE";
            Bronze = (ToSnake $cdcDb) + "_" + (ToSnake $n) }
    }
}
$nLote = @($Esperadas | Where-Object { $_.Origen -eq "lote" }).Count
$nCdc = @($Esperadas | Where-Object { $_.Origen -eq "cdc" }).Count
Say "  Cargas por lotes en la configuracion: $nLote; tablas del CDC: $nCdc (se esperan 90 y 29)"
$Bases = @($Esperadas | ForEach-Object { $_.Db } | Sort-Object -Unique)
foreach ($b in $Bases) { if (-not $Alias.ContainsKey($b)) { Say "  AVISO: una base de la configuracion no tiene alias y saldra como 'otra base'" } }

if (-not (EsperarSpark)) { exit 1 }
if (EnLaTarde) { Say "ERROR: la espera ha llegado a la franja de la tarde, no se lanza"; exit 1 }

# --- SQL Server: los guiones van a una carpeta del contenedor; la clave de sa sale de su entorno y no se imprime
Escribir "$Dir\sqlcmd.sh" @'
#!/bin/sh
# sh sqlcmd.sh <base> <fichero.sql>: sqlcmd con la clave de sa del entorno del contenedor (no se imprime).
exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -d "$1" -h -1 -W -b -i "$2"
'@
$lista = ($Bases | ForEach-Object { "N'" + $_.Replace("'", "''") + "'" }) -join ", "
Escribir "$Dir\estado.sql" (@'
SET NOCOUNT ON;
SELECT N'M102ESTADO|' + CONVERT(nvarchar(10), COUNT(*))
FROM sys.databases
WHERE name IN (__BASES__) AND state_desc = N'ONLINE';
'@).Replace("__BASES__", $lista)
# Una consulta por base, solo con sus tablas de la carga: metadatos de todas y COUNT_BIG(*) de las pequenas.
$Plantilla = @'
SET NOCOUNT ON;
DECLARE @limite bigint = __LIMITE__;
DECLARE @interes TABLE (tabla sysname NOT NULL);
INSERT INTO @interes (tabla) VALUES __TABLAS__;
DECLARE @r TABLE (tabla sysname NOT NULL, filas_meta bigint NULL, filas_exactas bigint NULL);
INSERT INTO @r (tabla, filas_meta)
SELECT t.name, SUM(p.row_count)
FROM sys.tables AS t
JOIN sys.dm_db_partition_stats AS p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
WHERE t.schema_id = SCHEMA_ID(N'dbo') AND t.is_ms_shipped = 0
  AND t.name COLLATE DATABASE_DEFAULT IN (SELECT tabla COLLATE DATABASE_DEFAULT FROM @interes)
GROUP BY t.name;
DECLARE @t sysname, @n bigint, @sql nvarchar(max);
DECLARE c CURSOR LOCAL STATIC READ_ONLY FORWARD_ONLY FOR SELECT tabla FROM @r WHERE filas_meta <= @limite;
OPEN c;
FETCH NEXT FROM c INTO @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'SELECT @n = COUNT_BIG(*) FROM dbo.' + QUOTENAME(@t) + N';';
    EXEC sys.sp_executesql @sql, N'@n bigint OUTPUT', @n = @n OUTPUT;
    UPDATE @r SET filas_exactas = @n WHERE tabla = @t;
    FETCH NEXT FROM c INTO @t;
END;
CLOSE c;
DEALLOCATE c;
SELECT N'M102SQL|' + tabla + N'|' + CONVERT(nvarchar(20), ISNULL(filas_meta, -1)) + N'|'
       + ISNULL(CONVERT(nvarchar(20), filas_exactas), N'')
FROM @r
ORDER BY tabla;
'@
$FicheroBase = @{}
for ($i = 0; $i -lt $Bases.Count; $i++) {
    $b = $Bases[$i]
    $tablas = @($Esperadas | Where-Object { $_.Db -eq $b } | ForEach-Object { $_.Tabla } | Sort-Object -Unique)
    $valores = ($tablas | ForEach-Object { "(N'" + $_.Replace("'", "''") + "')" }) -join ", "
    $FicheroBase[$b] = "recuentos_$($i + 1).sql"
    Escribir "$Dir\recuentos_$($i + 1).sql" ($Plantilla.Replace("__LIMITE__", "$Limite").Replace("__TABLAS__", $valores))
}

$SqlArrancadoAqui = $false
$activos = @(docker compose -f $Compose ps --status running --services 2>$null)
if ($LASTEXITCODE -ne 0) {
    # sin la lista no se sabe si SQL Server estaba arrancado: ni lo arranco ni lo paro al acabar
    Say "ERROR: docker compose ps fallo (codigo ${LASTEXITCODE}): estado de SQL Server desconocido"; exit 1
}
if (-not ($activos -contains "sqlserver")) {
    Say "  SQL Server no esta arrancado: lo arranco y lo paro al acabar"
    docker compose -f $Compose start sqlserver 2>&1 | ForEach-Object { Say "    $_" }
    if ($LASTEXITCODE -ne 0) { Say "ERROR: no se puede arrancar SQL Server (docker compose start sqlserver)"; exit 1 }
    $SqlArrancadoAqui = $true
}
function PararSql() {
    if ($SqlArrancadoAqui) {
        Say "  SQL Server no estaba arrancado: lo paro otra vez"
        docker compose -f $Compose stop sqlserver 2>&1 | ForEach-Object { Say "    $_" }
        if ($LASTEXITCODE -ne 0) {
            $Script:Fallos++
            Say "ERROR: no se pudo parar SQL Server (docker compose stop devolvio $LASTEXITCODE); sigue arrancado"
        }
    }
}
docker compose -f $Compose exec -T sqlserver mkdir -p $Tmp 2>&1 | Out-Null
foreach ($f in @(@("sqlcmd.sh", "estado.sql") + @($FicheroBase.Values))) {
    docker compose -f $Compose cp "$Dir\$f" "sqlserver:$Tmp/$f" 2>&1 | Out-Null
}
$enLinea = $false
for ($i = 0; $i -lt 100; $i++) {
    $e = @(SqlServer "master" "estado.sql" | Where-Object { $_ -like "M102ESTADO|*" })
    if ($e.Count -gt 0 -and $e[0] -eq "M102ESTADO|$($Bases.Count)") { $enLinea = $true; break }
    if ($i -eq 0) { Say "  Espero a que SQL Server tenga en linea las $($Bases.Count) bases (hasta 25 minutos)" }
    Start-Sleep -Seconds 15
}
if (-not $enLinea) { Say "ERROR: SQL Server no tiene las $($Bases.Count) bases en linea despues de 25 minutos"; PararSql; exit 1 }
Say "  SQL Server con las $($Bases.Count) bases en linea"

$Sql = @{}
foreach ($b in $Bases) {
    $t0 = Get-Date
    $lineas = @(SqlServer $b $FicheroBase[$b])
    if ($Script:SqlCodigo -ne 0) {
        $Script:Fallos++
        Say "  ERROR: recuentos de $(AliasDe $b) (codigo $($Script:SqlCodigo)):"
        @($lineas | Select-Object -First 5) | ForEach-Object { Say "    $_" }
        continue
    }
    $n = 0
    foreach ($linea in @($lineas | Where-Object { $_ -like "M102SQL|*" })) {
        $p = $linea.Split("|")
        $exacta = $null
        if ($p.Count -ge 4 -and $p[3] -ne "") { $exacta = [long]$p[3] }
        $Sql["$b|$($p[1])"] = [pscustomobject]@{ Meta = [long]$p[2]; Exacta = $exacta }
        $n++
    }
    Say ("  {0}: {1} tablas de la carga con recuento en {2:N0} s" -f (AliasDe $b), $n, ((Get-Date) - $t0).TotalSeconds)
}

# --- Bronze con Trino
if (-not (TrinoListo)) { Say "ERROR: Trino no responde"; PararSql; exit 1 }
docker compose -f $Compose exec -T trino mkdir -p $Tmp 2>&1 | Out-Null
$EnBronze = @{}
$r = Trino "tablas_bronze" "SHOW TABLES FROM lakehouse.l1_bronze"
if ($null -eq $r) { $Script:Fallos++; Say "ERROR: no se pueden listar las tablas de l1_bronze"; PararSql; exit 1 }
foreach ($x in @(Tabla $r)) { $EnBronze[$x.Table] = $true }
Say "  Tablas en l1_bronze: $($EnBronze.Count)"
$Conteos = @{}
$pend = @($Esperadas | Where-Object { $EnBronze.ContainsKey($_.Bronze) } | ForEach-Object { $_.Bronze } | Sort-Object -Unique)
$grandes = @($pend | Where-Object { $_ -like "*historic_tag_interval_values_big" })
$resto = @($pend | Where-Object { $_ -notlike "*historic_tag_interval_values_big" })
$lotes = @()
for ($i = 0; $i -lt $resto.Count; $i += 12) { $lotes += , @($resto[$i..([Math]::Min($i + 11, $resto.Count - 1))]) }
foreach ($g in $grandes) { $lotes += , @($g) }
$k = 0
foreach ($lote in $lotes) {
    $k++
    if (EnLaTarde) { $Script:Fallos++; Say "ERROR: son mas de las 18:30: dejo de contar en Bronze"; break }
    $consulta = ($lote | ForEach-Object { "SELECT '$_' AS tabla, count(*) AS filas FROM lakehouse.l1_bronze.$_" }) -join "`nUNION ALL "
    $res = Trino "bronze_$k" $consulta
    if ($null -eq $res) {
        $j = 0
        foreach ($t in $lote) {
            $j++
            $res1 = Trino "bronze_${k}_$j" "SELECT '$t' AS tabla, count(*) AS filas FROM lakehouse.l1_bronze.$t"
            if ($null -eq $res1) { $Script:Fallos++; continue }
            foreach ($x in @(Tabla $res1)) { $Conteos[$x.tabla] = [long]$x.filas }
        }
    } else {
        foreach ($x in @(Tabla $res)) { $Conteos[$x.tabla] = [long]$x.filas }
    }
}
Say "  Recuentos de Bronze: $($Conteos.Count) de $($pend.Count) tablas que existen"
if ($Conteos.Count -lt $pend.Count) { $Script:Fallos++; Say "  ERROR: Trino no ha devuelto el recuento de $($pend.Count - $Conteos.Count) tablas" }

# --- Comparacion
$Filas = @()
foreach ($e in $Esperadas) {
    $s = $Sql["$($e.Db)|$($e.Tabla)"]
    $meta = $null; $exacta = $null
    if ($s) { $meta = $s.Meta; $exacta = $s.Exacta }
    $origen = $exacta
    if ($null -eq $origen) { $origen = $meta }
    $bronze = $null
    if ($Conteos.ContainsKey($e.Bronze)) { $bronze = $Conteos[$e.Bronze] }
    $dif = $null
    if ($null -eq $s) { $lectura = "NO EXISTE EN SQL SERVER" }
    elseif (-not $EnBronze.ContainsKey($e.Bronze)) {
        if ($origen -eq 0) { $lectura = "origen vacio: Bronze no crea la tabla (esperado)" } else { $lectura = "FALTA EN BRONZE" }
    }
    elseif ($null -eq $bronze) { $lectura = "SIN RECUENTO EN BRONZE (error de Trino)" }
    else {
        $dif = $bronze - $origen
        if ($dif -eq 0) { $lectura = "igual" } else { $lectura = "DIFERENCIA" }
        if ($null -eq $exacta) { $lectura += " (origen por metadatos)" }
    }
    $Filas += [pscustomobject]@{ origen = $e.Origen; base = (AliasDe $e.Db); tabla = $e.Tabla; modo = $e.Modo;
        sql_metadatos = $meta; sql_exacto = $exacta; bronze = $bronze; diferencia = $dif; lectura = $lectura }
}
$Filas | Export-Csv -Path "$Dir\conciliacion.csv" -NoTypeInformation -Encoding ASCII

Say "--- Una fila por carga (sql_exacto solo en tablas de hasta $(Num $Limite) filas; diferencia = Bronze - origen)"
Say ("  {0,-4} {1,-16} {2,-41} {3,15} {4,13} {5,15} {6,12}  {7}" -f "tipo", "base", "tabla", "sql_metadatos", "sql_exacto",
    "bronze", "diferencia", "lectura")
foreach ($f in $Filas) {
    Say ("  {0,-4} {1,-16} {2,-41} {3,15} {4,13} {5,15} {6,12}  {7}" -f $f.origen, $f.base, $f.tabla, $f.sql_metadatos,
        $f.sql_exacto, $f.bronze, $f.diferencia, $f.lectura)
}

Say "--- Resumen"
foreach ($o in @("lote", "cdc")) {
    $g = @($Filas | Where-Object { $_.origen -eq $o })
    $iguales = @($g | Where-Object { $_.lectura -like "igual*" }).Count
    $difs = @($g | Where-Object { $_.lectura -like "DIFERENCIA*" })
    $vacias = @($g | Where-Object { $_.lectura -like "origen vacio*" }).Count
    $faltan = @($g | Where-Object { $_.lectura -eq "FALTA EN BRONZE" }).Count
    $noSql = @($g | Where-Object { $_.lectura -eq "NO EXISTE EN SQL SERVER" }).Count
    $sinCuenta = @($g | Where-Object { $_.lectura -like "SIN RECUENTO*" }).Count
    $sumO = 0L; $sumB = 0L
    foreach ($x in @($g | Where-Object { $null -ne $_.diferencia })) {
        $sumB += [long]$x.bronze
        $sumO += [long]$x.bronze - [long]$x.diferencia
    }
    $plantilla = "  {0}: {1} cargas; iguales {2}; con diferencia {3}; origen vacio sin tabla en Bronze {4}; faltan en Bronze {5}; " +
        "no existen en SQL Server {6}; sin recuento {7}. Filas comparadas: origen {8}, Bronze {9}"
    Say ($plantilla -f $o, $g.Count, $iguales, $difs.Count, $vacias, $faltan, $noSql, $sinCuenta, (Num $sumO), (Num $sumB))
    foreach ($x in $difs) { Say ("    diferencia en {0} {1}: {2}" -f $x.base, $x.tabla, (Num $x.diferencia)) }
}
$big = @($Filas | Where-Object { $_.tabla -eq "HistoricTagIntervalValuesBig" })
if ($big.Count -gt 0) {
    Say ("  HistoricTagIntervalValuesBig: SQL Server por metadatos {0}, Bronze {1}; la memoria (v10 4.1) da {2}" -f
        (Num $big[0].sql_metadatos), (Num $big[0].bronze), (Num $Referencia))
}

Say "--- Diferencias esperadas"
Say "  Lotes FULL/OVERWRITE (89 de 90): Bronze copia la tabla entera, sin filtro de ventana ni deduplicacion, y las copias"
Say "    de SQL Server estan quietas desde el 14.08.2026: se espera 0. Una tabla vacia en el origen no crea tabla en Bronze"
Say "    (run_single sale con 'No data to ingest')."
Say "  HistoricTagIntervalValuesBig, INCREMENTAL/APPEND por trozos de Id [a, b) desde MIN(Id) hasta MAX(Id): se espera 0."
Say "    Mas filas en Bronze querria decir un trozo cargado dos veces; menos, trozos sin cargar."
Say "  CDC (29 System*): instantanea inicial de Debezium y MERGE por clave primaria en Bronze, que deja una fila por clave"
Say "    (el ultimo cambio de cada clave gana): se espera 0. Una tabla vacia no crea tabla en Bronze (no hay eventos) y una"
Say "    sin clave primaria no tiene clave de Kafka y el job de Bronze la rechaza."
Say "  Los filtros de ventana (2021 - 2026, por instante de rejilla) y la deduplicacion por clave natural empiezan en Silver:"
Say "    no explican diferencias aqui. Bronze no escribe metricas propias (dq_metrics empieza en Silver)."
Say "  Las metricas de SQL Server son las de sys.dm_db_partition_stats; en las tablas de hasta $(Num $Limite) filas manda el COUNT_BIG(*)."
Say "  Tabla completa en $Dir\conciliacion.csv"

PararSql
if ($Script:Fallos -gt 0) { Say "Terminado con $($Script:Fallos) pasos con errores (ver el log)."; exit 1 }
Say "Terminado sin errores."
exit 0
