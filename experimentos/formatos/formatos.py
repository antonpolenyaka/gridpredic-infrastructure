"""
La comparación de formatos del laboratorio Hadoop III repetida con tablas del proyecto, sin Spark: cada tabla se
escribe como CSV, CSV con gzip, JSON por líneas, JSON por líneas con gzip, Avro (sin compresión y con deflate) y
Parquet (sin compresión, snappy y zstd de nivel 3, el nivel por defecto de parquet-mr), y se mide el tamaño en disco,
el tiempo de escritura, el de una lectura completa y el de una lectura de solo tres columnas (mediana de REPS
lecturas tras una de calentamiento; los ficheros están en la caché del sistema, así que se mide el coste de
decodificar y no el del disco).

Lectores: pyarrow 21 (CSV, JSON y Parquet, en C++ y con dos hilos) y fastavro (Avro, objetos de Python). fastavro no
es una dependencia del repositorio: si no está instalado, las filas de Avro no salen. Los tiempos de Avro no son
comparables con los demás; el tamaño y los bytes que hay que leer sí.

    python formatos.py <ml_resultados> <salida/> [carpeta temporal]

<ml_resultados> es la carpeta _runlogs/ml_resultados del portátil: lee perfil_dia/dia.csv.gz y la exportación de Gold
zonas_20261013_1041. Solo escribe cifras agregadas en <salida>/formatos.csv; los ficheros de cada formato se borran al
terminar cada tabla.
"""

import gc
import gzip
import os
import shutil
import statistics
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as pq

try:
    import fastavro
except ImportError:  # Avro solo si fastavro está instalado
    fastavro = None

REPS = 5


def load(base):
    """Las tablas del experimento y las tres columnas que lee la consulta estrecha."""
    tabs = {}
    dia = pacsv.read_csv(os.path.join(base, "perfil_dia", "dia.csv.gz"))
    tabs["dia"] = (dia, ["distribuidora_id", "fecha", "y_dia"])
    z = os.path.join(base, "zonas_20261013_1041")
    tabs["interrupciones"] = (pq.read_table(os.path.join(z, "interrupciones.parquet")),
                              ["distribuidora_id", "inicio_ts", "duracion_s"])
    tabs["map_aguas_arriba"] = (pq.read_table(os.path.join(z, "map_aguas_arriba.parquet")),
                                ["trafo_elemento_id", "elemento_corte_id", "n_cortes"])
    tabs["cortes_scada"] = (pq.read_table(os.path.join(z, "cortes_scada.parquet")),
                            ["distribuidora_id", "inicio_ts", "duracion_s"])
    cab = pq.read_table(os.path.join(z, "cabecera_hora__2025.parquet"))
    hora = cab.column("hora")
    start = pc.strptime(pa.array(["2025-01-01"]), format="%Y-%m-%d", unit="ns")[0]
    end = pc.strptime(pa.array(["2025-02-01"]), format="%Y-%m-%d", unit="ns")[0]
    mask = pc.and_(pc.greater_equal(hora, start), pc.less(hora, end))
    tabs["cabecera_hora_2025_01"] = (cab.filter(mask), ["distribuidora_id", "hora", "media_ev_disparo_24h"])
    return tabs


def json_ready(t):
    """date32 a texto ISO para que pandas lo escriba igual que un timestamp."""
    cols = []
    for f, c in zip(t.schema, t.columns):
        cols.append(c.cast(pa.string()) if pa.types.is_date32(f.type) else c)
    return pa.table(cols, names=t.schema.names)


def avro_schema(t, cols=None):
    fields = []
    for f in t.schema:
        if cols and f.name not in cols:
            continue
        ty = f.type
        if pa.types.is_null(ty):
            fields.append({"name": f.name, "type": "null"})
            continue
        if pa.types.is_int32(ty) or pa.types.is_int16(ty) or pa.types.is_int8(ty):
            a = "int"
        elif pa.types.is_integer(ty):
            a = "long"
        elif pa.types.is_floating(ty):
            a = "double"
        elif pa.types.is_boolean(ty):
            a = "boolean"
        elif pa.types.is_timestamp(ty):
            a = {"type": "long", "logicalType": "timestamp-micros"}
        elif pa.types.is_date32(ty):
            a = {"type": "int", "logicalType": "date"}
        else:
            a = "string"
        fields.append({"name": f.name, "type": ["null", a]})
    return fastavro.parse_schema({"type": "record", "name": "fila", "fields": fields})


def avro_records(t):
    cols = []
    for f, c in zip(t.schema, t.columns):
        cols.append(c.cast(pa.timestamp("us")) if pa.types.is_timestamp(f.type) else c)
    return pa.table(cols, names=t.schema.names).to_pylist()


def timed(fn):
    fn()
    ts = []
    for _ in range(REPS):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    return statistics.median(ts)


def parquet_bytes_for(path, cols):
    pf = pq.ParquetFile(path)
    md = pf.metadata
    idx = [md.schema.names.index(c) for c in cols]
    data = sum(md.row_group(r).column(i).total_compressed_size for r in range(md.num_row_groups) for i in idx)
    with open(path, "rb") as fh:
        fh.seek(-8, 2)
        footer = int.from_bytes(fh.read(4), "little")
    return data + footer + 8, footer


def run_table(name, t, cols, tmp):
    os.makedirs(tmp, exist_ok=True)
    out = []
    p = {k: os.path.join(tmp, f"{name}.{k}") for k in
         ("csv", "csv.gz", "jsonl", "jsonl.gz", "avro", "avro_deflate", "parquet_none", "parquet_snappy", "parquet_zstd")}

    def w_csv():
        pacsv.write_csv(t, p["csv"])

    def w_csv_gz():
        buf = pa.BufferOutputStream()
        pacsv.write_csv(t, buf)
        with open(p["csv.gz"], "wb") as fh:
            fh.write(gzip.compress(buf.getvalue().to_pybytes(), compresslevel=6))

    jt = json_ready(t).to_pandas()

    def w_jsonl():
        jt.to_json(p["jsonl"], orient="records", lines=True, date_format="iso", date_unit="us")

    def w_jsonl_gz():
        s = jt.to_json(orient="records", lines=True, date_format="iso", date_unit="us")
        with open(p["jsonl.gz"], "wb") as fh:
            fh.write(gzip.compress(s.encode("utf-8"), compresslevel=6))

    sch = avro_schema(t) if fastavro else None
    sub = avro_schema(t, cols) if fastavro else None

    def w_avro(codec, path):
        def f():
            recs = avro_records(t)
            with open(path, "wb") as fo:
                fastavro.writer(fo, sch, recs, codec=codec)
        return f

    def w_pq(codec, path):
        def f():
            kw = {"compression": codec}
            if codec == "zstd":
                kw["compression_level"] = 3
            pq.write_table(t, path, **kw)
        return f

    writers = {
        "csv": w_csv, "csv.gz": w_csv_gz, "jsonl": w_jsonl, "jsonl.gz": w_jsonl_gz,
        "avro": w_avro("null", p["avro"]), "avro_deflate": w_avro("deflate", p["avro_deflate"]),
        "parquet_none": w_pq("none", p["parquet_none"]), "parquet_snappy": w_pq("snappy", p["parquet_snappy"]),
        "parquet_zstd": w_pq("zstd", p["parquet_zstd"]),
    }
    if fastavro is None:
        writers = {k: w for k, w in writers.items() if not k.startswith("avro")}
    json_schema = {}

    def readers(k):
        path = p[k]
        if k.startswith("csv"):
            return (lambda: pacsv.read_csv(path),
                    lambda: pacsv.read_csv(path, convert_options=pacsv.ConvertOptions(include_columns=cols)))
        if k.startswith("jsonl"):
            def proj():
                s = json_schema["s"]
                po = pajson.ParseOptions(explicit_schema=pa.schema([s.field(c) for c in cols]),
                                         unexpected_field_behavior="ignore")
                return pajson.read_json(path, parse_options=po)
            return (lambda: pajson.read_json(path), proj)
        if k.startswith("avro"):
            def full():
                with open(path, "rb") as fo:
                    return list(fastavro.reader(fo))

            def proj():
                with open(path, "rb") as fo:
                    return list(fastavro.reader(fo, reader_schema=sub))
            return full, proj
        return (lambda: pq.read_table(path), lambda: pq.read_table(path, columns=cols))

    for k, w in writers.items():
        gc.collect()
        t0 = time.perf_counter()
        w()
        wt = time.perf_counter() - t0
        size = os.path.getsize(p[k])
        full, proj = readers(k)
        res = full()
        n = res.num_rows if hasattr(res, "num_rows") else len(res)
        assert n == t.num_rows, (name, k, n, t.num_rows)
        if k == "jsonl":
            json_schema["s"] = res.schema
        del res
        rf = timed(full)
        rp = timed(proj)
        if k.startswith("parquet"):
            b3, footer = parquet_bytes_for(p[k], cols)
        else:
            b3, footer = size, None
        out.append({"tabla": name, "filas": t.num_rows, "columnas": t.num_columns,
                    "arrow_MB": round(t.nbytes / 1e6, 2), "formato": k, "bytes": size,
                    "escritura_s": round(wt, 3), "lectura_s": round(rf, 4), "lectura_3col_s": round(rp, 4),
                    "bytes_3col": b3, "pie_parquet_B": footer})
        print(f"{name:24s} {k:15s} {size / 1e6:9.2f} MB  w {wt:6.2f}s  r {rf:7.3f}s  r3 {rp:7.3f}s  b3 {b3 / 1e6:8.2f} MB", flush=True)
    shutil.rmtree(tmp)
    return out


def main(base, out, tmp):
    os.makedirs(out, exist_ok=True)
    rows = []
    tabs = load(base)
    for name, (t, cols) in tabs.items():
        rows.extend(run_table(name, t, cols, os.path.join(tmp, name)))
    keys = list(rows[0].keys())
    csv_size = {r["tabla"]: r["bytes"] for r in rows if r["formato"] == "csv"}
    with open(os.path.join(out, "formatos.csv"), "w", encoding="utf-8") as fh:
        fh.write(",".join(keys + ["frente_csv", "parte_3col"]) + "\n")
        for r in rows:
            vals = [("" if r[k] is None else str(r[k])) for k in keys]
            vals.append(f"{r['bytes'] / csv_size[r['tabla']]:.4f}")
            vals.append(f"{r['bytes_3col'] / r['bytes']:.4f}")
            fh.write(",".join(vals) + "\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else tempfile.mkdtemp(prefix="formatos_"))
