"""
El ejercicio 2 del laboratorio Hadoop III (tamaño con 1 y con 1.000 tuplas) con filas del proyecto: para N filas
tomadas al azar (semilla fija) de tres tablas, el tamaño en CSV (con cabecera), JSON por líneas, Avro sin compresión
(si fastavro está instalado) y Parquet con snappy y con zstd, y el peso del pie de Parquet. Da el coste fijo de cada
formato y a partir de cuántas filas Parquet ocupa menos que el CSV, la pregunta que importa para un fichero por
micro-lote.

    python sobrecoste.py <ml_resultados> <salida/> [carpeta temporal]
"""

import io
import os
import shutil
import random
import sys
import tempfile

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from formatos import avro_records, avro_schema, fastavro, json_ready  # noqa: E402

NS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 10000]


def sizes(t, tmp):
    os.makedirs(tmp, exist_ok=True)
    out = {}
    buf = pa.BufferOutputStream()
    pacsv.write_csv(t, buf)
    out["csv"] = buf.getvalue().size
    hdr = io.BytesIO()
    pacsv.write_csv(t.slice(0, 0), hdr)
    out["cabecera_csv"] = len(hdr.getvalue())
    s = json_ready(t).to_pandas().to_json(orient="records", lines=True, date_format="iso", date_unit="us")
    out["jsonl"] = len(s.encode("utf-8"))
    if fastavro is not None:
        b = io.BytesIO()
        fastavro.writer(b, avro_schema(t), avro_records(t), codec="null")
        out["avro"] = len(b.getvalue())
    else:
        out["avro"] = float("nan")
    for codec in ("snappy", "zstd"):
        p = os.path.join(tmp, f"x.{codec}.parquet")
        kw = {"compression": codec}
        if codec == "zstd":
            kw["compression_level"] = 3
        pq.write_table(t, p, **kw)
        out[f"parquet_{codec}"] = os.path.getsize(p)
        if codec == "snappy":
            with open(p, "rb") as fh:
                fh.seek(-8, 2)
                out["pie_parquet"] = int.from_bytes(fh.read(4), "little")
    shutil.rmtree(tmp)
    return out


def main(base, out, tmp):
    os.makedirs(out, exist_ok=True)
    import formatos
    tabs = formatos.load(base)
    rows = []
    for name in ("map_aguas_arriba", "interrupciones", "dia"):
        t = tabs[name][0]
        for n in NS + [t.num_rows]:
            if n > t.num_rows:
                continue
            idx = pa.array(sorted(random.Random(42).sample(range(t.num_rows), n)))
            sub = t.take(idx)
            s = sizes(sub, os.path.join(tmp, name))
            s.update({"tabla": name, "columnas": t.num_columns, "filas": n})
            rows.append(s)
            print(name, n, {k: v for k, v in s.items() if k not in ("tabla",)}, flush=True)
    keys = ["tabla", "columnas", "filas", "csv", "cabecera_csv", "jsonl", "avro", "parquet_snappy", "parquet_zstd", "pie_parquet"]
    with open(os.path.join(out, "sobrecoste.csv"), "w", encoding="utf-8") as fh:
        fh.write(",".join(keys + ["avro_frente_csv", "parquet_frente_csv"]) + "\n")
        for r in rows:
            fh.write(",".join(str(r[k]) for k in keys) +
                     f",{r['avro'] / r['csv']:.3f},{r['parquet_snappy'] / r['csv']:.3f}\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else tempfile.mkdtemp(prefix="sobrecoste_"))
