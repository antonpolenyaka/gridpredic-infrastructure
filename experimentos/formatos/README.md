# Formatos de fichero

La comparación de formatos del laboratorio Hadoop III con tablas del proyecto (4.4.1 y 4.7.1 de la memoria): `formatos.py` escribe cada tabla en CSV, JSON por líneas, Avro y Parquet (con y sin compresión) y mide tamaño, escritura, lectura completa y lectura de tres columnas; `sobrecoste.py`, el coste fijo de cada formato con pocas filas. Leen `perfil_dia/dia.csv.gz` y la exportación de Gold `zonas_20261013_1041` de `_runlogs/ml_resultados`, sin Spark:

    python experimentos\formatos\formatos.py D:\GitHub\gridpredic-infrastructure\_runlogs\ml_resultados experimentos\formatos\salida
    python experimentos\formatos\sobrecoste.py D:\GitHub\gridpredic-infrastructure\_runlogs\ml_resultados experimentos\formatos\salida

Avro solo sale si fastavro está instalado (`pip install fastavro`), que no es una dependencia del repositorio. `formatos.py` tarda unos 8 minutos, casi todos en Avro (fastavro escribe y lee objetos de Python), y `sobrecoste.py` menos de uno. Los tamaños se repiten al byte; los tiempos dependen de la máquina.

En `salida/`: `formatos.csv` (tamaño, escritura, lecturas y bytes de tres columnas de cada tabla y formato, y su relación con el CSV) y `sobrecoste.csv` (bytes de cada formato con 1 a todas las filas y el pie de Parquet). El resumen está en [docs/cifras.md](../../docs/cifras.md) 4.
