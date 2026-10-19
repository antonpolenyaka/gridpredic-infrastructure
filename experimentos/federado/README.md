# Entrenamiento federado del modelo del día

`fedavg_dia.py` simula que cada distribuidora entrena en su casa y solo comparte números (un promedio en una ronda y el óptimo exacto por rondas con la estandarización por sumas), frente a la logística de producción con los datos juntos y a cada red sola, con la evaluación por años y la previsión real de `job_ml_riesgo_dia.py` (cuaderno 3w de [docs/investigacion-continua.md](../../docs/investigacion-continua.md)). Lee la tabla de `experimentos/perfil_dia/exportar_dia.py`, sin Spark:

    python experimentos\federado\fedavg_dia.py <dia.csv.gz> <bloques.json> experimentos\federado\salida

Deja `salida/fedavg_dia.txt` en menos de dos minutos. Las predicciones por día no se guardan.
