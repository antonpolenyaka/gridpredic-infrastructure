# Validación del modelo del día

`validacion_dia.py` compara la evaluación por años del repositorio con el protocolo del laboratorio de MLlib (randomSplit y CrossValidator de pliegues aleatorios) y con otras particiones, y la PR-AUC del repositorio (la average precision de scikit-learn) con el área PR de Spark (5.4 de la memoria). Lee la tabla de `experimentos/perfil_dia/exportar_dia.py`, sin Spark:

    python experimentos\validacion\validacion_dia.py <dia.csv.gz> <bloques.json> experimentos\validacion\salida

Deja `salida/validacion_dia.txt` en unos tres minutos.
