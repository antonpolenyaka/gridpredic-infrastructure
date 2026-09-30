# Notebooks

Carpeta para el trabajo exploratorio (análisis de Gold, pruebas de features, evaluación del modelo). Seguimos la convención de Cookiecutter Data Science para el nombre: un número para ordenar, las iniciales de quien lo escribe y un tema corto separado por guiones.

```text
1.0-asp-cobertura-telemetria-por-ct.ipynb
2.0-jmp-features-precursoras.ipynb
```

Reglas:

- Un notebook es para explorar y para contar algo, no para ejecutar el pipeline. Lo que funcione se refactoriza a un job de `etl/jobs/` con su test.
- Los notebooks leen del lakehouse (Trino o Spark), nunca de extractos copiados a mano. Así el resultado se puede repetir con el `run_id` de la ejecución.
- Antes de hacer commit, limpiar las salidas pesadas y no guardar datos en la carpeta.

Por ahora la exploración de las bases de datos se hizo con consultas SQL directas sobre SQL Server, documentadas en la memoria del TFM; los primeros notebooks llegarán con Gold.
