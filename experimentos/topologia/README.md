# Topología: la red como grafo

Los guiones de grafos del cuaderno de laboratorio (3x en [docs/investigacion-continua.md](../../docs/investigacion-continua.md)) y sus cifras en [docs/cifras.md](../../docs/cifras.md). Todos leen la exportación de Gold del 13.10.2026 (`_runlogs/ml_resultados/zonas_20261013_1041` en el portátil, unos 116 MB de parquet) con pandas, numpy, scipy y scikit-learn, sin Spark ni servidor de grafos, y no tocan `etl/`. Las salidas llevan recuentos y métricas, con los eventos del SCADA numerados por orden de inicio (del 1 al 7.373) y sin su identificador interno; ningún código de CT, cabecera ni municipio, y las redes como Distribuidora 1, 2 y 3 (o D1, D2 y D3).

Desde la raíz del repositorio:

    set EXP=D:\GitHub\gridpredic-infrastructure\_runlogs\ml_resultados
    set Z=%EXP%\zonas_20261013_1041
    set T=experimentos\topologia

    rem Con carpeta de salida: escriben ahí <guion>.txt (y las tablas que tengan)
    python %T%\e0_modelo.py %Z% %T%\salida
    rem igual: e1_saltos, e2_fallo_ayer, e3_simultaneas, m1_dos_redes, m2_cabecera_ayer, m3_triangulos, anillos_red
    python %T%\materializar_contexto.py %Z% %EXP%\rutas_20261014 %T%\salida
    python %T%\comunidades.py %Z% %T%\salida\comunidades.txt
    python %T%\comunidades_bootstrap.py %Z% %T%\salida\comunidades_bootstrap.txt

    rem Tablas en la carpeta de salida y el texto en la consola
    python %T%\prediccion_enlaces.py %Z% %T%\salida > %T%\salida\prediccion_enlaces.txt
    rem igual: prediccion_enlaces_detalle, ventana_validez, embedding_causas, embedding_una_relacion
    python %T%\grafo_riesgo.py %Z% %EXP%\rct_todas_20261012_0744\puntuaciones.csv %T%\salida > %T%\salida\grafo_riesgo.txt
    rem igual: grafo_riesgo_control

    rem Solo la consola
    python %T%\cabeceras_48.py %Z% > %T%\salida\cabeceras_48.txt
    rem igual: el resto de guiones de la tabla; tamano_frente_historia.txt lleva también la pasada con el 20 %:
    python %T%\tamano_frente_historia.py %Z% 0.20 >> %T%\salida\tamano_frente_historia.txt

| Guion | Qué mide | Tiempo |
| --- | --- | --- |
| `grafo_gp.py` | El grafo de propiedades leído como tablas (módulo de los cuatro siguientes) | |
| `e0_modelo.py` | Inventario de nodos y aristas y coste de materializar el co-corte como pares de CT | 10 s |
| `e1_saltos.py` | Vecinos a k saltos por conjuntos y filas de la cadena de joins sin DISTINCT | 25 s |
| `e2_fallo_ayer.py` | Si compartir elemento aguas arriba con un CT que falló ayer anticipa una interrupción, por CT-día y por incidencia | 15 s |
| `e3_simultaneas.py` | Incidencias simultáneas de dos redes con elemento común y el ciclo entre la cabecera de Calser y la topología del SCADA | 10 s |
| `comun.py`, `prediccion_enlaces.py`, `prediccion_enlaces_detalle.py` | Predicción de enlaces temporal del grafo de co-corte (2019 - 2026) y su detalle (`lp_*.csv`) | 1 - 2 min |
| `ventana_validez.py` | Cuánta historia del SCADA conviene mirar para saber qué CT caerán juntos el año siguiente (`lp_ventana_scada.csv`) | 30 s |
| `kge_lib.py`, `embedding_causas.py`, `embedding_una_relacion.py` | Embedding de grafo de conocimiento (TransE y DistMult en numpy) para el ranking anual por familia de causa (`kge_causas_por_anio.csv`) | 6 - 10 min |
| `grafo_gf.py`, `m1_dos_redes.py`, `m2_cabecera_ayer.py`, `m3_triangulos.py`, `am_equivalencia.py` | Los motivos de GraphFrames emulados en pandas: dos redes, la cabecera de ayer, triángulos y anidamiento, aggregateMessages | 1 min |
| `anillos_red.py`, `cobertura_calser.py`, `comprobar_bloques.py`, `materializar_contexto.py` | El patrón del anillo entre las dos redes (con las tablas de eventos), materializar o no y el contexto de un CT (`contexto_ct.csv`; lee también `rutas_20261014`) | 1 min |
| `cabeceras_48.py`, `cabeceras_grandes.py`, `cabecera_caducada.py`, `cabecera_reciente.py`, `brote_2021.py`, `parte_cabecera_mayor.py`, `concentracion_unidades.py`, `tamano_frente_historia.py`, `maniobra_por_linea.py` | La cabecera aprendida de los cortes: qué es, cuánto pesa y cuánto de su captura es tamaño | 5 s cada uno |
| `fuga_posiciones.py`, `riesgo_anual_replica.py` | La variable de toda la ventana del SCADA en el riesgo anual y la réplica sin ella | 10 s y 2 min |
| `grafo_lib.py`, `grafo_riesgo.py`, `grafo_riesgo_control.py` | La historia de los CT vecinos del grafo de cortes (o de un embedding) en el riesgo anual, con vecinos barajados como control (`grafo_riesgo_captura.csv`); leen también `rct_todas_20261012_0744/puntuaciones.csv` | 1 min |
| `comunidades.py`, `comunidades_bootstrap.py` | Comunidades de Louvain frente a las líneas de TedisNet como unidad de reparto | 2 - 6 min |

Los tiempos son los de un portátil sin otra carga; `salida/` se generó en una máquina compartida y los tiempos que imprimen algunos guiones son mayores. `comunidades.py` y `comunidades_bootstrap.py` necesitan networkx (`pip install networkx`), que no es una dependencia del repositorio; el resto solo usa lo de `requirements.txt`.
