# La serie diaria de averías

Tabla del modelo del día: 10479 días de red, del 08.01.2017 al 02.08.2026. statsmodels instalado.

## 1. Autocorrelación de la etiqueta

| redes | dias | dias_con_averia | tasa | r1 | r2 | r7 | r365 | banda_95 | q7 | p7 | q14 | p14 | p_averia_tras_averia | p_averia_tras_dia_sin | cociente | racha_media_dias |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distribuidora 1 | 3493 | 170 | 0.049 | 0.054 | -0.014 | 0.023 | -0.007 | 0.033 | 15.102 | 0.035 | 18.760 | 0.174 | 0.100 | 0.046 | 2.171 | 1.111 |
| Distribuidora 2 | 3493 | 127 | 0.036 | 0.061 | -0.013 | -0.013 | 0.009 | 0.033 | 15.953 | 0.026 | 22.320 | 0.072 | 0.095 | 0.034 | 2.788 | 1.104 |
| Distribuidora 3 | 3493 | 44 | 0.013 | 0.125 | 0.010 | 0.010 | 0.037 | 0.033 | 57.943 | 0.000 | 64.428 | 0.000 | 0.136 | 0.011 | 12.373 | 1.158 |
| Las tres redes | 3493 | 308 | 0.088 | 0.057 | -0.004 | 0.000 | 0.024 | 0.033 | 12.773 | 0.078 | 14.016 | 0.448 | 0.140 | 0.083 | 1.683 | 1.162 |

Chi cuadrado del mes (tres redes apiladas): 12.3, p = 0.34.
STL de la serie mensual: estacional 0.28, tendencia 0.09 y resto 0.70 de la varianza; ADF p = 0.000.

## 2. Referencias sin meteorología (días con previsión real de la víspera)

| anios | referencia | dias | dias_con_averia | pr_auc | veces_tasa | captura_10 | captura_20 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2024 - 2025 | tasa_base | 2133 | 58 | 0.040 | 1.471 | 0.155 | 0.310 |
| 2024 - 2025 | clima_mensual | 2133 | 58 | 0.048 | 1.775 | 0.207 | 0.379 |
| 2024 - 2025 | clima_15_dias | 2133 | 58 | 0.056 | 2.050 | 0.241 | 0.345 |
| 2024 - 2025 | averia_ayer | 2133 | 58 | 0.042 | 1.562 | 0.259 | 0.310 |
| 2024 - 2025 | logistica_autorregresiva | 2133 | 58 | 0.069 | 2.541 | 0.190 | 0.328 |
| 2024 - 2025 | arma_1_1 | 2133 | 58 | 0.060 | 2.203 | 0.293 | 0.397 |
| 2024 - 2026 | tasa_base | 2766 | 84 | 0.041 | 1.353 | 0.190 | 0.333 |
| 2024 - 2026 | clima_mensual | 2766 | 84 | 0.044 | 1.440 | 0.155 | 0.298 |
| 2024 - 2026 | clima_15_dias | 2766 | 84 | 0.052 | 1.702 | 0.202 | 0.333 |
| 2024 - 2026 | averia_ayer | 2766 | 84 | 0.050 | 1.634 | 0.250 | 0.298 |
| 2024 - 2026 | logistica_autorregresiva | 2766 | 84 | 0.056 | 1.829 | 0.179 | 0.298 |
| 2024 - 2026 | arma_1_1 | 2766 | 84 | 0.069 | 2.273 | 0.274 | 0.405 |

## 3. El hueco de 2025 con la evaluación de producción

237 días de red en el hueco, 2 con avería.

| anios | hueco | dias | dias_con_averia | tasa | pr_auc | veces_tasa | captura_10 | captura_20 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024 - 2025 | como días sin avería | 2133 | 58 | 0.027 | 0.188 | 6.904 | 0.534 | 0.672 |
| 2024 - 2025 | fuera del ajuste y de la puntuación | 1896 | 56 | 0.030 | 0.193 | 6.542 | 0.518 | 0.679 |
| 2024 - 2026 | como días sin avería | 2766 | 84 | 0.030 | 0.156 | 5.137 | 0.452 | 0.643 |
| 2024 - 2026 | fuera del ajuste y de la puntuación | 2529 | 82 | 0.032 | 0.158 | 4.865 | 0.439 | 0.646 |

## 4. De dos tardes antes a la víspera (logísticas orientativas)

| anios | dias | dias_con_averia | pr_auc_24h | veces_tasa_24h | parte_alerta_24h | recoge_24h | pr_auc_48h | veces_tasa_48h | parte_alerta_48h | recoge_48h | alerta_48h | alerta_24h | en_las_dos | siguen_en_alerta | ya_en_alerta | spearman | tasa_las_dos | tasa_solo_48h | tasa_solo_24h |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024 - 2025 | 2130 | 58 | 0.168 | 6.158 | 0.125 | 0.517 | 0.126 | 4.639 | 0.119 | 0.397 | 253 | 266 | 194 | 0.767 | 0.729 | 0.920 | 0.113 | 0.017 | 0.111 |
| 2024 - 2026 | 2757 | 84 | 0.146 | 4.777 | 0.124 | 0.464 | 0.125 | 4.117 | 0.118 | 0.357 | 326 | 341 | 250 | 0.767 | 0.733 | 0.922 | 0.116 | 0.013 | 0.110 |

## 5. Calendarios frente a la alerta (Distribuidoras 1 y 2)

1844 días de red del 2024-01-20 al 2026-08-02; días con incidencia en fin de semana 13 de 532 y en laborable 60 de 1312 (Fisher p = 0.035).

| regla | dias_anio_red | parte_dias | parte_incidencias | parte_horas_cliente | parte_horas_cliente_sin_recortar | parte_horas_cliente_sin_temporal |
| --- | --- | --- | --- | --- | --- | --- |
| GridPredic en alerta | 45.755 | 0.125 | 0.531 | 0.723 | 0.695 | 0.457 |
| Fines de semana de noviembre a marzo | 43.576 | 0.119 | 0.052 | 0.041 | 0.030 | 0.080 |
| Todos los fines de semana | 105.376 | 0.289 | 0.146 | 0.106 | 0.179 | 0.208 |
| Clima de 15 días, la parte de días de la alerta | 45.557 | 0.125 | 0.156 | 0.109 | 0.079 | 0.213 |
| Clima mensual, la parte de días de la alerta | 45.557 | 0.125 | 0.135 | 0.088 | 0.062 | 0.172 |
| GridPredic, la misma parte de días por red y año | 45.557 | 0.125 | 0.521 | 0.708 | 0.507 | 0.428 |

Tiempo: 56 s.
