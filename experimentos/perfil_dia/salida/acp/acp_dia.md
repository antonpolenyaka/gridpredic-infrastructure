# Componentes principales de las variables meteorológicas del modelo del día

8880 días de red desde el 25.06.2018 con las 16 variables, 276 con avería. Valores propios 6.94, 2.92, 1.70, 1.17, 0.87. Por encima de 1: 4 componentes con el 79.6 % de la inercia; el 90 % se pasa con 7.

Cargas rotadas con varimax en cuatro dimensiones (las de 0,3 o más):

| Variable | F1 | F2 | F3 | F4 |
| --- | --- | --- | --- | --- |
| dia_racha_max | 0.65 |  |  | 0.58 |
| dia_racha_media_max | 0.94 |  |  |  |
| dia_viento_max | 0.87 |  |  |  |
| dia_viento_media_max | 0.92 |  |  |  |
| dia_lluvia_max |  |  | 0.90 |  |
| dia_lluvia_media | 0.35 |  | 0.86 |  |
| dia_lluvia_hora_max |  |  | 0.86 |  |
| dia_temp_max |  | 0.95 |  |  |
| dia_temp_media_min |  | 0.93 |  |  |
| dia_humedad_media |  | -0.82 | 0.36 |  |
| ext_racha_clima_max | 0.88 |  |  | 0.32 |
| ext_racha_clima_media | 0.95 |  |  |  |
| ext_racha_ku | 0.31 |  | 0.37 |  |
| ext_dias_calor |  | 0.62 |  |  |
| ayer_racha_max |  |  |  | 0.85 |
| ayer_lluvia_max |  |  | 0.48 | 0.55 |

Evaluación por años con la de producción (PR-AUC, captura del 10 % y diferencia emparejada frente a la logística por bloques de siete días):

| Banco | Modelo | PR-AUC | Captura 10 % | Diferencia | Intervalo |
| --- | --- | --- | --- | --- | --- |
| reanálisis 2022 - 2026 | tasa_base | 0.031 | 0.128 | -0.105 | -0.181 a -0.051 |
| reanálisis 2022 - 2026 | logistica | 0.131 | 0.354 |  |  |
| reanálisis 2022 - 2026 | logistica_acp3 | 0.127 | 0.341 | -0.003 | -0.014 a +0.002 |
| reanálisis 2022 - 2026 | logistica_acp4 | 0.129 | 0.348 | -0.003 | -0.022 a +0.008 |
| reanálisis 2022 - 2026 | logistica_acp5 | 0.132 | 0.348 | +0.001 | -0.004 a +0.009 |
| reanálisis 2022 - 2026 | logistica_acp7 | 0.132 | 0.366 | -0.001 | -0.011 a +0.008 |
| previsión real 2024 - 2025 | tasa_base | 0.028 | 0.155 | -0.164 | -0.274 a -0.076 |
| previsión real 2024 - 2025 | logistica | 0.188 | 0.534 |  |  |
| previsión real 2024 - 2025 | logistica_acp3 | 0.193 | 0.517 | +0.003 | -0.007 a +0.017 |
| previsión real 2024 - 2025 | logistica_acp4 | 0.164 | 0.534 | -0.022 | -0.043 a -0.001 |
| previsión real 2024 - 2025 | logistica_acp5 | 0.181 | 0.517 | -0.003 | -0.025 a +0.009 |
| previsión real 2024 - 2025 | logistica_acp7 | 0.176 | 0.534 | -0.011 | -0.028 a +0.001 |
| previsión real 2024 - 2026 | tasa_base | 0.029 | 0.179 | -0.127 | -0.222 a -0.061 |
| previsión real 2024 - 2026 | logistica | 0.156 | 0.452 |  |  |
| previsión real 2024 - 2026 | logistica_acp3 | 0.146 | 0.440 | -0.006 | -0.032 a +0.006 |
| previsión real 2024 - 2026 | logistica_acp4 | 0.149 | 0.464 | -0.008 | -0.028 a +0.008 |
| previsión real 2024 - 2026 | logistica_acp5 | 0.152 | 0.452 | -0.003 | -0.019 a +0.006 |
| previsión real 2024 - 2026 | logistica_acp7 | 0.156 | 0.452 | -0.003 | -0.014 a +0.010 |
