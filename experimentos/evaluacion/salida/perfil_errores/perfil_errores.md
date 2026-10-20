# Perfil de los errores del modelo del día

Distribuidoras 1 y 2. Fallo: día con avería de causa local en nivel bajo; acierto: día con avería en alerta. Falsa alarma: día en alerta sin avería.

## Previsión real de la víspera

1844 días de red, 73 con avería, 231 en alerta.

Fallos: 25 días con avería en nivel bajo frente a 30 en alerta; 0 con aviso de viento o tormenta, 20 solo de equipos o de causa desconocida; racha media 43 km/h frente a 82. La frase de la memoria se sostiene.

| variable | v_test | media_clase | media_resto | auc |
| --- | --- | --- | --- | --- |
| dia_temp_max | 2.64 | 25.91 | 19.86 | 0.69 |
| dia_temp_media_min | 0.87 | 12.43 | 11.01 | 0.59 |
| hist_incidencias_365d | 0.73 | 19.04 | 17.90 | 0.58 |
| ayer_racha_max | -3.46 | 45.12 | 70.72 | 0.22 |
| dia_humedad_media | -4.13 | 57.90 | 78.93 | 0.17 |
| dia_racha_max | -4.50 | 43.40 | 82.03 | 0.10 |
| ext_racha_clima_max | -4.52 | 0.75 | 1.22 | 0.13 |
| dia_lluvia_max | -5.56 | 2.10 | 47.43 | 0.02 |

| variable | modalidad | dias | dias_averia | cla_mod | mod_cla | v_test |
| --- | --- | --- | --- | --- | --- | --- |
| aviso_viento_tormenta | sin aviso | 35 | 25 | 71.43 | 100.00 | 5.42 |
| solo_equipo_o_desconocida | 1 | 35 | 20 | 57.14 | 80.00 | 2.24 |
| equipo_o_desconocida | 1 | 38 | 20 | 52.63 | 80.00 | 1.54 |
| causa_dominante | desconocida | 13 | 8 | 61.54 | 32.00 | 1.27 |
| estacion | verano | 15 | 9 | 60.00 | 36.00 | 1.27 |
| hay_desconocida | 1 | 14 | 8 | 57.14 | 32.00 | 0.97 |
| causa_dominante | terceros | 1 | 1 | 100.00 | 4.00 | 0.75 |
| hay_terceros | 1 | 1 | 1 | 100.00 | 4.00 | 0.75 |
| causa_dominante | ninguna | 3 | 2 | 66.67 | 8.00 | 0.65 |
| causa_dominante | equipo | 24 | 12 | 50.00 | 48.00 | 0.57 |
| hay_equipo | 1 | 24 | 12 | 50.00 | 48.00 | 0.57 |
| estacion | primavera | 16 | 8 | 50.00 | 32.00 | 0.42 |
| hay_vegetacion | 1 | 1 | 0 | 0.00 | 0.00 | -0.60 |
| causa_dominante | vegetacion | 1 | 0 | 0.00 | 0.00 | -0.60 |
| estacion | invierno | 11 | 4 | 36.36 | 16.00 | -0.63 |
| hay_fauna | 1 | 4 | 1 | 25.00 | 4.00 | -0.74 |
| causa_dominante | fauna | 4 | 1 | 25.00 | 4.00 | -0.74 |
| estacion | otoño | 13 | 4 | 30.77 | 16.00 | -1.16 |
| causa_dominante | meteorologia | 9 | 1 | 11.11 | 4.00 | -2.20 |
| hay_meteorologia | 1 | 11 | 1 | 9.09 | 4.00 | -2.69 |
| aviso_viento_tormenta | con aviso | 20 | 0 | 0.00 | 0.00 | -5.42 |

Falsas alarmas: 201 de 231 días en alerta.
Racha medida frente a la prevista (mediana): 0.87 en las falsas alarmas y 0.89 en los aciertos (medias 0.89 y 0.89; valor test -0.28).

| variable | v_test | media_clase | media_resto | auc |
| --- | --- | --- | --- | --- |
| dia_humedad_media | 1.39 | 82.04 | 78.93 | 0.56 |
| hist_incidencias_365d | 0.70 | 18.68 | 17.90 | 0.56 |
| prev_racha_max | -1.24 | 84.93 | 92.85 | 0.44 |
| dia_lluvia_max | -1.36 | 40.66 | 47.43 | 0.41 |
| ayer_racha_max | -1.44 | 63.58 | 70.72 | 0.44 |
| dia_racha_max | -1.59 | 73.50 | 82.03 | 0.43 |
| dia_temp_media_min | -1.83 | 9.52 | 11.01 | 0.42 |
| prev_lluvia_max | -1.89 | 30.88 | 39.48 | 0.44 |
| dia_temp_max | -2.00 | 17.82 | 19.86 | 0.40 |
| prev_ext_racha_clima_max | -2.15 | 1.26 | 1.41 | 0.40 |
| ext_racha_clima_max | -2.16 | 1.10 | 1.22 | 0.43 |

| variable | modalidad | dias | dias_averia | cla_mod | mod_cla | v_test |
| --- | --- | --- | --- | --- | --- | --- |
| aviso_viento_tormenta | sin aviso | 141 | 131 | 92.91 | 65.17 | 3.24 |
| estacion | primavera | 95 | 87 | 91.58 | 43.28 | 1.72 |
| racha_llego | no llegó | 100 | 89 | 89.00 | 44.28 | 0.77 |
| estacion | invierno | 65 | 58 | 89.23 | 28.86 | 0.60 |
| racha_llego | llegó | 131 | 112 | 85.50 | 55.72 | -0.77 |
| estacion | otoño | 51 | 42 | 82.35 | 20.90 | -1.09 |
| estacion | verano | 20 | 14 | 70.00 | 6.97 | -2.08 |
| aviso_viento_tormenta | con aviso | 90 | 70 | 77.78 | 34.83 | -3.24 |

## Reanálisis

3348 días de red, 143 con avería, 451 en alerta.

Fallos: 64 días con avería en nivel bajo frente a 46 en alerta; 0 con aviso de viento o tormenta, 47 solo de equipos o de causa desconocida; racha media 45 km/h frente a 84. La frase de la memoria se sostiene.

| variable | v_test | media_clase | media_resto | auc |
| --- | --- | --- | --- | --- |
| dia_temp_max | 4.52 | 27.38 | 19.97 | 0.75 |
| dia_temp_media_min | 1.87 | 13.62 | 11.48 | 0.63 |
| hist_incidencias_365d | 0.79 | 19.91 | 18.89 | 0.54 |
| ayer_racha_max | -5.29 | 45.71 | 72.38 | 0.21 |
| dia_racha_max | -6.43 | 45.08 | 83.56 | 0.13 |
| dia_humedad_media | -6.62 | 54.01 | 80.23 | 0.13 |
| ext_racha_clima_max | -7.05 | 0.72 | 1.22 | 0.10 |
| dia_lluvia_max | -7.70 | 0.74 | 56.97 | 0.00 |

| variable | modalidad | dias | dias_averia | cla_mod | mod_cla | v_test |
| --- | --- | --- | --- | --- | --- | --- |
| aviso_viento_tormenta | sin aviso | 80 | 64 | 80.00 | 100.00 | 8.01 |
| estacion | verano | 31 | 25 | 80.65 | 39.06 | 3.01 |
| solo_equipo_o_desconocida | 1 | 70 | 47 | 67.14 | 73.44 | 2.47 |
| causa_dominante | desconocida | 33 | 25 | 75.76 | 39.06 | 2.43 |
| hay_desconocida | 1 | 34 | 25 | 73.53 | 39.06 | 2.16 |
| equipo_o_desconocida | 1 | 73 | 47 | 64.38 | 73.44 | 1.81 |
| causa_dominante | ninguna | 7 | 6 | 85.71 | 9.38 | 1.45 |
| hay_terceros | 1 | 5 | 4 | 80.00 | 6.25 | 0.91 |
| causa_dominante | terceros | 5 | 4 | 80.00 | 6.25 | 0.91 |
| estacion | primavera | 29 | 17 | 58.62 | 26.56 | 0.05 |
| hay_equipo | 1 | 39 | 22 | 56.41 | 34.38 | -0.28 |
| causa_dominante | equipo | 39 | 22 | 56.41 | 34.38 | -0.28 |
| causa_dominante | fauna | 10 | 5 | 50.00 | 7.81 | -0.53 |
| estacion | invierno | 20 | 10 | 50.00 | 15.62 | -0.80 |
| hay_vegetacion | 1 | 1 | 0 | 0.00 | 0.00 | -0.81 |
| causa_dominante | vegetacion | 1 | 0 | 0.00 | 0.00 | -0.81 |
| hay_fauna | 1 | 11 | 5 | 45.45 | 7.81 | -0.86 |
| estacion | otoño | 30 | 12 | 40.00 | 18.75 | -2.31 |
| causa_dominante | meteorologia | 15 | 2 | 13.33 | 3.12 | -3.72 |
| hay_meteorologia | 1 | 18 | 2 | 11.11 | 3.12 | -4.39 |
| aviso_viento_tormenta | con aviso | 30 | 0 | 0.00 | 0.00 | -8.01 |

Falsas alarmas: 405 de 451 días en alerta.

| variable | v_test | media_clase | media_resto | auc |
| --- | --- | --- | --- | --- |
| dia_humedad_media | 0.72 | 81.53 | 80.23 | 0.53 |
| hist_incidencias_365d | 0.09 | 18.99 | 18.89 | 0.49 |
| dia_temp_max | -1.78 | 18.56 | 19.97 | 0.42 |
| dia_temp_media_min | -2.01 | 10.20 | 11.48 | 0.42 |
| ayer_racha_max | -2.63 | 61.91 | 72.38 | 0.40 |
| dia_racha_max | -3.08 | 70.71 | 83.56 | 0.39 |
| dia_lluvia_max | -3.50 | 41.46 | 56.97 | 0.38 |
| ext_racha_clima_max | -3.68 | 1.08 | 1.22 | 0.40 |

| variable | modalidad | dias | dias_averia | cla_mod | mod_cla | v_test |
| --- | --- | --- | --- | --- | --- | --- |
| aviso_viento_tormenta | sin aviso | 310 | 294 | 94.84 | 72.59 | 4.97 |
| estacion | primavera | 162 | 150 | 92.59 | 37.04 | 1.46 |
| estacion | invierno | 119 | 109 | 91.60 | 26.91 | 0.73 |
| estacion | verano | 37 | 31 | 83.78 | 7.65 | -1.20 |
| estacion | otoño | 133 | 115 | 86.47 | 28.40 | -1.47 |
| aviso_viento_tormenta | con aviso | 141 | 111 | 78.72 | 27.41 | -4.97 |
