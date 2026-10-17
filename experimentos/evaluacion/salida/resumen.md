# Calibración y valor de la probabilidad de la víspera

Tabla de días del caso de negocio (dias.csv). Intervalos del 95 % con 1000 réplicas bootstrap por fechas (1d) y por bloques de siete días (7d). La muestra prevision es la previsión real de ICON-EU; reanalisis, el tiempo del día sacado del reanálisis (una previsión perfecta).

## Calibración

| muestra | redes | dias | dias_con_averia | tasa | prob_media | brier | bss | bss_ic1d | bss_ic7d | pendiente | pendiente_ic1d | pendiente_ic7d | intercepto | intercepto_ic7d |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prevision | Las tres redes | 2766 | 73 | 0,026 | 0,034 | 0,024 | 0,052 | 0,009 a 0,088 | 0,003 a 0,096 | 1,273 | 1,026 a 1,543 | 1,026 a 1,549 | -0,282 | -0,572 a -0,043 |
| prevision | Distribuidora 1 | 922 | 42 | 0,046 | 0,047 | 0,041 | 0,062 | 0,001 a 0,117 | -0,006 a 0,129 | 1,131 | 0,814 a 1,465 | 0,786 a 1,481 | -0,038 | -0,420 a 0,255 |
| prevision | Distribuidora 2 | 922 | 31 | 0,034 | 0,035 | 0,031 | 0,034 | -0,009 a 0,069 | -0,008 a 0,064 | 1,170 | 0,523 a 1,738 | 0,574 a 1,701 | -0,056 | -0,553 a 0,298 |
| reanalisis | Las tres redes | 5022 | 144 | 0,029 | 0,033 | 0,027 | 0,042 | 0,020 a 0,067 | 0,018 a 0,065 | 1,245 | 1,043 a 1,452 | 1,059 a 1,435 | -0,144 | -0,325 a 0,026 |
| reanalisis | Distribuidora 1 | 1674 | 83 | 0,050 | 0,044 | 0,045 | 0,040 | 0,011 a 0,068 | 0,009 a 0,071 | 1,059 | 0,772 a 1,347 | 0,752 a 1,341 | 0,136 | -0,107 a 0,363 |
| reanalisis | Distribuidora 2 | 1674 | 60 | 0,036 | 0,035 | 0,034 | 0,027 | -0,002 a 0,059 | -0,003 a 0,054 | 0,949 | 0,445 a 1,345 | 0,418 a 1,340 | 0,010 | -0,277 a 0,237 |
| prevision_sin_28_01 | Las tres redes | 2763 | 71 | 0,026 | 0,034 | 0,024 | 0,045 | 0,002 a 0,082 | -0,002 a 0,085 | 1,239 | 0,999 a 1,502 | 0,991 a 1,527 | -0,305 | -0,590 a -0,071 |
| prevision_sin_28_01 | Distribuidora 1 | 921 | 41 | 0,045 | 0,047 | 0,040 | 0,057 | -0,008 a 0,117 | -0,010 a 0,125 | 1,102 | 0,777 a 1,438 | 0,768 a 1,462 | -0,060 | -0,432 a 0,226 |
| prevision_sin_28_01 | Distribuidora 2 | 921 | 30 | 0,033 | 0,035 | 0,031 | 0,024 | -0,014 a 0,058 | -0,017 a 0,054 | 1,078 | 0,456 a 1,663 | 0,443 a 1,638 | -0,084 | -0,564 a 0,268 |

## Fiabilidad por tramos y por niveles (previsión real, las tres redes)

| tramo | dias | dias_con_averia | prob_media | tasa | tasa_ic_bajo | tasa_ic_alto |
|---|---|---|---|---|---|---|
| 0,00 a 0,02 | 882 | 4 | 0,015 | 0,005 | 0,002 | 0,012 |
| 0,02 a 0,04 | 1382 | 23 | 0,028 | 0,017 | 0,011 | 0,025 |
| 0,04 a 0,06 | 245 | 14 | 0,049 | 0,057 | 0,034 | 0,094 |
| 0,06 a 0,10 | 149 | 13 | 0,075 | 0,087 | 0,052 | 0,144 |
| 0,10 a 0,15 | 52 | 6 | 0,123 | 0,115 | 0,054 | 0,230 |
| 0,15 a 0,25 | 40 | 9 | 0,190 | 0,225 | 0,123 | 0,375 |
| 0,25 a 1,00 | 16 | 4 | 0,336 | 0,250 | 0,102 | 0,495 |
| nivel bajo | 1948 | 25 | 0,023 | 0,013 | 0,009 | 0,019 |
| nivel medio | 476 | 18 | 0,036 | 0,038 | 0,024 | 0,059 |
| nivel alto | 154 | 8 | 0,060 | 0,052 | 0,027 | 0,099 |
| nivel muy alto | 188 | 22 | 0,128 | 0,117 | 0,079 | 0,171 |

## Valor relativo en los cocientes de referencia

El valor relativo es la parte del ahorro de una previsión perfecta que consigue cada regla: 1 es perfecta, 0 es lo mismo que decidir sin previsión (siempre o nunca, lo que salga más barato) y por debajo de 0 sale más caro que eso.

| muestra | perdida | alfa_o_umbral | regla | bloque_dias | valor | ic | prob_positivo |
|---|---|---|---|---|---|---|---|
| prevision | dias | 0,05 | alerta | 1 | 0,186 | 0,05 a 0,31 | 1,00 |
| prevision | dias | 0,05 | muy_alto | 1 | 0,182 | 0,07 a 0,30 | 1,00 |
| prevision | dias | 0,05 | medio_o_mas | 1 | 0,102 | -0,11 a 0,25 | 0,85 |
| prevision | dias | 0,05 | probabilidad | 1 | 0,287 | 0,15 a 0,41 | 1,00 |
| prevision | dias | 0,10 | alerta | 1 | -0,064 | -0,25 a 0,08 | 0,20 |
| prevision | dias | 0,10 | muy_alto | 1 | 0,049 | -0,09 a 0,18 | 0,76 |
| prevision | dias | 0,10 | medio_o_mas | 1 | -0,514 | -0,92 a -0,27 | 0,00 |
| prevision | dias | 0,10 | probabilidad | 1 | 0,125 | 0,02 a 0,24 | 0,99 |
| prevision | dias | 0,20 | alerta | 1 | -0,658 | -1,01 a -0,40 | 0,00 |
| prevision | dias | 0,20 | muy_alto | 1 | -0,267 | -0,48 a -0,10 | 0,00 |
| prevision | dias | 0,20 | medio_o_mas | 1 | -1,979 | -2,84 a -1,44 | 0,00 |
| prevision | dias | 0,20 | probabilidad | 1 | 0,031 | -0,05 a 0,11 | 0,77 |
| prevision | horas_cliente | 0,05 | alerta | 1 | 0,483 | -0,32 a 0,76 | 0,88 |
| prevision | horas_cliente | 0,05 | muy_alto | 1 | 0,363 | -0,12 a 0,73 | 0,80 |
| prevision | horas_cliente | 0,05 | medio_o_mas | 1 | 0,227 | -1,12 a 0,60 | 0,70 |
| prevision | horas_cliente | 0,05 | probabilidad | 1 | 0,514 | -0,24 a 0,77 | 0,89 |
| prevision | horas_cliente | 0,10 | alerta | 1 | 0,248 | -1,22 a 0,63 | 0,71 |
| prevision | horas_cliente | 0,10 | muy_alto | 1 | 0,238 | -0,57 a 0,65 | 0,63 |
| prevision | horas_cliente | 0,10 | medio_o_mas | 1 | -0,376 | -3,35 a 0,32 | 0,18 |
| prevision | horas_cliente | 0,10 | probabilidad | 1 | 0,357 | -0,18 a 0,73 | 0,75 |
| prevision | horas_cliente | 0,20 | alerta | 1 | -0,283 | -3,72 a 0,39 | 0,23 |
| prevision | horas_cliente | 0,20 | muy_alto | 1 | -0,047 | -1,87 a 0,49 | 0,33 |
| prevision | horas_cliente | 0,20 | medio_o_mas | 1 | -1,717 | -9,70 a -0,32 | 0,00 |
| prevision | horas_cliente | 0,20 | probabilidad | 1 | 0,197 | -0,24 a 0,43 | 0,68 |
| prevision | dias | 0,05 | alerta | 7 | 0,186 | 0,03 a 0,31 | 0,99 |
| prevision | dias | 0,05 | muy_alto | 7 | 0,182 | 0,05 a 0,31 | 1,00 |
| prevision | dias | 0,05 | medio_o_mas | 7 | 0,102 | -0,13 a 0,27 | 0,84 |
| prevision | dias | 0,05 | probabilidad | 7 | 0,287 | 0,14 a 0,40 | 1,00 |
| prevision | dias | 0,10 | alerta | 7 | -0,064 | -0,27 a 0,10 | 0,24 |
| prevision | dias | 0,10 | muy_alto | 7 | 0,049 | -0,12 a 0,20 | 0,74 |
| prevision | dias | 0,10 | medio_o_mas | 7 | -0,514 | -0,93 a -0,22 | 0,00 |
| prevision | dias | 0,10 | probabilidad | 7 | 0,125 | 0,01 a 0,23 | 0,98 |
| prevision | dias | 0,20 | alerta | 7 | -0,658 | -1,04 a -0,35 | 0,00 |
| prevision | dias | 0,20 | muy_alto | 7 | -0,267 | -0,53 a -0,06 | 0,01 |
| prevision | dias | 0,20 | medio_o_mas | 7 | -1,979 | -2,86 a -1,35 | 0,00 |
| prevision | dias | 0,20 | probabilidad | 7 | 0,031 | -0,06 a 0,12 | 0,74 |
| prevision | horas_cliente | 0,05 | alerta | 7 | 0,483 | -0,32 a 0,73 | 0,89 |
| prevision | horas_cliente | 0,05 | muy_alto | 7 | 0,363 | -0,12 a 0,70 | 0,83 |
| prevision | horas_cliente | 0,05 | medio_o_mas | 7 | 0,227 | -1,22 a 0,57 | 0,70 |
| prevision | horas_cliente | 0,05 | probabilidad | 7 | 0,514 | -0,23 a 0,74 | 0,91 |
| prevision | horas_cliente | 0,10 | alerta | 7 | 0,248 | -1,28 a 0,62 | 0,71 |
| prevision | horas_cliente | 0,10 | muy_alto | 7 | 0,238 | -0,54 a 0,63 | 0,65 |
| prevision | horas_cliente | 0,10 | medio_o_mas | 7 | -0,376 | -3,56 a 0,31 | 0,17 |
| prevision | horas_cliente | 0,10 | probabilidad | 7 | 0,357 | -0,18 a 0,71 | 0,78 |
| prevision | horas_cliente | 0,20 | alerta | 7 | -0,283 | -3,94 a 0,37 | 0,22 |
| prevision | horas_cliente | 0,20 | muy_alto | 7 | -0,047 | -1,80 a 0,47 | 0,36 |
| prevision | horas_cliente | 0,20 | medio_o_mas | 7 | -1,717 | -10,35 a -0,29 | 0,00 |
| prevision | horas_cliente | 0,20 | probabilidad | 7 | 0,197 | -0,21 a 0,42 | 0,71 |
| prevision_sin_28_01 | dias | 0,05 | alerta | 1 | 0,164 | 0,02 a 0,28 | 0,99 |
| prevision_sin_28_01 | dias | 0,05 | muy_alto | 1 | 0,159 | 0,04 a 0,26 | 1,00 |
| prevision_sin_28_01 | dias | 0,05 | medio_o_mas | 1 | 0,078 | -0,13 a 0,23 | 0,80 |
| prevision_sin_28_01 | dias | 0,05 | probabilidad | 1 | 0,268 | 0,13 a 0,39 | 1,00 |
| prevision_sin_28_01 | dias | 0,10 | alerta | 1 | -0,092 | -0,29 a 0,05 | 0,11 |
| prevision_sin_28_01 | dias | 0,10 | muy_alto | 1 | 0,023 | -0,11 a 0,13 | 0,64 |
| prevision_sin_28_01 | dias | 0,10 | medio_o_mas | 1 | -0,556 | -0,94 a -0,30 | 0,00 |
| prevision_sin_28_01 | dias | 0,10 | probabilidad | 1 | 0,102 | -0,00 a 0,20 | 0,97 |
| prevision_sin_28_01 | dias | 0,20 | alerta | 1 | -0,701 | -1,06 a -0,45 | 0,00 |
| prevision_sin_28_01 | dias | 0,20 | muy_alto | 1 | -0,299 | -0,52 a -0,14 | 0,00 |
| prevision_sin_28_01 | dias | 0,20 | medio_o_mas | 1 | -2,060 | -2,86 a -1,51 | 0,00 |
| prevision_sin_28_01 | dias | 0,20 | probabilidad | 1 | 0,018 | -0,06 a 0,10 | 0,64 |
| prevision_sin_28_01 | horas_cliente | 0,05 | alerta | 1 | 0,310 | -0,17 a 0,63 | 0,83 |
| prevision_sin_28_01 | horas_cliente | 0,05 | muy_alto | 1 | 0,070 | -0,07 a 0,23 | 0,84 |
| prevision_sin_28_01 | horas_cliente | 0,05 | medio_o_mas | 1 | 0,089 | -0,69 a 0,48 | 0,60 |
| prevision_sin_28_01 | horas_cliente | 0,05 | probabilidad | 1 | 0,364 | -0,07 a 0,67 | 0,92 |
| prevision_sin_28_01 | horas_cliente | 0,10 | alerta | 1 | 0,062 | -0,79 a 0,48 | 0,57 |
| prevision_sin_28_01 | horas_cliente | 0,10 | muy_alto | 1 | -0,070 | -0,35 a 0,06 | 0,14 |
| prevision_sin_28_01 | horas_cliente | 0,10 | medio_o_mas | 1 | -0,538 | -2,25 a 0,13 | 0,07 |
| prevision_sin_28_01 | horas_cliente | 0,10 | probabilidad | 1 | 0,053 | -0,10 a 0,21 | 0,76 |
| prevision_sin_28_01 | horas_cliente | 0,20 | alerta | 1 | -0,503 | -2,50 a 0,15 | 0,08 |
| prevision_sin_28_01 | horas_cliente | 0,20 | muy_alto | 1 | -0,387 | -1,20 a -0,17 | 0,00 |
| prevision_sin_28_01 | horas_cliente | 0,20 | medio_o_mas | 1 | -1,955 | -6,47 a -0,63 | 0,00 |
| prevision_sin_28_01 | horas_cliente | 0,20 | probabilidad | 1 | -0,013 | -0,15 a 0,11 | 0,32 |
| prevision_sin_28_01 | dias | 0,05 | alerta | 7 | 0,164 | 0,03 a 0,28 | 0,99 |
| prevision_sin_28_01 | dias | 0,05 | muy_alto | 7 | 0,159 | 0,03 a 0,28 | 1,00 |
| prevision_sin_28_01 | dias | 0,05 | medio_o_mas | 7 | 0,078 | -0,15 a 0,25 | 0,79 |
| prevision_sin_28_01 | dias | 0,05 | probabilidad | 7 | 0,268 | 0,14 a 0,38 | 1,00 |
| prevision_sin_28_01 | dias | 0,10 | alerta | 7 | -0,092 | -0,29 a 0,07 | 0,14 |
| prevision_sin_28_01 | dias | 0,10 | muy_alto | 7 | 0,023 | -0,13 a 0,16 | 0,63 |
| prevision_sin_28_01 | dias | 0,10 | medio_o_mas | 7 | -0,556 | -0,97 a -0,26 | 0,00 |
| prevision_sin_28_01 | dias | 0,10 | probabilidad | 7 | 0,102 | -0,00 a 0,19 | 0,97 |
| prevision_sin_28_01 | dias | 0,20 | alerta | 7 | -0,701 | -1,08 a -0,41 | 0,00 |
| prevision_sin_28_01 | dias | 0,20 | muy_alto | 7 | -0,299 | -0,55 a -0,10 | 0,00 |
| prevision_sin_28_01 | dias | 0,20 | medio_o_mas | 7 | -2,060 | -2,92 a -1,43 | 0,00 |
| prevision_sin_28_01 | dias | 0,20 | probabilidad | 7 | 0,018 | -0,07 a 0,10 | 0,66 |
| prevision_sin_28_01 | horas_cliente | 0,05 | alerta | 7 | 0,310 | -0,19 a 0,61 | 0,84 |
| prevision_sin_28_01 | horas_cliente | 0,05 | muy_alto | 7 | 0,070 | -0,06 a 0,27 | 0,86 |
| prevision_sin_28_01 | horas_cliente | 0,05 | medio_o_mas | 7 | 0,089 | -0,75 a 0,48 | 0,58 |
| prevision_sin_28_01 | horas_cliente | 0,05 | probabilidad | 7 | 0,364 | -0,08 a 0,66 | 0,93 |
| prevision_sin_28_01 | horas_cliente | 0,10 | alerta | 7 | 0,062 | -0,87 a 0,47 | 0,56 |
| prevision_sin_28_01 | horas_cliente | 0,10 | muy_alto | 7 | -0,070 | -0,38 a 0,09 | 0,19 |
| prevision_sin_28_01 | horas_cliente | 0,10 | medio_o_mas | 7 | -0,538 | -2,44 a 0,15 | 0,08 |
| prevision_sin_28_01 | horas_cliente | 0,10 | probabilidad | 7 | 0,053 | -0,10 a 0,24 | 0,81 |
| prevision_sin_28_01 | horas_cliente | 0,20 | alerta | 7 | -0,503 | -2,67 a 0,16 | 0,10 |
| prevision_sin_28_01 | horas_cliente | 0,20 | muy_alto | 7 | -0,387 | -1,32 a -0,14 | 0,00 |
| prevision_sin_28_01 | horas_cliente | 0,20 | medio_o_mas | 7 | -1,955 | -6,95 a -0,57 | 0,00 |
| prevision_sin_28_01 | horas_cliente | 0,20 | probabilidad | 7 | -0,013 | -0,17 a 0,13 | 0,39 |

## Beneficio neto en los umbrales de referencia

| muestra | alfa_o_umbral | regla | bloque_dias | valor | ic | prob_positivo |
|---|---|---|---|---|---|---|
| prevision | 0,05 | alerta | 1 | 0,0049 | 0,0011 a 0,0090 | 1,00 |
| prevision | 0,05 | muy_alto | 1 | 0,0048 | 0,0016 a 0,0087 | 1,00 |
| prevision | 0,05 | medio_o_mas | 1 | 0,0027 | -0,0024 a 0,0077 | 0,85 |
| prevision | 0,05 | modelo | 1 | 0,0076 | 0,0032 a 0,0123 | 1,00 |
| prevision | 0,10 | alerta | 1 | -0,0017 | -0,0057 a 0,0025 | 0,20 |
| prevision | 0,10 | muy_alto | 1 | 0,0013 | -0,0019 a 0,0051 | 0,76 |
| prevision | 0,10 | medio_o_mas | 1 | -0,0136 | -0,0190 a -0,0083 | 0,00 |
| prevision | 0,10 | modelo | 1 | 0,0033 | 0,0004 a 0,0068 | 0,99 |
| prevision | 0,05 | alerta | 7 | 0,0049 | 0,0008 a 0,0102 | 0,99 |
| prevision | 0,05 | muy_alto | 7 | 0,0048 | 0,0010 a 0,0097 | 1,00 |
| prevision | 0,05 | medio_o_mas | 7 | 0,0027 | -0,0028 a 0,0089 | 0,84 |
| prevision | 0,05 | modelo | 7 | 0,0076 | 0,0031 a 0,0127 | 1,00 |
| prevision | 0,10 | alerta | 7 | -0,0017 | -0,0057 a 0,0033 | 0,24 |
| prevision | 0,10 | muy_alto | 7 | 0,0013 | -0,0024 a 0,0061 | 0,74 |
| prevision | 0,10 | medio_o_mas | 7 | -0,0136 | -0,0196 a -0,0071 | 0,00 |
| prevision | 0,10 | modelo | 7 | 0,0033 | 0,0002 a 0,0071 | 0,98 |
| prevision_sin_28_01 | 0,05 | alerta | 1 | 0,0042 | 0,0006 a 0,0079 | 0,99 |
| prevision_sin_28_01 | 0,05 | muy_alto | 1 | 0,0041 | 0,0010 a 0,0071 | 1,00 |
| prevision_sin_28_01 | 0,05 | medio_o_mas | 1 | 0,0020 | -0,0025 a 0,0069 | 0,80 |
| prevision_sin_28_01 | 0,05 | modelo | 1 | 0,0069 | 0,0029 a 0,0113 | 1,00 |
| prevision_sin_28_01 | 0,10 | alerta | 1 | -0,0024 | -0,0061 a 0,0015 | 0,11 |
| prevision_sin_28_01 | 0,10 | muy_alto | 1 | 0,0006 | -0,0026 a 0,0036 | 0,65 |
| prevision_sin_28_01 | 0,10 | medio_o_mas | 1 | -0,0143 | -0,0193 a -0,0092 | 0,00 |
| prevision_sin_28_01 | 0,10 | modelo | 1 | 0,0026 | -0,0001 a 0,0055 | 0,97 |
| prevision_sin_28_01 | 0,05 | alerta | 7 | 0,0042 | 0,0007 a 0,0088 | 0,99 |
| prevision_sin_28_01 | 0,05 | muy_alto | 7 | 0,0041 | 0,0008 a 0,0081 | 1,00 |
| prevision_sin_28_01 | 0,05 | medio_o_mas | 7 | 0,0020 | -0,0031 a 0,0077 | 0,79 |
| prevision_sin_28_01 | 0,05 | modelo | 7 | 0,0069 | 0,0029 a 0,0116 | 1,00 |
| prevision_sin_28_01 | 0,10 | alerta | 7 | -0,0024 | -0,0061 a 0,0020 | 0,15 |
| prevision_sin_28_01 | 0,10 | muy_alto | 7 | 0,0006 | -0,0028 a 0,0046 | 0,63 |
| prevision_sin_28_01 | 0,10 | medio_o_mas | 7 | -0,0143 | -0,0201 a -0,0082 | 0,00 |
| prevision_sin_28_01 | 0,10 | modelo | 7 | 0,0026 | -0,0000 a 0,0059 | 0,97 |

Con la pérdida por dias, la regla probabilística tiene valor positivo para cocientes entre 0,01 y 0,44; su máximo es 0,37 en 0,04.
Con la pérdida por horas-cliente, la regla probabilística tiene valor positivo para cocientes entre 0,01 y 0,35; su máximo es 0,53 en 0,06.

## Autocorrelación de los residuos

Residuo de cada día: si hubo avería (1 o 0) menos la probabilidad de la víspera, sumado entre redes para las tres juntas. r1 es la correlación de un día con el siguiente; q7 y q14, el contraste de Ljung y Box con 7 y 14 días de retardo, y p7 y p14 sus valores p. Con valores p altos, días seguidos no se parecen más de lo que toca y los intervalos por fechas y por bloques de siete días deben coincidir.

| muestra | redes | dias | r1 | q7 | p7 | q14 | p14 |
|---|---|---|---|---|---|---|---|
| prevision | Las tres redes | 922 | 0,104 | 19,0 | 0,008 | 28,5 | 0,012 |
| prevision | Distribuidora 1 | 922 | 0,079 | 21,1 | 0,004 | 24,6 | 0,039 |
| prevision | Distribuidora 2 | 922 | 0,118 | 17,8 | 0,013 | 26,9 | 0,020 |
| reanalisis | Las tres redes | 1674 | 0,051 | 7,0 | 0,427 | 11,0 | 0,683 |
| reanalisis | Distribuidora 1 | 1674 | 0,073 | 16,5 | 0,021 | 20,1 | 0,127 |
| reanalisis | Distribuidora 2 | 1674 | 0,058 | 11,5 | 0,118 | 13,6 | 0,482 |
| prevision_sin_28_01 | Las tres redes | 921 | 0,086 | 14,9 | 0,037 | 26,3 | 0,024 |
| prevision_sin_28_01 | Distribuidora 1 | 921 | 0,067 | 20,9 | 0,004 | 25,4 | 0,031 |
| prevision_sin_28_01 | Distribuidora 2 | 921 | 0,094 | 13,7 | 0,056 | 26,0 | 0,026 |
