# Reglas frente a TF-IDF en las causas de los partes

563 partes de CT desde 2016 (518 textos distintos tras normalizar); 421 con código oficial y causa leída del texto; clases agrupadas en 'otras': 3. Las reglas cuadran con el código en 0.912 (dentro de muestra).

| bloque | metodo | particion | acierto | macro_f1 | cuadra_codigo |
| --- | --- | --- | --- | --- | --- |
| 1. combinaciones | recuento + Naive Bayes | 5 x 5 estratificada | 0.873 | 0.709 |  |
| 1. combinaciones | TF-IDF palabras + Naive Bayes | 5 x 5 estratificada | 0.832 | 0.562 |  |
| 1. combinaciones | TF-IDF palabras + logística | 5 x 5 estratificada | 0.895 | 0.775 |  |
| 1. combinaciones | TF-IDF palabras y caracteres + logística | 5 x 5 estratificada | 0.912 | 0.810 |  |
| 1. combinaciones | TF-IDF palabras y caracteres + SVM lineal | 5 x 5 estratificada | 0.920 | 0.815 |  |
| 2. partes repetidos | TF-IDF palabras y caracteres + logística | estratificada, 5 semillas, cada repetición entera | 0.914 | 0.816 | 0.865 |
| 2. partes repetidos | TF-IDF palabras y caracteres + logística | agrupando textos iguales, 5 semillas, cada repetición entera | 0.905 | 0.781 | 0.859 |
| 3. parecidos | parecido_k1 | 5 x 5 estratificada | 0.791 | 0.695 | 0.769 |
| 3. parecidos | parecidos_k5 | 5 x 5 estratificada | 0.799 | 0.669 | 0.775 |
| 3. parecidos | centroide | 5 x 5 estratificada | 0.864 | 0.770 | 0.822 |
| 3. parecidos | logistica | 5 x 5 estratificada | 0.912 | 0.810 | 0.863 |
| 3. parecidos | logistica_sin_instalacion | 5 x 5 estratificada | 0.920 | 0.824 | 0.874 |
| 3. parecidos | parecido_k1 | 5 x 5 agrupando textos iguales | 0.778 | 0.650 | 0.756 |
| 3. parecidos | parecidos_k5 | 5 x 5 agrupando textos iguales | 0.789 | 0.634 | 0.766 |
| 3. parecidos | centroide | 5 x 5 agrupando textos iguales | 0.858 | 0.737 | 0.813 |
| 3. parecidos | logistica | 5 x 5 agrupando textos iguales | 0.904 | 0.782 | 0.859 |
| 3. parecidos | logistica_sin_instalacion | 5 x 5 agrupando textos iguales | 0.905 | 0.780 | 0.862 |
| 4. código oficial | reglas | dentro de muestra |  |  | 0.912 |
| 4. código oficial | logística entrenada con el código | 5 estratificada | 0.812 | 0.815 |  |
| 5. curva de aprendizaje | TF-IDF palabras y caracteres + logística | 50 partes de entrenamiento, test fijo del 30 % | 0.752 | 0.456 |  |
| 5. curva de aprendizaje | TF-IDF palabras y caracteres + logística | 100 partes de entrenamiento, test fijo del 30 % | 0.804 | 0.542 |  |
| 5. curva de aprendizaje | TF-IDF palabras y caracteres + logística | 200 partes de entrenamiento, test fijo del 30 % | 0.854 | 0.667 |  |
| 5. curva de aprendizaje | TF-IDF palabras y caracteres + logística | 300 partes de entrenamiento, test fijo del 30 % | 0.888 | 0.734 |  |
| 5. curva de aprendizaje | TF-IDF palabras y caracteres + logística | 394 partes de entrenamiento, test fijo del 30 % | 0.911 | 0.786 |  |

Desacuerdos entre las reglas y la logística de la semilla 0 (causa del texto): 40, sin claves repetidas; con código oficial 40: la regla cuadra y la logística no 24; la logística cuadra y la regla no 3; ninguna 5.

Tiempo: 1787 s.
