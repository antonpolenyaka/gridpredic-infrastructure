# La página pública de GridPredic.com

Una sola página estática (`index.html`, `gridpredic-web.css`, `web.js`) que cuenta el producto a una distribuidora: qué hace cada tarde, lo que habría valido en 2022 - 2026, cómo funciona, el argumento económico, lo que no hace y cómo pedir la demo de un mes. Usa los mismos tokens de color y la misma tipografía que el cuadro de mando (IBM Plex Sans, servida desde `vendor/fonts`, licencia OFL) y no depende de ningún servicio externo: ni fuentes de Google, ni CDN, ni analítica.

## Abrirla en local

Desde la raíz del repositorio:

```powershell
python -m http.server 8795 --directory web
```

y abrir http://localhost:8795. Abrir `index.html` con doble clic también funciona (todo es relativo), pero algunos navegadores no cargan las fuentes desde `file://`.

## El movimiento

`web.js` usa GSAP y ScrollTrigger (`vendor/gsap`, licencia estándar sin cargo, válida para esta página) y hace tres cosas:

- La escena del inicio (`#red`, un SVG) es una historia corta movida por el scroll: las líneas de la red se dibujan y pasa el viento (la previsión de la víspera a las 20:30), los municipios toman el color de su riesgo (el día siguiente) y la furgoneta de la brigada va de su base a la base propuesta para mañana. En pantallas de más de 1024 px (el mismo corte que el CSS, donde el inicio deja de ir en dos columnas) el inicio queda fijado justo debajo del menú durante 1,6 pantallas y el scroll mueve la escena adelante y atrás; en pantallas de 1024 px o menos se reproduce una vez al cargar. Los tres pasos bajo la escena se iluminan al ritmo de la historia, y la caja de la etiqueta se ajusta al largo de su texto.
- Las tarjetas, los pasos y las cifras aparecen una vez al entrar en la vista (`[data-reveal]`, por lotes con un escalonado corto), y las cifras grandes cuentan hacia arriba (`data-cuenta` y `data-sufijo`). Un seguro enseña lo que haya quedado por encima de la vista tras un salto por ancla o una rueda muy rápida.
- Con "reducir movimiento" en el sistema, o si el script no carga, la página se muestra en su estado final: nada queda escondido detrás de una animación.

Lo que no hay, a propósito: paralaje sobre textos o figuras, secuencias de imágenes (la escena es un SVG de unos pocos KB) y efectos sobre las capturas del cuadro de mando.

## Antes de publicarla

- Las capturas de `img/` salen del cuadro de mando con los datos reales de las tres redes, que aparecen como Distribuidora 1, 2 y 3; los municipios y las líneas sí son los reales. Se regeneran con `experimentos/figuras/capturar.py` a la mitad de resolución y se renombran: `index_20260128_1_distribuidora_2.png` recortada es `img/riesgo_tormenta.png` y `negocio_la_guardia_a_igual_presupuesto_mas_cober.png` es `img/guardia.png`; `img/dias_archivados.png` sale del mismo script y la página no la usa ahora. Si cambia el tamaño de una captura, se cambian también `width` y `height` de su `<img>`, que reservan el hueco antes de que cargue.
- El contacto es `hola@gridpredic.com`; el buzón tiene que existir antes de publicar.
- Las cifras de la sección "Lo que habría pasado" son las de `docs/cifras.md` y `docs/caso-de-negocio.md` (ejecución `caso_v10_20261014_1543`): la previsión real de la víspera, del 20.01.2024 al 02.08.2026, en la Distribuidora 1 y la Distribuidora 2. Las de "Para la distribuidora" (el incentivo del -4 % al +4 %, los 12.600 euros del minuto de TIEPI en la Distribuidora 1, la licencia y la implantación, el artículo 15 de la Circular 8/2025 y la guardia a igual presupuesto) salen de la sección 3 de `docs/cifras.md` y del apartado 4 de `docs/caso-de-negocio.md`. Si se vuelve a ejecutar el caso de negocio, se actualizan aquí a mano.
- Publicar es copiar la carpeta a cualquier hosting estático (GitHub Pages, Cloudflare Pages, Netlify o el propio nginx del proyecto); no hay build.
