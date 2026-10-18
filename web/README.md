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

## Cómo se publica (gridpredic.com)

La web vive en GitHub Pages y se publica sola: el workflow `.github/workflows/pages.yml` corre en cada push a `main` que toque `web/`, `dashboard/` o el propio workflow, monta el sitio con `web/montar_sitio.py` (esta página en la raíz, el cuadro de mando en `/demo/` con la copia de datos de `web/demo/data/`, `version.json` con el commit) y, después de desplegar, comprueba que `https://gridpredic.com/version.json` devuelve ese commit y que la página, la demo y el día repetido del temporal responden. Si la web no se ha actualizado, el último job falla y el commit de `main` aparece en rojo en GitHub: esa es la revisión que hay que mirar después de cada merge. Para verla en local antes de publicar:

```
python web/montar_sitio.py _site
python -m http.server 8795 --directory _site
```

Lo que está configurado una sola vez, fuera del repositorio:

- En GitHub, Settings > Pages: "Build and deployment > Source: GitHub Actions"; "Custom domain: gridpredic.com"; "Enforce HTTPS" cuando GitHub tenga el certificado (lo emite solo, hasta una hora después de que el DNS apunte bien).
- En DonDominio (donde está el dominio, su DNS y su correo), la zona DNS de gridpredic.com: cuatro registros A del dominio raíz a 185.199.108.153, 185.199.109.153, 185.199.110.153 y 185.199.111.153 (opcionales, los AAAA a 2606:50c0:8000::153, 8001::153, 8002::153 y 8003::153), y un CNAME de `www` a `antonpolenyaka.github.io`. El registro A que apuntaba al aparcamiento del registrador se quita. Los MX y el SPF del correo no se tocan.
- El buzón `hola@gridpredic.com` se crea en el correo de DonDominio (o una redirección a otro buzón).

## La demo (gridpredic.com/demo/)

Es el cuadro de mando tal cual (`dashboard/`, las mismas cuatro páginas y los mismos ficheros que sirve el portátil en el puerto 8086) con una copia congelada de sus datos en `web/demo/data/` (los JSON de cada tarde, los días archivados con el temporal del 28.01.2026 y los partes de guardia; unos 4 MB). `dashboard/demo.js` solo actúa fuera del portátil: quita los enlaces a Airflow y a las direcciones locales, pone el aviso de demo bajo el menú con la fecha de la copia y cambia el aviso de "riesgo viejo" por uno que explica que la copia está congelada. Para renovar la copia desde el portátil, con los datos que los jobs dejan en `dashboard\data`:

```powershell
.\web\congelar_demo.ps1
git add web/demo/data
```

y un commit en un PR; al mezclarlo en `main`, el workflow publica la copia nueva. El script se detiene si algún fichero lleva el nombre real de una distribuidora (`_runlogs\anonimizar_nombres.json`, que no se versiona).
