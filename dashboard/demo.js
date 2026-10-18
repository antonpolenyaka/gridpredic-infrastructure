/* GridPredic: the dashboard served as the public demo (gridpredic.com/demo/).

   The four pages are the same files the operation serves from the laptop (compose.yaml, port 8086) and read the
   same data/*.json; on the public site data/ is a frozen copy (web/demo/data, see web/README.md). This script runs
   only when the page is not served from the laptop (anything but localhost or 127.0.0.1) and does two things:
   - it removes the links to Airflow and the other local addresses, which mean nothing outside the laptop;
   - it puts a notice under the menu saying that this is the demo, which three networks it shows and when the copy
     of the data was made (the "generado" of data/riesgo.json), with the way back to the public page.
   Without the script, or on the laptop, the pages look as they always did. */
(function () {
  "use strict";
  var host = window.location.hostname;
  if (host === "localhost" || host === "127.0.0.1" || host === "" || host === "[::1]") return;

  function removeLocalLinks() {
    var links = document.querySelectorAll('a[href^="http://localhost"], a[href^="http://127.0.0.1"]');
    for (var i = 0; i < links.length; i++) links[i].parentNode.removeChild(links[i]);
    softenStale();
  }

  // index.html warns when the risk is older than today ("la ejecución de anoche no ha dejado un riesgo nuevo") and
  // sends the operator to Airflow; in the demo the copy is frozen on purpose, so the warning says that instead.
  function softenStale() {
    var box = document.getElementById("vejez");
    if (!box || box.getAttribute("data-demo") === "1" || box.textContent.indexOf("no ha dejado un riesgo nuevo") < 0) return;
    var chip = document.getElementById("fecha-chip");
    var day = chip ? chip.textContent.replace(/^[a-záéíóú]+\s+/i, "") : "";
    box.setAttribute("data-demo", "1");
    box.innerHTML = "<b>Demo</b>: este es el riesgo que el sistema dio la tarde anterior" + (day ? " al " + day : "")
      + " con la previsión y los avisos de aquella tarde; en la operación real se renueva cada día a las 20:30. "
      + 'Los días anteriores están en <a href="dias.html">Días archivados</a>.';
  }

  function notice(generated) {
    var nav = document.querySelector("nav.menu");
    if (!nav || document.getElementById("demo-aviso")) return;
    var box = document.createElement("div");
    box.id = "demo-aviso";
    box.className = "demo-aviso";
    var when = generated ? " Los datos son una copia del cuadro de mando hecha el " + generated + "; la operación real los renueva cada tarde a las 20:30." : "";
    box.innerHTML = "<b>Demo pública de GridPredic</b> con las tres redes con las que se ha construido (Distribuidora 1, 2 y 3): "
      + "el riesgo de mañana, los días archivados (entre ellos el temporal del 28.01.2026), el día a día desde 2022 y el negocio."
      + when + ' <a href="../">Volver a gridpredic.com</a>';
    nav.parentNode.insertBefore(box, nav.nextSibling);
  }

  function start() {
    removeLocalLinks();
    // Links added later by the pages (the notices of a stale file name Airflow) go too.
    var seen = new MutationObserver(removeLocalLinks);
    seen.observe(document.body, { childList: true, subtree: true });
    var request = new XMLHttpRequest();
    request.open("GET", "data/riesgo.json", true);
    request.onload = function () {
      var generated = null;
      try { generated = JSON.parse(request.responseText).generado || null; } catch (error) { generated = null; }
      notice(generated);
    };
    request.onerror = function () { notice(null); };
    request.send();
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
  else start();
})();
