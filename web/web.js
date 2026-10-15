/* GridPredic.com: the motion of the public page (GSAP and ScrollTrigger, served from vendor/gsap).

   - The scene of the hero is a short story told with the scroll: the lines of the network are drawn and the wind
     sweeps (the evening forecast), the towns take the colour of their risk (the morning's levels), and the crew
     van moves from its base to the middle of the towns at risk. On screens of 1024 px or more the hero is pinned
     and the story is scrubbed by the scroll (going back up undoes it); on smaller screens it plays once on load.
   - The cards, figures and steps appear once when they enter the view; the big figures count up.
   - With "reduce motion" everything is shown in its final state, without animation. Without the library, the
     page is simply static: nothing is hidden behind a script. */
(function () {
  "use strict";
  const reduce = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const ok = window.gsap && window.ScrollTrigger;
  const RISK = { bajo: "#2f7a4b", medio: "#d9a400", alto: "#d9641a", "muy-alto": "#bf2a2a" };
  const LABELS = ["20:30, la víspera: previsión y avisos", "Mañana: el riesgo por municipio y línea",
                  "La brigada, en el punto medio: 35 minutos, no 90"];

  function finalState() {
    document.querySelectorAll(".escena .nodo[data-riesgo]").forEach((n) => n.classList.add(n.dataset.riesgo));
    const label = document.querySelector(".escena .etiqueta");
    if (label) { label.setAttribute("opacity", "1"); label.querySelector("text").textContent = LABELS[2]; }
    const crew = document.querySelector(".escena .brigada");
    if (crew) {
      crew.setAttribute("opacity", "1");
      const path = document.getElementById("ruta");
      const p = path.getPointAtLength(path.getTotalLength());
      crew.querySelector(".furgon").setAttribute("transform", `translate(${p.x} ${p.y})`);
    }
    document.querySelectorAll(".pasos li").forEach((li) => li.classList.add("activo"));
  }

  if (!ok || reduce) { finalState(); return; }
  gsap.registerPlugin(ScrollTrigger);

  // ---- the scene
  function scene() {
    const svg = document.getElementById("red");
    if (!svg) return null;
    const lines = Array.from(svg.querySelectorAll(".linea"));
    lines.forEach((l) => { const n = l.getTotalLength(); l.style.strokeDasharray = n; l.style.strokeDashoffset = n; });
    const nodes = Array.from(svg.querySelectorAll(".nodo[data-riesgo]"));
    const gusts = svg.querySelector(".rachas");
    const crew = svg.querySelector(".brigada");
    const route = svg.getElementById("ruta");
    const van = svg.querySelector(".furgon");
    const label = svg.querySelector(".etiqueta");
    const labelText = label.querySelector("text");
    const steps = Array.from(document.querySelectorAll(".pasos li"));
    const routeLength = route.getTotalLength();
    route.style.strokeDasharray = routeLength; route.style.strokeDashoffset = routeLength;
    const along = { t: 0 };
    const setStep = (k) => steps.forEach((li, i) => li.classList.toggle("activo", i <= k));

    const tl = gsap.timeline({ defaults: { ease: "none" } });
    tl.set(label, { opacity: 1 }).call(() => { labelText.textContent = LABELS[0]; setStep(0); }, null, 0)
      .to(lines, { strokeDashoffset: 0, duration: 1.2, stagger: 0.08 }, 0)
      .fromTo(gusts, { x: -260, opacity: 0 }, { x: 320, opacity: 1, duration: 1.4 }, 0.3)
      .to(gusts, { opacity: 0, duration: 0.4 }, 1.7)
      .call(() => { labelText.textContent = LABELS[1]; setStep(1); }, null, 1.9)
      .to(nodes, { fill: (i, el) => RISK[el.dataset.riesgo], duration: 0.5, stagger: 0.07 }, 1.9)
      .call(() => { labelText.textContent = LABELS[2]; setStep(2); }, null, 2.9)
      .to(crew, { opacity: 1, duration: 0.3 }, 2.9)
      .to(route, { strokeDashoffset: 0, duration: 0.9 }, 3.0)
      .to(along, { t: 1, duration: 1.0, onUpdate: () => {
        const p = route.getPointAtLength(along.t * routeLength);
        van.setAttribute("transform", `translate(${p.x.toFixed(1)} ${p.y.toFixed(1)})`);
      } }, 3.1);
    // Before the story starts the van waits at its base.
    const start = route.getPointAtLength(0);
    van.setAttribute("transform", `translate(${start.x} ${start.y})`);
    return tl;
  }

  const tl = scene();
  if (tl) {
    const mm = gsap.matchMedia();
    mm.add("(min-width: 1024px)", () => {
      tl.pause();
      const st = ScrollTrigger.create({ trigger: ".hero", start: "top top", end: "+=160%", pin: true, scrub: 0.6, animation: tl, anticipatePin: 1 });
      return () => st.kill();
    });
    mm.add("(max-width: 1023px)", () => {
      tl.timeScale(1.4).play(0);
      return () => tl.pause();
    });
  }

  // ---- reveals and counters
  const revealed = gsap.utils.toArray("[data-reveal]");
  revealed.forEach((el) => gsap.set(el, { autoAlpha: 0, y: 18 }));
  ScrollTrigger.batch(revealed, {
    start: "top 90%", once: true,
    onEnter: (batch) => gsap.to(batch, { autoAlpha: 1, y: 0, duration: 0.6, ease: "power2.out", stagger: 0.08, clearProps: "transform,opacity,visibility" }),
  });
  // A jump of the scroll (a link to an anchor, a fast wheel) can skip a trigger: whatever is above the view is shown.
  let pending = null;
  window.addEventListener("scroll", () => {
    clearTimeout(pending);
    pending = setTimeout(() => {
      revealed.forEach((el) => {
        if (el.getBoundingClientRect().top < window.innerHeight && getComputedStyle(el).opacity !== "1") {
          gsap.to(el, { autoAlpha: 1, y: 0, duration: 0.4, clearProps: "transform,opacity,visibility" });
        }
      });
    }, 250);
  }, { passive: true });
  gsap.utils.toArray(".cifra .v[data-cuenta]").forEach((el) => {
    const target = Number(el.dataset.cuenta), suffix = el.dataset.sufijo || "";
    const counter = { n: 0 };
    ScrollTrigger.create({
      trigger: el, start: "top 88%", once: true,
      onEnter: () => gsap.to(counter, { n: target, duration: 1.1, ease: "power2.out", onUpdate: () => { el.textContent = Math.round(counter.n) + suffix; } }),
    });
  });
  window.addEventListener("load", () => ScrollTrigger.refresh());
})();
