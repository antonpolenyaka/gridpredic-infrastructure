/* GridPredic: the motion of the dashboard pages, with GSAP and ScrollTrigger (vendor/gsap).

   Three effects, all of them quiet, none of them on the data the operator reads:
   - Reveal: every section of main appears once (opacity and a few pixels up) when it enters the view, and the
     figures of a section follow it with a short stagger. New sections (the pages render them from JSON) are
     picked up as they are added.
   - Parallax: the decorative layer of the page header ([data-parallax-layer]) drifts a little slower than the
     page. Only on screens of 1024 px or more, never on maps, charts or tables.
   - Scrub: the row with the map and the profile settles into place (scale 0.985 -> 1) tied to the scroll, so going
     back up undoes it. Subtle on purpose.
   Everything is skipped when the person prefers reduced motion, and the content is never hidden behind an
   animation: without GSAP, or with reduced motion, the page simply shows. */
(function () {
  "use strict";
  const reduce = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const ok = !reduce && window.gsap && window.ScrollTrigger;
  if (ok) gsap.registerPlugin(ScrollTrigger);
  const done = new WeakSet();

  function reveal(el) {
    if (!el || done.has(el)) return;
    done.add(el);
    if (!ok) { el.classList.add("visto"); return; }
    const figures = el.querySelectorAll(":scope > .kpis > .kpi, :scope > .cifras > .cifra");
    gsap.set(el, { autoAlpha: 0, y: 14 });
    if (figures.length) gsap.set(figures, { autoAlpha: 0, y: 10 });
    ScrollTrigger.create({
      trigger: el,
      start: "top 92%",
      once: true,
      onEnter: () => {
        gsap.to(el, { autoAlpha: 1, y: 0, duration: 0.55, ease: "power2.out", clearProps: "transform,opacity,visibility" });
        if (figures.length) gsap.to(figures, { autoAlpha: 1, y: 0, duration: 0.45, ease: "power2.out", stagger: 0.05, delay: 0.1, clearProps: "transform,opacity,visibility" });
      },
    });
    // A section already in the view when it is added (the first ones) must not wait for a scroll.
    ScrollTrigger.refresh();
  }

  function scrub(el) {
    if (!ok || !el || done.has(el) || window.innerWidth < 768) return;
    done.add(el);
    gsap.fromTo(el, { scale: 0.985, y: 10, transformOrigin: "50% 0%" }, {
      scale: 1, y: 0, ease: "none",
      scrollTrigger: { trigger: el, start: "top 90%", end: "top 55%", scrub: 0.5 },
    });
  }

  function parallax() {
    if (!ok) return;
    const layers = document.querySelectorAll("[data-parallax-layer]");
    if (!layers.length) return;
    const mm = gsap.matchMedia();
    mm.add("(min-width: 1024px)", () => {
      layers.forEach((layer) => {
        gsap.fromTo(layer, { yPercent: -6 }, {
          yPercent: 6, ease: "none",
          scrollTrigger: { trigger: layer.parentElement, start: "top top", end: "bottom top", scrub: 0.6 },
        });
      });
    });
  }

  function scan(root) {
    root.querySelectorAll("main > section, main > div > section").forEach(reveal);
    root.querySelectorAll(".dos.mapa-perfil").forEach(scrub);
  }

  function start() {
    parallax();
    scan(document);
    const main = document.querySelector("main");
    if (main && "MutationObserver" in window) {
      new MutationObserver(() => scan(document)).observe(main, { childList: true, subtree: true });
    }
    // Leaflet and the fonts change heights after the first paint: the triggers are measured again.
    window.addEventListener("load", () => ok && ScrollTrigger.refresh());
  }

  window.GPMotion = { reveal, scrub, scan, refresh: () => ok && ScrollTrigger.refresh() };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start); else start();
})();
