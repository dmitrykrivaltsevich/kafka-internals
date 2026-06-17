/* ============================================================================
   Apache Kafka Architecture Docs — client runtime (vanilla, offline-safe)
   ========================================================================== */
(function () {
  "use strict";

  /* ---- Page manifest (from assets/manifest.js — single source of truth) --- */
  var M = window.KAFKA_DOCS || { PAGES: {}, PARTS: [] };
  var PAGES = M.PAGES;
  var PARTS = M.PARTS;
  var FLAT = PARTS.reduce(function (a, p) {
    return p.groups.reduce(function (b, g) { return b.concat(g.items); }, a);
  }, []);
  var current = document.body.getAttribute("data-page") || "";

  /* ---- Theme ------------------------------------------------------------- */
  function applyTheme(t) {
    document.documentElement.setAttribute("data-theme", t);
    try { localStorage.setItem("kafka-docs-theme", t); } catch (e) {}
    var btn = document.getElementById("theme-toggle");
    if (btn) btn.textContent = t === "dark" ? "☀" : "☽"; // sun / moon
  }
  (function initTheme() {
    var saved;
    try { saved = localStorage.getItem("kafka-docs-theme"); } catch (e) {}
    if (!saved) saved = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    applyTheme(saved);
  })();

  /* ---- Sidebar nav ------------------------------------------------------- */
  function buildSidebar() {
    var sb = document.getElementById("sidebar");
    if (!sb) return;
    var filter = document.createElement("input");
    filter.className = "filter";
    filter.type = "search";
    filter.placeholder = "Filter chapters…";
    sb.appendChild(filter);

    PARTS.forEach(function (part) {
      var ph = document.createElement("div");
      ph.className = "nav-part";
      ph.textContent = part.title;
      sb.appendChild(ph);
      part.groups.forEach(function (g) {
        var box = document.createElement("div");
        box.className = "nav-group";
        var t = document.createElement("div");
        t.className = "nav-group-title";
        t.textContent = g.title;
        box.appendChild(t);
        g.items.forEach(function (slug) {
          var p = PAGES[slug];
          if (!p) return;
          var a = document.createElement("a");
          a.href = slug + ".html";
          a.setAttribute("data-slug", slug);
          a.setAttribute("data-text", (p.num + " " + p.title).toLowerCase());
          if (slug === current) a.className = "active";
          var num = document.createElement("span");
          num.className = "num";
          num.textContent = p.num;
          var lbl = document.createElement("span");
          lbl.textContent = p.title;
          a.appendChild(num);
          a.appendChild(lbl);
          box.appendChild(a);
        });
        sb.appendChild(box);
      });
    });

    // center the active item in the sidebar on load so navigating doesn't leave it scrolled off-screen
    var act = sb.querySelector("a.active");
    if (act) requestAnimationFrame(function () {
      var sr = sb.getBoundingClientRect(), ar = act.getBoundingClientRect();
      sb.scrollTop += (ar.top - sr.top) - (sb.clientHeight - act.offsetHeight) / 2;
    });

    filter.addEventListener("input", function () {
      var q = filter.value.trim().toLowerCase();
      var kids = Array.prototype.slice.call(sb.children);
      kids.forEach(function (el) {
        if (!el.classList.contains("nav-group")) return;
        var any = false;
        el.querySelectorAll("a").forEach(function (a) {
          var hit = !q || a.getAttribute("data-text").indexOf(q) !== -1;
          a.style.display = hit ? "" : "none";
          if (hit) any = true;
        });
        el.style.display = any ? "" : "none";
      });
      // hide a part header when all of its groups are filtered out
      var curPart = null, visible = false;
      function flush() { if (curPart) curPart.style.display = visible ? "" : "none"; }
      kids.forEach(function (el) {
        if (el.classList.contains("nav-part")) { flush(); curPart = el; visible = false; }
        else if (el.classList.contains("nav-group") && el.style.display !== "none") { visible = true; }
      });
      flush();
    });
  }

  /* ---- Heading anchors + right-hand TOC ---------------------------------- */
  function slugify(s) {
    return s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 60);
  }
  function buildToc() {
    var article = document.querySelector("article");
    var toc = document.getElementById("toc");
    if (!article) return;
    var heads = article.querySelectorAll("h2, h3");
    var seen = {};
    var links = [];
    heads.forEach(function (h) {
      if (!h.id) {
        var base = slugify(h.textContent || "section");
        var id = base, i = 2;
        while (seen[id] || document.getElementById(id)) { id = base + "-" + i++; }
        h.id = id;
      }
      seen[h.id] = true;
      // anchor link
      var anchor = document.createElement("a");
      anchor.className = "heading-anchor";
      anchor.href = "#" + h.id;
      anchor.textContent = "¶";
      anchor.setAttribute("aria-label", "Permalink");
      h.appendChild(anchor);

      if (toc) {
        var a = document.createElement("a");
        a.href = "#" + h.id;
        a.textContent = h.textContent.replace("¶", "");
        a.className = h.tagName === "H3" ? "h3" : "h2";
        a.setAttribute("data-target", h.id);
        toc.appendChild(a);
        links.push({ a: a, h: h });
      }
    });

    if (toc && links.length) {
      var title = document.createElement("div");
      title.className = "toc-title";
      title.textContent = "On this page";
      toc.insertBefore(title, toc.firstChild);

      // scroll-spy
      var spy = function () {
        var pos = window.scrollY + 90;
        var cur = links[0];
        for (var i = 0; i < links.length; i++) {
          if (links[i].h.offsetTop <= pos) cur = links[i];
        }
        links.forEach(function (l) { l.a.classList.toggle("active", l === cur); });
      };
      var ticking = false;
      window.addEventListener("scroll", function () {
        if (!ticking) { window.requestAnimationFrame(function () { spy(); ticking = false; }); ticking = true; }
      }, { passive: true });
      spy();
    } else if (toc) {
      toc.style.display = "none";
    }
  }

  /* ---- Copy buttons on code blocks --------------------------------------- */
  function addCopyButtons() {
    document.querySelectorAll("pre").forEach(function (pre) {
      if (pre.classList.contains("mermaid") || pre.querySelector("svg")) return;
      var btn = document.createElement("button");
      btn.className = "copy-btn";
      btn.type = "button";
      btn.textContent = "Copy";
      btn.addEventListener("click", function () {
        var text = pre.innerText.replace(/\s*Copy\s*$/, "");
        var done = function () {
          btn.textContent = "Copied";
          btn.classList.add("copied");
          setTimeout(function () { btn.textContent = "Copy"; btn.classList.remove("copied"); }, 1400);
        };
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(done, done);
        } else {
          var ta = document.createElement("textarea");
          ta.value = text; document.body.appendChild(ta); ta.select();
          try { document.execCommand("copy"); } catch (e) {}
          document.body.removeChild(ta); done();
        }
      });
      pre.appendChild(btn);
    });
  }

  /* ---- Prev / next ------------------------------------------------------- */
  function buildPageNav() {
    var article = document.querySelector("article");
    if (!article || !current) return;
    var idx = FLAT.indexOf(current);
    if (idx === -1) return;
    var nav = document.createElement("nav");
    nav.className = "page-nav";
    function link(slug, dir, cls) {
      var p = PAGES[slug];
      var a = document.createElement("a");
      a.href = slug + ".html";
      a.className = cls;
      a.innerHTML = '<div class="dir">' + dir + '</div><div class="ttl">' + p.title + "</div>";
      return a;
    }
    if (idx > 0) nav.appendChild(link(FLAT[idx - 1], "← Previous", "prev"));
    if (idx < FLAT.length - 1) nav.appendChild(link(FLAT[idx + 1], "Next →", "next"));
    if (nav.children.length) article.appendChild(nav);
  }

  /* ---- Mobile menu ------------------------------------------------------- */
  function wireChrome() {
    var toggle = document.getElementById("theme-toggle");
    if (toggle) toggle.addEventListener("click", function () {
      var t = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
      applyTheme(t);
    });
    var menu = document.getElementById("menu-toggle");
    var sb = document.querySelector(".sidebar");
    var scrim = document.querySelector(".scrim");
    function close() { if (sb) sb.classList.remove("open"); if (scrim) scrim.classList.remove("show"); }
    if (menu && sb) menu.addEventListener("click", function () {
      sb.classList.toggle("open");
      if (scrim) scrim.classList.toggle("show");
    });
    if (scrim) scrim.addEventListener("click", close);
    if (sb) sb.addEventListener("click", function (e) { if (e.target.tagName === "A") close(); });
  }

  /* ---- Boot -------------------------------------------------------------- */
  function boot() {
    buildSidebar();
    buildToc();
    addCopyButtons();
    buildPageNav();
    wireChrome();
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
