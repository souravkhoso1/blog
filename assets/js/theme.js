(function () {
  var media = window.matchMedia("(prefers-color-scheme: light)");

  function getPreference() {
    return localStorage.getItem("theme") || "auto";
  }

  function resolveTheme(pref) {
    return pref === "auto" ? (media.matches ? "light" : "dark") : pref;
  }

  function applyTheme(pref) {
    document.documentElement.setAttribute("data-theme", resolveTheme(pref));
    document.documentElement.setAttribute("data-theme-pref", pref);
    document.querySelectorAll(".theme-btn").forEach(function (btn) {
      var active = btn.getAttribute("data-theme-choice") === pref;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  function setPreference(pref) {
    localStorage.setItem("theme", pref);
    applyTheme(pref);
  }

  document.addEventListener("DOMContentLoaded", function () {
    applyTheme(getPreference());
    document.querySelectorAll(".theme-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        setPreference(btn.getAttribute("data-theme-choice"));
      });
    });
  });

  media.addEventListener("change", function () {
    if (getPreference() === "auto") applyTheme("auto");
  });
})();
