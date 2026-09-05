const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const revealElements = document.querySelectorAll(".reveal");
if (reducedMotion || !("IntersectionObserver" in window)) {
  revealElements.forEach((element) => element.classList.add("is-visible"));
} else {
  const revealObserver = new IntersectionObserver(
    (entries, observer) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add("is-visible");
        observer.unobserve(entry.target);
      });
    },
    { rootMargin: "0px 0px -8%", threshold: 0.06 },
  );
  revealElements.forEach((element) => revealObserver.observe(element));
}

const siteHeader = document.querySelector(".site-header");
const hero = document.querySelector("#top");
if (siteHeader && hero && "IntersectionObserver" in window) {
  const headerObserver = new IntersectionObserver(
    ([entry]) => siteHeader.classList.toggle("is-scrolled", !entry.isIntersecting),
    { rootMargin: "-74px 0px 0px", threshold: 0 },
  );
  headerObserver.observe(hero);
}

const sectionLinks = [...document.querySelectorAll('.desktop-nav a[href^="#"]')];
const observedSections = sectionLinks
  .map((link) => document.querySelector(link.getAttribute("href")))
  .filter(Boolean);

if (observedSections.length && "IntersectionObserver" in window) {
  const sectionObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        sectionLinks.forEach((link) => {
          const active = link.getAttribute("href") === `#${entry.target.id}`;
          link.classList.toggle("is-active", active);
          if (active) link.setAttribute("aria-current", "location");
          else link.removeAttribute("aria-current");
        });
      });
    },
    { rootMargin: "-30% 0px -58%", threshold: 0 },
  );
  observedSections.forEach((section) => sectionObserver.observe(section));
}

const menuButton = document.querySelector(".menu-toggle");
const mobileMenu = document.querySelector("#mobile-menu");
const mobileLinks = [...(mobileMenu?.querySelectorAll("a") ?? [])];
let menuFocusTimer;

function setMenu(open) {
  if (!menuButton || !mobileMenu) return;
  if (menuFocusTimer !== undefined) {
    window.clearTimeout(menuFocusTimer);
    menuFocusTimer = undefined;
  }
  menuButton.setAttribute("aria-expanded", String(open));
  menuButton.setAttribute("aria-label", open ? "Close navigation" : "Open navigation");
  mobileMenu.setAttribute("aria-hidden", String(!open));
  mobileMenu.toggleAttribute("inert", !open);
  mobileMenu.classList.toggle("is-open", open);
  document.body.style.overflow = open ? "hidden" : "";
  if (open) {
    menuFocusTimer = window.setTimeout(() => {
      menuFocusTimer = undefined;
      if (menuButton.getAttribute("aria-expanded") === "true") mobileLinks[0]?.focus();
    }, reducedMotion ? 0 : 220);
  }
}

menuButton?.addEventListener("click", () => {
  setMenu(menuButton.getAttribute("aria-expanded") !== "true");
});

mobileLinks.forEach((link) => link.addEventListener("click", () => setMenu(false)));

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && menuButton?.getAttribute("aria-expanded") === "true") {
    setMenu(false);
    menuButton.focus();
  }
});

window.matchMedia("(min-width: 941px)").addEventListener("change", (event) => {
  if (event.matches) setMenu(false);
});

const copyStatus = document.querySelector("#copy-status");
const copyTimers = new WeakMap();

async function copyText(button) {
  const target = document.getElementById(button.dataset.copy);
  if (!target) return;

  const text = target.innerText;
  const original = button.textContent;
  const currentTimer = copyTimers.get(button);
  if (currentTimer !== undefined) window.clearTimeout(currentTimer);
  try {
    await navigator.clipboard.writeText(text);
    button.textContent = "Copied";
    button.classList.add("is-copied");
    if (copyStatus) copyStatus.textContent = "Copied to clipboard";
  } catch {
    const selection = window.getSelection();
    const range = document.createRange();
    range.selectNodeContents(target);
    selection.removeAllRanges();
    selection.addRange(range);
    button.textContent = "Selected";
    if (copyStatus) copyStatus.textContent = "Code selected for copying";
  }

  const timer = window.setTimeout(() => {
    button.textContent = original;
    button.classList.remove("is-copied");
    copyTimers.delete(button);
  }, 1700);
  copyTimers.set(button, timer);
}

document.querySelectorAll(".copy-button").forEach((button) => {
  button.addEventListener("click", () => copyText(button));
});

const year = document.getElementById("year");
if (year) year.textContent = String(new Date().getFullYear());
