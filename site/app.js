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
    { rootMargin: "0px 0px -10%", threshold: 0.08 },
  );
  revealElements.forEach((element) => revealObserver.observe(element));
}

const menuButton = document.querySelector(".menu-toggle");
const mobileMenu = document.querySelector("#mobile-menu");
const mobileLinks = mobileMenu?.querySelectorAll("a") ?? [];

function setMenu(open) {
  if (!menuButton || !mobileMenu) return;
  menuButton.setAttribute("aria-expanded", String(open));
  menuButton.setAttribute("aria-label", open ? "Close navigation" : "Open navigation");
  mobileMenu.setAttribute("aria-hidden", String(!open));
  mobileMenu.classList.toggle("is-open", open);
  document.body.style.overflow = open ? "hidden" : "";
  if (open) window.setTimeout(() => mobileLinks[0]?.focus(), reducedMotion ? 0 : 340);
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

async function copyText(button) {
  const target = document.getElementById(button.dataset.copy);
  if (!target) return;
  const text = target.innerText;
  const original = button.textContent;
  try {
    await navigator.clipboard.writeText(text);
    button.textContent = "Copied";
  } catch {
    const selection = window.getSelection();
    const range = document.createRange();
    range.selectNodeContents(target);
    selection.removeAllRanges();
    selection.addRange(range);
    button.textContent = "Select";
  }
  window.setTimeout(() => {
    button.textContent = original;
  }, 1600);
}

document.querySelectorAll(".copy-button").forEach((button) => {
  button.addEventListener("click", () => copyText(button));
});

const stageStatus = document.querySelector(".machine-stage");
const statusMessages = [
  "running build in isolated sandbox",
  "executing hermetic test suite",
  "recording signed-off artifacts",
  "preparing read-only review",
];

if (stageStatus && !reducedMotion) {
  let statusIndex = 0;
  window.setInterval(() => {
    stageStatus.classList.add("is-changing");
    window.setTimeout(() => {
      statusIndex = (statusIndex + 1) % statusMessages.length;
      stageStatus.textContent = statusMessages[statusIndex];
      stageStatus.classList.remove("is-changing");
    }, 300);
  }, 2600);
}

const year = document.getElementById("year");
if (year) year.textContent = String(new Date().getFullYear());
