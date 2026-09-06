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

/* The TUI frame player. The panels ship visible in the HTML, so the page reads as a stack of
   frames with no script at all; everything below is the enhancement that collapses that stack to
   one panel and gives it tabs. Nothing here fetches or generates a frame: the text on screen is
   the snapshot output the markup already carries. */
const tuiTabs = document.querySelector("#tui-tabs");
const tuiNav = document.querySelector("#tui-nav");
const tuiCount = document.querySelector("#tui-count");
const tuiTabButtons = [...(tuiTabs?.querySelectorAll('[role="tab"]') ?? [])];
const tuiPanels = tuiTabButtons.map((tab) =>
  document.getElementById(tab.getAttribute("aria-controls")),
);

function showFrame(index, { focusTab = false } = {}) {
  tuiTabButtons.forEach((tab, position) => {
    const active = position === index;
    const panel = tuiPanels[position];
    tab.setAttribute("aria-selected", String(active));
    tab.tabIndex = active ? 0 : -1;
    panel.hidden = !active;
    panel.classList.remove("is-entering");
    // Re-adding the class on the next paint is what restarts the fade on a repeat selection.
    if (active && !reducedMotion) {
      window.requestAnimationFrame(() => panel.classList.add("is-entering"));
    }
  });
  if (tuiCount) tuiCount.textContent = `${index + 1} / ${tuiTabButtons.length}`;
  if (focusTab) tuiTabButtons[index].focus();
}

function currentFrame() {
  const index = tuiTabButtons.findIndex((tab) => tab.getAttribute("aria-selected") === "true");
  return index === -1 ? 0 : index;
}

function stepFrame(delta, options) {
  const total = tuiTabButtons.length;
  showFrame((currentFrame() + delta + total) % total, options);
}

if (tuiTabButtons.length && tuiPanels.every(Boolean)) {
  tuiTabs.hidden = false;
  if (tuiNav) tuiNav.hidden = false;

  tuiTabButtons.forEach((tab, index) => {
    tab.addEventListener("click", () => showFrame(index));
  });

  // Arrow keys move between tabs and Home/End reach the ends: without them the strip is a row of
  // buttons that only claims to be a tablist.
  tuiTabs.addEventListener("keydown", (event) => {
    const moves = { ArrowRight: 1, ArrowDown: 1, ArrowLeft: -1, ArrowUp: -1 };
    if (event.key in moves) stepFrame(moves[event.key], { focusTab: true });
    else if (event.key === "Home") showFrame(0, { focusTab: true });
    else if (event.key === "End") showFrame(tuiTabButtons.length - 1, { focusTab: true });
    else return;
    event.preventDefault();
  });

  document.querySelectorAll(".tui-step").forEach((button) => {
    button.addEventListener("click", () => stepFrame(Number(button.dataset.tuiStep)));
  });

  showFrame(0);
}

const year = document.getElementById("year");
if (year) year.textContent = String(new Date().getFullYear());

// An explanatory walkthrough only: never submit work or answer a real gate from this page.
const factoryPlayer = document.querySelector("[data-factory-player]");
if (factoryPlayer) {
  const stations = [
    {
      title: "Admit a work order", owner: "Rust console → Python backend",
      description: "The backend validates the installed line and target repositories, then creates one Airflow batch. Each issue × target becomes an independent mapped job.",
      command: "swf submit --blueprint factory --issue 42", state: "Work order ready to submit",
    },
    {
      title: "Prepare an isolated work cell", owner: "Airflow schedules → Python runtime",
      description: "Python claims the run journal and prepares the job's sandbox. A competing mutation must wait for ownership; a lost worker is not silently replaced with an empty one.",
      command: "swf jobs list", state: "job.setup · preparing sandbox and evidence",
    },
    {
      title: "Understand the intent", owner: "Python stage → isolated agent",
      description: "The agent turns the issue into an intent artifact. Airflow schedules the next station; the Rust console displays the saved task state and logs.",
      command: "swf logs '<job-id>' --task intent", state: "job.intent · intent.md produced",
    },
    {
      title: "A person approves the intent", owner: "Human decision → Python backend → Airflow HITL",
      description: "Read the actual gate evidence. The task waits for an explicit answer. The backend checks that this exact mapped gate is still waiting before forwarding the approval.",
      command: "swf gates list\nswf gates review '<gate-id>'\nswf gates approve '<gate-id>'", state: "Paused at intent gate · approve this demo to continue", gate: true,
    },
    {
      title: "Specify acceptance", owner: "Python specification stage",
      description: "The approved intent becomes a specification: behavior, scope and acceptance criteria. Artifacts stay with the work order instead of living only in a terminal session.",
      command: "swf logs '<job-id>' --task spec", state: "job.spec · spec.md produced",
    },
    {
      title: "Plan the change", owner: "Python planning stage",
      description: "The implementation plan connects the specification to a concrete set of changes. It is saved before a person is asked to let implementation begin.",
      command: "swf logs '<job-id>' --task plan", state: "job.plan · plan.md produced",
    },
    {
      title: "Approve the plan", owner: "Human decision → Python backend → Airflow HITL",
      description: "The operator reviews the plan and the current gate revision. Bulk approval preserves per-gate outcomes; an uncertain write cannot count as success.",
      command: "swf gates list\nswf gates review '<gate-id>'\nswf gates approve '<gate-id>'", state: "Paused at plan gate · approve this demo to continue", gate: true,
    },
    {
      title: "Build and run quality checks", owner: "Python stage → isolated coding agent",
      description: "Implementation and tests run inside the work cell. Iterations, turns and spend are bounded by the line's limits. Failure remains visible; this illustration does not claim that any test ran.",
      command: "swf logs '<job-id>' --task build_and_test", state: "job.build_and_test · bounded implementation loop",
    },
    {
      title: "Review the result", owner: "Python review and policy",
      description: "Review records findings and may request a bounded fix. Unresolved blockers remain attached to the delivery; they are not hidden behind a green-looking terminal.",
      command: "swf logs '<job-id>' --task review", state: "job.review · findings and decisions saved",
    },
    {
      title: "Publish the delivery", owner: "Python control plane → GitHub",
      description: "The trusted control plane publishes the patch and evidence to a pull request. GitHub credentials never enter the coding sandbox. Publication is distinct from independent verification and merge.",
      command: "swf deliveries list", state: "job.deliver · PR and evidence published in this illustration",
    },
    {
      title: "Release the work cell", owner: "Python cleanup → human release decision",
      description: "Cleanup records its operation and releases the sandbox. A human still reviews checks and merges the PR. Interrupted operations and recovered journal fragments remain inspectable on the backend host.",
      command: "swf jobs list\n# On the backend host:\nswfactory state inspect '<run-id>'", state: "Walkthrough complete · human merge remains separate",
    },
  ];
  const stepButtons = [...factoryPlayer.querySelectorAll("[data-factory-step]")];
  const play = factoryPlayer.querySelector("[data-factory-play]");
  const next = factoryPlayer.querySelector("[data-factory-next]");
  const reset = factoryPlayer.querySelector("[data-factory-reset]");
  let position = 0;
  let timer = null;
  let playing = false;

  function pauseFactoryDemo() {
    window.clearTimeout(timer);
    timer = null;
    playing = false;
    play.textContent = "Play walkthrough";
  }

  function renderFactoryStation(index) {
    position = Math.max(0, Math.min(index, stations.length - 1));
    const station = stations[position];
    for (const [key, value] of Object.entries({
      owner: station.owner, title: station.title, description: station.description,
      command: station.command, state: `Illustration · ${station.state}`,
      count: `${position + 1} / ${stations.length}`,
    })) {
      factoryPlayer.querySelector(`[data-factory-${key}]`).textContent = value;
    }
    stepButtons.forEach((button, index) => {
      if (index === position) button.setAttribute("aria-current", "step");
      else button.removeAttribute("aria-current");
      button.classList.toggle("is-complete", index < position);
    });
    next.textContent = station.gate ? "Approve demo gate" : "Next station";
    next.disabled = position === stations.length - 1;
    factoryPlayer.classList.toggle("is-gate", Boolean(station.gate));
    // A running walkthrough always stops at a human decision. Play cannot answer it.
    if (station.gate || position === stations.length - 1) pauseFactoryDemo();
    play.disabled = Boolean(station.gate);
  }

  function scheduleFactoryStation() {
    timer = window.setTimeout(() => {
      if (!playing) return;
      renderFactoryStation(position + 1);
      if (playing) scheduleFactoryStation();
    }, 2800);
  }

  play.addEventListener("click", () => {
    if (playing) return pauseFactoryDemo();
    if (position === stations.length - 1) renderFactoryStation(0);
    playing = true;
    play.textContent = "Pause walkthrough";
    scheduleFactoryStation();
  });
  next.addEventListener("click", () => {
    pauseFactoryDemo();
    renderFactoryStation(position + 1);
  });
  reset.addEventListener("click", () => {
    pauseFactoryDemo();
    renderFactoryStation(0);
  });
  stepButtons.forEach((button, index) => button.addEventListener("click", () => {
    pauseFactoryDemo();
    renderFactoryStation(index);
  }));
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) pauseFactoryDemo();
  });
  renderFactoryStation(0);
}
