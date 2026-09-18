"""Static contract tests for the GitHub Pages product site."""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).parents[1]
SITE = ROOT / "site"
PAGES_WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"


class PageProbe(HTMLParser):
    """Collect the small set of HTML facts used by the site contract."""

    def __init__(self) -> None:
        super().__init__()
        self.ids: list[str] = []
        self.references: list[str] = []
        self.fragment_links: list[str] = []
        self.h1_count = 0
        self.html_lang = ""
        self.meta: dict[str, str] = {}
        self.canonical = ""
        self.unlabelled_buttons: list[dict[str, str]] = []
        self.images: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key: value or "" for key, value in attrs}
        if node_id := values.get("id"):
            self.ids.append(node_id)
        if tag == "html":
            self.html_lang = values.get("lang", "")
        if tag == "h1":
            self.h1_count += 1
        if tag == "meta":
            key = values.get("name") or values.get("property")
            if key:
                self.meta[key] = values.get("content", "")
        if tag == "link":
            href = values.get("href", "")
            self.references.append(href)
            if values.get("rel") == "canonical":
                self.canonical = href
        if tag in {"script", "img"} and (src := values.get("src")):
            self.references.append(src)
        if tag == "img":
            self.images.append(values)
        if tag == "a" and (href := values.get("href")):
            if href.startswith("#"):
                self.fragment_links.append(href[1:])
            elif href:
                self.references.append(href)
        if tag == "button" and not values.get("aria-label"):
            self.unlabelled_buttons.append(values)


def parse_page(name: str) -> tuple[str, PageProbe]:
    """Return source and parsed facts for one site page."""
    source = (SITE / name).read_text()
    probe = PageProbe()
    probe.feed(source)
    return source, probe


def assert_local_references_exist(probe: PageProbe) -> None:
    """Ensure every relative page asset resolves inside the site root."""
    for reference in probe.references:
        parsed = urlsplit(reference)
        if parsed.scheme or parsed.netloc or reference.startswith(("mailto:", "#")):
            continue
        target = (SITE / parsed.path).resolve()
        assert target.is_relative_to(SITE.resolve()), reference
        assert target.exists(), reference


def test_homepage_accessibility_and_discovery_contract() -> None:
    source, page = parse_page("index.html")

    assert source.lower().startswith("<!doctype html>")
    assert page.html_lang == "en"
    assert page.h1_count == 1
    assert len(page.ids) == len(set(page.ids))
    assert set(page.fragment_links) <= set(page.ids)
    assert not page.unlabelled_buttons
    assert page.meta["viewport"] == "width=device-width, initial-scale=1"
    assert 80 <= len(page.meta["description"]) <= 180
    assert page.meta["og:image"].endswith("/social-card.png")
    assert page.canonical == "https://zozo123.github.io/ariflow-swfactory/"
    assert 'class="skip-link"' in source
    assert_local_references_exist(page)


def test_homepage_is_small_static_and_content_first() -> None:
    source, _ = parse_page("index.html")
    css = (SITE / "styles.css").read_text()
    javascript = (SITE / "app.js").read_text()

    assert len(source.encode()) < 15_000
    assert len(css.encode()) < 12_000
    assert len(javascript.encode()) < 300
    assert 'src="app.js"' not in source
    assert "factory-line.webp" not in source
    assert "IntersectionObserver" not in javascript
    assert 'addEventListener("scroll"' not in javascript
    for decorative_surface in (
        "ambient",
        "hero-glow",
        "reveal",
        "pipeline-board",
        "factory-visual",
        "mobile-menu",
    ):
        assert decorative_surface not in source


def test_homepage_explains_the_actual_algorithm_and_authorities() -> None:
    source, _ = parse_page("index.html")

    assert "Issue in. Verified PR out." in source
    assert "issue -&gt;" not in source
    assert "issue -> cell -> airflow -> sandbox -> evidence -> pull request" in source
    assert "Airflow is the only lifecycle scheduler" in source
    assert "one durable Factory Cell" in source
    assert "(cell_id, epoch, operation_key)" in source
    assert "Humans keep final merge authority" in source
    assert "Matter, motion, and authority" in source
    assert "accelerate motion != mint authority" in source
    assert "an accelerator may change time-to-answer, never the answer" in source


def test_sandbox_table_is_complete_and_honest() -> None:
    source, page = parse_page("index.html")

    rows = re.findall(r'data-sandbox="([^"]+)"', source)
    assert rows == [
        "local",
        "srt",
        "docker",
        "islo",
        "toolset",
        "daytona",
        "e2b",
        "tensorlake",
        "box / ascii",
    ]
    assert "policy support is backend-specific" in source.lower()
    assert source.count("custom backend required") == 4
    assert "--sandbox toolset" in source
    assert "The factory owns the issue-to-PR route" in source
    assert set(page.fragment_links) <= set(page.ids)


def test_toolset_boundary_does_not_fake_provider_capabilities() -> None:
    source, _ = parse_page("index.html")

    assert "common-ai" in source
    assert "SandboxBackend" in source
    assert 'toolset_backend = "package.module:Class"' in source
    assert "required isolation policy is unsupported" in source
    assert "StageError" in source
    assert "does not fake provider capabilities" in source


def test_astronomer_blueprint_bridge_is_visible_and_linked() -> None:
    source, _ = parse_page("index.html")

    assert "Astronomer Blueprint" in source
    assert "software_factory" in source
    assert "docs/astronomer-blueprint.md" in source


def test_custom_not_found_page_is_self_contained() -> None:
    source, page = parse_page("404.html")

    assert source.lower().startswith("<!doctype html>")
    assert page.html_lang == "en"
    assert page.h1_count == 1
    assert page.meta["robots"] == "noindex"
    assert 'href="./"' in source
    assert_local_references_exist(page)


def test_static_assets_remain_valid_without_driving_the_layout() -> None:
    css = (SITE / "styles.css").read_text()
    manifest = (SITE / "site.webmanifest").read_text()

    assert "@media (prefers-reduced-motion: reduce)" in css
    assert "min-height: 100dvh" in css
    assert '"start_url": "./"' in manifest
    assert (SITE / ".nojekyll").exists()
    assert re.search(r"<svg\b", (SITE / "favicon.svg").read_text())
    assert re.search(r"<svg\b", (SITE / "social-card.svg").read_text())
    assert (SITE / "social-card.png").stat().st_size > 10_000


def test_pages_workflow_deploys_only_the_site_artifact() -> None:
    workflow = PAGES_WORKFLOW.read_text()

    assert "actions/checkout@v6" in workflow
    assert "actions/configure-pages@v5" in workflow
    assert "actions/upload-pages-artifact@v4" in workflow
    assert "actions/deploy-pages@v4" in workflow
    assert "pages: write" in workflow
    assert "id-token: write" in workflow
    assert "name: github-pages" in workflow
    assert "path: ./site" in workflow


# --------------------------------------------------------------- reachability of what ships

TEXT_SUFFIXES = frozenset(
    {".html", ".css", ".js", ".json", ".md", ".py", ".sh", ".toml", ".txt", ".webmanifest", ".xml", ".yml", ".yaml"}
)
SKIP_DIRECTORIES = frozenset(
    {".git", ".venv", ".ruff_cache", ".pytest_cache", "__pycache__", "node_modules", ".factory"}
)
# Build output only. Named by path, because ``demo/target`` is a real fixture directory.
SKIP_PREFIXES = ("rust/target/",)
# Files a web server hands out without any page linking to them.
SERVED_WITHOUT_A_LINK = frozenset({"index.html", "404.html", ".nojekyll", "robots.txt", "sitemap.xml", "install.sh"})
# Files kept on purpose that nothing links, each for a stated reason. They used to pass only because
# `tests/test_site.py` happened to name them, which is an accident rather than a decision -- and the
# same accident hid `osai-week-2026.html`, a real page reachable from nothing.
KEPT_WITHOUT_A_LINK: dict[str, str] = {
    "app.js": "a tombstone: the suite asserts it stays under 300 bytes and that no page loads it",
    "social-card.svg": "the source the published social-card.png is rendered from",
}
ADVERTISED_ANCHOR = re.compile(r"zozo123\.github\.io/ariflow-swfactory/#([\w-]+)")
CSS_CLASS = re.compile(r"\.(-?[_a-zA-Z][\w-]*)")
HTML_CLASS = re.compile(r'class="([^"]*)"')
COLOUR_LITERAL = re.compile(r"#[0-9a-fA-F]{3,8}\b")
TOKEN_BLOCK = re.compile(r"(?::root\s*\{[^}]*\})|(?:@media \(prefers-color-scheme: dark\)\s*\{.*?\n\})", re.S)


def repository_text(root: Path) -> dict[str, str]:
    """Every tracked-looking text file, so a reference from anywhere in the repo counts."""
    found: dict[str, str] = {}
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in TEXT_SUFFIXES:
            continue
        relative = path.relative_to(root)
        if SKIP_DIRECTORIES & set(relative.parts) or str(relative).startswith(SKIP_PREFIXES):
            continue
        found[str(relative)] = path.read_text(encoding="utf-8", errors="ignore")
    return found


def undefined_classes(html: str, css: str) -> list[str]:
    """Class names a page asks for that the one stylesheet it loads never defines."""
    defined = set(CSS_CLASS.findall(css))
    used: set[str] = set()
    for value in HTML_CLASS.findall(html):
        used.update(value.split())
    return sorted(used - defined)


def unreferenced_site_files(root: Path) -> list[str]:
    """Files under ``site/`` that deploy to Pages while nothing in the repository names them."""
    site = root / "site"
    # Neither `site/` (handled per-file below) nor `tests/`: a test that audits a file is not a
    # reference that reaches it. The comment explaining this fix named `osai-week-2026.html` and
    # thereby vouched for it, which is the same self-reference bug one directory over.
    corpus = "\n".join(
        text
        for name, text in repository_text(root).items()
        if not name.startswith("site/") and not name.startswith("tests/")
    )
    # Keyed by path so a file can be excluded from its OWN corpus below. A page that names itself
    # -- in a canonical link, an og:url, a self-referential anchor -- was counting as referenced,
    # which is how `osai-week-2026.html` sat deployed and reachable from nothing for weeks while
    # this check stayed green. Nothing can vouch for its own reachability.
    inside = {
        str(path.relative_to(site)): path.read_text(encoding="utf-8", errors="ignore")
        for path in sorted(site.rglob("*"))
        if path.is_file() and path.suffix in TEXT_SUFFIXES
    }
    orphans = []
    for path in sorted(site.rglob("*")):
        if not path.is_file():
            continue
        relative = str(path.relative_to(site))
        if relative in SERVED_WITHOUT_A_LINK or relative in KEPT_WITHOUT_A_LINK:
            continue
        others = corpus + "\n".join(text for name, text in inside.items() if name != relative)
        if relative not in others and path.name not in others:
            orphans.append(relative)
    return orphans


def advertised_anchors(documents: dict[str, str]) -> set[str]:
    """Fragments the repository tells a reader to open on the published site."""
    return {anchor for text in documents.values() for anchor in ADVERTISED_ANCHOR.findall(text)}


def hardcoded_colours(css: str) -> list[str]:
    """Colour literals outside the token blocks -- the ones a theme switch cannot reach."""
    return COLOUR_LITERAL.findall(TOKEN_BLOCK.sub("", css))


def test_every_class_a_page_uses_is_defined_in_the_stylesheet() -> None:
    """404.html shipped for weeks styled by classes -- ``error-card``, ``ambient``, ``kicker`` --
    that were deleted with the previous design. Nothing failed: the page still parsed, still had
    one ``h1``, still linked home, and rendered to visitors as unstyled black text on white."""
    css = (SITE / "styles.css").read_text()

    for name in ("index.html", "404.html"):
        assert undefined_classes((SITE / name).read_text(), css) == [], name


def test_a_page_styled_by_a_class_the_stylesheet_dropped_is_caught() -> None:
    css = (SITE / "styles.css").read_text()
    page = (SITE / "404.html").read_text().replace('class="label"', 'class="kicker"', 1)

    assert undefined_classes(page, css) == ["kicker"]


def test_every_image_resolves_and_carries_alternative_text() -> None:
    """The probe read ``link`` and ``script`` but never ``img``, so the one drawing on the page
    was outside the reference check that exists to keep the site free of dead files."""
    for name in ("index.html", "404.html"):
        _, page = parse_page(name)
        assert_local_references_exist(page)
        for image in page.images:
            assert image.get("alt", "").strip(), f"{name}: image without alt text"


def test_nothing_deploys_to_pages_that_the_repository_never_references() -> None:
    """``assets/lifecycle-demo.gif`` was published on every deploy after the page that embedded it
    was rewritten. The existing check ran one way only -- references must resolve -- so a file
    nobody pointed at was invisible to it."""
    assert unreferenced_site_files(ROOT) == []


def test_an_asset_nothing_points_at_is_reported(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    (tree / "site" / "assets").mkdir(parents=True)
    (tree / "README.md").write_text("styles.css and assets/kept.svg", encoding="utf-8")
    (tree / "site" / "index.html").write_text('<link href="styles.css" />', encoding="utf-8")
    (tree / "site" / "styles.css").write_text("body{}", encoding="utf-8")
    (tree / "site" / "assets" / "kept.svg").write_text("<svg></svg>", encoding="utf-8")
    (tree / "site" / "assets" / "stale.gif").write_bytes(b"GIF89a")

    assert unreferenced_site_files(tree) == ["assets/stale.gif"]


def test_every_anchor_the_repository_advertises_exists_on_the_page() -> None:
    """README linked twice to ``/#factory-demo`` -- once from the header nav, once from the
    quickstart -- for as long as the section it names was absent from the page."""
    _, page = parse_page("index.html")

    missing = sorted(advertised_anchors(repository_text(ROOT)) - set(page.ids))

    assert missing == [], f"advertised on the site but not on the page: {missing}"


def test_an_advertised_anchor_with_no_section_is_reported() -> None:
    documents = {"README.md": "see https://zozo123.github.io/ariflow-swfactory/#factory-demo now"}

    assert advertised_anchors(documents) == {"factory-demo"}


def test_the_page_answers_to_the_readers_colour_scheme() -> None:
    """``color-scheme: light`` was pinned while the manifest declared a dark theme colour, so the
    two disagreed and a reader on a dark system got a full-brightness page either way."""
    css = (SITE / "styles.css").read_text()

    assert "color-scheme: light dark" in css
    assert "@media (prefers-color-scheme: dark)" in css
    for token in ("--bg", "--text", "--muted", "--line", "--soft", "--accent"):
        assert css.count(f"{token}:") >= 2, token


def test_no_colour_escapes_the_theme_tokens() -> None:
    """``.lede`` carried a literal ``#333333``. A token block cannot re-point what never read it,
    so that one rule stayed dark-on-dark when everything around it inverted."""
    assert hardcoded_colours((SITE / "styles.css").read_text()) == []


def test_a_colour_written_outside_the_token_blocks_is_caught() -> None:
    css = (SITE / "styles.css").read_text().replace("color: var(--lede);", "color: #333333;", 1)

    assert hardcoded_colours(css) == ["#333333"]


def test_the_manifest_theme_matches_the_light_scheme_the_page_declares() -> None:
    """The manifest painted an installed window ``#07090d`` while the stylesheet had no dark mode
    at all, so the splash screen and the page it opened were different products."""
    source, _ = parse_page("index.html")
    manifest = json.loads((SITE / "site.webmanifest").read_text())

    declared = dict(
        re.findall(
            r'<meta name="theme-color" media="\(prefers-color-scheme: (\w+)\)" content="([^"]+)"',
            source,
        )
    )

    assert set(declared) == {"light", "dark"}
    assert declared["light"] != declared["dark"]
    assert declared["light"] == manifest["theme_color"] == manifest["background_color"]


def test_every_deliberately_unlinked_file_still_exists_and_says_why() -> None:
    """A standing exemption has to keep earning itself: a file removed from the tree must lose its
    entry, and an entry without a reason is an exemption nobody can review."""
    for name, reason in KEPT_WITHOUT_A_LINK.items():
        assert (SITE / name).exists(), f"{name} is exempted but no longer present"
        assert len(reason.split()) >= 5, f"{name}: exemption needs a real reason"
