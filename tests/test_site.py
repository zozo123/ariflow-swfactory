"""Static contract tests for the GitHub Pages product site."""

from __future__ import annotations

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
        if tag == "script" and (src := values.get("src")):
            self.references.append(src)
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


def test_custom_not_found_page_is_self_contained() -> None:
    source, page = parse_page("404.html")

    assert source.lower().startswith("<!doctype html>")
    assert page.html_lang == "en"
    assert page.h1_count == 1
    assert page.meta["robots"] == "noindex"
    assert 'href="./"' in source
    assert_local_references_exist(page)


def test_site_assets_and_interactions_are_resilient() -> None:
    css = (SITE / "styles.css").read_text()
    javascript = (SITE / "app.js").read_text()
    manifest = (SITE / "site.webmanifest").read_text()

    assert "@media (prefers-reduced-motion: reduce)" in css
    assert "min-height: 100dvh" in css
    assert "IntersectionObserver" in javascript
    assert "prefers-reduced-motion" in javascript
    assert 'addEventListener("scroll"' not in javascript
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
