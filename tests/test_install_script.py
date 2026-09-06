"""``scripts/install.sh``: the one program in this repo that strangers pipe into a shell.

Everything here runs against a LOCAL archive over a LOCAL http server — no GitHub, no network —
because the properties worth pinning are about what the script does with what it downloads, not
about whether a release exists today.

The load-bearing test is :func:`test_a_tampered_archive_is_refused_and_nothing_is_installed`. A
``curl | sh`` installer that fetches a binary and runs it without checking the published checksum
offers its users nothing but a shorter command, so the refusal path is the feature and it is tested
first-class rather than assumed.
"""

from __future__ import annotations

import functools
import hashlib
import http.server
import shutil
import socket
import subprocess
import tarfile
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
# It lives in site/ because that directory IS what GitHub Pages publishes: one file,
# served straight at https://zozo123.github.io/ariflow-swfactory/install.sh, with no
# copy step to drift and no relative link in the page pointing at something absent.
INSTALLER = REPO / "site" / "install.sh"
VERSION = "9.9.9"  # never a real release, so a bug that reaches the network fails loudly
TARGET_BY_PLATFORM = {
    ("Darwin", "arm64"): "aarch64-apple-darwin",
    ("Darwin", "aarch64"): "aarch64-apple-darwin",
    ("Darwin", "x86_64"): "x86_64-apple-darwin",
    ("Linux", "x86_64"): "x86_64-unknown-linux-gnu",
    ("Linux", "aarch64"): "aarch64-unknown-linux-gnu",
    ("Linux", "arm64"): "aarch64-unknown-linux-gnu",
}


def _this_target() -> str:
    """The triple the script will resolve on this machine — the archive has to match it."""
    system = subprocess.run(["uname", "-s"], capture_output=True, text=True, check=True)
    machine = subprocess.run(["uname", "-m"], capture_output=True, text=True, check=True)
    key = (system.stdout.strip(), machine.stdout.strip())
    if key not in TARGET_BY_PLATFORM:
        pytest.skip(f"no published swf build for {key}; the installer refuses it by design")
    return TARGET_BY_PLATFORM[key]


@pytest.fixture(scope="module")
def release(tmp_path_factory: pytest.TempPathFactory) -> dict:
    """A directory shaped exactly like a published release: one archive plus its SHA256SUMS.

    The archive holds a stand-in executable rather than a real build, because what is under test is
    the install path and not the binary — and a real ``cargo build --release`` would make this a
    minutes-long test of something it does not check.
    """
    target = _this_target()
    root = tmp_path_factory.mktemp("release")
    stage = root / "stage" / f"swf-{VERSION}-{target}"
    (stage / "completions").mkdir(parents=True)
    binary = stage / "swf"
    binary.write_text("#!/bin/sh\necho 'swf 9.9.9'\n", encoding="utf-8")
    binary.chmod(0o755)
    for shell in ("bash", "zsh", "fish"):
        (stage / "completions" / f"swf.{shell}").write_text(f"# {shell}\n", encoding="utf-8")

    served = root / "served"
    served.mkdir()
    archive = served / f"swf-{VERSION}-{target}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(stage, arcname=stage.name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    # Two spaces between digest and name: `shasum -a 256` writes it that way and the script's awk
    # reads $2, so a one-space file would silently match nothing.
    (served / "SHA256SUMS").write_text(f"{digest}  {archive.name}\n", encoding="utf-8")
    return {"dir": served, "archive": archive, "target": target, "digest": digest}


@pytest.fixture(scope="module")
def base_url(release: dict):
    """Serve the release directory on a free port for the lifetime of the module."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(release["dir"]))
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


def _install(tmp_path: Path, base_url: str, into: str = "target") -> subprocess.CompletedProcess:
    return subprocess.run(
        ["sh", str(INSTALLER)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={
            "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": str(tmp_path),
            "SWF_VERSION": f"v{VERSION}",
            "SWF_BASE_URL": base_url,
            "SWF_INSTALL_DIR": str(tmp_path / into),
        },
    )


def test_the_installer_is_posix_sh_and_shellcheck_clean() -> None:
    """It runs under `sh`, not bash: a stock Debian container is a supported install target."""
    subprocess.run(["sh", "-n", str(INSTALLER)], check=True, timeout=30)
    if shutil.which("shellcheck"):
        subprocess.run(["shellcheck", "-s", "sh", str(INSTALLER)], check=True, timeout=60)


def test_a_verified_archive_installs_and_the_binary_runs(tmp_path: Path, base_url: str) -> None:
    proc = _install(tmp_path, base_url)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "checksum ok" in proc.stdout
    installed = tmp_path / "target" / "swf"
    assert installed.is_file() and installed.stat().st_mode & 0o111, "not installed executable"
    ran = subprocess.run([str(installed)], capture_output=True, text=True, timeout=30, check=True)
    assert "9.9.9" in ran.stdout
    for shell in ("bash", "zsh", "fish"):
        assert (tmp_path / "target" / "swf-completions" / f"swf.{shell}").is_file()


def test_a_tampered_archive_is_refused_and_nothing_is_installed(
    tmp_path: Path, base_url: str, release: dict
) -> None:
    """The whole point of publishing SHA256SUMS. Nothing may land when the digest disagrees."""
    original = release["archive"].read_bytes()
    release["archive"].write_bytes(original + b"tampered")
    try:
        proc = _install(tmp_path, base_url, into="refused")
    finally:
        release["archive"].write_bytes(original)
    assert proc.returncode != 0
    assert "checksum mismatch" in proc.stderr
    assert not (tmp_path / "refused" / "swf").exists(), "a rejected download was installed anyway"


def test_a_release_without_checksums_is_refused(
    tmp_path: Path, base_url: str, release: dict
) -> None:
    """No SHA256SUMS means the download cannot be verified, so it is not installed."""
    sums = release["dir"] / "SHA256SUMS"
    original = sums.read_text(encoding="utf-8")
    sums.unlink()
    try:
        proc = _install(tmp_path, base_url, into="unverifiable")
    finally:
        sums.write_text(original, encoding="utf-8")
    assert proc.returncode != 0
    assert "SHA256SUMS" in proc.stderr
    assert not (tmp_path / "unverifiable" / "swf").exists()


def test_a_missing_archive_names_the_release_instead_of_saving_the_404(
    tmp_path: Path, base_url: str
) -> None:
    """A 404 body written out as a tarball is the confusing failure this guards against."""
    proc = subprocess.run(
        ["sh", str(INSTALLER)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={
            "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": str(tmp_path),
            "SWF_VERSION": "v0.0.0",  # served directory holds no such archive
            "SWF_BASE_URL": base_url,
            "SWF_INSTALL_DIR": str(tmp_path / "missing"),
        },
    )
    assert proc.returncode != 0
    assert "0.0.0" in proc.stderr
    assert not (tmp_path / "missing" / "swf").exists()


def test_the_install_is_atomic_and_never_needs_sudo(tmp_path: Path, base_url: str) -> None:
    """Installing twice replaces the binary and leaves no partial file behind."""
    assert _install(tmp_path, base_url, into="twice").returncode == 0
    assert _install(tmp_path, base_url, into="twice").returncode == 0
    into = tmp_path / "twice"
    assert (into / "swf").is_file()
    leftovers = [p.name for p in into.iterdir() if p.name.startswith(".swf.")]
    assert not leftovers, f"a staged file survived: {leftovers}"
    # No privilege escalation anywhere in the script. Checked against CODE, not the whole file:
    # the header legitimately says "never needs sudo", and a test that cannot tell a comment from
    # a command would fail on its own documentation.
    code = [
        line.split("#", 1)[0]
        for line in INSTALLER.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    ]
    escalations = [line for line in code if "sudo" in line or "doas" in line]
    assert not escalations, f"the installer escalates privileges: {escalations}"
