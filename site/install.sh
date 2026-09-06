#!/bin/sh
# Install `swf`, the software factory's operator binary.
#
#   curl -fsSL https://zozo123.github.io/ariflow-swfactory/install.sh | sh
#
# Or, for anyone who would rather read a script before running it — which is the correct instinct
# for anything piped into a shell:
#
#   curl -fsSL https://zozo123.github.io/ariflow-swfactory/install.sh -o install.sh
#   less install.sh && sh install.sh
#
# What it does: works out this machine's platform, resolves the release, downloads the archive AND
# its SHA256SUMS, VERIFIES the checksum, and puts `swf` in a directory on your own account. It
# never needs sudo, never writes outside the install directory, and does nothing at all if the
# checksum does not match.
#
# Knobs (all optional):
#   SWF_VERSION       a tag such as v2.1.0. Default: the latest release. Pin it for a reproducible
#                     install — "latest" is not a version, it is a moving target.
#   SWF_INSTALL_DIR   where the binary goes. Default: $HOME/.local/bin
#   SWF_REPO          owner/name to install from. Default: zozo123/ariflow-swfactory
#   SWF_BASE_URL      where the assets live. Default: this repo's release downloads. Point it at a
#                     mirror for an air-gapped install — and it is what lets this script be tested
#                     against a real archive without publishing one.
#
# POSIX sh on purpose: this has to run on a stock Debian container and on a Mac, and neither is
# guaranteed to have bash where the script expects it.
#
# It lives beside the site because site/ is what GitHub Pages publishes, so this file is served
# directly at the URL above. It is a program, not a page: tests/test_install_script.py runs it
# against a local archive over a local server, including the refusal paths.
set -eu

REPO="${SWF_REPO:-zozo123/ariflow-swfactory}"
INSTALL_DIR="${SWF_INSTALL_DIR:-$HOME/.local/bin}"
BIN=swf

say() { printf '%s\n' "$*"; }
err() { printf 'install: %s\n' "$*" >&2; }
die() { err "$*"; exit 1; }

# ---------------------------------------------------------------- fetch
#
# curl or wget, whichever exists. Every download is fatal on HTTP error (`-f` / `--server-response`
# semantics), because a 404 body silently saved as a tarball is the confusing failure mode here.

if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1" -o "$2"; }
  resolve() { curl -fsSLI -o /dev/null -w '%{url_effective}' "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -qO "$2" "$1"; }
  resolve() { wget -qS --max-redirect=10 -O /dev/null "$1" 2>&1 | awk '/^  Location:/ {u=$2} END {print u}'; }
else
  die "needs curl or wget on PATH"
fi

# ---------------------------------------------------------------- platform

os="$(uname -s)"
arch="$(uname -m)"
case "$os:$arch" in
  Darwin:arm64 | Darwin:aarch64) target=aarch64-apple-darwin ;;
  Darwin:x86_64) target=x86_64-apple-darwin ;;
  Linux:x86_64 | Linux:amd64) target=x86_64-unknown-linux-gnu ;;
  Linux:aarch64 | Linux:arm64) target=aarch64-unknown-linux-gnu ;;
  *)
    die "no published build for $os $arch.
    swf ships for macOS (arm64, x86_64) and Linux (x86_64, aarch64).
    Build it from source instead: cargo build --release --manifest-path rust/Cargo.toml"
    ;;
esac

# ---------------------------------------------------------------- version
#
# `/releases/latest` redirects to the tag, so the tag comes out of the redirect rather than the
# API. The API would work too and would also be rate-limited to 60 requests an hour per IP, which
# is a miserable thing to hit halfway through an install on a shared network.

tag="${SWF_VERSION:-}"
if [ -z "$tag" ]; then
  url="$(resolve "https://github.com/$REPO/releases/latest")" ||
    die "cannot reach github.com to resolve the latest release"
  tag="${url##*/}"
  case "$tag" in
    v*) ;;
    *) die "could not resolve the latest release of $REPO (got '$tag'); set SWF_VERSION=vX.Y.Z" ;;
  esac
fi
version="${tag#v}"

archive="$BIN-$version-$target.tar.gz"
base="${SWF_BASE_URL:-https://github.com/$REPO/releases/download/$tag}"

# ---------------------------------------------------------------- download and verify

tmp="$(mktemp -d "${TMPDIR:-/tmp}/swf-install.XXXXXX")" || die "cannot create a temp directory"
trap 'rm -rf "$tmp"' EXIT INT TERM

say "swf $version ($target)"
say "  downloading $archive"
fetch "$base/$archive" "$tmp/$archive" ||
  die "no $archive in release $tag.
    Check https://github.com/$REPO/releases/$tag for the assets this release actually published."
fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS" ||
  die "release $tag publishes no SHA256SUMS, so the download cannot be verified; refusing to install"

# The checksum is the entire reason a release publishes SHA256SUMS. A `curl | sh` that downloads a
# binary and runs it without checking is theatre, so there is deliberately no flag to skip this.
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$tmp/$archive" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  actual="$(shasum -a 256 "$tmp/$archive" | awk '{print $1}')"
else
  die "needs sha256sum or shasum to verify the download; refusing to install unverified"
fi
expected="$(awk -v want="$archive" '$2 == want || $2 == "*" want {print $1}' "$tmp/SHA256SUMS")"
[ -n "$expected" ] || die "SHA256SUMS in $tag does not list $archive; refusing to install"
if [ "$actual" != "$expected" ]; then
  die "checksum mismatch for $archive
    expected $expected
    actual   $actual
    Someone or something changed this file between the release and you. Do not install it."
fi
say "  checksum ok"

# ---------------------------------------------------------------- install

tar -xzf "$tmp/$archive" -C "$tmp" || die "cannot unpack $archive"
found="$(find "$tmp" -type f -name "$BIN" -perm -u+x 2>/dev/null | head -1)"
[ -n "$found" ] || die "no executable named $BIN inside $archive"

mkdir -p "$INSTALL_DIR" || die "cannot create $INSTALL_DIR"
# Install to a temporary name in the SAME directory and then move it: a rename is atomic, so an
# interrupted install can never leave a half-written binary where a working one used to be.
staged="$INSTALL_DIR/.$BIN.$$"
if ! (cp "$found" "$staged" && chmod 0755 "$staged" && mv -f "$staged" "$INSTALL_DIR/$BIN"); then
  rm -f "$staged"
  die "cannot write $INSTALL_DIR/$BIN"
fi
say "  installed $INSTALL_DIR/$BIN"

# Completions ship in the archive. Where they belong differs per shell and per machine, so say
# where they are rather than guessing at a directory and writing outside the install dir.
comp="$(find "$tmp" -type d -name completions 2>/dev/null | head -1)"
if [ -n "$comp" ]; then
  cp -R "$comp" "$INSTALL_DIR/swf-completions" 2>/dev/null || true
fi

# ---------------------------------------------------------------- what now

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    say ""
    say "  $INSTALL_DIR is not on your PATH. Add it:"
    say "    export PATH=\"\$PATH:$INSTALL_DIR\""
    ;;
esac

say ""
say "Next:"
say "  swf context add prod --airflow-url https://airflow.example.com \\"
say "      --repo owner/name --user \"\$USER\" --password-env AIRFLOW_PASSWORD --use"
say "  swf doctor          # what is not ready yet, and the fix for each row"
say "  swf tui             # the interactive factory"
say ""
say "The config file never stores a secret — only the name of the variable that holds it."
