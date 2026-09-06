#!/usr/bin/env python3
"""Launch `swf tui` in a real terminal, read a frame, quit it, and check the terminal survived.

Render snapshots prove the widgets draw what we think they draw. They cannot prove the thing an
operator actually does: that the binary takes over a terminal, paints a live factory into it,
responds to a keystroke, and gives the terminal back. That needs a pty, so this allocates one.

Three properties, each of which has burned somebody:

1. **It renders.** The frame has to contain the live factory's own identity — the run id it was
   pointed at — not just a chrome-and-borders skeleton that would look fine while every pane was
   empty.
2. **It quits on `q`,** rather than needing the operator to kill the process.
3. **It restores the terminal.** The alternate screen is left (``ESC[?1049l``) and raw mode is
   released. A TUI that exits without doing this leaves a shell that does not echo, which is a
   support ticket that never mentions the TUI.

    scripts/tui_smoke.py --expect <substring> [--expect <substring>...] [--out frame.txt]

Reads nothing from the network itself: it inherits SWF_CONFIG and friends from its caller, so it
sees exactly the factory the rest of the end-to-end run is driving.
"""

from __future__ import annotations

import argparse
import os
import pty
import select
import signal
import sys
import time

import pyte

ALT_SCREEN_ENTER = b"\x1b[?1049h"
ALT_SCREEN_LEAVE = b"\x1b[?1049l"


def drain(fd: int, seconds: float) -> bytes:
    """Read whatever the child paints for ``seconds``, without blocking past it."""
    chunks: list[bytes] = []
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        ready, _, _ = select.select([fd], [], [], 0.2)
        if not ready:
            continue
        try:
            data = os.read(fd, 65536)
        except OSError:  # the child closed the pty: normal at exit
            break
        if not data:
            break
        chunks.append(data)
    return b"".join(chunks)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bin", default=os.environ.get("SWF_BIN", "swf"))
    ap.add_argument("--expect", action="append", default=[])
    ap.add_argument("--out", default=None, help="write the captured frame here")
    ap.add_argument("--settle", type=float, default=8.0, help="seconds to let the first paint land")
    ap.add_argument("--key", action="append", default=[], help="keys to press before capturing")
    ap.add_argument("--dwell", type=float, default=4.0, help="seconds to wait after each key")
    ap.add_argument("--cols", type=int, default=120)
    ap.add_argument("--rows", type=int, default=40)
    args = ap.parse_args(argv[1:])

    pid, fd = pty.fork()
    if pid == 0:  # child: become the TUI
        os.environ["TERM"] = "xterm-256color"
        os.environ["COLUMNS"] = str(args.cols)
        os.environ["LINES"] = str(args.rows)
        os.execvp(args.bin, [args.bin, "tui"])
        os._exit(127)  # unreachable unless exec failed

    try:
        import fcntl
        import struct
        import termios

        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", args.rows, args.cols, 0, 0))
    except Exception:  # noqa: BLE001 - a pty without window sizing still renders
        pass

    painted = drain(fd, args.settle)
    # Walk to the view the caller wants to assert on before capturing: the default screen is
    # `attention`, which is legitimately empty on a healthy factory, so asserting a run id against
    # it would be asserting that something is wrong.
    if args.key:
        for key in args.key:
            os.write(fd, key.encode())
            painted += drain(fd, args.dwell)
    os.write(fd, b"q")  # the documented quit key
    painted += drain(fd, 4.0)

    deadline = time.monotonic() + 10
    status = None
    while time.monotonic() < deadline:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            break
        time.sleep(0.1)
    else:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        print("tui: did not exit on 'q' within 10s", file=sys.stderr)
        return 1

    # A TUI paints by MOVING THE CURSOR, not by emitting newlines, so stripping the escapes and
    # splitting on "\n" flattens the whole screen onto one line and every assertion about content
    # becomes meaningless. Replaying the byte stream through a terminal emulator reconstructs the
    # screen an operator would actually be looking at — which is the only thing worth asserting on.
    screen = pyte.Screen(args.cols, args.rows)
    stream = pyte.Stream(screen)
    stream.feed(painted.decode("utf-8", "replace"))
    text = "\n".join(screen.display)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)

    problems: list[str] = []
    code = os.waitstatus_to_exitcode(status) if status is not None else 1
    if code != 0:
        problems.append(f"exited {code}, not 0")
    if ALT_SCREEN_ENTER not in painted:
        problems.append("never entered the alternate screen, so it never took the terminal over")
    if ALT_SCREEN_LEAVE not in painted:
        problems.append("never left the alternate screen: the terminal was not restored")
    for want in args.expect:
        if want not in text:
            problems.append(f"the frame never showed {want!r}")

    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    print(f"tui: {len(painted)} bytes painted, {len(lines)} non-blank lines, exit {code}")
    for line in lines[:24]:
        print(f"  | {line[:118]}")
    for problem in problems:
        print(f"tui: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
