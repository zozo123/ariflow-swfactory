from pathlib import Path

path = Path("tests/test_backend_control_plane.py")
text = path.read_text(encoding="utf-8")
old = "from swfactory.cell_runtime import identity_for_job\nfrom swfactory.backend.service import Factory, Refused\n"
new = "from swfactory.backend.service import Factory, Refused\nfrom swfactory.cell_runtime import identity_for_job\n"
if text.count(old) != 1:
    raise SystemExit("expected generated import pair exactly once")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
