from pathlib import Path

path = Path("src/swfactory/idempotency.py")
text = path.read_text(encoding="utf-8")
old = '''            retry_at = row.get("next_attempt_at")
            if retry_at is not None and float(retry_at) > now:
                raise OperationInDoubt(ref.key, "retry_not_due")
'''
if text.count(old) != 1:
    raise SystemExit("expected retry timestamp guard exactly once")
path.write_text(text.replace(old, "", 1), encoding="utf-8")
