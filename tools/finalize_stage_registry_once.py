from pathlib import Path

path = Path("dags/blueprints.py")
text = path.read_text()
old = '''def _stage_fn(stage: str):
    if stage == "build_and_test":
        from swfactory.work_stage import build_and_test

        return build_and_test
    from swfactory.stages import STAGES

    return STAGES[stage]
'''
new = '''def _stage_fn(stage: str):
    from swfactory.stage_registry import resolve

    return resolve(stage)
'''
if old not in text:
    raise SystemExit("expected DAG stage resolver block not found")
path.write_text(text.replace(old, new))
