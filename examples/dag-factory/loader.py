"""Load every dag-factory YAML beside this file as an Airflow DAG.

Copy this module and ``software_factory.yml`` into a directory Airflow parses (``dags/composed/``).
It imports nothing from ``swfactory`` on purpose: DAG parsing must stay independent of the
factory's runtime, exactly as ``dags/blueprints.py`` does.
"""

from __future__ import annotations

from pathlib import Path

from dagfactory import load_yaml_dags

load_yaml_dags(globals_dict=globals(), dags_folder=str(Path(__file__).resolve().parent))
