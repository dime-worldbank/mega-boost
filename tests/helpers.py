"""Shared test helpers: build small in-memory .xlsx fixtures and run a Databricks
notebook (.py source) end-to-end via its non-Databricks / no-Spark code path."""
import os
import subprocess
import sys

import openpyxl

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_script(nb_relpath, env=None):
    """Run a notebook source file as a plain Python script in a subprocess, the way a local
    run would. Suitable for notebooks whose non-Databricks path is env-driven and needs no
    injected globals (e.g. TGO_ETL reads INPUT_DIR/OUTPUT_DIR)."""
    child_env = dict(os.environ)
    child_env.pop("DATABRICKS_RUNTIME_VERSION", None)  # force the non-Databricks path
    child_env.update(env or {})
    result = subprocess.run(
        [sys.executable, os.path.join(REPO_ROOT, nb_relpath)],
        env=child_env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise AssertionError(f"{nb_relpath} failed (exit {result.returncode}):\n{result.stderr}")
    return result


def write_workbook(path, sheets):
    """sheets: dict {sheet_name: [row, ...]} where each row is a list of cell values."""
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(title=name)
        for row in rows:
            ws.append(row)
    wb.save(path)
