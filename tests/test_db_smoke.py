"""Smoke test for the stage 1 source database and analytical warehouse."""

from logistics.readiness import run_checks


def test_stage1_databases_are_ready():
    report = run_checks()
    assert report.ok, report.text
