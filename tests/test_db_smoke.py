"""Smoke test for the stage 1 source database and analytical warehouse."""

import pytest

from logistics.readiness import run_checks

pytestmark = pytest.mark.integration


def test_stage1_databases_are_ready():
    report = run_checks()
    assert report.ok, report.text
