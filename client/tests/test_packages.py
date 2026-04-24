"""
Package auto-detection — the AST walker and sys.modules scan that pick
what to ship to workers.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


# ------------------------------------------------ _imports_inside_function_body


def test_imports_inside_function_body_detects_import():
    from burla._helpers import _imports_inside_function_body

    def fn(x):
        import numpy
        return numpy.sum(x)

    assert "numpy" in _imports_inside_function_body(fn)


def test_imports_inside_function_body_detects_from_import():
    from burla._helpers import _imports_inside_function_body

    def fn(x):
        from pandas import DataFrame
        return DataFrame([x])

    assert "pandas" in _imports_inside_function_body(fn)


def test_imports_inside_function_body_strips_submodules():
    from burla._helpers import _imports_inside_function_body

    def fn(x):
        import google.cloud.firestore
        return x

    names = _imports_inside_function_body(fn)
    assert "google" in names


def test_imports_inside_function_body_returns_empty_on_no_imports():
    from burla._helpers import _imports_inside_function_body

    def fn(x):
        return x + 1

    assert _imports_inside_function_body(fn) == set()


# ------------------------------------------------ _scan_sys_modules


def test_scan_sys_modules_excludes_burla():
    from burla._helpers import _scan_sys_modules

    _, packages, _ = _scan_sys_modules()
    # burla is explicitly excluded from package detection to avoid
    # self-sync in dev mode.
    assert "burla" not in packages


def test_get_modules_required_on_remote_returns_fresh_copies():
    from burla._helpers import get_modules_required_on_remote

    def fn(x):
        return x

    c1, _ = get_modules_required_on_remote(fn)
    c1.add("mutation")
    c2, _ = get_modules_required_on_remote(fn)
    assert "mutation" not in c2
