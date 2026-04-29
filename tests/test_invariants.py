"""
Section 39 of the test plan: cross-cutting invariants across the three
Python packages.

- Version string must match between client, main_service, node_service.
- MIN_COMPATIBLE_CLIENT_VERSION <= CURRENT_BURLA_VERSION.
- Firestore database is always "burla" (no `firestore.Client()` without it).
- The client package (`burla`) never directly imports the firestore client.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit


REPO_ROOT = Path(__file__).parent.parent


def _pyproject_version(pyproject_path: Path) -> str:
    text = pyproject_path.read_text()
    # Support both PEP 621 ([project]) and Poetry ([tool.poetry]) shapes.
    match = re.search(r'version\s*=\s*"([^"]+)"', text)
    assert match, f"no version found in {pyproject_path}"
    return match.group(1)


def test_client_pyproject_version_matches___version__():
    py = _pyproject_version(REPO_ROOT / "client" / "pyproject.toml")
    init = (REPO_ROOT / "client" / "src" / "burla" / "__init__.py").read_text()
    match = re.search(r'__version__ = "([^"]+)"', init)
    assert match
    assert py == match.group(1)


def test_main_service_pyproject_version_matches_CURRENT_BURLA_VERSION():
    py = _pyproject_version(REPO_ROOT / "main_service" / "pyproject.toml")
    init = (REPO_ROOT / "main_service" / "src" / "main_service" / "__init__.py").read_text()
    match = re.search(r'CURRENT_BURLA_VERSION\s*=\s*"([^"]+)"', init)
    assert match
    assert py == match.group(1)


def test_node_service_pyproject_version_matches___version__():
    py = _pyproject_version(REPO_ROOT / "node_service" / "pyproject.toml")
    init = (REPO_ROOT / "node_service" / "src" / "node_service" / "__init__.py").read_text()
    match = re.search(r'__version__\s*=\s*"([^"]+)"', init)
    assert match
    assert py == match.group(1)


def test_all_three_service_versions_agree():
    client = _pyproject_version(REPO_ROOT / "client" / "pyproject.toml")
    main = _pyproject_version(REPO_ROOT / "main_service" / "pyproject.toml")
    node = _pyproject_version(REPO_ROOT / "node_service" / "pyproject.toml")
    assert client == main == node


def test_MIN_COMPATIBLE_CLIENT_VERSION_is_le_CURRENT():
    init = (REPO_ROOT / "main_service" / "src" / "main_service" / "__init__.py").read_text()
    current = re.search(r'CURRENT_BURLA_VERSION\s*=\s*"([^"]+)"', init).group(1)
    lower = re.search(r'MIN_COMPATIBLE_CLIENT_VERSION\s*=\s*"([^"]+)"', init).group(1)

    def to_tuple(s):
        return tuple(int(p) for p in s.split("."))

    assert to_tuple(lower) <= to_tuple(current), (
        f"MIN_COMPATIBLE_CLIENT_VERSION ({lower}) must be <= "
        f"CURRENT_BURLA_VERSION ({current})"
    )


def test_firestore_client_always_has_database_burla():
    """Every firestore.Client(...) / AsyncClient(...) call in main_service
    and node_service must specify database='burla'."""
    for svc in ("main_service", "node_service"):
        for path in (REPO_ROOT / svc / "src").rglob("*.py"):
            text = path.read_text()
            for match in re.finditer(r"firestore\.(Client|AsyncClient)\(([^)]*)\)", text):
                args = match.group(2)
                assert "burla" in args, (
                    f"{path.relative_to(REPO_ROOT)}: firestore client without "
                    f"database=burla: {match.group(0)}"
                )


def test_client_package_does_not_import_google_cloud_firestore():
    """The burla client must not touch firestore directly — it goes through
    main_service HTTP endpoints."""
    client_dir = REPO_ROOT / "client" / "src" / "burla"
    for path in client_dir.rglob("*.py"):
        text = path.read_text()
        # Allow `from google.cloud import logging` etc., but `firestore`
        # must not appear in any import.
        for line in text.splitlines():
            if line.strip().startswith(("import ", "from ")) and "firestore" in line:
                pytest.fail(
                    f"{path.relative_to(REPO_ROOT)} imports firestore: {line.strip()}"
                )


def test_client_tests_subprocess_pattern_uses_spawn_context():
    """`conftest.run_rpm_in_subprocess` uses mp.get_context('spawn'),
    which is critical for cloudpickle to pickle closures cleanly on macOS."""
    text = (REPO_ROOT / "conftest.py").read_text()
    assert 'mp.get_context("spawn")' in text or "get_context('spawn')" in text
