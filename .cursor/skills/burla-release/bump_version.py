#!/usr/bin/env python3
"""Bump every version location Burla's release CI checks, then refresh uv locks.

`.github/workflows/pypi-on-release.yml` asserts that all of these equal the
release tag, so they must move together:

    client/pyproject.toml                              [project].version
    main_service/pyproject.toml                        [project].version
    node_service/pyproject.toml                        [project].version
    client/src/burla/__init__.py                       __version__
    main_service/src/main_service/__init__.py          CURRENT_BURLA_VERSION
    main_service/src/main_service/__init__.py          MIN_COMPATIBLE_CLIENT_VERSION
    node_service/src/node_service/__init__.py          __version__

Each uv.lock records its own package version too, so this also runs `uv lock`
in the three projects.

Usage: python bump_version.py X.Y.Z [--min-compatible X.Y.Z] [--no-lock]
"""
from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


def _sub_once(path: Path, pattern: str, replacement: str) -> None:
    text = path.read_text()
    new_text, n = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if n != 1:
        raise SystemExit(f"expected 1 match for {pattern!r} in {path}, got {n}")
    path.write_text(new_text)
    print(f"  {path.relative_to(REPO_ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("version")
    # The release CI requires MIN_COMPATIBLE_CLIENT_VERSION == the release tag,
    # so it defaults to the new version. Overriding it needs a matching change
    # to that CI check, otherwise the release will fail to publish.
    parser.add_argument("--min-compatible")
    parser.add_argument("--no-lock", action="store_true")
    args = parser.parse_args()

    version = args.version
    min_compatible = args.min_compatible or version
    if not SEMVER.match(version):
        raise SystemExit(f"version must be X.Y.Z, got {version!r}")
    if not SEMVER.match(min_compatible):
        raise SystemExit(f"--min-compatible must be X.Y.Z, got {min_compatible!r}")

    print(f"Bumping Burla to {version} (MIN_COMPATIBLE_CLIENT_VERSION={min_compatible}):")
    for name in ("client", "main_service", "node_service"):
        _sub_once(
            REPO_ROOT / name / "pyproject.toml",
            r'^version = "\d+\.\d+\.\d+"',
            f'version = "{version}"',
        )
    _sub_once(
        REPO_ROOT / "client" / "src" / "burla" / "__init__.py",
        r'^__version__ = "\d+\.\d+\.\d+"',
        f'__version__ = "{version}"',
    )
    _sub_once(
        REPO_ROOT / "node_service" / "src" / "node_service" / "__init__.py",
        r'^__version__ = "\d+\.\d+\.\d+"',
        f'__version__ = "{version}"',
    )
    main_init = REPO_ROOT / "main_service" / "src" / "main_service" / "__init__.py"
    _sub_once(
        main_init,
        r'^CURRENT_BURLA_VERSION = "\d+\.\d+\.\d+"',
        f'CURRENT_BURLA_VERSION = "{version}"',
    )
    _sub_once(
        main_init,
        r'^MIN_COMPATIBLE_CLIENT_VERSION = "\d+\.\d+\.\d+"',
        f'MIN_COMPATIBLE_CLIENT_VERSION = "{min_compatible}"',
    )

    if args.no_lock:
        print("Skipped uv lock (--no-lock); refresh the three locks before committing.")
        return
    for name in ("client", "main_service", "node_service"):
        print(f"uv lock: {name}")
        subprocess.run(["uv", "lock"], cwd=REPO_ROOT / name, check=True)


if __name__ == "__main__":
    main()
