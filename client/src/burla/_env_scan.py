"""
Detect every python distribution importable by this interpreter, with exact
versions, by reading the on-disk package database directly: no subprocesses,
no imports, and no METADATA parsing unless a directory name is ambiguous.

This is the approach Coiled's package-sync scanner uses: read every standard
that defines how installed packages land on disk, instead of trusting any one
package manager's view of the environment.
  - PEP 376 `*.dist-info` directories
  - PEP 610 `direct_url.json` (git / URL / local-dir / editable installs)
  - legacy setuptools `*.egg-info` (directory or single file)
  - legacy `pip install -e` `*.egg-link` pointers
  - `.pth` files that graft extra directories onto sys.path
  - anything else that ends up on sys.path (PYTHONPATH, PEP 582
    `__pypackages__`, notebook working directories, ...)

Distributions replicable from an index become uv requirement strings.
Editable / local-directory installs cannot be fetched by a node, so their
modules are shipped inside the pickled function instead (cloudpickle
by-value), which is decided by `modules_to_pickle_by_value`.
"""

import json
import os
import re
import sys
import sysconfig
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

# Not installable or not wanted on workers: burla is installed by the worker
# itself (pinned to the cluster version), notebook kernels are useless there,
# and windows/macOS-only dists have no linux artifacts on any index.
_EXCLUDED_DISTS = {"burla", "ipython", "google-colab", "pywin32", "pywinpty"}
_EXCLUDED_PREFIXES = ("pyobjc",)

_CANONICALIZE_RE = re.compile(r"[-_.]+")


def _canonical(name: str) -> str:
    return _CANONICALIZE_RE.sub("-", name).lower()


def _resolve(path: str) -> str:
    # realpath so symlinked layouts (pyenv, conda, macOS /tmp) compare equal
    # to the already-resolved paths in module __spec__.origin values.
    return os.path.normcase(os.path.realpath(path))


@dataclass(frozen=True)
class ScannedEnvironment:
    # canonical dist name -> uv requirement string, e.g. "numpy==2.1.1",
    # "flask @ git+https://github.com/pallets/flask@abc123"
    requirements: dict
    # canonical dist name -> source dir of editable / local-dir installs
    local_dists: dict
    # resolved dirs (with trailing separator) that host dist metadata
    dist_dir_prefixes: tuple
    # resolved stdlib dirs (with trailing separator)
    stdlib_prefixes: tuple
    # top-level module names owned by local-dir installs whose code was
    # copied into site-packages (path checks alone cannot spot those)
    local_top_levels: frozenset


def _sys_path_locations() -> list:
    locations = []
    seen = set()
    for raw_path in sys.path:
        if not raw_path:  # "" means the cwd, importable in REPLs / notebooks
            try:
                raw_path = os.getcwd()
            except OSError:
                continue
        resolved = _resolve(raw_path)
        # zipimport entries (.zip / .egg files) are skipped: nothing modern
        # installs them and setuptools >= 80 refuses to.
        if resolved in seen or not os.path.isdir(resolved):
            continue
        seen.add(resolved)
        locations.append(resolved)
    return locations


def _paths_from_pth_file(pth_path: str, location: str):
    """Directories a .pth file grafts onto sys.path (PEP 328-era site docs)."""
    try:
        with open(pth_path, encoding="utf-8", errors="ignore") as f:
            lines = f.read().splitlines()
    except OSError:
        return
    for line in lines:
        line = line.strip()
        is_code = line.startswith(("import ", "import\t"))
        if not line or line.startswith("#") or is_code or line == ".":
            continue
        grafted = _resolve(os.path.join(location, line))
        if os.path.isdir(grafted):
            yield grafted


def _egg_link_target(egg_link_path: str, location: str):
    try:
        with open(egg_link_path, encoding="utf-8", errors="ignore") as f:
            first_line = f.readline().strip()
    except OSError:
        return None
    if not first_line:
        return None
    return _resolve(os.path.join(location, first_line))


def _parse_metadata_dirname(entry_name: str):
    """name/version straight from the directory name when unambiguous."""
    if entry_name.endswith(".dist-info"):
        # spec: {normalized-name}-{version}.dist-info, name has no dashes
        stem = entry_name[: -len(".dist-info")]
        name, _, version = stem.rpartition("-")
    else:
        # legacy: name[-version[-pyX.Y[-platform]]].egg-info, name has no
        # dashes (setuptools to_filename turns them into underscores)
        stem = entry_name[: -len(".egg-info")]
        name, _, rest = stem.partition("-")
        version = rest.split("-", 1)[0]
    return (name or None), (version or None)


def _read_name_version(metadata_path: str, is_dist_info: bool):
    """Fallback: read Name/Version headers from METADATA / PKG-INFO."""
    if os.path.isdir(metadata_path):
        file_name = "METADATA" if is_dist_info else "PKG-INFO"
        file_path = os.path.join(metadata_path, file_name)
    else:
        file_path = metadata_path  # single-file .egg-info is the PKG-INFO
    name = version = None
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            for line in f:
                if line.startswith("Name:"):
                    name = line[len("Name:") :].strip()
                elif line.startswith("Version:"):
                    version = line[len("Version:") :].strip()
                if (name and version) or not line.strip():
                    break  # blank line ends the headers
    except OSError:
        pass
    return name, version


def _file_uri_to_path(url: str) -> str:
    path = unquote(urlparse(url).path)
    if os.name == "nt":
        path = path.lstrip("/")  # file:///C:/x parses with a leading slash
    return path


def _version_pin(raw_name: str, version: str) -> str:
    # "+cu121"-style local version segments (PEP 440) never exist on an
    # index; the public part is the closest installable description.
    return f"{raw_name}=={version.split('+', 1)[0]}"


def _read_installer(metadata_path: str) -> str:
    try:
        with open(os.path.join(metadata_path, "INSTALLER"), encoding="utf-8") as f:
            return f.read().strip().lower()
    except OSError:
        return ""


def _classify_from_direct_url(metadata_path: str, raw_name: str):
    """PEP 610: how a dist-info's package was actually obtained.

    Returns ("req", spec), ("local", source_dir, editable), or None when the
    package should be treated as a plain index install.
    """
    direct_url_path = os.path.join(metadata_path, "direct_url.json")
    try:
        with open(direct_url_path, encoding="utf-8") as f:
            direct_url = json.load(f)
    except (OSError, ValueError):
        return None
    # conda writes direct_url.json on its BUILD machines: the urls point at
    # feedstock work dirs that don't exist here, and stat'ing them can stall
    # for seconds on macOS automount paths like /home. The dist-info name and
    # version are the real PyPI ones, so treat it as a plain index install.
    if _read_installer(metadata_path) == "conda":
        return None
    url = direct_url.get("url") or ""
    # poetry < 1.2 wrote direct_url.json pointing at its own artifact cache
    # for normal index installs (its PEP 610 implementation was wrong)
    if not url or "pypoetry/artifacts" in url.replace("\\", "/"):
        return None
    vcs_info = direct_url.get("vcs_info")
    if isinstance(vcs_info, dict) and vcs_info.get("vcs"):
        spec = f"{vcs_info['vcs']}+{url}"
        commit = vcs_info.get("commit_id")  # uv < 0.5.23 omitted it
        if commit:
            spec += f"@{commit}"
        return ("req", f"{raw_name} @ {spec}")
    if direct_url.get("dir_info") is not None:
        source_dir = _file_uri_to_path(url)
        # a source dir that isn't on this machine means the metadata was
        # written on a build system (system packages do this too); the
        # version pin is then the only replicable description
        if not os.path.isdir(source_dir):
            return None
        editable = bool(direct_url["dir_info"].get("editable"))
        return ("local", _resolve(source_dir), editable)
    archive_info = direct_url.get("archive_info")
    if archive_info is not None:
        parsed_url = urlparse(url)
        if parsed_url.scheme in ("http", "https"):
            return ("req", f"{raw_name} @ {url}")
        # a local wheel/sdist file: unreachable from a node, fall through to
        # the version pin, the closest index-replicable description
    return None


def _top_level_modules(metadata_path: str) -> set:
    """Import names a dist owns, from top_level.txt or RECORD."""
    try:
        with open(os.path.join(metadata_path, "top_level.txt"), encoding="utf-8") as f:
            names = {line.strip() for line in f}
            return {name for name in names if name}
    except OSError:
        pass
    names = set()
    try:
        with open(os.path.join(metadata_path, "RECORD"), encoding="utf-8") as f:
            for line in f:
                relative_path = line.split(",", 1)[0].replace("\\", "/")
                if relative_path.startswith(("/", "..")):
                    continue
                first_segment = relative_path.split("/", 1)[0]
                if first_segment.endswith(".py"):
                    names.add(first_segment[:-3])
                elif not first_segment.endswith((".dist-info", ".data", ".pth")):
                    names.add(first_segment)
    except OSError:
        pass
    return names


def _stdlib_prefixes() -> tuple:
    paths = sysconfig.get_paths()
    stdlib_dirs = {paths.get("stdlib"), paths.get("platstdlib")}
    stdlib_dirs.add(os.path.join(sys.base_prefix, "DLLs"))  # windows
    resolved = {_resolve(d) + os.sep for d in stdlib_dirs if d}
    return tuple(sorted(resolved))


def _is_excluded(canonical_name: str) -> bool:
    if canonical_name in _EXCLUDED_DISTS:
        return True
    return canonical_name.startswith(_EXCLUDED_PREFIXES)


def _scan(locations: list) -> ScannedEnvironment:
    # (location_index, location, entry_name); location order decides which
    # copy of a dist wins, mirroring how imports resolve along sys.path
    metadata_candidates = []
    egg_link_roots = {}  # canonical name -> develop-install source dir
    scan_queue = [(location, True) for location in locations]
    seen_locations = set(locations)
    queue_index = 0
    while queue_index < len(scan_queue):
        location, may_expand = scan_queue[queue_index]
        queue_index += 1
        try:
            with os.scandir(location) as dir_entries:
                entry_names = [e.name for e in dir_entries]
        except OSError:
            continue
        for entry_name in entry_names:
            # "~" prefixes are pip's crash leftovers (half-removed dists)
            if entry_name.startswith(("~", ".")):
                continue
            if entry_name.endswith((".dist-info", ".egg-info")):
                metadata_candidates.append((queue_index - 1, location, entry_name))
            elif may_expand and entry_name.endswith(".pth"):
                pth_path = os.path.join(location, entry_name)
                for grafted in _paths_from_pth_file(pth_path, location):
                    if grafted not in seen_locations:
                        seen_locations.add(grafted)
                        scan_queue.append((grafted, False))
            elif may_expand and entry_name.endswith(".egg-link"):
                target = _egg_link_target(os.path.join(location, entry_name), location)
                if target:
                    dist_name = _canonical(entry_name[: -len(".egg-link")])
                    egg_link_roots[dist_name] = target
                    if os.path.isdir(target) and target not in seen_locations:
                        seen_locations.add(target)
                        scan_queue.append((target, False))

    requirements = {}
    local_dists = {}
    local_top_levels = set()
    dist_dirs = set()
    # (location_index, prefer_egg_info) per chosen dist, for conflict rules
    chosen_from = {}
    for location_index, location, entry_name in metadata_candidates:
        dist_dirs.add(location)
        is_dist_info = entry_name.endswith(".dist-info")
        metadata_path = os.path.join(location, entry_name)
        raw_name, version = _parse_metadata_dirname(entry_name)
        version_is_sane = version is not None and version[:1].isdigit()
        if raw_name is None or not version_is_sane:
            raw_name, version = _read_name_version(metadata_path, is_dist_info)
            if not raw_name or not version:
                continue  # unreadable metadata: nothing usable to replicate
        name = _canonical(raw_name)
        if _is_excluded(name):
            continue

        previous = chosen_from.get(name)
        if previous is not None:
            previous_index, previous_was_egg_info = previous
            # earlier sys.path entry shadows later ones; within one
            # directory a dist-info beats a stale egg-info
            same_dir = previous_index == location_index
            if not (same_dir and previous_was_egg_info and is_dist_info):
                continue

        if name in egg_link_roots:
            classification = ("local", egg_link_roots[name], True)
        elif is_dist_info:
            classification = _classify_from_direct_url(metadata_path, raw_name)
        else:
            classification = None

        chosen_from[name] = (location_index, not is_dist_info)
        requirements.pop(name, None)
        local_dists.pop(name, None)
        if classification is None:
            requirements[name] = _version_pin(raw_name, version)
        elif classification[0] == "req":
            requirements[name] = classification[1]
        else:
            _, source_dir, editable = classification
            local_dists[name] = source_dir
            if not editable:
                # code was copied into site-packages, so origin-path checks
                # can't tell it apart from an index install
                local_top_levels |= _top_level_modules(metadata_path)

    dist_dir_prefixes = tuple(sorted(d + os.sep for d in dist_dirs))
    return ScannedEnvironment(
        requirements=requirements,
        local_dists=local_dists,
        dist_dir_prefixes=dist_dir_prefixes,
        stdlib_prefixes=_stdlib_prefixes(),
        local_top_levels=frozenset(local_top_levels),
    )


_scan_cache = None


def scan_environment() -> ScannedEnvironment:
    """Scan sys.path; ~5ms cold on a fat env, ~stat-only when unchanged."""
    global _scan_cache
    locations = _sys_path_locations()
    cache_key = []
    for location in locations:
        try:
            cache_key.append((location, os.stat(location).st_mtime_ns))
        except OSError:
            cache_key.append((location, 0))
    cache_key = tuple(cache_key)
    if _scan_cache is not None and _scan_cache[0] == cache_key:
        return _scan_cache[1]
    scan = _scan(locations)
    _scan_cache = (cache_key, scan)
    return scan


def modules_to_pickle_by_value(scan: ScannedEnvironment) -> set:
    """Names in sys.modules whose code no index can provide: local/editable
    dists and loose modules (cwd, PYTHONPATH, manual loads). Everything under
    a dist directory or the stdlib is replicated by uv instead."""
    module_names = set()
    resolved_dir_cache = {}
    known_prefixes = scan.dist_dir_prefixes + scan.stdlib_prefixes
    for module_name, module in list(sys.modules.items()):
        top_level = module_name.partition(".")[0]
        if top_level == "burla":
            continue
        spec = getattr(module, "__spec__", None)
        origin = getattr(spec, "origin", None)
        if not origin or origin in ("built-in", "frozen"):
            continue
        if top_level in scan.local_top_levels:
            module_names.add(module_name)
            continue
        directory = os.path.dirname(origin)
        resolved = resolved_dir_cache.get(directory)
        if resolved is None:
            resolved = _resolve(directory) + os.sep
            resolved_dir_cache[directory] = resolved
        if not resolved.startswith(known_prefixes):
            module_names.add(module_name)
    return module_names
