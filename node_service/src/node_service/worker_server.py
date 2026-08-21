import hashlib
import importlib
import importlib.metadata
import io
import json
import os
import pickle
import re
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time
import traceback
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

FUNCTION_PAYLOAD_MAGIC = b"BURLA_FUNCTION_V2\0"

# Do not move. Node assumes first line printed is the Python version.
print(f"{sys.version_info.major}.{sys.version_info.minor}", flush=True)

MACHINE_TO_UV_ARCH = {
    "x86_64": "x86_64",
    "amd64": "x86_64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
}

if not shutil.which("uv"):
    uv_bin_directory = "/worker_service_python_env/bin"
    os.makedirs(uv_bin_directory, exist_ok=True)
    os.environ["PATH"] = f"{uv_bin_directory}:{os.environ['PATH']}"
    libc = "musl" if os.path.exists("/etc/alpine-release") else "gnu"
    architecture = MACHINE_TO_UV_ARCH[os.uname().machine]
    target = f"{architecture}-unknown-linux-{libc}"
    url = f"https://github.com/astral-sh/uv/releases/latest/download/uv-{target}.tar.gz"
    with urllib.request.urlopen(url) as response:
        with tarfile.open(fileobj=io.BytesIO(response.read()), mode="r:gz") as tarball:
            with tarball.extractfile(f"uv-{target}/uv") as uv_binary:
                with open(f"{uv_bin_directory}/uv", "wb") as output_file:
                    output_file.write(uv_binary.read())
    os.chmod(f"{uv_bin_directory}/uv", 0o755)

try:
    installed_burla_version = importlib.metadata.version("burla")
except importlib.metadata.PackageNotFoundError:
    installed_burla_version = None

target_burla_version = sys.argv[2]
if installed_burla_version != target_burla_version:
    package_spec = (
        "/opt/burla/client"
        if os.path.isdir("/opt/burla/client")
        else f"burla=={target_burla_version}"
    )
    install_command = [
        "uv",
        "pip",
        "install",
        "--python",
        "python",
        "--target",
        "/worker_service_python_env",
        package_spec,
    ]
    subprocess.run(install_command, check=True)

import cloudpickle
from tblib import Traceback

LOG_START_MARKER_PREFIX = "__burla_input_start__:"
LOG_END_MARKER_PREFIX = "__burla_input_end__:"


def kill_all_other_processes():
    my_pid = os.getpid()
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        if pid == my_pid:
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def receive_exactly(connection, byte_count):
    payload = b""
    while len(payload) < byte_count:
        chunk = connection.recv(byte_count - len(payload))
        if not chunk:
            return payload
        payload += chunk
    return payload


def env_dir_distributions():
    """canonical name -> (version, direct-url spec) for every dist burla
    itself installed into /worker_service_python_env (bootstrap or any
    previous job). Read from dist-info dirnames + direct_url.json."""
    dists = {}
    if not os.path.isdir("/worker_service_python_env"):
        return dists
    for entry in os.listdir("/worker_service_python_env"):
        if entry.startswith(("~", ".")) or not entry.endswith(".dist-info"):
            continue
        name, _, version = entry[: -len(".dist-info")].rpartition("-")
        if not name:
            continue
        canonical_name = re.sub(r"[-_.]+", "-", name).lower()
        direct_url_spec = None
        direct_url_path = f"/worker_service_python_env/{entry}/direct_url.json"
        try:
            with open(direct_url_path, encoding="utf-8") as f:
                direct_url = json.load(f)
            url = direct_url.get("url") or ""
            vcs_info = direct_url.get("vcs_info")
            if isinstance(vcs_info, dict) and vcs_info.get("vcs"):
                direct_url_spec = f"{vcs_info['vcs']}+{url}"
                if vcs_info.get("commit_id"):
                    direct_url_spec += f"@{vcs_info['commit_id']}"
            elif url:
                direct_url_spec = url
        except (OSError, ValueError):
            pass
        dists[canonical_name] = (version.split("+", 1)[0], direct_url_spec)
    return dists


def _subprocess_output(result):
    return "\n\n".join(
        f"{stream}:\n{content.strip()}"
        for stream, content in (
            ("stdout", result.stdout),
            ("stderr", result.stderr),
        )
        if content.strip()
    )


# Read by node_service to answer the client's install-progress poll (path
# duplicated in job_endpoints.py): this worker's TCP socket is blocked inside
# the `i` command for the whole install, so progress leaves the container
# through this bind-mounted file instead.
INSTALLING_PACKAGE_PATH = "/worker_service_storage/installing_package.txt"

_installing_lock = threading.Lock()
_in_flight_package_names = []


def _write_installing_package(package_name):
    temp_path = f"{INSTALLING_PACKAGE_PATH}.tmp"
    with open(temp_path, "w") as file:
        file.write(package_name or "")
    os.replace(temp_path, INSTALLING_PACKAGE_PATH)


def _watch_single_install(ordered_package_names, stop_event):
    # One uv command installs the whole batch, so per-package progress is
    # approximated the way the pre-1.6 worker did it: report the first
    # requested package whose dist-info hasn't appeared in the env dir yet.
    while not stop_event.wait(0.2):
        installed_names = set(env_dir_distributions())
        remaining = [
            name for name in ordered_package_names if name not in installed_names
        ]
        _write_installing_package(remaining[0] if remaining else None)


def _stage_package(item):
    index, package_name, requirement, staging_root = item
    stage_dir = os.path.join(staging_root, str(index))
    environment = os.environ.copy()
    environment.update(
        UV_CONCURRENT_BUILDS="1",
        UV_CONCURRENT_DOWNLOADS="1",
        UV_CONCURRENT_INSTALLS="1",
    )
    with _installing_lock:
        # Newest start owns the display: names cycle while the queue drains,
        # then the long build (pyspark) is the only one left showing.
        _in_flight_package_names.append(package_name)
        _write_installing_package(package_name)
    started_at = time.perf_counter()
    result = subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            "python",
            "--target",
            stage_dir,
            "--no-deps",
            requirement,
        ],
        capture_output=True,
        text=True,
        env=environment,
    )
    with _installing_lock:
        _in_flight_package_names.remove(package_name)
        _write_installing_package(
            _in_flight_package_names[-1] if _in_flight_package_names else None
        )
    return package_name, time.perf_counter() - started_at, result


def _remove_path(path):
    if os.path.islink(path) or not os.path.isdir(path):
        os.unlink(path)
    else:
        shutil.rmtree(path)


def _merge_stage(source_dir, destination_dir):
    os.makedirs(destination_dir, exist_ok=True)
    for entry in os.scandir(source_dir):
        source_path = entry.path
        destination_path = os.path.join(destination_dir, entry.name)
        if entry.is_symlink():
            if os.path.lexists(destination_path):
                _remove_path(destination_path)
            os.symlink(os.readlink(source_path), destination_path)
        elif entry.is_dir(follow_symlinks=False):
            destination_is_not_directory = os.path.islink(
                destination_path
            ) or not os.path.isdir(destination_path)
            if os.path.lexists(destination_path) and destination_is_not_directory:
                _remove_path(destination_path)
            _merge_stage(source_path, destination_path)
        else:
            if os.path.lexists(destination_path):
                _remove_path(destination_path)
            os.link(source_path, destination_path)


def install_client_environment(packages):
    _write_installing_package(None)  # residue from a prior install
    env_dir_dists = env_dir_distributions()
    packages_to_install = []
    packages_to_uninstall = []
    for package_name, requirement in packages.items():
        env_dir_dist = env_dir_dists.get(package_name)
        needs_install = False
        if env_dir_dist is None:
            # Not installed by burla, so if it's importable it is baked into
            # the image. Reinstalling would replace CUDA or ABI-pinned wheels.
            try:
                importlib.metadata.version(package_name)
            except importlib.metadata.PackageNotFoundError:
                needs_install = True
        elif " @ " in requirement:
            requested_spec = requirement.split(" @ ", 1)[1]
            needs_install = env_dir_dist[1] != requested_spec
        else:
            requested_version = requirement.split("==", 1)[1]
            needs_install = env_dir_dist[1] or env_dir_dist[0] != requested_version
        if needs_install:
            packages_to_install.append((package_name, requirement))
            if env_dir_dist is not None:
                packages_to_uninstall.append(package_name)

    # PySpark's source build is this environment's critical path. Starting it
    # first with two neighboring installs minimized wall time without starving
    # the build of CPU, disk, or network bandwidth.
    packages_to_install.sort(key=lambda item: item[0] != "pyspark")
    total_started_at = time.perf_counter()
    metrics = {
        "requested_packages": len(packages),
        "staged_packages": len(packages_to_install),
        "install_workers": min(3, len(packages_to_install)),
    }
    if not packages_to_install:
        metrics["install_mode"] = "cached"
        metrics.update(stage_seconds=0, uninstall_seconds=0, merge_seconds=0)
        metrics["total_seconds"] = time.perf_counter() - total_started_at
        return metrics

    if "pyspark" not in {name for name, _ in packages_to_install}:
        single_install_started_at = time.perf_counter()
        stop_event = threading.Event()
        watcher_thread = threading.Thread(
            target=_watch_single_install,
            args=([name for name, _ in packages_to_install], stop_event),
            daemon=True,
        )
        _write_installing_package(packages_to_install[0][0])
        watcher_thread.start()
        try:
            result = subprocess.run(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    "python",
                    "--target",
                    "/worker_service_python_env",
                    "--no-deps",
                    *(requirement for _, requirement in packages_to_install),
                ],
                capture_output=True,
                text=True,
            )
        finally:
            stop_event.set()
            watcher_thread.join()
            _write_installing_package(None)
        if result.returncode:
            raise RuntimeError(
                "uv failed to install the client environment "
                f"(exit status {result.returncode}).\n\n"
                f"{_subprocess_output(result)}"
            )
        importlib.invalidate_caches()
        metrics["install_mode"] = "single"
        metrics["install_workers"] = 1
        metrics.update(
            stage_seconds=time.perf_counter() - single_install_started_at,
            uninstall_seconds=0,
            merge_seconds=0,
        )
        metrics["total_seconds"] = time.perf_counter() - total_started_at
        return metrics

    metrics["install_mode"] = "staged"
    staging_root = "/worker_service_storage/package_staging"
    shutil.rmtree(staging_root, ignore_errors=True)
    os.makedirs(staging_root)
    try:
        stage_started_at = time.perf_counter()
        items = [
            (index, package_name, requirement, staging_root)
            for index, (package_name, requirement) in enumerate(packages_to_install)
        ]
        with ThreadPoolExecutor(max_workers=metrics["install_workers"]) as executor:
            staged = list(executor.map(_stage_package, items))
        metrics["stage_seconds"] = time.perf_counter() - stage_started_at

        failed = [item for item in staged if item[2].returncode]
        if failed:
            failures = "\n\n".join(
                f"{package_name} (exit status {result.returncode}):\n"
                f"{_subprocess_output(result)}"
                for package_name, _, result in failed
            )
            raise RuntimeError(f"uv failed to stage the client environment.\n\n{failures}")

        metrics["slowest_packages"] = [
            {"name": package_name, "seconds": round(seconds, 3)}
            for package_name, seconds, _ in sorted(
                staged, key=lambda item: item[1], reverse=True
            )[:5]
        ]

        uninstall_started_at = time.perf_counter()
        if packages_to_uninstall:
            result = subprocess.run(
                [
                    "uv",
                    "pip",
                    "uninstall",
                    "--system",
                    "--target",
                    "/worker_service_python_env",
                    *packages_to_uninstall,
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode:
                raise RuntimeError(
                    "uv failed to remove replaced packages "
                    f"(exit status {result.returncode}).\n\n"
                    f"{_subprocess_output(result)}"
                )
        metrics["uninstall_seconds"] = time.perf_counter() - uninstall_started_at

        merge_started_at = time.perf_counter()
        for index in range(len(packages_to_install)):
            _merge_stage(
                os.path.join(staging_root, str(index)),
                "/worker_service_python_env",
            )
        metrics["merge_seconds"] = time.perf_counter() - merge_started_at
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
        _write_installing_package(None)

    importlib.invalidate_caches()
    metrics["total_seconds"] = time.perf_counter() - total_started_at
    return metrics


def load_function_payload(payload):
    if not payload.startswith(FUNCTION_PAYLOAD_MAGIC):
        return cloudpickle.loads(payload), [], None

    module_names, module_sources, function_pkl = pickle.loads(
        payload[len(FUNCTION_PAYLOAD_MAGIC) :]
    )
    digest = hashlib.sha256(module_sources).hexdigest()
    module_path = f"/worker_service_storage/local-modules-{digest}.zip"
    # Workers share this volume but not a PID namespace, so os.getpid() here
    # collides across concurrently-assigned workers (two PID-15s racing the
    # same temp file, observed as FileNotFoundError from os.replace).
    temporary_path = f"{module_path}.{uuid4().hex}"
    with open(temporary_path, "wb") as output:
        output.write(module_sources)
    os.replace(temporary_path, module_path)
    sys.path.insert(0, module_path)
    importlib.invalidate_caches()
    return cloudpickle.loads(function_pkl), module_names, module_path


# Become a session + process group leader so the node_service can kill this worker together with
# any subprocess the user's UDF spawned via a single os.killpg from the host. Runs after uv/pip
# setup so subprocess.run above still inherits the container's original session cleanly.
os.setsid()

port = int(sys.argv[1])
with socket.create_server(("0.0.0.0", port)) as listener:
    connection, _ = listener.accept()
    with connection:
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        loaded_function = None
        local_module_names = []
        local_module_path = None
        ping = receive_exactly(connection, 1)
        connection.sendall(ping)
        while True:
            command = receive_exactly(connection, 1)
            if not command:
                break
            payload_size = int.from_bytes(receive_exactly(connection, 8), "big")
            request_payload = receive_exactly(connection, payload_size)

            response_payload = b""
            try:
                if command == b"r":
                    kill_all_other_processes()
                    loaded_function = None
                    for module_name in local_module_names:
                        sys.modules.pop(module_name, None)
                    if local_module_path is not None:
                        sys.path.remove(local_module_path)
                    local_module_names = []
                    local_module_path = None
                    importlib.invalidate_caches()
                    # Worker process persists across jobs; otherwise cached
                    # creds from a prior nested RPM leak to the next user.
                    auth_module = sys.modules.get("burla._auth")
                    if auth_module is not None:
                        auth_module._get_auth_info.cache_clear()
                if command == b"i":
                    # {canonical dist name: uv requirement string}, the
                    # client's entire environment, pinned exactly.
                    packages = pickle.loads(request_payload)
                    response_payload = pickle.dumps(
                        install_client_environment(packages)
                    )
                if command == b"l":
                    loaded_function, local_module_names, local_module_path = (
                        load_function_payload(request_payload)
                    )
                if command == b"c":
                    request = pickle.loads(request_payload)
                    input_index = request["input_index"]
                    argument = cloudpickle.loads(request["argument_bytes"])
                    try:
                        print(f"{LOG_START_MARKER_PREFIX}{input_index}", flush=True)
                        return_value = loaded_function(argument)
                    finally:
                        print(f"{LOG_END_MARKER_PREFIX}{input_index}", flush=True)
                    response_payload = cloudpickle.dumps(return_value)
            except BaseException as e:
                tb_dict = Traceback(e.__traceback__).to_dict()
                error_info = dict(type=type(e), exception=e, traceback_dict=tb_dict)
                response_payload = pickle.dumps(
                    {
                        "error_info_pkl": pickle.dumps(error_info),
                        "traceback_str": "".join(
                            traceback.format_exception(type(e), e, e.__traceback__)
                        ),
                    }
                )
                status = b"e"
            else:
                status = b"s"
            response_size = len(response_payload).to_bytes(8, "big")
            connection.sendall(status + response_size + response_payload)
