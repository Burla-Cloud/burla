###
# Important: This file MUST be located adjecent to the node.py file.
# It's mounted into the container at runtim at
###
import importlib
import importlib.metadata
import io
import json
import os
import re
import signal
import sys
import pickle
import shutil
import socket
import subprocess
import tarfile
import traceback
import urllib.request

# Do not move. Node assumes first line printed is the Python version.
print(f"{sys.version_info.major}.{sys.version_info.minor}", flush=True)

if sys.platform != "linux":
    raise RuntimeError("Worker container must be Linux.")

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
                    # Worker process persists across jobs; otherwise cached
                    # creds from a prior nested RPM leak to the next user.
                    auth_module = sys.modules.get("burla._auth")
                    if auth_module is not None:
                        auth_module._get_auth_info.cache_clear()
                if command == b"i":
                    # {canonical dist name: uv requirement string}, the
                    # client's entire environment, pinned exactly.
                    packages = pickle.loads(request_payload)
                    env_dir_dists = env_dir_distributions()
                    packages_to_install = []
                    for package_name, requirement in packages.items():
                        env_dir_dist = env_dir_dists.get(package_name)
                        if env_dir_dist is None:
                            # Not installed by burla, so if it's importable it
                            # is baked into the image. Reinstalling to match
                            # the client's version would silently replace
                            # pre-baked GPU wheels (CUDA-built torch,
                            # ABI-pinned numpy) with incompatible ones from
                            # PyPI's default index.
                            try:
                                importlib.metadata.version(package_name)
                            except importlib.metadata.PackageNotFoundError:
                                packages_to_install.append(requirement)
                        elif " @ " in requirement:
                            requested_spec = requirement.split(" @ ", 1)[1]
                            if env_dir_dist[1] != requested_spec:
                                packages_to_install.append(requirement)
                        else:
                            requested_version = requirement.split("==", 1)[1]
                            version_matches = env_dir_dist[0] == requested_version
                            if env_dir_dist[1] or not version_matches:
                                packages_to_install.append(requirement)
                    if packages_to_install:
                        # --no-deps: the client's list is its whole environment,
                        # so it is already closed under dependencies; resolving
                        # would only let uv "upgrade" pre-baked image packages.
                        subprocess.run(
                            [
                                "uv",
                                "pip",
                                "install",
                                "--python",
                                "python",
                                "--target",
                                "/worker_service_python_env",
                                "--no-deps",
                                *packages_to_install,
                            ],
                            check=True,
                        )
                    importlib.invalidate_caches()
                if command == b"l":
                    loaded_function = cloudpickle.loads(request_payload)
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
