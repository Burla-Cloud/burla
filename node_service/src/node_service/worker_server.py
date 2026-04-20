###
# Important: This file MUST be located adjecent to the node.py file.
# It's mounted into the container at runtim at
###
import importlib
import importlib.metadata
import io
import os
import signal
import sys
import pickle
import shutil
import socket
import subprocess
import tarfile
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
    import cloudpickle
    from tblib import Traceback
except ImportError:
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            "python",
            "--target",
            "/worker_service_python_env",
            "cloudpickle",
            "tblib",
        ],
        check=True,
    )
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
                if command == b"i":
                    packages = pickle.loads(request_payload)
                    missing_packages = []
                    for package_name, version in packages.items():
                        try:
                            installed_version = importlib.metadata.version(package_name)
                        except importlib.metadata.PackageNotFoundError:
                            installed_version = None
                        if installed_version != version:
                            missing_packages.append(f"{package_name}=={version}")
                    if missing_packages:
                        subprocess.run(
                            [
                                "uv",
                                "pip",
                                "install",
                                "--python",
                                "python",
                                "--target",
                                "/worker_service_python_env",
                                *missing_packages,
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
            except Exception as e:
                tb_dict = Traceback(e.__traceback__).to_dict()
                response_payload = pickle.dumps(
                    {"error_info": dict(type=type(e), exception=e, traceback_dict=tb_dict)}
                )
                status = b"e"
            else:
                status = b"s"
            response_size = len(response_payload).to_bytes(8, "big")
            connection.sendall(status + response_size + response_payload)
