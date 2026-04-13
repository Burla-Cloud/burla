###
# Important: This file MUST be located adjecent to the node.py file.
# It's mounted into the container at runtim at
###
import io
import os
import sys
import pickle
import shutil
import socket
import subprocess
import tarfile
import urllib.request

# Do not move. Node assumes first line printed is the Python version.
print(f"{sys.version_info.major}.{sys.version_info.minor}", flush=True)

if sys.platform != "linux" or os.uname().machine not in ("x86_64", "amd64"):
    raise RuntimeError("Worker container must be Linux x86_64.")

# Install UV if missing
if not shutil.which("uv"):
    os.makedirs("/tmp/uv-bin", exist_ok=True)
    os.environ["PATH"] = f"/tmp/uv-bin:{os.environ['PATH']}"
    libc = "musl" if os.path.exists("/etc/alpine-release") else "gnu"
    target = f"x86_64-unknown-linux-{libc}"
    url = f"https://github.com/astral-sh/uv/releases/latest/download/uv-{target}.tar.gz"
    with urllib.request.urlopen(url) as response:
        with tarfile.open(fileobj=io.BytesIO(response.read()), mode="r:gz") as tarball:
            with tarball.extractfile(f"uv-{target}/uv") as uv_binary:
                with open("/tmp/uv-bin/uv", "wb") as output_file:
                    output_file.write(uv_binary.read())
    os.chmod("/tmp/uv-bin/uv", 0o755)

# Use UV to install dependencies
subprocess.run(["uv", "pip", "install", "--system", "cloudpickle", "tblib", "burla"], check=True)

import cloudpickle
from tblib import Traceback


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
                if command == b"l":
                    loaded_function = cloudpickle.loads(request_payload)
                if command == b"c":
                    argument = cloudpickle.loads(request_payload)
                    return_value = loaded_function(argument)
                    response_payload = cloudpickle.dumps(return_value)
            except Exception as e:
                tb_dict = Traceback(e.__traceback__).to_dict()
                response_dict = dict(type=type(e), exception=e, traceback_dict=tb_dict)
                response_payload = pickle.dumps(response_dict)
                status = b"e"
            else:
                status = b"s"
            response_size = len(response_payload).to_bytes(8, "big")
            connection.sendall(status + response_size + response_payload)
