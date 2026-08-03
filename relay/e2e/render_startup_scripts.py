"""Renders the node/head startup scripts in relay mode and bash-syntax
checks them. Run by run_e2e.sh: `... node` in a main_service venv and
`... head` in a client venv (their dependency pins conflict)."""

import base64
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PART = sys.argv[1]  # "node" or "head"
tmp = tempfile.mkdtemp(prefix="burla-relay-e2e-")

os.environ["PROJECT_ID"] = "test-project"
os.environ["CLUSTER_ID_TOKEN"] = "test-token"
os.environ["MAIN_SERVICE_URL_FOR_NODES"] = "https://10.0.0.2:8443"
os.environ["BURLA_TLS_DIR"] = f"{tmp}/tls"
os.environ["HISTORY_DB_PATH"] = f"{tmp}/history.db"
os.environ["BURLA_RELAY_HOST"] = "relay.burla.dev"


def bash_check(name: str, script: str):
    path = Path(tmp) / name
    path.write_text(script)
    subprocess.run(["bash", "-n", str(path)], check=True)
    print(f"bash -n OK: {name}")


if PART == "node":
    os.chdir(REPO_ROOT / "main_service")  # static file paths are relative
    sys.path.insert(0, str(REPO_ROOT / "main_service" / "src"))
    from main_service.node import Container, Node

    node = Node.__new__(Node)
    node.instance_name = "burla-node-1a2b3c4d"
    node.port = 8080
    node.sync_bucket_name = None
    node.num_gpus = 0
    node.containers = [Container(image="python:3.12")]
    node.inactivity_shutdown_time_sec = 600
    node.reserved_for_job = None
    node_script = node._Node__get_startup_script()

    assert "frpc" in node_script, "node script is missing the frpc tunnel block"
    assert 'subdomain = "burla-node-1a2b3c4d--test-project"' in node_script
    assert "\nFRPC_EOF" in node_script, "heredoc terminator is not at column 0"
    bash_check("node_startup.sh", node_script)

elif PART == "head":
    sys.path.insert(0, str(REPO_ROOT / "client" / "src"))
    from burla._deploy import _head_startup_script

    head_script = _head_startup_script(
        "test-project",
        "test-token",
        "head--test-project.relay.burla.dev",
    )
    assert "burla-head-frpc" in head_script
    assert "uvicorn main_service:app" in head_script
    assert "pip install --no-cache-dir" in head_script
    encoded_config = re.search(
        r'echo "([^"]+)" \| base64 -d > /etc/burla/frpc.toml', head_script
    ).group(1)
    frpc_config = base64.b64decode(encoded_config).decode()
    assert 'subdomain = "head--test-project"' in frpc_config
    assert "chmod 600 /etc/burla/frpc.toml" in head_script
    assert 'BURLA_RELAY_HOST="relay.burla.dev"' in head_script
    assert "-p 80:80" not in head_script, "head must not publish public ports"
    bash_check("head_startup.sh", head_script)

print(f"startup script rendering OK ({PART})")
