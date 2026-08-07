"""
Scenario 6: input-steal HTTP contract between nodes.

No test in the suite touches /jobs/{id}/get_inputs or /ack_transfer
despite `_input_steal_loop` being a core burla subsystem. Here we run
a real job and manually execute one steal from node A to node B using
the same HTTP calls the loop would use. If the contract is right, the
job completes with all results even though we hijacked some inputs.
"""

from __future__ import annotations

import pickle
import ssl
import threading
import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def _node_port(host: str) -> str:
    # `http://node_xxx:8080` -> `8080`; `https://1.2.3.4:8080` -> `8080`.
    return host.rsplit(":", 1)[-1]


def _local_url(host: str) -> str:
    # local-dev node hosts are docker network names (`http://node_xxx:8080`),
    # reachable from the VM at localhost:<port>. Remote nodes advertise a
    # public IP that is directly reachable.
    if "node_" not in host:
        return host
    port = _node_port(host)
    return f"http://localhost:{port}"


@pytest.mark.timeout(600)
def test_input_steal_between_nodes(
    rpm_subprocess,
    local_dev_cluster,
    cluster_with_n_nodes,
    main_http_client,
    burla_auth_headers,
    wait_for_fixture,
):
    import httpx

    # Stealing is only meaningful between two nodes.
    cluster_with_n_nodes(2)
    state = main_http_client.get("/v1/cluster/state").json()

    # Sleep-heavy UDF so the inputs queue stays deep while we poke
    # endpoints. 20 inputs * 15s / 4 worker slots = 75s — plenty of time.
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    time.sleep(15)\n"
        "    return x\n"
    )
    n_inputs = 20

    result_box: dict = {}

    def _run():
        result_box["result"] = rpm_subprocess(
            source, list(range(n_inputs)), timeout_seconds=300, grow=False
        )

    rpm_thread = threading.Thread(target=_run, daemon=True)
    rpm_thread.start()
    node_client = None

    try:
        # Wait until two nodes are both RUNNING the same job (both got the POST /jobs/{id}).
        def _two_active_nodes():
            current = main_http_client.get("/v1/cluster/state").json()
            ready_and_running = [
                n for n in current.get("ready_nodes", []) if n.get("current_job")
            ]
            if len(ready_and_running) >= 2:
                return ready_and_running
            # ready_nodes only lists READY nodes; nodes already flipped to
            # RUNNING show up in the full live-node list instead.
            all_nodes = main_http_client.get("/v1/cluster/nodes").json()["nodes"]
            running = [
                n
                for n in all_nodes
                if n.get("status") == "RUNNING" and n.get("current_job")
            ]
            return running if len(running) >= 2 else None

        nodes = wait_for_fixture(_two_active_nodes, timeout=60)
        job_id = nodes[0]["current_job"]
        assert all(
            n["current_job"] == job_id for n in nodes[:2]
        ), "Expected both nodes on the same job, got mixed assignments"
        node_a, node_b = nodes[0], nodes[1]
        url_a = _local_url(node_a["host"])
        url_b = _local_url(node_b["host"])
        verify = (
            ssl.create_default_context(cadata=state["cluster_ca"])
            if url_a.startswith("https://")
            else True
        )
        node_client = httpx.Client(
            headers=burla_auth_headers,
            timeout=10,
            verify=verify,
        )

        # Give both nodes a moment to upload inputs so A's queue is non-empty.
        time.sleep(5)

        transfer_id = "test-steal-t1"

        # 1. Steal a batch from A.
        resp_a1 = node_client.get(
            f"{url_a}/jobs/{job_id}/get_inputs",
            params={"transfer_id": transfer_id, "requester_queue_size": 0},
        )
        assert resp_a1.status_code == 200, resp_a1.text
        items = pickle.loads(resp_a1.content)
        assert isinstance(items, list)

        # If A's queue was empty at the moment we asked (other node ran fast), skip.
        if not items:
            pytest.skip("node A's inputs_queue was empty at steal-time; race, retry")

        # 2. Idempotency: same transfer_id returns same batch.
        resp_a2 = node_client.get(
            f"{url_a}/jobs/{job_id}/get_inputs",
            params={"transfer_id": transfer_id, "requester_queue_size": 0},
        )
        items2 = pickle.loads(resp_a2.content)
        assert (
            items == items2
        ), "get_inputs with the same transfer_id must be idempotent"

        # 3. Hand-carry to B via POST /jobs/{id}/inputs.
        payload = pickle.dumps(items)
        resp_b = node_client.post(
            f"{url_b}/jobs/{job_id}/inputs",
            files={"inputs_pkl_with_idx": ("inputs", payload)},
        )
        assert resp_b.status_code == 200, resp_b.text

        # 4. Ack A with received=true so A discards the batch.
        resp_ack = node_client.post(
            f"{url_a}/jobs/{job_id}/ack_transfer",
            params={"transfer_id": transfer_id, "received": "true"},
        )
        assert resp_ack.status_code == 200

        # 5. Ack again — idempotent (pending_transfers already popped).
        resp_ack2 = node_client.post(
            f"{url_a}/jobs/{job_id}/ack_transfer",
            params={"transfer_id": transfer_id, "received": "true"},
        )
        assert resp_ack2.status_code == 200
    finally:
        if node_client is not None:
            node_client.close()
        rpm_thread.join(timeout=300)

    assert "result" in result_box, "rpm thread never stored a result"
    result = result_box["result"]
    assert result["ok"], result.get("traceback")
    assert len(result["outputs"]) == n_inputs
    assert set(result["outputs"]) == set(
        range(n_inputs)
    ), "Manual steal lost or duplicated an input"
