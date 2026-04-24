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
import threading
import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def _node_port(host: str) -> str:
    # `http://node_xxx:8081` -> `8081`; `http://1.2.3.4:8081` -> `8081`.
    return host.rsplit(":", 1)[-1]


def _local_url(host: str) -> str:
    # On the VM, nodes bind to the host on their docker port, reachable at localhost:<port>.
    port = _node_port(host)
    return f"http://localhost:{port}"


def test_input_steal_between_nodes(
    rpm_subprocess,
    local_dev_cluster,
    firestore_db,
    main_http_client,
    burla_auth_headers,
    wait_for_fixture,
):
    import httpx

    # Need at least 2 nodes for stealing to be meaningful.
    state = main_http_client.get("/v1/cluster/state").json()
    if len(state["ready_nodes"]) < 2:
        pytest.skip(f"need >=2 ready nodes, got {len(state['ready_nodes'])}")

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

    try:
        # Wait until two nodes are both RUNNING the same job (both got the POST /jobs/{id}).
        def _two_active_nodes():
            current = main_http_client.get("/v1/cluster/state").json()
            ready_and_running = [
                n for n in current.get("ready_nodes", [])
                if n.get("current_job")
            ]
            if len(ready_and_running) >= 2:
                return ready_and_running
            # Also check firestore directly in case cache is stale.
            docs = list(firestore_db.collection("nodes").stream())
            running = []
            for d in docs:
                data = d.to_dict() or {}
                if data.get("status") == "RUNNING" and data.get("current_job"):
                    running.append(data)
            return running if len(running) >= 2 else None

        nodes = wait_for_fixture(_two_active_nodes, timeout=60)
        job_id = nodes[0]["current_job"]
        assert all(n["current_job"] == job_id for n in nodes[:2]), (
            "Expected both nodes on the same job, got mixed assignments"
        )
        node_a, node_b = nodes[0], nodes[1]
        url_a = _local_url(node_a["host"])
        url_b = _local_url(node_b["host"])

        # Give both nodes a moment to upload inputs so A's queue is non-empty.
        time.sleep(5)

        transfer_id = "test-steal-t1"

        # 1. Steal a batch from A.
        resp_a1 = httpx.get(
            f"{url_a}/jobs/{job_id}/get_inputs",
            params={"transfer_id": transfer_id, "requester_queue_size": 0},
            headers=burla_auth_headers,
            timeout=10,
        )
        assert resp_a1.status_code == 200, resp_a1.text
        items = pickle.loads(resp_a1.content)
        assert isinstance(items, list)

        # If A's queue was empty at the moment we asked (other node ran fast), skip.
        if not items:
            pytest.skip("node A's inputs_queue was empty at steal-time; race, retry")

        # 2. Idempotency: same transfer_id returns same batch.
        resp_a2 = httpx.get(
            f"{url_a}/jobs/{job_id}/get_inputs",
            params={"transfer_id": transfer_id, "requester_queue_size": 0},
            headers=burla_auth_headers,
            timeout=10,
        )
        items2 = pickle.loads(resp_a2.content)
        assert items == items2, "get_inputs with the same transfer_id must be idempotent"

        # 3. Hand-carry to B via POST /jobs/{id}/inputs.
        payload = pickle.dumps(items)
        resp_b = httpx.post(
            f"{url_b}/jobs/{job_id}/inputs",
            files={"inputs_pkl_with_idx": ("inputs", payload)},
            headers=burla_auth_headers,
            timeout=10,
        )
        assert resp_b.status_code == 200, resp_b.text

        # 4. Ack A with received=true so A discards the batch.
        resp_ack = httpx.post(
            f"{url_a}/jobs/{job_id}/ack_transfer",
            params={"transfer_id": transfer_id, "received": "true"},
            headers=burla_auth_headers,
            timeout=10,
        )
        assert resp_ack.status_code == 200

        # 5. Ack again — idempotent (pending_transfers already popped).
        resp_ack2 = httpx.post(
            f"{url_a}/jobs/{job_id}/ack_transfer",
            params={"transfer_id": transfer_id, "received": "true"},
            headers=burla_auth_headers,
            timeout=10,
        )
        assert resp_ack2.status_code == 200
    finally:
        rpm_thread.join(timeout=300)

    assert "result" in result_box, "rpm thread never stored a result"
    result = result_box["result"]
    assert result["ok"], result.get("traceback")
    assert len(result["outputs"]) == n_inputs
    assert set(result["outputs"]) == set(range(n_inputs)), (
        "Manual steal lost or duplicated an input"
    )
