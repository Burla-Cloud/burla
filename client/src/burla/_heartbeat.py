import asyncio
import subprocess
import sys
import tempfile
import textwrap
from asyncio import create_task
from time import time

import aiohttp
import cloudpickle

from burla._auth import get_auth_headers
from burla._cluster_client import ClusterClient


async def run_in_subprocess(func, *args):
    # I do it like this so it works in google colab, multiprocesing doesn't
    code = textwrap.dedent(
        """
        import sys, cloudpickle
        func, args = cloudpickle.load(sys.stdin.buffer)
        func(*args)
        """
    )
    cmd = [sys.executable, "-u", "-c", code]
    stderr_buffer = tempfile.TemporaryFile()
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=stderr_buffer)
    process.stderr_buffer = stderr_buffer
    process.stdin.write(cloudpickle.dumps((func, args)))
    process.stdin.close()
    return process


async def _send_node_pings(session: aiohttp.ClientSession, node_host: str, headers: dict):
    while True:
        try:
            url = f"{node_host}/client-heartbeat"
            async with session.post(url, data=b".", timeout=20, headers=headers):
                # ignore all error responses.
                pass
        except Exception:
            pass
        await asyncio.sleep(0.5)


async def send_alive_pings_async(node_hosts: list[str], job_id: str):
    auth_headers = get_auth_headers()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for node_host in node_hosts:
            tasks.append(create_task(_send_node_pings(session, node_host, auth_headers)))

        # Each PATCH bumps the job doc's `update_time`, which the node's
        # `_on_job_snapshot` watches to detect a dead client. Interval
        # leaves headroom under JOB_DOC_CONTACT_TIMEOUT_SEC for a slow PATCH.
        client = ClusterClient(session)
        while True:
            for task in tasks:
                if task.done() and task.exception():
                    raise task.exception()

            try:
                await client.patch_job(job_id, {"client_heartbeat_at": time()})
            except Exception as error:
                # Don't let a transient main_service failure (5xx, network
                # blip, brief cold start) kill this subprocess - the parent
                # polling loop checks `ping_process.poll()` and would fail
                # the whole job. The per-node direct `/client-heartbeat`
                # path (above) is still running; if main_service is gone
                # for longer than JOB_DOC_CONTACT_TIMEOUT_SEC the node's
                # own liveness check will detect it.
                print(f"heartbeat PATCH failed, retrying: {error}", file=sys.stderr)
            await asyncio.sleep(2)


def send_alive_pings(node_hosts: list[str], job_id: str):
    asyncio.run(send_alive_pings_async(node_hosts, job_id))
