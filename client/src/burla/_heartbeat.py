import asyncio
import subprocess
import sys
import textwrap
from asyncio import create_task
from time import time

import aiohttp
import cloudpickle

from burla._auth import get_auth_headers
from burla._helpers import SuppressNativeStderr, get_db_clients


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
    with SuppressNativeStderr():
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
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

        # important to get this after tasks have started, not before, because it takes a sec.
        job_doc = get_db_clients()[1].collection("jobs").document(job_id)

        while True:
            for task in tasks:
                if task.done() and task.exception():
                    raise task.exception()

            await job_doc.update({"client_heartbeat_at": time()})
            await asyncio.sleep(3)


def send_alive_pings(node_hosts: list[str], job_id: str):
    asyncio.run(send_alive_pings_async(node_hosts, job_id))
