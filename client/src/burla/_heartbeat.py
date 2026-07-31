import asyncio
import subprocess
import ssl
import sys
import tempfile
import textwrap
from asyncio import create_task

import aiohttp
import cloudpickle

from burla._auth import get_auth_headers


async def run_in_subprocess(func, *args):
    # I do it like this so it works in google colab, multiprocesing doesn't
    code = textwrap.dedent("""
        import sys, cloudpickle
        func, args = cloudpickle.load(sys.stdin.buffer)
        func(*args)
        """)
    cmd = [sys.executable, "-u", "-c", code]
    stderr_buffer = tempfile.TemporaryFile()
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=stderr_buffer)
    process.stderr_buffer = stderr_buffer
    process.stdin.write(cloudpickle.dumps((func, args)))
    process.stdin.close()
    return process


async def _send_node_pings(
    session: aiohttp.ClientSession, node_host: str, headers: dict
):
    while True:
        try:
            url = f"{node_host}/client-heartbeat"
            async with session.post(url, data=b".", timeout=20, headers=headers):
                # ignore all error responses.
                pass
        except Exception:
            pass
        await asyncio.sleep(0.5)


async def send_alive_pings_async(node_hosts: list[str], cluster_ca: str | None):
    """Direct per-node liveness pings, run in a subprocess so they keep
    beating while the main process is blocked in user code. Nodes report
    their contact flag to the head, which answers the disconnect-quorum
    question, so this is the only liveness channel."""
    auth_headers = get_auth_headers()
    ssl_context = ssl.create_default_context(cadata=cluster_ca) if cluster_ca else None
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for node_host in node_hosts:
            tasks.append(
                create_task(_send_node_pings(session, node_host, auth_headers))
            )

        while True:
            for task in tasks:
                if task.done() and task.exception():
                    raise task.exception()
            await asyncio.sleep(2)


def send_alive_pings(node_hosts: list[str], cluster_ca: str | None):
    asyncio.run(send_alive_pings_async(node_hosts, cluster_ca))
