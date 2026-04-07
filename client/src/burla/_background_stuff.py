import requests
from time import sleep

from burla._auth import get_auth_headers


def _ping_generator():
    while True:
        yield b"."
        sleep(0.5)


def send_alive_pings(node_hosts: list[str]):
    """Must run in a separate process so it is not blocked by client CPU spikes."""
    current_node_index = 0
    auth_headers = get_auth_headers()
    with requests.Session() as session:
        session.headers.update(auth_headers)
        while True:
            try:
                url = f"{node_hosts[current_node_index]}/client-heartbeat"
                with session.post(url, data=_ping_generator(), timeout=(2, None)) as response:
                    if response.status_code in [404, 410]:
                        sleep(0.2)
                        continue
                    response.raise_for_status()
            except requests.exceptions.RequestException:
                current_node_index = (current_node_index + 1) % len(node_hosts)
