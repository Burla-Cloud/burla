import requests
from time import sleep

from burla._auth import get_auth_headers

HEARTBEAT_INTERVAL_SECONDS = 0.5
HEARTBEAT_FAILURE_RETRY_SECONDS = 0.1
HEARTBEAT_TIMEOUT_SECONDS = 2

def _ping_node(session: requests.Session, node_host: str) -> bool:
    url = f"{node_host}/client-heartbeat"
    timeout = (HEARTBEAT_TIMEOUT_SECONDS, HEARTBEAT_TIMEOUT_SECONDS)
    try:
        response = session.post(url, data=b".", timeout=timeout)
    except requests.exceptions.RequestException:
        return False

    if response.status_code in [404, 410]:
        return False
    response.raise_for_status()
    return True


def send_alive_pings(node_hosts: list[str]):
    """Must run in a separate process so it is not blocked by client CPU spikes."""
    if not node_hosts:
        return

    current_node_index = 0
    auth_headers = get_auth_headers()
    with requests.Session() as session:
        session.headers.update(auth_headers)
        while True:
            found_reachable_node = False
            for _ in range(len(node_hosts)):
                node_host = node_hosts[current_node_index]
                current_node_index = (current_node_index + 1) % len(node_hosts)
                if _ping_node(session, node_host):
                    found_reachable_node = True
                    break

            if found_reachable_node:
                sleep(HEARTBEAT_INTERVAL_SECONDS)
            else:
                sleep(HEARTBEAT_FAILURE_RETRY_SECONDS)
