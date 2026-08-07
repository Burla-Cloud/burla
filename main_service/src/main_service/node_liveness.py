"""
Is a node that stopped pushing state actually gone?

Silence is not death. A node running an intense workload can starve
node_service badly enough that it stops pushing state for minutes while the
user's function keeps running on it, so treating silence as failure kills
healthy jobs. The only evidence that settles it is the cloud's: if the VM is
gone (preempted, terminated, deleted), the job on it is gone with it.

Anything inconclusive (provider error, missing credentials) counts as alive:
the cost of waiting on a dead node is a slow failure, the cost of the reverse
is killing a job that was fine.
"""

from time import time

from main_service.providers import get_provider

# The client polls per unreachable node, so cache long enough that a stuck
# node doesn't turn into a cloud API call per poll.
_CACHE_TTL_SEC = 10

_cache: dict[str, tuple[float, bool]] = {}


def instance_is_gone(instance_name: str) -> bool:
    cached = _cache.get(instance_name)
    if cached and time() - cached[0] < _CACHE_TTL_SEC:
        return cached[1]
    try:
        gone = not get_provider().instance_exists(instance_name)
    except Exception:
        gone = False
    _cache[instance_name] = (time(), gone)
    return gone
