import os
from fire import Fire


# needed so main_service can associate a client version with a request
__version__ = "0.9.14"
_BURLA_BACKEND_URL = "https://backend.burla.dev"

from burla._auth import login
from burla._install import install
from burla._remote_parallel_map import remote_parallel_map


def init_cli():
    Fire({"login": login, "install": install, "version": lambda: print(__version__)})
