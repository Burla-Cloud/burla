import webbrowser
from fire import Fire


# needed so main_service can associate a client version with a request
__version__ = "1.0.12"
_BURLA_BACKEND_URL = "https://backend.burla.dev"

from burla._auth import login, dashboard
from burla._install import install
from burla._remote_parallel_map import remote_parallel_map


def version():
    """Print current Burla client version."""
    print(__version__)


def init_cli():
    Fire(
        {
            "login": login,
            "install": install,
            "dashboard": dashboard,
            "--version": version,
            "-v": version,
        }
    )
