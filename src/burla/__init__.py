import tomli
import pathlib
from fire import Fire

pyproject_path = pathlib.Path(__file__).resolve().parent.parent.parent / "pyproject.toml"
pyproject_config = tomli.loads(pyproject_path.read_text()) if pyproject_path.exists() else {}
IN_DEV = pyproject_config.get("tool", {}).get("burla", {}).get("config", {}).get("in_dev", False)

if IN_DEV:
    _BURLA_SERVICE_URL = "http://127.0.0.1:5001"
    # _BURLA_BACKEND_URL = "http://127.0.0.1:5002"

    # _BURLA_SERVICE_URL = "https://cluster.test.burla.dev"
    _BURLA_BACKEND_URL = "https://backend.test.burla.dev"

    _BURLA_JOBS_BUCKET = "burla-jobs"
    _BURLA_GCP_PROJECT = "burla-test"
else:
    _BURLA_SERVICE_URL = "https://cluster.burla.dev"
    _BURLA_BACKEND_URL = "https://backend.burla.dev"
    _BURLA_JOBS_BUCKET = "burla-jobs-prod"
    _BURLA_GCP_PROJECT = "burla-prod"

INPUTS_TOPIC_PATH = f"projects/{_BURLA_GCP_PROJECT}/topics/burla_job_inputs"
OUTPUTS_SUBSCRIPTION_PATH = f"projects/{_BURLA_GCP_PROJECT}/subscriptions/burla_job_outputs"
LOGS_SUBSCRIPTION_PATH = f"projects/{_BURLA_GCP_PROJECT}/subscriptions/burla_job_logs"

__version__ = "0.1.3-alpha"

from burla._auth import login, login_cmd as _login_cmd
from burla._remote_parallel_map import remote_parallel_map


def init_cli():
    Fire({"login": _login_cmd})
