from fire import Fire

# Imported here to avoid cyclic imports
_BURLA_SERVICE_URL = "http://127.0.0.1:5001"
# _BURLA_BACKEND_URL = "http://127.0.0.1:5002"

# _BURLA_SERVICE_URL = "https://cluster.test.burla.dev"
_BURLA_BACKEND_URL = "https://backend.test.burla.dev"

# _BURLA_SERVICE_URL = "https://cluster.burla.dev"
# _BURLA_BACKEND_URL = "https://backend.burla.dev"

BURLA_JOBS_BUCKET = "burla-jobs"
# BURLA_JOBS_BUCKET = "burla-jobs-prod"

BURLA_GCP_PROJECT = "burla-test"
# BURLA_GCP_PROJECT = "burla-prod"

INPUTS_TOPIC_PATH = f"projects/{BURLA_GCP_PROJECT}/topics/burla_job_inputs"
OUTPUTS_SUBSCRIPTION_PATH = f"projects/{BURLA_GCP_PROJECT}/subscriptions/burla_job_outputs"
LOGS_SUBSCRIPTION_PATH = f"projects/{BURLA_GCP_PROJECT}/subscriptions/burla_job_logs"

__version__ = "0.1.2-alpha.1"

from burla._auth import login, login_cmd as _login_cmd
from burla._remote_parallel_map import remote_parallel_map


def init_cli():
    Fire({"login": _login_cmd})
