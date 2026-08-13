"""
Picks which cloud Burla uses when the user never chose one.

Called by `get_cloud()` only when `BURLA_CLOUD` and the saved config are both
absent. Detects which cloud CLIs are signed in: exactly one means use it
silently, several means ask once and save the answer to the config file.
"""

import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor


def _cli_output(command: list[str]) -> str | None:
    result = subprocess.run(command, capture_output=True, text=True)
    output = result.stdout.strip()
    return output if result.returncode == 0 and output else None


def _aws_detail() -> str | None:
    if not shutil.which("aws"):
        return None
    account = _cli_output(
        ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"]
    )
    return f"account {account}" if account else None


def _azure_detail() -> str | None:
    if not shutil.which("az"):
        return None
    subscription = _cli_output(
        ["az", "account", "show", "--query", "name", "--output", "tsv"]
    )
    return f'subscription "{subscription}"' if subscription else None


def _gcp_detail() -> str | None:
    if not shutil.which("gcloud"):
        return None
    account = _cli_output(
        ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"]
    )
    if not account:
        return None
    project = _cli_output(["gcloud", "config", "get-value", "project"])
    if not project or project == "(unset)":
        return None
    return f"project {project}"


_DETECTORS = [
    ("aws", "AWS", _aws_detail),
    ("azure", "Azure", _azure_detail),
    ("gcp", "GCP", _gcp_detail),
]


def signed_in_clouds() -> list[tuple[str, str, str]]:
    """[(cloud, label, detail)] for every cloud CLI that is signed in."""
    # Parallel because each CLI call takes about a second.
    with ThreadPoolExecutor(max_workers=len(_DETECTORS)) as pool:
        details = list(pool.map(lambda detector: detector[2](), _DETECTORS))
    return [
        (cloud, label, detail)
        for (cloud, label, _), detail in zip(_DETECTORS, details)
        if detail
    ]


def choose_cloud() -> str:
    from burla import set_config
    from burla._local_head import LocalHeadError

    options = signed_in_clouds()
    if not options:
        raise LocalHeadError(
            "No cloud CLI is signed in. Sign in to the cloud Burla should use:\n"
            "  AWS:    `aws configure` or `aws sso login`\n"
            "  Azure:  `az login`\n"
            "  GCP:    `gcloud auth login` and `gcloud config set project <id>`"
        )
    if len(options) == 1:
        return options[0][0]

    cannot_ask = LocalHeadError(
        "You're signed in to multiple cloud providers "
        f"({', '.join(cloud for cloud, _, _ in options)}) and none is selected. "
        "Run `burla config set cloud <aws|gcp|azure>` or set the BURLA_CLOUD "
        "environment variable."
    )
    if not sys.stdin.isatty():
        raise cannot_ask

    print("You're signed in to multiple cloud providers. Which should Burla use?")
    for number, (_, label, detail) in enumerate(options, start=1):
        print(f"  {number}) {label:<7}{detail}")
    valid_answers = {str(n) for n in range(1, len(options) + 1)}
    answer = ""
    while answer not in valid_answers:
        try:
            answer = input(f"Choose [1-{len(options)}]: ").strip()
        except EOFError:
            # A pty whose input has closed (observed with `script`) can't answer.
            raise cannot_ask from None

    cloud = options[int(answer) - 1][0]
    set_config("cloud", cloud)
    print(f"Using {cloud}. Change anytime with `burla config set cloud <aws|gcp|azure>`.")
    return cloud
