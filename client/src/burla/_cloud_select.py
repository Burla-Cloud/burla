"""
Picks which cloud Burla uses when the user never chose one.

Called by `get_cloud()` only when `BURLA_CLOUD` and the saved config are both
absent. Detects which cloud CLIs are installed: exactly one means use it
silently, several means ask once and save the answer to the config file.
"""

import shutil
import sys

_CLOUDS = [("aws", "AWS", "aws"), ("azure", "Azure", "az"), ("gcp", "GCP", "gcloud")]


def installed_clouds() -> list[tuple[str, str]]:
    """[(cloud, label)] for every cloud whose CLI is installed."""
    return [
        (cloud, label) for cloud, label, executable in _CLOUDS if shutil.which(executable)
    ]


def choose_cloud() -> str:
    from burla import set_config
    from burla._auth import LocalHeadError

    options = installed_clouds()
    if not options:
        raise LocalHeadError(
            "No cloud CLI is installed. Install the CLI for the cloud Burla should use:\n"
            "  AWS:    https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html\n"
            "  Azure:  https://learn.microsoft.com/cli/azure/install-azure-cli\n"
            "  GCP:    https://cloud.google.com/sdk/docs/install"
        )
    if len(options) == 1:
        return options[0][0]

    cannot_ask = LocalHeadError(
        "Multiple cloud CLIs are installed "
        f"({', '.join(cloud for cloud, _ in options)}) and none is selected. "
        "Run `burla config set cloud <aws|gcp|azure>` or set the BURLA_CLOUD "
        "environment variable."
    )
    if not sys.stdin.isatty():
        raise cannot_ask

    print("Multiple cloud CLIs are installed. Which cloud should Burla use?")
    for number, (_, label) in enumerate(options, start=1):
        print(f"  {number}) {label}")
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
