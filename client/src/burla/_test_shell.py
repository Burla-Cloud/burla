"""`make <python-version>-dev`: a shell inside Burla's internal test environment.

Run as `python -m burla._test_shell` rather than a `burla` subcommand, so dev
entry points never show up in the CLI, not even from a source checkout.
"""

import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from burla import _IN_SOURCE_CHECKOUT, _SOURCE_ROOT


def _configure_test_shell_prompt(
    shell: str, environment: dict[str, str], startup_dir: Path, label: str
) -> list[str]:
    """Give the child shell a visible test prompt without changing dotfiles."""
    shell_name = Path(shell).name
    burla_bin_dir = str(Path(sys.executable).parent)
    burla_bin_dir_quoted = shlex.quote(burla_bin_dir)
    environment["PATH"] = f"{burla_bin_dir}{os.pathsep}{environment.get('PATH', '')}"
    if shell_name == "zsh":
        original_zdotdir = Path(
            environment.get("ZDOTDIR") or environment.get("HOME") or str(Path.home())
        ).expanduser()
        startup_dir_quoted = shlex.quote(str(startup_dir))
        original_zdotdir_quoted = shlex.quote(str(original_zdotdir))

        for filename in (".zshenv", ".zshrc"):
            original_file = original_zdotdir / filename
            source_original = ""
            if original_file.exists():
                source_original = (
                    f"export ZDOTDIR={original_zdotdir_quoted}\n"
                    f"source {shlex.quote(str(original_file))}\n"
                    f"export ZDOTDIR={startup_dir_quoted}\n"
                )
            (startup_dir / filename).write_text(source_original)

        with (startup_dir / ".zshrc").open("a") as zshrc:
            zshrc.write(
                f"\nexport PATH={burla_bin_dir_quoted}:$PATH\n"
                "rehash\n"
                "autoload -Uz add-zsh-hook\n"
                "_burla_test_prompt() {\n"
                f'  if [[ "$PROMPT" != *"[{label}]"* ]]; then\n'
                f"    PROMPT='%F{{yellow}}%B[{label}]%b%f '$PROMPT\n"
                "  fi\n"
                "}\n"
                "add-zsh-hook precmd _burla_test_prompt\n"
                "_burla_test_prompt\n"
            )
        environment["ZDOTDIR"] = str(startup_dir)
        return [shell, "-i"]

    if shell_name == "bash":
        original_bashrc = Path(environment.get("HOME") or str(Path.home())) / ".bashrc"
        bashrc = startup_dir / "bashrc"
        source_original = (
            f"source {shlex.quote(str(original_bashrc))}\n"
            if original_bashrc.exists()
            else ""
        )
        bashrc.write_text(
            source_original
            + f"export PATH={burla_bin_dir_quoted}:$PATH\n"
            + f"PS1='\\[\\e[33;1m\\][{label}]\\[\\e[0m\\] '$PS1\n"
        )
        return [shell, "--rcfile", str(bashrc), "-i"]

    environment["PS1"] = f"[{label}] {environment.get('PS1', '')}"
    return [shell, "-i"]


def test_shell():
    """Enter Burla's isolated internal test environment. Type `exit` to leave."""
    if not _IN_SOURCE_CHECKOUT:
        raise RuntimeError("Test mode requires an editable Burla source checkout.")

    environment = dict(os.environ)
    environment["BURLA_ENVIRONMENT"] = "test"
    for name in (
        "BURLA_BACKEND_URL",
        "BURLA_RELAY_HOST",
        "BURLA_RELAY_SERVER_ADDR",
        "BURLA_RELAY_SERVER_PORT",
        "BURLA_NODE_SOURCE_REF",
        "BURLA_CLUSTER_DASHBOARD_URL",
    ):
        environment.pop(name, None)

    shell = environment.get("SHELL") or shutil.which("zsh") or shutil.which("bash")
    if shell is None:
        raise RuntimeError("Test shell requires zsh or bash when SHELL is unset.")
    # Several of these run at once, on different interpreters, so the prompt
    # names the interpreter this shell is actually using.
    label = f"burla test {sys.version_info.major}.{sys.version_info.minor}"
    with tempfile.TemporaryDirectory(prefix="burla-test-shell-") as temp_dir:
        command = _configure_test_shell_prompt(shell, environment, Path(temp_dir), label)
        result = subprocess.run(command, cwd=_SOURCE_ROOT, env=environment)
    if result.returncode:
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    test_shell()
