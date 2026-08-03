from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        root = Path(self.root)
        source = root.parent / "main_service" / "src" / "main_service"
        if not source.exists():
            source = root / "main_service"

        # Absent when only `client/` is available, which is how local-dev workers
        # install burla (they bind-mount the client dir alone and never run a
        # head). Forcing a missing path makes hatchling fail the whole build.
        if source.exists():
            build_data["force_include"][str(source)] = "main_service"
