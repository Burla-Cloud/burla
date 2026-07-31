from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        root = Path(self.root)
        source = root.parent / "main_service" / "src" / "main_service"
        if not source.exists():
            source = root / "main_service"

        build_data["force_include"][str(source)] = "main_service"
