"""Run pinned static tools without adding candidate sources to Python's import path."""

from __future__ import annotations

import argparse
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
import json
from pathlib import Path
import sys
import tomllib


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tool", choices=("grimp", "import-linter"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    if not sys.flags.isolated:
        parser.error("Run with python -I")
    root = args.root.resolve()
    config = tomllib.loads(args.config.read_text(encoding="utf-8"))["tool"]["importlinter"]
    packages = config["root_packages"]

    import grimp
    class StaticSourceLoader(Loader):
        def create_module(self, spec):
            return None

        def exec_module(self, module):
            raise ImportError("Static quality checks must not execute source packages")

    class SourcePackageFinder(MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname not in packages:
                return None
            directory = root / fullname
            if not directory.is_dir() or directory.is_symlink():
                raise ValueError(f"Missing source package: {fullname}")
            spec = ModuleSpec(fullname, StaticSourceLoader(), is_package=True)
            spec.submodule_search_locations = [str(directory)]
            return spec

    # Both tools inspect root package specs. Expose directories, never executable loaders.
    sys.meta_path.insert(0, SourcePackageFinder())
    if args.tool == "grimp":
        graph = grimp.build_graph(
            *packages, cache_dir=None,
            exclude_type_checking_imports=False,
        )
        sys.stdout.write(json.dumps({
            module: sorted(graph.find_modules_directly_imported_by(module))
            for module in sorted(graph.modules)
        }))
        return 0

    from importlinter import configuration
    from importlinter.application import use_cases

    configuration.configure()
    # The CLI inserts cwd into sys.path; the existing application API does not.
    passed = use_cases.lint_imports(
        config_filename=str(args.config), limit_to_contracts=(), cache_dir=None,
        is_debug_mode=True, show_timings=False, no_logo=True, verbose=False,
    )
    sys.stdout.write("\nQUALITY_IMPORT_LINTER_RESULT=" + json.dumps({"passed": passed}) + "\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
