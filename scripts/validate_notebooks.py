"""Validate committed example notebooks without executing external calls."""

from __future__ import annotations

import ast
from pathlib import Path

import nbformat

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"


def main() -> None:
    paths = sorted(EXAMPLES.glob("*.ipynb"))
    if not paths:
        raise SystemExit("No example notebooks found.")

    for path in paths:
        notebook = nbformat.read(path, as_version=4)
        nbformat.validate(notebook)
        if not notebook.cells:
            raise SystemExit(f"{path.name} has no cells.")
        for index, cell in enumerate(notebook.cells):
            if cell.get("outputs") or cell.get("execution_count") is not None:
                raise SystemExit(f"{path.name} cell {index} contains saved output.")
            source = cell.get("source", "")
            if cell.cell_type == "markdown" and "\u2014" in source:
                raise SystemExit(f"{path.name} cell {index} contains an em dash.")
            if cell.cell_type == "code":
                ast.parse(source, filename=f"{path.name}:cell-{index}")

    print(f"Validated {len(paths)} output-free notebooks.")


if __name__ == "__main__":
    main()
