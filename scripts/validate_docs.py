"""Validate local Markdown links and documentation style constraints."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKDOWN_FILES = [
    ROOT / "README.md",
    ROOT / "examples" / "README.md",
    *(ROOT / "docs").rglob("*.md"),
]
LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def main() -> None:
    for path in MARKDOWN_FILES:
        text = path.read_text(encoding="utf-8")
        if "\u2014" in text:
            raise SystemExit(f"{path.relative_to(ROOT)} contains an em dash.")
        for target in LINK.findall(text):
            if target.startswith(("http://", "https://", "#")):
                continue
            location = target.split("#", 1)[0]
            local = (
                ROOT / location.lstrip("/") if location.startswith("/") else path.parent / location
            ).resolve()
            if local.parent == ROOT / "docs" / "_notebooks":
                local = ROOT / "examples" / local.name
            if not local.exists():
                raise SystemExit(f"{path.relative_to(ROOT)} links to missing path {target!r}.")
    print(f"Validated {len(MARKDOWN_FILES)} Markdown files.")


if __name__ == "__main__":
    main()
