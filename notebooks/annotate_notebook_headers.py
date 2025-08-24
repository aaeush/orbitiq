import json
from pathlib import Path
from typing import List


HEADER_PREFIXES: List[str] = [
    "# Inputs:",
    "# Input:",
    "# Purpose:",
]


def has_header(cell_source: List[str]) -> bool:
    if not cell_source:
        return False
    first_line = cell_source[0].lstrip()
    return any(first_line.startswith(prefix) for prefix in HEADER_PREFIXES)


def infer_basic_header(cell_source: List[str]) -> str:
    # Fallback generic header when no better inference
    return "# Inputs: - | Process: - | Outputs: -\n"


def annotate_notebook(nb_path: Path) -> None:
    text = nb_path.read_text(encoding="utf-8")
    nb = json.loads(text)

    if "cells" not in nb:
        return

    modified = False
    for cell in nb["cells"]:
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source")
        # Normalize source to list of lines as per nbformat
        if isinstance(source, str):
            lines = source.splitlines(True)
        else:
            lines = list(source or [])

        if has_header(lines):
            continue

        header = infer_basic_header(lines)
        # Prepend header
        new_lines = [header]
        new_lines.extend(lines)
        cell["source"] = new_lines
        modified = True

    if modified:
        nb_path.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Annotate code cells with standardized headers.")
    parser.add_argument(
        "notebook",
        type=str,
        help="Path to the .ipynb file to annotate",
    )
    args = parser.parse_args()

    nb_path = Path(args.notebook)
    annotate_notebook(nb_path)


if __name__ == "__main__":
    main()


