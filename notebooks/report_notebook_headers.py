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


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Report header coverage in a notebook.")
    parser.add_argument("notebook", type=str, help="Path to .ipynb file")
    args = parser.parse_args()

    nb_path = Path(args.notebook)
    nb = json.loads(nb_path.read_text(encoding="utf-8"))

    total_code = 0
    with_header = 0
    without_header = 0

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        total_code += 1
        source = cell.get("source")
        lines = source.splitlines(True) if isinstance(source, str) else list(source or [])
        if has_header(lines):
            with_header += 1
        else:
            without_header += 1

    print(f"Total code cells: {total_code}")
    print(f"With header: {with_header}")
    print(f"Without header: {without_header}")


if __name__ == "__main__":
    main()


