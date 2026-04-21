#!/usr/bin/env python3
"""Regenerate transcriptions/language_constants.go from Pipecat language.py."""
from __future__ import annotations

import re
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "pipecat" / "src" / "pipecat" / "transcriptions" / "language.py"
OUT = ROOT / "llmpipe" / "transcriptions" / "language_constants.go"


def main() -> None:
    text = SRC.read_text(encoding="utf-8")
    pairs: list[tuple[str, str]] = []
    for line in text.splitlines():
        line = line.split("#")[0]
        m = re.match(r"\s+([A-Z][A-Z0-9_]*)\s*=\s*\"([^\"]*)\"", line)
        if m:
            pairs.append((m.group(1), m.group(2)))
    lines = [
        "// Code generated from Pipecat pipecat/transcriptions/language.py; DO NOT EDIT.\n",
        "\npackage transcriptions\n",
        "\nconst (\n",
    ]
    for name, val in pairs:
        lines.append(f'\t{name} Language = "{val}"\n')
    lines.append(")\n")
    OUT.write_text("".join(lines), encoding="utf-8")
    print(f"wrote {len(pairs)} constants to {OUT}")


if __name__ == "__main__":
    main()
