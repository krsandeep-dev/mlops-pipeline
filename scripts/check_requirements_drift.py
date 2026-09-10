"""Fail if serving/requirements.txt and the dev group disagree on a shared package.

Two files pin the serving stack: serving/requirements.txt (authoritative for the image)
and pyproject.toml's dev group (what the host tests run against). Nothing keeps them in
step, so a `uv add` that bumps fastapi on the host would leave the image a version behind
and the tests would still pass -- they would just be testing a different stack from the
one that ships.

uv-native by construction: this runs under `uv run`, so importlib.metadata reports the
versions uv resolved from the lockfile. No pip freeze, no second resolver.
"""

from __future__ import annotations

import importlib.metadata as md
import re
import sys
from pathlib import Path

REQUIREMENTS = Path(__file__).resolve().parent.parent / "serving" / "requirements.txt"
PIN = re.compile(r"^([A-Za-z0-9._-]+)==([A-Za-z0-9._-]+)\s*$")


def parse_pins(path: Path = REQUIREMENTS) -> dict[str, str]:
    pins = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = PIN.match(line)
        if match:
            pins[match.group(1).lower().replace("_", "-")] = match.group(2)
    return pins


def main() -> int:
    pins = parse_pins()
    checked, drifted, absent = 0, [], []

    for name, pinned in sorted(pins.items()):
        try:
            resolved = md.version(name)
        except md.PackageNotFoundError:
            # Only in the image (skops, psutil, ...) -- nothing to compare against.
            absent.append(name)
            continue
        checked += 1
        if resolved != pinned:
            drifted.append((name, pinned, resolved))

    for name, pinned, resolved in drifted:
        print(f"DRIFT {name}: image pins {pinned}, this environment resolves {resolved}", file=sys.stderr)

    print(f"compared {checked} shared packages; {len(absent)} image-only, {len(drifted)} drifted")
    if drifted:
        print(
            "\nserving/requirements.txt and the uv environment disagree. Align them: the "
            "image pin is authoritative for what ships, the lockfile for what is tested.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
