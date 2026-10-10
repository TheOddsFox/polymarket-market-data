"""Check built distributions without executing their contents."""

import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

FORBIDDEN = {"data", ".state", "artifacts", "captures", ".pad", ".git", ".venv", "__pycache__"}
EXTENSIONS = {".parquet", ".sqlite", ".duckdb", ".pem", ".key", ".pyc"}


def validate(name):
    path = PurePosixPath(name)
    if (
        path.is_absolute()
        or ".." in path.parts
        or FORBIDDEN.intersection(path.parts)
        or path.suffix in EXTENSIONS
        or any(part.startswith(".env") for part in path.parts)
    ):
        raise ValueError("operator data or unsafe member in distribution: " + name)


for argument in sys.argv[1:] or ["dist"]:
    directory = Path(argument)
    files = sorted(directory.iterdir())
    if not any(p.suffix == ".whl" for p in files) or not any(
        p.name.endswith(".tar.gz") for p in files
    ):
        raise ValueError("wheel and sdist required")
    for file in files:
        if file.suffix == ".whl":
            with zipfile.ZipFile(file) as archive:
                for name in archive.namelist():
                    validate(name)
        elif file.name.endswith(".tar.gz"):
            with tarfile.open(file) as archive:
                for member in archive.getmembers():
                    validate(member.name)
                    if member.issym() or member.islnk() or member.isdev():
                        raise ValueError("non-regular distribution member")
print("Distribution boundaries verified")
