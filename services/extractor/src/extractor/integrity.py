import hashlib
import zipfile
from pathlib import Path

CHUNK_SIZE = 1 << 20


class IntegrityError(Exception):
    pass


def inner_json_sha256(archive: Path) -> str:
    """The portal publishes the SHA-256 of the JSON inside the zip, not of the zip itself."""
    with zipfile.ZipFile(archive) as zf:
        members = [m for m in zf.infolist() if m.filename.endswith(".json")]
        if len(members) != 1:
            raise IntegrityError(f"expected one JSON member, found {len(members)}")
        digest = hashlib.sha256()
        with zf.open(members[0]) as fh:
            while chunk := fh.read(CHUNK_SIZE):
                digest.update(chunk)
    return digest.hexdigest()
