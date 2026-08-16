"""Build stamps, so "is what I am looking at what I edited?" has an answer.

TWO hashes, not one version number, because the answer genuinely differs by layer and
that difference has already cost time here (README "Gotchas"):

  ui   `GET /` re-reads dashboard.html from disk on every request, so an HTML edit is
       live on browser reload. Recomputed per call -- it describes the file NOW.
  srv  Python is imported once at process start, so a .py edit does nothing until the
       node restarts. Computed at import and then frozen, which is the entire point:
       it describes the code actually executing, not the code on disk.

One number covering both would confirm neither. The failure this exists to make visible
is a cluster whose nodes are running different builds, which reads as Raft misbehaving.

Digests are content hashes, not a counter: nothing to remember to bump, so the stamp
cannot drift from what it describes.
"""

import hashlib
from importlib import resources
from importlib.metadata import PackageNotFoundError, version

_SHORT = 6  # 24 bits: ample to spot "these two differ", short enough to read aloud


def _short(digest: hashlib._Hash) -> str:
    return digest.hexdigest()[:_SHORT]


def ui_build() -> str:
    """Digest of the dashboard as it sits on disk right now.

    Deliberately NOT cached. The file is re-read per request, so a cached value would
    claim a page that is no longer being served -- the exact class of lie this module
    exists to prevent.
    """
    data = (resources.files("raftkv") / "static" / "dashboard.html").read_bytes()
    return _short(hashlib.sha256(data))


def _server_digest() -> str:
    """Digest of every .py in the package, in name order.

    Names are hashed alongside contents so a rename registers as a change; without it,
    moving code between two files leaves the digest untouched.
    """
    h = hashlib.sha256()
    for entry in sorted(resources.files("raftkv").iterdir(), key=lambda p: p.name):
        if entry.name.endswith(".py"):
            h.update(entry.name.encode())
            h.update(entry.read_bytes())
    return _short(h)


try:
    PACKAGE_VERSION = version("raftkv")
except PackageNotFoundError:  # running from a source tree that was never installed
    PACKAGE_VERSION = "0.0.0+src"

# Frozen at import, on purpose. See the module docstring.
SERVER_BUILD = f"{PACKAGE_VERSION}+{_server_digest()}"
