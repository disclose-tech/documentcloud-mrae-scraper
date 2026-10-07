"""Event data helpers, shared by the UploadPipeline and the archive refresh script.

Event data is a dict {source_file_url: {"last_modified": ..., "last_seen": ...}} of the
documents already uploaded.

It is split in two:
- the archive (event_data/archive.json), committed in the repo and loaded from disk
- the live event data, stored on DocumentCloud, holding only the entries that are
  not archived yet (keeps the PATCH requests to DocumentCloud small)
"""

import json
from pathlib import Path

ARCHIVE_PATH = Path(__file__).parent.parent / "event_data" / "archive.json"


def load_archive(path=ARCHIVE_PATH):
    """Loads the archived event data, or {} if there is no archive."""

    if not path.exists():
        return {}

    with open(path, "r") as file:
        return json.load(file)


def write_archive(event_data, path=ARCHIVE_PATH):
    """Writes the archived event data sorted by URL, one entry per line."""

    path.parent.mkdir(exist_ok=True)

    lines = [f"{json.dumps(url)}: {json.dumps(event_data[url])}" for url in sorted(event_data)]

    with open(path, "w") as file:
        file.write("{\n" + ",\n".join(lines) + "\n}\n")
