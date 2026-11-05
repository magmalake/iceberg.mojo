"""How many previous metadata files a table remembers, and whether it deletes
the ones it forgets.

Every commit writes a whole new `*.metadata.json` and adds the one it replaced
to the `metadata-log`. Left alone, both grow by one per commit for ever: the
log inside every new file, and the files themselves in `metadata/`. Two table
properties bound them, with Java's defaults:

* `write.metadata.previous-versions-max` (100) — the log keeps the newest
  this many entries. This applies to every commit, as in Java.
* `write.metadata.delete-after-commit.enabled` (false) — after a successful
  commit, the files that fell off the log are deleted. Off, they stay where
  they are and nothing points at them.

Deleting happens only after the new metadata is in place, and a file that is
already gone is not an error, so a crash in between leaves at worst a few
unreferenced files.
"""

from .io import FileIO
from .metadata import MetadataLogEntry, TableMetadata


comptime PREVIOUS_VERSIONS_MAX = String("write.metadata.previous-versions-max")
comptime DELETE_AFTER_COMMIT = String(
    "write.metadata.delete-after-commit.enabled"
)
comptime DEFAULT_PREVIOUS_VERSIONS_MAX = 100


def previous_versions_max(metadata: TableMetadata) -> Int:
    var v = metadata.property_(PREVIOUS_VERSIONS_MAX, "")
    if v == "":
        return DEFAULT_PREVIOUS_VERSIONS_MAX
    try:
        var n = Int(v)
        return n if n >= 1 else 1
    except:
        return DEFAULT_PREVIOUS_VERSIONS_MAX


def delete_after_commit(metadata: TableMetadata) -> Bool:
    return metadata.property_(DELETE_AFTER_COMMIT, "false").lower() == "true"


def record_previous(
    mut metadata: TableMetadata, previous: String, previous_updated_ms: Int64
):
    """Make sure the file this commit replaces is the log's last entry.

    The `prepare_*` functions add it themselves; a commit made from metadata
    edited by hand (expiry, a property change) may not have.
    """
    if previous == "":
        return
    var n = len(metadata.metadata_log)
    if n > 0 and metadata.metadata_log[n - 1].metadata_file == previous:
        return
    metadata.metadata_log.append(
        MetadataLogEntry(previous_updated_ms, previous)
    )


def trim_metadata_log(mut metadata: TableMetadata) -> List[String]:
    """Keep the newest `previous-versions-max` entries; return the files of
    the ones dropped, oldest first."""
    var keep = previous_versions_max(metadata)
    var n = len(metadata.metadata_log)
    var dropped = List[String]()
    if n <= keep:
        return dropped^
    var log = List[MetadataLogEntry](capacity=keep)
    for k in range(n):
        if k < n - keep:
            dropped.append(metadata.metadata_log[k].metadata_file)
        else:
            log.append(metadata.metadata_log[k].copy())
    metadata.metadata_log = log^
    return dropped^


def delete_metadata_files(io: FileIO, files: List[String]) -> Int:
    """Delete what `trim_metadata_log` dropped; returns how many went."""
    var n = 0
    for k in range(len(files)):
        try:
            io.delete(files[k])
            n += 1
        except:
            pass
    return n
