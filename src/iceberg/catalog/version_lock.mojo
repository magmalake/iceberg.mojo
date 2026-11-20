"""Local locks on metadata versions, for the filesystem catalog's commits
(morca-dev/morca#531).

The filesystem catalog's claims (`filesystem.claim_file_name`) settle a race
only if the later claim's listing sees the earlier claim or its metadata
file. A local directory listing is one `readdir`, and POSIX leaves
unspecified whether it returns entries added or removed while it runs: a
rival that writes its metadata file and removes its claim during the
listing can be missed altogether, and then both writers win the version.

So on a local filesystem a writer also takes the version's lock,
`<V>.lock`, which `link(2)` creates atomically or fails on because the name
exists. A writer that holds it lists only after the previous holder's
metadata file was written, and a listing always returns an entry made
before it began. Object stores keep the claims alone: S3's listing is
strongly consistent.
"""

from std.ffi import external_call
from std.os import remove
from std.os.path import exists as path_exists

from ..io import FileIO, join_path
from ..util import uuid4, zero_pad


comptime LOCK_SUFFIX = String(".lock")


def version_lock_name(version: Int) -> String:
    """`<V>.lock`: the local lock on metadata version `V`. It holds the time
    it was taken, in milliseconds, and isn't a `<V>-` name, so neither
    readers nor claim listings see it."""
    return zero_pad(version, 5) + LOCK_SUFFIX


@fieldwise_init
struct VersionLock(Copyable, Movable):
    """What `take_version_lock` got: the lock's `path`, or "" with `busy`
    when another writer holds it, or "" alone when there is nothing to lock
    (an object store, or a filesystem without hard links)."""

    var path: String
    var busy: Bool


def _c_path(path: String) -> List[UInt8]:
    var out = List[UInt8](capacity=path.byte_length() + 1)
    for b in path.as_bytes():
        out.append(b)
    out.append(0)
    return out^


def _link(existing: String, new: String) -> Bool:
    """`link(2)`: whether `new` was created, as another name of `existing`.
    It fails, atomically, if `new` exists."""
    var a = _c_path(existing)
    var b = _c_path(new)
    var rc = external_call["link", Int32](a.unsafe_ptr(), b.unsafe_ptr())
    _ = a^
    _ = b^
    return rc == 0


def _rename(old: String, new: String) -> Bool:
    var a = _c_path(old)
    var b = _c_path(new)
    var rc = external_call["rename", Int32](a.unsafe_ptr(), b.unsafe_ptr())
    _ = a^
    _ = b^
    return rc == 0


def _unlink(path: String):
    try:
        remove(path)
    except:
        pass


def _lock_ms(io: FileIO, path: String) -> Int64:
    """When the lock at `path` was taken, or -1 if it can't be read."""
    try:
        return Int64(Int(io.read_text(path).strip()))
    except:
        return -1


def take_version_lock(
    io: FileIO,
    metadata_dir: String,
    version: Int,
    now: Int64,
    stale_ms: Int64,
) raises -> VersionLock:
    """Takes the local lock on `version` (`version_lock_name`).

    The lock's content, the time, is written to a file of its own first and
    linked into place, so a lock is never seen half-written. A lock older
    than `stale_ms` is from a writer that died holding it, as with claims:
    it is renamed away, and the lock taken again. The rename moves whichever
    lock has the name by then, so one that turns out to be newer than the
    stale one is linked back and counts as busy.
    """
    if not io.is_local(metadata_dir):
        return VersionLock(String(""), False)
    var dir = io.local_path(metadata_dir)
    var lock = join_path(dir, version_lock_name(version))
    var mine = lock + "." + uuid4() + ".tmp"
    io.write_new(mine, String(now).as_bytes())
    try:
        for _ in range(3):
            if _link(mine, lock):
                return VersionLock(lock, False)
            if not path_exists(lock):
                # No hard links here: the claims alone settle the race.
                return VersionLock(String(""), False)
            var at = _lock_ms(io, lock)
            if at < 0 or now - at <= stale_ms:
                return VersionLock(String(""), True)
            var gone = lock + "." + uuid4() + ".stale"
            if not _rename(lock, gone):
                continue
            if _lock_ms(io, gone) != at:
                _ = _link(gone, lock)
                _unlink(gone)
                return VersionLock(String(""), True)
            _unlink(gone)
        return VersionLock(String(""), True)
    finally:
        _unlink(mine)


def version_locked(
    io: FileIO,
    metadata_dir: String,
    version: Int,
    now: Int64,
    stale_ms: Int64,
) -> Bool:
    """Whether a live writer holds the local lock on `version`."""
    try:
        if not io.is_local(metadata_dir):
            return False
        var lock = join_path(
            io.local_path(metadata_dir), version_lock_name(version)
        )
        if not path_exists(lock):
            return False
        var at = _lock_ms(io, lock)
        return at < 0 or now - at <= stale_ms
    except:
        return False


def release_version_lock(lock: VersionLock):
    """Removes a lock `take_version_lock` took; nothing for the others."""
    if lock.path:
        _unlink(lock.path)
