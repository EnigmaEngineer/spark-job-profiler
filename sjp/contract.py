"""Test the claim a command makes about its own effect.

The store this tool reads is a directory of event logs, so "changed nothing" means the
directory came back with the same files holding the same bytes. Snapshot it, run the
command, snapshot it again.

A declaration on its own is not evidence. An earlier project of mine shipped ten checks
that all read the name of a thing and all passed while the behaviour underneath was wrong.
"""
import contextlib
import hashlib
import io
import os

from sjp import cli


def snapshot(root):
    """Map every file under `root` to its size and digest.

    Directories are recorded too, under their path with a None digest, so a command that
    creates an empty directory is caught as well.
    """
    found = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames:
            found[os.path.relpath(os.path.join(dirpath, name), root)] = None
        for name in filenames:
            path = os.path.join(dirpath, name)
            with open(path, "rb") as handle:
                blob = handle.read()
            key = os.path.relpath(path, root)
            found[key] = (len(blob), hashlib.sha256(blob).hexdigest())
    return found


def differences(before, after):
    """Every path the two snapshots disagree about, sorted."""
    return sorted(set(before) ^ set(after)) + sorted(
        path for path in set(before) & set(after) if before[path] != after[path])


def reads_that_touched_the_store(registry, store):
    """Run every read command against `store` and return the names that changed it.

    Raises when a read has no probe. A read nobody can drive is a read nobody has tested,
    and reporting it as clean is worse than refusing.
    """
    unprobed = [name for name, entry in sorted(registry.items())
                if entry["effect"] == cli.READ and entry["probe"] is None]
    if unprobed:
        raise cli.Registration(
            "no probe for read commands {}. Cannot check what cannot be run".format(unprobed))

    guilty = []
    for name, entry in sorted(registry.items()):
        if entry["effect"] != cli.READ:
            continue
        before = snapshot(store)
        with contextlib.redirect_stdout(io.StringIO()):
            entry["run"](entry["probe"](store))
        moved = differences(before, snapshot(store))
        if moved:
            guilty.append((name, moved))
    return guilty
