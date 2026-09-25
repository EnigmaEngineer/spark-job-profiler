"""Getting a Spark event log off disk, and saying what is in it.

The format is one JSON object per line. Every object carries an `Event` key naming the
listener event it came from. Nothing else is guaranteed to be on every line, which is why
this module counts event names and leaves the meaning of them to `sjp.model`.

What counts as a needed event is `sjp.model.CONSUMES`, which the model builds out of its
own handlers. It used to be a list kept here by hand. A list of names in one module and a
parser reading names in another is two places to remember, and the second one to change
is the one nobody changes.
"""
import json
import os

from sjp import model


class NotAnEventLog(Exception):
    """Raised when a file does not look like an event log at all."""


def read_events(path):
    """Yield one decoded object per line.

    A blank line is skipped. A line that is not JSON raises, because a log this tool
    cannot read fully is a log it should not report on partially.
    """
    with open(path, "r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except ValueError as problem:
                raise NotAnEventLog("{} line {}: {}".format(path, number, problem))


def event_counts(path):
    """Count the lines by their `Event` name.

    A line with no `Event` key is counted under the empty string rather than dropped,
    because a silent drop is how a format surprise stays invisible.
    """
    counts = {}
    for event in read_events(path):
        name = event.get("Event", "")
        counts[name] = counts.get(name, 0) + 1
    if not counts:
        raise NotAnEventLog("{} holds no events".format(path))
    return counts


def logs_under(root):
    """Every event log file at `root`, which may be a file or a directory.

    Spark writes a `.inprogress` suffix while an application is running and renames on a
    clean stop. Those are included. A half written log is a real thing to be handed and
    the tool should say what is in it rather than pretend it is not there.
    """
    if os.path.isfile(root):
        return [root]
    found = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in sorted(filenames):
            found.append(os.path.join(dirpath, name))
    return sorted(found)


def spark_properties(path):
    """The config the run really used, out of `SparkListenerEnvironmentUpdate`.

    Returned as a plain dict. Spark writes this section as a list of pairs rather than an
    object, which is the sort of thing worth finding out before a parser assumes otherwise.
    """
    for event in read_events(path):
        if event.get("Event") == "SparkListenerEnvironmentUpdate":
            return dict(event.get("Spark Properties", []))
    raise NotAnEventLog("{} records no environment".format(path))


def profile(path):
    """The stage and task model for one log."""
    return model.build(read_events(path))


def inventory(root):
    """Per file, what it holds and which needed events are missing.

    A file that cannot be read comes back carrying an `error` rather than aborting the
    walk. Aborting would hide every log after it, and skipping it quietly would let a
    directory of junk report as an empty success. Neither is an answer.
    """
    reports = []
    for path in logs_under(root):
        try:
            counts = event_counts(path)
        except NotAnEventLog as problem:
            reports.append({"path": path, "error": str(problem)})
            continue
        missing = sorted(name for name in model.CONSUMES if name not in counts)
        reports.append({"path": path, "counts": counts, "missing": missing,
                        "lines": sum(counts.values())})
    if not reports:
        raise NotAnEventLog("no files under {}".format(root))
    return reports
