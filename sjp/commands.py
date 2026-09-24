"""The commands that exist so far.

There is no emergency command yet. The profiler has nothing to recover from, and adding
one to fill in the third row of the table would be a command written for a document.

Each command's parser is built by its own named function so that a check can hand it real
argv without running the command. `capture` needs a Spark session and a check cannot have
one, so its parser is the only part of it anything can reach.
"""
import argparse

from sjp import eventlog
from sjp.cli import READ, WRITE, command

CAPTURE_DEFAULT_ROWS = 2000000


def inventory_parser():
    parser = argparse.ArgumentParser(prog="sjp inventory")
    parser.add_argument("path", help="an event log file, or a directory of them")
    return parser


def capture_parser():
    parser = argparse.ArgumentParser(prog="sjp capture")
    parser.add_argument("--job", required=True, choices=("skewed", "balanced"))
    parser.add_argument("--out", required=True, help="directory to write the log into")
    parser.add_argument("--rows", type=int, default=CAPTURE_DEFAULT_ROWS)
    return parser


def render(report):
    """The lines one file's report turns into. Returned rather than printed, so a check
    can read them without capturing stdout."""
    if "error" in report:
        return ["{}  UNREADABLE  {}".format(report["path"], report["error"])]

    lines = ["{}  {} lines".format(report["path"], report["lines"])]
    for name, count in sorted(report["counts"].items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append("  {:>7}  {}".format(count, name or "<no Event key>"))
    if report["missing"]:
        lines.append("  MISSING, so the profiler cannot answer everything here:")
        for name in report["missing"]:
            lines.append("    {}   {}".format(name, eventlog.NEEDED[name]))
    else:
        lines.append("  every event the profiler needs is present")
    return lines


@command("inventory", READ, "what a log holds and which needed events are missing",
         probe=lambda store: [store])
def inventory(rest):
    args = inventory_parser().parse_args(rest)

    reports = eventlog.inventory(args.path)
    unreadable = 0
    for report in reports:
        unreadable += 1 if "error" in report else 0
        for line in render(report):
            print(line)
    if unreadable:
        # Said out loud so the count is a number somebody reads rather than a flag
        # dressed as one.
        print("{} of {} files could not be read".format(unreadable, len(reports)))
    return 1 if unreadable else 0


@command("capture", WRITE, "run a sample job and keep its event log")
def capture(rest):
    args = capture_parser().parse_args(rest)

    # Imported here rather than at module level so that reading a log needs no pyspark.
    from jobs import sample
    path = sample.run(args.job, args.out, args.rows)
    print("wrote {}".format(path))
    return 0
