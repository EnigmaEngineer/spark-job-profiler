"""The commands that exist so far.

There is no emergency command yet. The profiler has nothing to recover from, and adding
one to fill in the third row of the table would be a command written for a document.

Each command's parser is built by its own named function so that a check can hand it real
argv without running the command. `capture` needs a Spark session and a check cannot have
one, so its parser is the only part of it anything can reach.
"""
import argparse
import os

from sjp import eventlog, model
from sjp.cli import READ, WRITE, command

CAPTURE_DEFAULT_ROWS = 2000000


def _first_log(store):
    """One log out of a directory, for the read safety check to drive `stages` with.

    `stages` takes a file rather than a directory because a model is of one application.
    The contract check hands every read the store root, so this is the step from one to
    the other and it lives here rather than in the check.
    """
    for path in eventlog.logs_under(store):
        if not path.endswith(".md"):
            return path
    raise eventlog.NotAnEventLog("no log under {}".format(store))


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


def stages_parser():
    parser = argparse.ArgumentParser(prog="sjp stages")
    parser.add_argument("path", help="one event log file")
    return parser


def counted(number, word):
    """`1 job` and `2 jobs`. A log with one of something is the common case here."""
    return "{} {}".format(number, word if number == 1 else word + "s")


def stage_lines(app):
    """The model as text. Facts only, and no verdict about any of them.

    Deciding that a spread of 44 is a problem is the detector's job and the detector does
    not exist yet. Printing a number beside the number it should be compared against is
    what this is for.
    """
    lines = ["{}  {}  {}  {} ms  {}  {}".format(
        app.app_id, app.name, counted(app.cores, "core"), app.wall_time,
        counted(len(app.jobs), "job"), counted(len(app.stages), "stage"))]
    lines.append("  shuffle partitions {}".format(
        app.properties.get("spark.sql.shuffle.partitions", "unset")))
    for stage in app.stages:
        lines.append("  stage {}  {} of {} tasks  {} ms wall".format(
            stage.stage_id, len(stage.tasks), stage.declared_tasks, stage.wall_time))
        for label, name in (("duration ms", "duration"), ("records read", "records_read"),
                            ("outside run", "outside_run_time")):
            lines.append("      {:<13} median {}  max {}  spread {:.2f}".format(
                label, stage.median(name), stage.largest(name), stage.spread(name)))
        lines.append("      {:<13} {} memory  {} disk".format(
            "spilled", stage.total("memory_spilled"), stage.total("disk_spilled")))
        lines.append("      {:<13} {} largest task  {} summed by the stage".format(
            "peak memory", stage.peak_memory,
            stage.reported_total("internal.metrics.peakExecutionMemory")))
        absent = [name for name in model.STAGE_TOTAL_FIELDS
                  if not stage.reported_total(name)]
        off = model.disagreements(stage)
        lines.append("      {:<13} {} mapped, {} absent, {} disagree with the tasks".format(
            "stage totals", len(model.STAGE_TOTAL_FIELDS), len(absent), len(off)))
        for name, reported, summed in off:
            lines.append("        {} reports {} against {} over the tasks".format(
                name, reported, summed))
    return lines


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
            lines.append("    {}   {}".format(name, model.CONSUMES[name]))
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


@command("stages", READ, "the stage and task model, one line per measurement",
         probe=lambda store: [_first_log(store)])
def stages(rest):
    args = stages_parser().parse_args(rest)

    for line in stage_lines(eventlog.profile(args.path)):
        print(line)
    return 0


@command("capture", WRITE, "run a sample job and keep its event log")
def capture(rest):
    args = capture_parser().parse_args(rest)

    # Imported here rather than at module level so that reading a log needs no pyspark.
    from jobs import sample
    path = sample.run(args.job, args.out, args.rows)
    print("wrote {}".format(path))
    return 0
