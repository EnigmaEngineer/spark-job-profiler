"""The commands that exist so far.

There is no emergency command yet. The profiler has nothing to recover from, and adding
one to fill in the third row of the table would be a command written for a document.

Each command's parser is built by its own named function so that a check can hand it real
argv without running the command. `capture` needs a Spark session and a check cannot have
one, so its parser is the only part of it anything can reach.
"""
import argparse
import os
import sys

import sjp
from sjp import eventlog, layout, memory, model, skew
from sjp.cli import READ, REFUSED, WRITE, command

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
    # Imported here rather than at module level. The tuple costs no pyspark, and taking
    # it from the module that runs the jobs is what stops the two lists drifting.
    from jobs.sample import JOBS

    parser.add_argument("--job", required=True, choices=JOBS)
    parser.add_argument("--out", required=True, help="directory to write the log into")
    parser.add_argument("--rows", type=int, default=CAPTURE_DEFAULT_ROWS)
    # The session's shuffle partition count. Only reaches a job that has not written a
    # count into its own query, so on most of these it changes nothing. It is here because
    # a fixture captured at a count other than the default needs a command that reproduces
    # it, and a fixture nobody can regenerate is a number with a story attached.
    parser.add_argument("--partitions", type=int,
                        help="spark.sql.shuffle.partitions for the session")
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
            lines.append("      {:<13} median {}  max {}  spread {}".format(
                label, stage.median(name), stage.largest(name), model.spread_text(stage, name)))
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


def skew_parser():
    parser = argparse.ArgumentParser(prog="sjp skew")
    parser.add_argument("path", help="one event log file")
    parser.add_argument("--threshold", type=float, default=skew.DEFAULT_THRESHOLD,
                        help="largest task over the median, above which a stage is skewed")
    parser.add_argument("--metric", action="append", dest="metrics",
                        help="a task field to judge, repeatable")
    # Narrows the printed body and nothing else. Every verdict is still computed, the
    # tally still counts all of them, and the exit status is still the whole scan's.
    parser.add_argument("--only", choices=skew.OUTCOMES,
                        help="print only the verdicts with this outcome")
    return parser


def ratio_text(verdict):
    """A verdict's ratio for printing.

    Three cases and each of them needs its own words. `inf` formatted as a float prints
    `inf`, which looks like a bug in the profiler rather than a fact about the stage.
    """
    if verdict.ratio is None:
        return "no ratio"
    if verdict.unbounded:
        return "unbounded"
    return "{:.4f}".format(verdict.ratio)


def skew_lines(app, verdicts, threshold, metrics, only=None):
    """The answer, then the tally, then the evidence behind them.

    Takes the verdicts rather than computing them, so the lines and the command's exit
    status cannot come from two separate scans that disagree.

    The order here is the day 2 change and the reason is the wide fixture. 48 stages times
    14 metrics is 672 verdict lines for a job that is not skewed anywhere, and the two
    lines answering the question used to be 674 and 675. Nothing about the length was the
    problem. A reader scrolling to the bottom of a screen of noise to find out there was
    no problem is the problem, and it does not get better by trimming.

    `only` filters the body. The summary above it is always computed over everything, so a
    filtered run still reports how many verdicts it is not showing.

    The tally prints even when a column is zero, because a summary that drops an empty
    outcome cannot report the absence of a problem.

    `metrics` is here for the contract line under the tally and for nothing else. The
    status this command returns is counted over whatever was judged, so a shell that
    stored a 0 last week cannot read it without the set that produced it. Day 5 widened
    the set from three names to fourteen and the same log changed its answer, with nothing
    in the output saying so.
    """
    lines = ["{}  {}  threshold {}  {}".format(
        app.app_id, app.name, threshold, counted(len(verdicts), "verdict"))]
    # One worst per unit rather than one overall. A ratio has no unit, so a single answer
    # across every metric is decided by whichever one the scan reached first.
    found = skew.worst_by_kind(verdicts)
    if not found:
        lines.append("  nothing skewed at this threshold")
    for kind in model.MEASURED:
        first = found.get(kind)
        if first is not None:
            lines.append("  worst {:<7} stage {} on {} at {}".format(
                kind, first.stage_id, first.metric, ratio_text(first)))
    tally = skew.counts(verdicts)
    lines.append("  {} skewed, {} even, {} undecided".format(
        tally[skew.SKEWED], tally[skew.EVEN], tally[skew.UNDECIDED]))
    # The status comes from `skew.exit_status` rather than from the tally above it, so the
    # printed number and the returned one cannot be two readings of the same thing.
    lines.append("  sjp {}  exit {} over metric set {} {}".format(
        sjp.__version__, skew.exit_status(verdicts),
        skew.metric_set_label(metrics), skew.metric_set_id(metrics)))

    body = skew.ranked(verdicts if only is None else skew.only(verdicts, only))
    if only is not None:
        lines.append("  showing {} of {}, {} only".format(
            len(body), len(verdicts), only))
    for verdict in body:
        lines.append("  {:<9} stage {}  {:<17} {:>10}  {}".format(
            verdict.outcome, verdict.stage_id, verdict.metric,
            ratio_text(verdict), verdict.why))
    return lines


@command("skew", READ, "which stages skewed, against a median relative threshold",
         probe=lambda store: [_first_log(store)])
def skew_command(rest):
    args = skew_parser().parse_args(rest)

    metrics = tuple(args.metrics) if args.metrics else skew.DEFAULT_METRICS
    try:
        skew.refuse_unmeasured(metrics)
    except skew.MetricRefused as refusal:
        # Refused before the log is opened, so a bad metric name costs a parse of nothing.
        #
        # 2 and not 0 or 1. Those two are this command's answer about the job and a
        # refusal is not an answer. Before today `--metric executor_id` crashed and the
        # interpreter exited 1, which a shell reads as a stage that skewed.
        print(refusal, file=sys.stderr)
        return REFUSED
    app = eventlog.profile(args.path)
    verdicts = skew.scan(app, metrics, args.threshold)
    for line in skew_lines(app, verdicts, args.threshold, metrics, args.only):
        print(line)
    return skew.exit_status(verdicts)


def version_parser():
    parser = argparse.ArgumentParser(prog="sjp version")
    return parser


@command("version", READ, "the build, and the metric set the skew status is counted over",
         probe=lambda store: [])
def version_command(rest):
    """The exit status contract, without needing a log to read it off.

    `sjp skew` prints the same two facts on every run, and a caller deciding whether a
    status it stored last week still means what it meant should not have to find a log and
    pay for a parse to ask. The kinds are listed because the set is derived from them, so
    the list is the reason the id is what it is rather than a restatement of it.

    A parser with no arguments, so a trailing word is refused rather than ignored. That is
    the rule the `commands` word already follows.
    """
    version_parser().parse_args(rest)
    print("sjp {}".format(sjp.__version__))
    print("metric set {} {}".format(skew.metric_set_label(skew.DEFAULT_METRICS),
                                    skew.metric_set_id(skew.DEFAULT_METRICS)))
    for metric in skew.DEFAULT_METRICS:
        print("  {:<18} {}".format(metric, model.kind_of(metric)))
    return 0


def spill_parser():
    parser = argparse.ArgumentParser(prog="sjp spill")
    parser.add_argument("path", help="one event log file")
    return parser


@command("spill", READ, "what spilled, how concentrated it was, and what the log cannot say",
         probe=lambda store: [_first_log(store)])
def spill(rest):
    args = spill_parser().parse_args(rest)

    app = eventlog.profile(args.path)
    print("{}  {}".format(app.app_id, app.name))
    for line in memory.pressure_lines(app):
        print(line)
    spilled = [report for report in memory.scan(app) if report.spilling]
    # Exit 1 means this job spilled. It does not mean this job has a problem, and both
    # committed logs exit 1 because both spill the same 117,440,288 bytes on their first
    # stage. Which spill is pathological is a question for `sjp skew` on memory_spilled,
    # where the skewed log answers unbounded and the balanced one answers even.
    return 1 if spilled else 0


def layout_parser():
    parser = argparse.ArgumentParser(prog="sjp layout")
    parser.add_argument("path", help="one event log file")
    parser.add_argument("--advisory", type=int, default=layout.ADVISORY_BYTES,
                        help="bytes a partition should hold, measured and compressed")
    parser.add_argument("--broadcast", type=int, default=layout.BROADCAST_BYTES,
                        help="estimated bytes under which a join side could be broadcast")
    return parser


@command("layout", READ, "partition counts and broadcast candidates, from the plan",
         probe=lambda store: [_first_log(store)])
def layout_command(rest):
    args = layout_parser().parse_args(rest)

    app = eventlog.profile(args.path)
    for line in layout.layout_lines(app, args.advisory, args.broadcast):
        print(line)
    # Exit 1 means something here is worth changing. A broadcast candidate or a partition
    # count the config can move and the volume disagrees with. A log whose counts are all
    # written into the query exits 0, because there is nothing this tool can advise on.
    # The counting is in sjp.layout so that a mutation pass can reach it.
    return 1 if layout.actionable(app, args.advisory, args.broadcast) else 0


@command("capture", WRITE, "run a sample job and keep its event log")
def capture(rest):
    args = capture_parser().parse_args(rest)

    # Imported here rather than at module level so that reading a log needs no pyspark.
    from jobs import sample
    path = sample.run(args.job, args.out, args.rows, args.partitions)
    print("wrote {}".format(path))
    return 0
