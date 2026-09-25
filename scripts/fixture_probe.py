"""Print what the committed fixtures hold, per stage, and check the stage totals.

    python scripts/fixture_probe.py

This drives and prints. Every number it shows comes out of `sjp.model`, which the suite
imports and a mutation pass grades. On the first day of this repo the arithmetic lived
here instead, because there was no model to put it in. There is one now.

The controls at the end matter more than the table. A run reporting that twelve stage
totals agree with the tasks is the same output a run comparing nothing would print, so
the probe damages a stage on purpose and fails if the disagreement is not found.
"""
import dataclasses
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sjp import eventlog, model  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(ROOT, "tests", "fixtures", "eventlogs")


def only_log(directory):
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    if len(names) != 1:
        raise ValueError("expected one log in {}, found {}".format(directory, names))
    return os.path.join(directory, names[0])


def report(job, app):
    lines = []
    for stage in app.stages:
        lines.append("{:<9} stage {}  tasks {} of {}".format(
            job, stage.stage_id, len(stage.tasks), stage.declared_tasks))
        lines.append("    records   median {}  max {}  spread {:.2f}".format(
            stage.median("records_read"), stage.largest("records_read"),
            stage.spread("records_read")))
        lines.append("    duration  median {} ms  max {} ms  spread {:.2f}".format(
            stage.median("duration"), stage.largest("duration"), stage.spread("duration")))
        lines.append("    spilled   {} memory  {} disk".format(
            stage.total("memory_spilled"), stage.total("disk_spilled")))
        lines.append("    peak      {} largest task  {} summed by the stage".format(
            stage.peak_memory,
            stage.reported_total("internal.metrics.peakExecutionMemory")))
    return lines


def agreement(app):
    """How many mapped totals the stage reported, how many it left out, and any that differ."""
    present = absent = 0
    off = []
    for stage in app.stages:
        for name, reported, summed in model.totals_against_tasks(stage):
            if stage.reported_total(name):
                present += 1
            else:
                absent += 1
            if reported != summed:
                off.append((stage.stage_id, name, reported, summed))
    return present, absent, off


REPEATED_NAME = "internal.metrics.executorRunTime"


def disagreement_from_dropping_a_task(stage):
    """What stops matching when one task is taken out of a stage.

    The stage keeps the total the log reported and loses a task from the sum, so every
    metric that task contributed to has to come apart. A run where this returns nothing
    is a run where the comparison is not comparing.
    """
    fewer = dataclasses.replace(stage, tasks=stage.tasks[:-1])
    return model.disagreements(fewer)


def refuses_a_repeated_name(stage):
    """Whether asking for a total by a name that appears twice raises rather than answers."""
    doubled = dataclasses.replace(stage, totals=stage.totals + stage.totals)
    try:
        doubled.reported_total(REPEATED_NAME)
    except model.UnexpectedLog:
        return True
    return False


def controls(app):
    """Two ways the agreement line could be printed by something checking nothing."""
    stage = app.stages[-1]
    off = disagreement_from_dropping_a_task(stage)
    return [
        ("a stage missing one task disagrees", bool(off),
         ", ".join(name for name, _reported, _summed in off)),
        ("a repeated total name is refused", refuses_a_repeated_name(stage), ""),
    ]


def failures_in(results):
    """How many controls did not do what they exist to do."""
    failed = 0
    for _label, ok, _detail in results:
        if not ok:
            failed += 1
    return failed


def exit_code(failed):
    """Separate from `main` so both answers are reachable from a check.

    A healthy run never takes the failing branch, so leaving the arithmetic inside `main`
    leaves half of it ungraded.
    """
    return 1 if failed else 0


def main():
    apps = {}
    for job in ("skewed", "balanced"):
        app = eventlog.profile(only_log(os.path.join(FIXTURES, job)))
        apps[job] = app
        for line in report(job, app):
            print(line)

    for job in ("skewed", "balanced"):
        present, absent, off = agreement(apps[job])
        print("{:<9} stage totals: {} present, {} absent, {} disagree with the task sums"
              .format(job, present, absent, len(off)))
        for stage_id, name, reported, summed in off:
            print("    stage {} {} reports {} against {}".format(
                stage_id, name, reported, summed))

    results = controls(apps["skewed"])
    for label, ok, detail in results:
        print("control, {}: {}".format(label, "yes" if ok else "NO"))
        if detail:
            print("    {}".format(detail))
    failed = failures_in(results)
    print("controls: {} of {} failed".format(failed, len(results)))
    return exit_code(failed)


if __name__ == "__main__":
    sys.exit(main())
