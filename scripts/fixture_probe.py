"""Print what the committed fixtures hold, per stage.

    python scripts/fixture_probe.py

The arithmetic lives here rather than in the document that publishes it, and
`tests/test_fixture_pathologies.py` imports it, so a mutation pass over this file is
graded by the suite. That is the whole reason it is not a block of code inside the
printing loop.

This is not the profiler. The stage and task model is the next thing to build and it will
not look like this. What this exists for is to say whether the fixtures still carry the
pathologies they were captured for.
"""
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sjp import eventlog  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(ROOT, "tests", "fixtures", "eventlogs")


def only_log(directory):
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    if len(names) != 1:
        raise ValueError("expected one log in {}, found {}".format(directory, names))
    return os.path.join(directory, names[0])


def per_stage(path):
    """Median and max task duration and shuffle records, plus spill totals, by stage."""
    tasks = {}
    for event in eventlog.read_events(path):
        if event.get("Event") != "SparkListenerTaskEnd":
            continue
        info = event["Task Info"]
        metrics = event["Task Metrics"]
        tasks.setdefault(event["Stage ID"], []).append((
            info["Finish Time"] - info["Launch Time"],
            metrics["Shuffle Read Metrics"]["Total Records Read"],
            metrics["Memory Bytes Spilled"],
            metrics["Disk Bytes Spilled"],
        ))
    if not tasks:
        raise eventlog.NotAnEventLog("{} holds no task events".format(path))

    summary = {}
    for stage, rows in tasks.items():
        durations = sorted(row[0] for row in rows)
        records = sorted(row[1] for row in rows)
        summary[stage] = {
            "tasks": len(rows),
            "duration_median": statistics.median(durations),
            "duration_max": max(durations),
            "records_median": statistics.median(records),
            "records_max": max(records),
            "memory_spilled": sum(row[2] for row in rows),
            "disk_spilled": sum(row[3] for row in rows),
        }
    return summary


def ratio(smaller, larger):
    """Guarded so a stage that shuffles nothing reads as 0 rather than raising."""
    return 0.0 if smaller == 0 else larger / smaller


def main():
    for job in ("skewed", "balanced"):
        summary = per_stage(only_log(os.path.join(FIXTURES, job)))
        for stage in sorted(summary):
            row = summary[stage]
            print("{:<9} stage {}  tasks {}".format(job, stage, row["tasks"]))
            print("    records   median {}  max {}  ratio {:.2f}".format(
                row["records_median"], row["records_max"],
                ratio(row["records_median"], row["records_max"])))
            print("    duration  median {} ms  max {} ms  ratio {:.2f}".format(
                row["duration_median"], row["duration_max"],
                ratio(row["duration_median"], row["duration_max"])))
            print("    spilled   {} memory  {} disk".format(
                row["memory_spilled"], row["disk_spilled"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
