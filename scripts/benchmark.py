"""Run one job at several partition counts and report what moved.

    python scripts/benchmark.py --job skewed_join --rows 8000000 \
        --arm current=8 --arm advised=2 --passes 4 --out /tmp/bench
    python scripts/benchmark.py --report /tmp/bench/manifest.json

Each run is its own process. A session started in a process that has already run one is
cheaper than the first, and folding that saving into whichever arm happens to run second
is the oldest way to win a benchmark by accident.

Running and reporting are separate subcommands because a full schedule is longer than one
shell call here. The manifest is what makes a split run worth the same as a whole one, and
it holds the log path for every pass so a report can be rebuilt without running anything.

The decisions live in `sjp.bench`. This drives and prints.
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sjp import bench  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class NotComparableArm(Exception):
    """Raised when the arm asked for as the baseline is not one the manifest holds."""

    def __init__(self, wanted, known):
        super().__init__("no arm called {!r}. The manifest holds {}".format(
            wanted, ", ".join(known)))

# What one pass measures. The stage total and the application wall are both here because
# a change that moves one and not the other happened outside the work.
PICKS = (
    ("stage wall total", lambda one: one.stage_wall_total),
    ("application wall", lambda one: one.app_wall),
)


def parse_arm(text):
    """`name=count` into an `Arm`. The job is filled in by the caller."""
    if "=" not in text:
        raise argparse.ArgumentTypeError(
            "{!r} is not name=count".format(text))
    name, _, count = text.partition("=")
    if not name or not count.isdigit():
        raise argparse.ArgumentTypeError(
            "{!r} is not name=count with a whole count".format(text))
    return bench.Arm(name=name, job="", partitions=int(count))


def run_once(arm, rows, out_dir):
    """One run in its own process. Returns the log it wrote and the seconds it took."""
    before = set(os.listdir(out_dir)) if os.path.isdir(out_dir) else set()
    started = time.time()
    subprocess.run(
        [sys.executable, "-c",
         "from jobs import sample; sample.run({!r}, {!r}, {!r}, partitions={!r})".format(
             arm.job, out_dir, rows, arm.partitions)],
        cwd=ROOT, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elapsed = time.time() - started
    fresh = sorted(set(os.listdir(out_dir)) - before)
    if len(fresh) != 1:
        raise RuntimeError("one run wrote {} logs: {}".format(len(fresh), fresh))
    return os.path.join(out_dir, fresh[0]), elapsed


def load(path):
    return json.load(open(path, encoding="utf-8")) if os.path.exists(path) else None


def save(path, manifest):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)


def do_run(args):
    arms = [bench.Arm(name=arm.name, job=args.job, partitions=arm.partitions)
            for arm in args.arm]
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, "manifest.json")
    manifest = load(path) or {"job": args.job, "rows": args.rows, "passes": args.passes,
                              "arms": [{"name": a.name, "partitions": a.partitions}
                                       for a in arms],
                              "warmup": None, "runs": []}

    warm, order = bench.schedule(arms, args.passes)
    if manifest["warmup"] is None:
        log, seconds = run_once(warm, args.rows, args.out)
        manifest["warmup"] = {"arm": warm.name, "path": log, "seconds": seconds}
        save(path, manifest)
        print("warmup {} {:.1f}s, discarded".format(warm.name, seconds))

    done = len(manifest["runs"])
    for index, arm in enumerate(order):
        if index < done:
            continue
        if args.limit and index - done >= args.limit:
            break
        log, seconds = run_once(arm, args.rows, args.out)
        manifest["runs"].append({"arm": arm.name, "path": log, "seconds": seconds})
        save(path, manifest)
        print("run {} of {}  {:<9} {:.1f}s".format(
            index + 1, len(order), arm.name, seconds))

    left = len(order) - len(manifest["runs"])
    print("{} of {} runs done, {} left".format(len(manifest["runs"]), len(order), left))
    return 0 if left == 0 else 3


def arm_names(manifest):
    return [arm["name"] for arm in manifest["arms"]]


def report_lines(manifest, against=None):
    passes = [bench.read_pass(run["arm"], run["path"]) for run in manifest["runs"]]
    shape = bench.check_comparable(passes)
    names = arm_names(manifest)
    counts = {arm["name"]: arm["partitions"] for arm in manifest["arms"]}

    lines = ["{} at {} rows, {} stages, {} rows across them".format(
        manifest["job"], manifest["rows"], shape[0], sum(shape[1]))]
    lines.append("{} passes an arm, warmup discarded".format(
        len(passes) // max(1, len(names))))

    for label, pick in PICKS:
        lines.append("")
        lines.append(label)
        for name in names:
            values = bench.readings(passes, name, pick)
            lines.append("  {:<9} {} partitions  {}  mean {:.0f} ms".format(
                name, counts[name], " ".join("{}".format(v) for v in values),
                sum(values) / len(values)))
        base = against or names[0]
        if base not in names:
            raise NotComparableArm(base, names)
        for name in names[1:]:
            found = bench.verdict(bench.readings(passes, base, pick),
                                  bench.readings(passes, name, pick))
            ratio = "no baseline to divide by" if found.ratio is None \
                else "{:.4f} of {}".format(found.ratio, base)
            lines.append("  {} against {}  {}  {}  {}".format(
                name, base, ratio, "separated" if found.decided else "undecided",
                found.why))

    lines.append("")
    lines.append("the stage that reads every row, which is the one the job is about")
    for name in names:
        stages = [bench.carrying_stage(one) for one in passes if one.arm == name]
        lines.append("  {:<9} {} tasks  wall {}  largest task {}  median task {}".format(
            name, stages[0].tasks,
            " ".join(str(stage.wall) for stage in stages),
            " ".join(str(stage.largest_duration) for stage in stages),
            " ".join("{:.0f}".format(stage.median_duration) for stage in stages)))
    base = against or names[0]
    for label, pick in (("wall", lambda stage: stage.wall),
                        ("largest task", lambda stage: stage.largest_duration)):
        for name in names[1:] if base == names[0] else names:
            if name == base:
                continue
            found = bench.verdict(
                [pick(bench.carrying_stage(one)) for one in passes if one.arm == base],
                [pick(bench.carrying_stage(one)) for one in passes if one.arm == name])
            lines.append("  {} {} against {}  {:.4f}  {}  {}".format(
                label, name, base, found.ratio,
                "separated" if found.decided else "undecided", found.why))

    lines.append("")
    lines.append("the stage a spread ranking would name, which is not always that one")
    for name in names:
        for one in [p for p in passes if p.arm == name]:
            stage = bench.worst_stage(one)
            carrying = bench.carrying_stage(one)
            lines.append("  {:<9} stage {}  {} tasks  spread {:.2f}{}".format(
                name, stage.stage_id, stage.tasks, stage.spread,
                "" if stage.stage_id == carrying.stage_id else "  not the carrying stage"))
    return lines


def do_report(args):
    manifest = load(args.report)
    if manifest is None:
        print("no manifest at {}".format(args.report), file=sys.stderr)
        return 2
    if not manifest["runs"]:
        print("manifest at {} holds no runs".format(args.report), file=sys.stderr)
        return 2
    for line in report_lines(manifest, args.against):
        print(line)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", help="print the report for an existing manifest")
    parser.add_argument("--against", help="the arm every other arm is compared to. "
                                          "Defaults to the first one declared")
    parser.add_argument("--job", default="skewed_join")
    parser.add_argument("--rows", type=int, default=8000000)
    parser.add_argument("--arm", action="append", type=parse_arm, default=[])
    parser.add_argument("--passes", type=int, default=4)
    parser.add_argument("--out")
    parser.add_argument("--limit", type=int, default=0,
                        help="stop after this many runs, so a long schedule can be split")
    args = parser.parse_args(argv)
    if args.report:
        return do_report(args)
    if not args.out or not args.arm:
        parser.error("--out and at least one --arm are required to run")
    return do_run(args)


if __name__ == "__main__":
    sys.exit(main())
