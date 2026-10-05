"""Where the byte and count magnitude floors came from, and what they change.

    python scripts/floor_probe.py

Both ends of both brackets are read off the committed logs here rather than quoted, so a
fixture change moves the printed bracket and the derivation moves with it.

The controls at the end are the point. A floor that silences the small log is easy and a
floor that silences the small log without touching either pathology log is the only one
worth shipping, so the probe checks both directions and fails if either moves the wrong way.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sjp import eventlog, model, skew  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(ROOT, "tests", "fixtures", "eventlogs")

NO_FLOORS = {model.MILLIS: None, model.BYTES: None, model.COUNT: None}
BYTE_CANDIDATES = (0, 10000, 100000, 700000, 10000000, 59191004, 60000000)
COUNT_CANDIDATES = (0, 1000, 10000, 60000, 1000000, 6932663, 7000000)
# The log the small end came from, and the two that carry a pathology at eight million rows.
SMALL = "small"
PATHOLOGY = ("skewed", "skewed_join")


def only_log(directory):
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    if len(names) != 1:
        raise ValueError("expected one log in {}, found {}".format(directory, names))
    return os.path.join(directory, names[0])


def jobs():
    return sorted(name for name in os.listdir(FIXTURES)
                  if os.path.isdir(os.path.join(FIXTURES, name)))


def verdicts(app, kind, floors=NO_FLOORS):
    """Every skewed verdict of one kind, largest task first."""
    found = [v for v in skew.scan(app, floors=floors)
             if v.outcome == skew.SKEWED and model.kind_of(v.metric) == kind]
    return sorted(found, key=lambda v: -v.largest)


def brackets_from(apps):
    """Re-derive both ends of the byte and count brackets off the logs.

    The noise end is the largest value the small log reaches. The real end is the smallest
    value anything else reaches, over every other log rather than over the one that happens
    to be open, because a smaller real value sitting in another log would move the answer.
    """
    read = {}
    for kind in (model.BYTES, model.COUNT):
        noise = max([v.largest for v in verdicts(apps[SMALL], kind)] or [0])
        real = None
        for job, app in apps.items():
            if job == SMALL:
                continue
            for verdict in verdicts(app, kind):
                real = verdict.largest if real is None else min(real, verdict.largest)
        read[kind] = (noise, real)
    return read


def sweep(apps, kind, candidates):
    lines = ["{:16s}".format("log") + "".join("{:>12d}".format(c) for c in candidates)]
    for job in sorted(apps):
        larges = [v.largest for v in verdicts(apps[job], kind)]
        row = "{:16s}".format(job)
        for candidate in candidates:
            row += "{:>12d}".format(sum(1 for value in larges if value >= candidate))
        lines.append(row)
    return lines


def controls(apps):
    """Each one returns a label, whether it held, and what it read."""
    results = []

    before = sum(len(verdicts(apps[SMALL], k)) for k in (model.BYTES, model.COUNT))
    after = sum(len(verdicts(apps[SMALL], k, floors=skew.FLOORS))
                for k in (model.BYTES, model.COUNT))
    results.append(("the floors silence the small log",
                    before > 0 and after == 0,
                    "{} byte and count verdicts with no floor, {} with the floors".format(
                        before, after)))

    moved = []
    for job in PATHOLOGY:
        for kind in (model.BYTES, model.COUNT):
            was = [v.largest for v in verdicts(apps[job], kind)]
            now = [v.largest for v in verdicts(apps[job], kind, floors=skew.FLOORS)]
            if was != now:
                moved.append((job, kind, len(was), len(now)))
    results.append(("the floors touch neither pathology log", not moved, str(moved)))

    # Without this the control above passes on a floor of zero, which silences nothing and
    # touches nothing. The floor has to be doing work in one direction to mean anything.
    results.append(("both floors are above zero",
                    skew.FLOORS[model.BYTES] > 0 and skew.FLOORS[model.COUNT] > 0,
                    "bytes {} count {}".format(skew.FLOORS[model.BYTES],
                                               skew.FLOORS[model.COUNT])))

    try:
        skew.floor_from(skew.MILLIS_BRACKET_AS_MEASURED)
        refused = False
    except ValueError:
        refused = True
    results.append(("the measured millisecond bracket is refused", refused,
                    "bracket {}".format(skew.MILLIS_BRACKET_AS_MEASURED)))
    return results


def main():
    apps = {job: eventlog.profile(only_log(os.path.join(FIXTURES, job))) for job in jobs()}

    print("brackets re-derived from the logs")
    read = brackets_from(apps)
    for kind in (model.BYTES, model.COUNT):
        noise, real = read[kind]
        agrees = "agrees" if (noise, real) == skew.BRACKETS[kind] else "DISAGREES"
        print("  {:6s} noise {:>10d}  real {:>10d}  span {:>6d}x  {} with skew.BRACKETS"
              .format(kind, noise, real, real // noise, agrees))

    print("floors derived from them")
    for kind in (model.BYTES, model.COUNT):
        noise, real = skew.BRACKETS[kind]
        floor = skew.floor_from(skew.BRACKETS[kind])
        print("  {:6s} floor {:>10d}  which is {:>4d}x the noise end and a {:d}th of the real"
              .format(kind, floor, floor // noise, real // floor))

    print("the millisecond floor is not derived")
    noise, real = skew.MILLIS_BRACKET_AS_MEASURED
    print("  millis measured bracket is ({}, {}), a noise end above a real end".format(
        noise, real))
    print("  so the floor stays at {}, placed by hand".format(skew.MILLIS_FLOOR))

    for kind, candidates in ((model.BYTES, BYTE_CANDIDATES), (model.COUNT, COUNT_CANDIDATES)):
        print("{} verdicts surviving each candidate floor".format(kind))
        for line in sweep(apps, kind, candidates):
            print("  " + line)

    results = controls(apps)
    failed = 0
    for label, ok, detail in results:
        print("control, {}: {}".format(label, "yes" if ok else "NO"))
        if detail:
            print("    {}".format(detail))
        if not ok:
            failed += 1
    print("controls: {} of {} failed".format(failed, len(results)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
