"""What a stage's accumulable list holds, and why the map onto task fields is declared.

    python scripts/accumulable_probe.py

The map used to be twelve names kept beside the fields. The question it left open was
what the other accumulables are, and nobody could fail that question because nothing
counted them. This prints the count, splits every name by what the model does with it,
and measures the two ways somebody would try to generate the map instead.

Both derivations fail and the numbers are the reason the names are declared on the
fields. Camel casing a field name reaches one of the twelve. Building the name out of the
task metrics leaf path reaches fourteen of thirty four, and the misses are not a pattern
anybody could code around.

The controls at the end drive the tolerant class and the two refusals from both sides. A
coverage report that has only ever seen a clean log is a report nobody has tested.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dataclasses  # noqa: E402

from sjp import eventlog, model  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(ROOT, "tests", "fixtures", "eventlogs")

# What the shuffle and input sections of the task metrics block are called in the name a
# stage accumulable carries. Used only by the second derivation below, which is being
# measured rather than relied on.
SECTIONS = {"Shuffle Read Metrics": "shuffle.read", "Shuffle Write Metrics": "shuffle.write",
            "Input Metrics": "input", "Output Metrics": "output",
            "Push Based Shuffle": "push"}


def logs():
    found = [path for path in eventlog.logs_under(FIXTURES) if not path.endswith(".md")]
    if not found:
        raise ValueError("no event logs under {}".format(FIXTURES))
    return found


def job_of(path):
    return os.path.basename(os.path.dirname(path))


def names_in(apps):
    """Every accumulable name any stage of any log carries."""
    found = set()
    for app in apps.values():
        for stage in app.stages:
            found.update(total.name for total in stage.totals)
    return found


def task_metric_leaves(path):
    """The task metrics block of the first task event, flattened to its leaf paths.

    This is the other view of the same quantities. A stage accumulable is the sum over
    the tasks and this is where each task's own copy sits, so it is the honest answer to
    how many of these the model could keep rather than how many one stage happens to
    report.
    """
    def walk(value, prefix=()):
        out = {}
        for key, inner in value.items():
            if isinstance(inner, dict):
                out.update(walk(inner, prefix + (key,)))
            elif not isinstance(inner, list):
                out[prefix + (key,)] = inner
        return out

    for event in eventlog.read_events(path):
        if event.get("Event") == "SparkListenerTaskEnd":
            return walk(event["Task Metrics"])
    raise ValueError("{} holds no task event".format(path))


def from_field_name(field_name):
    """The first derivation. Camel case the field name and hope."""
    head, *rest = field_name.split("_")
    return model.TASK_METRIC + head + "".join(word.capitalize() for word in rest)


def from_leaf_path(path):
    """The second derivation. Build the name out of the task metrics leaf path."""
    sections, words = [], []
    for segment in path:
        if segment in SECTIONS:
            sections.append(SECTIONS[segment])
        else:
            words.extend(segment.split())
    camel = words[0][0].lower() + words[0][1:]
    camel += "".join(word[0].upper() + word[1:] for word in words[1:])
    return model.TASK_METRIC + ".".join(sections + [camel])


def derivation_rows():
    """Field name against the real accumulable name, and whether the guess matched."""
    rows = []
    for name, field_name in sorted(model.STAGE_TOTAL_FIELDS.items(), key=lambda kv: kv[1]):
        guess = from_field_name(field_name)
        rows.append((field_name, guess, name, guess == name))
    return rows


def leaf_derivation(apps, path):
    """How many task metrics leaves the path rule turns into a name the logs really hold."""
    real = {name for name in names_in(apps) if name.startswith(model.TASK_METRIC)}
    leaves = task_metric_leaves(path)
    hits = [leaf for leaf in leaves if from_leaf_path(leaf) in real]
    return len(hits), len(leaves), real


def moved(apps):
    """Which metrics the model reads past have ever been nonzero in these logs."""
    seen = set()
    for app in apps.values():
        for stage in app.stages:
            for total in stage.totals:
                if model.classify(total.name) != model.READ_PAST:
                    continue
                try:
                    value = int(total.value)
                except (TypeError, ValueError):
                    continue
                if value:
                    seen.add(total.name)
    return seen


def leaf(name):
    return re.sub(r"[^a-z]", "", name.split(".")[-1].lower())


def task_metrics_by_leaf():
    """Task metric names grouped by their leaf spelling.

    A dict from leaf to one name was the first shape here and it was wrong in the way
    this whole day is about. `recordsread` is the leaf of three different names, so
    keying on it kept whichever the build order left and printed one twin for a name that
    has two. The value is a list because the question has more than one answer.
    """
    grouped = {}
    for name in sorted(set(model.STAGE_TOTAL_FIELDS) | set(model.DROPPED)):
        grouped.setdefault(leaf(name), []).append(name)
    return grouped


def spelling_collisions(apps):
    """Plan metric names that spell the same thing as a task metric, once the case is gone."""
    grouped = task_metrics_by_leaf()
    found = []
    for name in sorted(names_in(apps)):
        if model.classify(name) == model.PLAN and leaf(name) in grouped:
            found.append((name, grouped[leaf(name)]))
    return found


def answered_before_the_refusal(apps):
    """How many stages each plan metric name would have answered with a value.

    `reported_total` refuses these now. This counts what it used to do by going at the
    totals directly, so the number behind the refusal stays reproducible rather than
    becoming a sentence about a reader that no longer behaves that way.
    """
    counts = {}
    for app in apps.values():
        for stage in app.stages:
            rows = {}
            for total in stage.totals:
                rows.setdefault(total.name, []).append(total.value)
            for name, values in rows.items():
                if model.classify(name) != model.PLAN:
                    continue
                answered = len(values) == 1 and bool(values[0])
                counts[name] = counts.get(name, 0) + (1 if answered else 0)
    return counts


def unruled_is_reported_rather_than_refused(app):
    """A task metric nobody ruled on is named by the report and does not stop it."""
    stage = app.stages[-1]
    invented = model.Total(acc_id=-1, name=model.TASK_METRIC + "notAThingSparkWrites", value=1)
    damaged = dataclasses.replace(stage, totals=stage.totals + (invented,))
    cover = model.coverage(damaged)
    return cover.unruled == (invented.name,), "unruled came back {}".format(cover.unruled)


def a_clean_log_has_nothing_unruled(apps):
    """The other side of the control above, so the first one is not passing on noise."""
    for app in apps.values():
        for stage in app.stages:
            if model.coverage(stage).unruled:
                return False, "{} stage {} has unruled names".format(app.app_id, stage.stage_id)
    return True, ""


def a_plan_metric_name_is_refused(app):
    stage = app.stages[-1]
    try:
        stage.reported_total("number of output rows")
    except model.UnexpectedLog as problem:
        return "plan metric" in str(problem), str(problem)
    return False, "it answered"


def a_repeated_task_metric_is_refused(app):
    stage = app.stages[-1]
    doubled = dataclasses.replace(stage, totals=stage.totals + stage.totals)
    try:
        doubled.reported_total(model.TASK_METRIC + "executorRunTime")
    except model.UnexpectedLog as problem:
        return "2 times" in str(problem), str(problem)
    return False, "it answered"


def two_fields_cannot_claim_one_accumulable():
    name = model.TASK_METRIC + "executorRunTime"

    @dataclasses.dataclass(frozen=True)
    class Clash:
        one: int = model.measured(model.MILLIS, name)
        two: int = model.measured(model.MILLIS, name)

    try:
        model.stage_total_fields(Clash)
    except model.UnexpectedLog as problem:
        return "claimed by" in str(problem), str(problem)
    return False, "the collision was accepted"


def controls(apps):
    app = apps["skewed"]
    return [("a task metric nobody ruled on is reported",) + unruled_is_reported_rather_than_refused(app),
            ("a clean log reports none",) + a_clean_log_has_nothing_unruled(apps),
            ("a plan metric name is refused a stage total",) + a_plan_metric_name_is_refused(app),
            ("a repeated task metric name is refused",) + a_repeated_task_metric_is_refused(app),
            ("two fields claiming one accumulable is refused",) + two_fields_cannot_claim_one_accumulable()]


def summary(apps):
    """The counts the report leads with, worked out here rather than inside a print.

    Two of these are subtractions over numbers printed on the same line. A subtraction
    written into a format string is a number nobody can recompute, and the two that were
    there survived a mutation pass turning them into additions.
    """
    has_moved = moved(apps)
    return {"with_a_total": len(model.STAGE_TOTAL_FIELDS),
            "without_one": len(dataclasses.fields(model.Task)) - len(model.STAGE_TOTAL_FIELDS),
            "read_past": len(model.DROPPED),
            "has_moved": len(has_moved),
            "never_moved": len(model.DROPPED) - len(has_moved)}


def main():
    paths = logs()
    apps = {job_of(path): eventlog.profile(path) for path in paths}
    counts = summary(apps)

    print("the map, read off the fields")
    print("  {} task fields carry a stage total, {} do not".format(
        counts["with_a_total"], counts["without_one"]))
    print("  {} task metrics are read past, with a reason recorded for each".format(
        counts["read_past"]))

    print("deriving the name from the field name instead")
    hits = 0
    for field_name, guess, real, ok in derivation_rows():
        hits += ok
        if not ok:
            print("  miss  {:20s} {:46s} against {}".format(field_name, guess, real))
    print("  derived {} of {}".format(hits, len(model.STAGE_TOTAL_FIELDS)))

    print("deriving the name from the task metrics leaf path instead")
    hits, total, _real = leaf_derivation(apps, paths[0])
    print("  derived {} of {} leaves to a name the logs carry".format(hits, total))
    sizes = {len(task_metric_leaves(path)) for path in paths}
    print("  the task metrics block is {} leaves on every one of {} logs".format(
        sorted(sizes)[0] if len(sizes) == 1 else sorted(sizes), len(paths)))

    print("every accumulable name over all {} logs".format(len(paths)))
    found = names_in(apps)
    buckets = {}
    for name in found:
        buckets.setdefault(model.classify(name), []).append(name)
    for label in (model.KEPT, model.READ_PAST, model.PLAN, model.UNRULED):
        print("  {:11s} {}".format(label, len(buckets.get(label, []))))
    print("  {} distinct names in all".format(len(found)))

    print("what the model reads past, and whether it has ever moved here")
    has_moved = moved(apps)
    print("  {} of {} nonzero somewhere, {} zero on every stage of every log".format(
        counts["has_moved"], counts["read_past"], counts["never_moved"]))
    for name in sorted(has_moved):
        print("    moved  {}".format(name))

    print("coverage per stage, as the stages output prints it")
    for job in sorted(apps):
        for stage in apps[job].stages[:3]:
            cover = model.coverage(stage)
            print("  {:15s} stage {:<3d} {:>3d} rows  {:>2d} kept  {:>2d} read past"
                  "  {:>2d} plan  {} unruled".format(
                      job, stage.stage_id, cover.rows, len(cover.kept),
                      len(cover.read_past), len(cover.plan), len(cover.unruled)))

    print("plan metric names that spell the same thing as a task metric")
    answered = answered_before_the_refusal(apps)
    for name, twins in spelling_collisions(apps):
        print("  {:18s} answered on {:>3d} stages, and spells".format(
            name, answered.get(name, 0)))
        for twin in twins:
            print("      {:11s} {}".format(model.classify(twin), twin))
    shared = {spelling: names for spelling, names in task_metrics_by_leaf().items()
              if len(names) > 1}
    print("  task metric leaf spellings carried by more than one name: {}".format(len(shared)))
    for spelling, names in sorted(shared.items()):
        print("      {} is the leaf of {}".format(spelling, ", ".join(names)))

    results = controls(apps)
    failed = 0
    for label, ok, detail in results:
        print("control, {}: {}".format(label, "yes" if ok else "NO"))
        if not ok:
            print("    {}".format(detail))
            failed += 1
    print("controls: {} of {} failed".format(failed, len(results)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
