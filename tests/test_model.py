"""The stage and task model, and the one list this repo is allowed to keep by hand.

The event names the model reads are registered by its handlers. The first check here
walks the module's syntax tree and refuses any other Spark event name written into it,
because an inline comparison against a name nobody registered is how the list and the
parser come apart again.
"""
import ast
import dataclasses
import re
import os
import shutil
import sys
import tempfile

from sjp import eventlog, model

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
MODEL_SOURCE = os.path.join(ROOT, "sjp", "model.py")
SKEWED = os.path.join(HERE, "fixtures", "eventlogs", "skewed")
BALANCED = os.path.join(HERE, "fixtures", "eventlogs", "balanced")

PEAK = "internal.metrics.peakExecutionMemory"
GROUPING_STAGE = 2

def _only_log(directory):
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])

def _app(directory):
    return eventlog.profile(_only_log(directory))

def _every_log():
    """Every committed event log, so a coverage claim is over the whole fixture set."""
    root = os.path.join(HERE, "fixtures", "eventlogs")
    return [path for path in eventlog.logs_under(root) if not path.endswith(".md")]

def spark_names_in(source):
    """Every Spark event name written as a string literal in some source text."""
    found = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value.startswith("SparkListener") or ".SparkListener" in node.value:
                found.add(node.value)
    return found

def check_the_model_reads_no_event_name_it_did_not_register():
    with open(MODEL_SOURCE, encoding="utf-8") as handle:
        found = spark_names_in(handle.read())
    assert found == set(model.CONSUMES), sorted(found ^ set(model.CONSUMES))

def check_the_derivation_check_catches_a_name_that_was_never_registered():
    """Otherwise the check above passes on a module with a hand written name in it."""
    sneaked = 'if event["Event"] == "SparkListenerBlockManagerAdded":\n    pass\n'
    found = spark_names_in(sneaked)
    assert found == {"SparkListenerBlockManagerAdded"}, found
    assert not found <= set(model.CONSUMES), found

def check_every_registered_handler_fires_on_a_real_log():
    """A registry carrying an entry no log reaches is a list kept by hand again."""
    for directory in (SKEWED, BALANCED):
        app = _app(directory)
        assert app.events_used == frozenset(model.CONSUMES), sorted(
            app.events_used ^ frozenset(model.CONSUMES))

def check_registering_one_name_twice_is_refused():
    try:
        model.handles("SparkListenerTaskEnd", "a second opinion")
    except model.UnexpectedLog:
        pass
    else:
        raise AssertionError("one event name was handled twice")

def check_an_event_with_no_handler_is_skipped_rather_than_refused():
    app = model.build([
        {"Event": "SparkListenerApplicationStart", "App ID": "x", "Timestamp": 1},
        {"Event": "SparkListenerSomethingNewInTheNextVersion"},
        {"Event": "SparkListenerStageSubmitted",
         "Stage Info": {"Stage ID": 0, "Stage Attempt ID": 0, "Number of Tasks": 1}},
    ])
    assert "SparkListenerSomethingNewInTheNextVersion" not in app.events_used, app.events_used
    assert len(app.stages) == 1, app.stages

def check_a_file_with_no_application_start_is_refused():
    try:
        model.build([{"Event": "SparkListenerJobStart", "Job ID": 0}])
    except model.UnexpectedLog:
        pass
    else:
        raise AssertionError("a log with no application in it built an application")

def check_an_application_that_ran_no_stages_is_refused():
    try:
        model.build([{"Event": "SparkListenerApplicationStart", "App ID": "x",
                      "Timestamp": 1}])
    except model.UnexpectedLog:
        pass
    else:
        raise AssertionError("an application with no stages built anyway")

def check_the_application_carries_what_the_log_says_about_the_run():
    app = _app(SKEWED)
    assert app.name == "sjp-skewed", app.name
    assert app.cores == 2, app.cores
    assert app.properties["spark.sql.shuffle.partitions"] == "8", app.properties
    assert app.wall_time == app.end_time - app.start_time, app.wall_time
    assert len(app.jobs) == 1, app.jobs
    assert app.jobs[0].stage_ids == (0, 1, 2), app.jobs[0]
    assert app.jobs[0].result == "JobSucceeded", app.jobs[0]

def check_every_stage_a_job_names_was_actually_built():
    for directory in (SKEWED, BALANCED):
        app = _app(directory)
        built = {stage.stage_id for stage in app.stages}
        for job in app.jobs:
            assert set(job.stage_ids) <= built, (job.stage_ids, built)

def check_asking_for_a_stage_that_is_not_there_raises():
    try:
        _app(SKEWED).stage(99)
    except model.UnexpectedLog:
        pass
    else:
        raise AssertionError("a stage nobody ran came back")

def check_spark_planned_as_many_tasks_as_it_ran():
    """A retry would break this and there is no retry in either fixture."""
    for directory in (SKEWED, BALANCED):
        for stage in _app(directory).stages:
            assert len(stage.tasks) == stage.declared_tasks, (stage.stage_id, stage.tasks)

def check_a_task_duration_is_a_subtraction_and_not_the_executor_run_time():
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    for task in stage.tasks:
        assert task.duration == task.finish_time - task.launch_time, task
        assert task.outside_run_time == task.duration - task.executor_run_time, task
        assert task.executor_run_time < task.duration, task

def check_the_records_and_the_durations_are_not_the_same_column():
    """Both are read off one task and a swap between them would go unnoticed."""
    for directory in (SKEWED, BALANCED):
        stage = _app(directory).stage(1)
        assert stage.median("records_read") == 1000000, stage.median("records_read")
        assert stage.largest("duration") < 10000, stage.largest("duration")

def check_a_stage_with_no_median_has_no_spread_rather_than_a_spread_of_zero():
    """It answered 0.0 before the detector was written, which reads as perfectly even.

    The stage picked here is the harmless case, where the maximum is zero too. The one
    that made the old answer wrong is the skewed grouping stage's spill, and that lives in
    `tests/test_skew.py` beside the detector it would have fooled.
    """
    stage = _app(SKEWED).stage(0)
    assert stage.median("records_read") == 0, stage.median("records_read")
    assert stage.spread("records_read") is None, stage.spread("records_read")
    assert stage.spread("duration") > 0, stage.spread("duration")

def check_the_absent_spread_is_printed_as_words_rather_than_as_a_blank():
    stage = _app(SKEWED).stage(0)
    assert model.spread_text(stage, "records_read") == "no median to divide by"
    assert model.spread_text(stage, "duration") == "1.00", model.spread_text(stage, "duration")

def check_the_spread_is_the_largest_over_the_median():
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    expected = stage.largest("records_read") / stage.median("records_read")
    assert stage.spread("records_read") == expected, stage.spread("records_read")
    assert round(stage.spread("records_read"), 2) == 44.22, stage.spread("records_read")

def check_every_stage_total_the_log_reports_equals_the_sum_over_its_tasks():
    """Twelve metrics over six stages. The totals are right and that is the problem."""
    for directory in (SKEWED, BALANCED):
        for stage in _app(directory).stages:
            assert model.disagreements(stage) == [], (stage.stage_id,
                                                      model.disagreements(stage))

def check_that_agreement_check_would_notice_a_stage_whose_tasks_moved():
    """A comparison that has never failed is indistinguishable from one comparing nothing."""
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    damaged = dataclasses.replace(stage, tasks=stage.tasks[:-1])
    names = [name for name, _reported, _summed in model.disagreements(damaged)]
    assert "internal.metrics.memoryBytesSpilled" in names, names
    assert "internal.metrics.executorRunTime" in names, names

def check_a_total_that_measured_zero_is_absent_rather_than_zero():
    """The key set is a property of the data. Both directions are asserted here."""
    stage = _app(BALANCED).stage(GROUPING_STAGE)
    names = {total.name for total in stage.totals}
    assert "internal.metrics.memoryBytesSpilled" not in names, sorted(names)
    assert stage.reported_total("internal.metrics.memoryBytesSpilled") == 0, stage.totals
    assert stage.total("memory_spilled") == 0, stage.total("memory_spilled")

    spilled = _app(SKEWED).stage(GROUPING_STAGE)
    present = {total.name for total in spilled.totals}
    assert "internal.metrics.memoryBytesSpilled" in present, sorted(present)

def check_two_runs_of_almost_the_same_job_report_different_numbers_of_totals():
    """Which is the point. A reader keying on presence is reading the data."""
    skewed = len(_app(SKEWED).stage(GROUPING_STAGE).totals)
    balanced = len(_app(BALANCED).stage(GROUPING_STAGE).totals)
    assert (skewed, balanced) == (37, 35), (skewed, balanced)

def check_a_plan_metric_name_is_refused_by_the_stage_total_reader():
    """`number of output rows` belongs to a plan node and has no stage total at all.

    This check used to assert the repeated name refusal instead, and it is the check that
    caught the reader changing underneath it, because it asserted the message rather than
    the exception type. The two refusals are separate now and so are their checks.
    """
    for directory in (SKEWED, BALANCED):
        stage = _app(directory).stage(GROUPING_STAGE)
        names = [total.name for total in stage.totals]
        assert names.count("number of output rows") == 2, names
        try:
            stage.reported_total("number of output rows")
        except model.UnexpectedLog as problem:
            assert "plan metric" in str(problem), problem
            assert "accumulator id" in str(problem), problem
        else:
            raise AssertionError("a plan metric name answered with a stage total")


def _leaf(name):
    """The last segment of an accumulable name with the spelling taken out of it."""
    return re.sub(r"[^a-z]", "", name.split(".")[-1].lower())


def check_three_plan_metric_names_spell_the_same_thing_as_a_task_metric():
    """The reason the refusal above exists, measured rather than argued.

    Strip the namespace and the capitals off and three plan metric names in these logs
    are byte identical to a task metric leaf. Two of the three reach a metric the model
    keeps. The reader answered each of the three with a plan node's number on 36 stages
    and refused it on 2, and the two it never refused at all are a different pair.
    `check_the_plan_metric_names_the_old_reader_always_answered` has those.
    """
    found = set()
    for path in _every_log():
        for stage in eventlog.profile(path).stages:
            found.update(total.name for total in stage.totals)
    task_leaves = {_leaf(name) for name in set(model.STAGE_TOTAL_FIELDS) | set(model.DROPPED)}
    collisions = sorted(name for name in found
                        if model.classify(name) == model.PLAN and _leaf(name) in task_leaves)
    assert collisions == ["fetch wait time", "local bytes read", "records read"], collisions
    kept_leaves = {_leaf(name) for name in model.STAGE_TOTAL_FIELDS}
    on_the_map = [name for name in collisions if _leaf(name) in kept_leaves]
    assert on_the_map == ["local bytes read", "records read"], on_the_map


def check_the_plan_metric_names_the_old_reader_always_answered():
    """Two names the stage total reader answered on every stage that carried them.

    A duplicated name was refused by count even before the namespace refusal existed, so
    the names at real risk were the ones appearing exactly once. These two appear once on
    all 40 stages that carry them and both spell a shuffle write quantity. The figures
    come out of the totals rather than out of the reader, because the reader refuses them
    now and a number that cannot be recomputed is a sentence.
    """
    sys.path.insert(0, os.path.join(ROOT, "scripts"))
    import accumulable_probe

    apps = {}
    root = os.path.join(ROOT, "tests", "fixtures", "eventlogs")
    for job in sorted(os.listdir(root)):
        directory = os.path.join(root, job)
        if os.path.isdir(directory):
            apps[job] = _app(directory)
    answered = accumulable_probe.answered_before_the_refusal(apps)
    assert answered["shuffle bytes written"] == 40, answered["shuffle bytes written"]
    assert answered["shuffle write time"] == 40, answered["shuffle write time"]
    assert answered["peak memory"] == 5, answered["peak memory"]
    assert answered["local bytes read"] == 36, answered["local bytes read"]
    for name in ("shuffle bytes written", "shuffle write time", "peak memory"):
        assert model.classify(name) == model.PLAN, name


def check_the_plan_metric_a_word_away_from_the_summed_peak_is_a_plan_metric():
    """`peak memory` is the collision that would have cost the most.

    It does not spell the same thing as `peakExecutionMemory` and it reads as the same
    question, and the summed peak is the one metric in this file that ranks the job which
    spread its work out as the memory problem. So the two names a reader would confuse
    are the stage total that is already known to mislead and a number about one node.
    """
    assert model.classify("peak memory") == model.PLAN
    assert model.classify(PEAK) == model.KEPT
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    assert "peak memory" in {total.name for total in stage.totals}, stage.totals
    assert stage.reported_total(PEAK) > stage.peak_memory, stage.totals


def check_a_repeated_task_metric_name_is_refused_by_count():
    """The other refusal, which no committed log can reach.

    Only the plan metric names repeat in these eight logs, so after the namespace refusal
    nothing real reaches the count branch. It stays because the format permits a repeat
    under any name and the whole reason the key is the id is that names are not unique. A
    stage carrying its own totals twice is what tests it.
    """
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    doubled = dataclasses.replace(stage, totals=stage.totals + stage.totals)
    try:
        doubled.reported_total("internal.metrics.executorRunTime")
    except model.UnexpectedLog as problem:
        assert "2 times" in str(problem), problem
    else:
        raise AssertionError("a task metric appearing twice answered with one value")
    assert stage.reported_total("internal.metrics.executorRunTime") == 4560, stage.totals


def check_every_task_field_declares_whether_it_has_a_stage_total():
    """A field arriving without an answer to that question is how the map drifted before."""
    for entry in dataclasses.fields(model.Task):
        assert "total" in entry.metadata, entry.name
        name = entry.metadata["total"]
        assert name is None or name.startswith(model.TASK_METRIC), (entry.name, name)


def check_the_stage_total_map_is_read_off_the_fields_rather_than_kept():
    """Twelve entries, and every one of them points at a field that exists."""
    derived = model.stage_total_fields()
    assert derived == model.STAGE_TOTAL_FIELDS, derived
    assert len(derived) == 12, len(derived)
    names = {entry.name for entry in dataclasses.fields(model.Task)}
    assert set(derived.values()) <= names, set(derived.values()) - names
    declared = {entry.metadata["total"] for entry in dataclasses.fields(model.Task)
                if entry.metadata["total"]}
    assert declared == set(derived), declared ^ set(derived)


def check_two_fields_claiming_one_accumulable_is_refused():
    """Otherwise the refusal is a branch nothing has ever taken."""
    name = model.TASK_METRIC + "executorRunTime"

    @dataclasses.dataclass(frozen=True)
    class Clash:
        one: int = model.measured(model.MILLIS, name)
        two: int = model.measured(model.MILLIS, name)

    try:
        model.stage_total_fields(Clash)
    except model.UnexpectedLog as problem:
        assert "claimed by one and by two" in str(problem), problem
    else:
        raise AssertionError("two fields claimed one accumulable and nothing objected")


def check_camel_casing_a_field_name_does_not_produce_the_accumulable_name():
    """The measured reason the accumulable is declared on the field.

    This is the derivation the thread asking for the map to be generated proposed, and it
    reaches one of the twelve. The one it reaches is `executorRunTime`, which is the only
    field in the record whose name was already Spark's.
    """
    hits = []
    for name, field_name in model.STAGE_TOTAL_FIELDS.items():
        head, *rest = field_name.split("_")
        guess = model.TASK_METRIC + head + "".join(word.capitalize() for word in rest)
        if guess == name:
            hits.append(field_name)
    assert hits == ["executor_run_time"], hits


def check_every_declared_accumulable_appears_in_a_committed_log():
    """A declared name no log carries is a name somebody typed."""
    found = set()
    for path in _every_log():
        for stage in eventlog.profile(path).stages:
            found.update(total.name for total in stage.totals)
    missing = sorted(set(model.STAGE_TOTAL_FIELDS) - found)
    assert missing == [], missing
    unseen = sorted(set(model.DROPPED) - found)
    assert unseen == [], unseen


def check_every_task_metric_in_every_committed_log_is_kept_or_read_past():
    """The completeness report. Twelve kept plus nineteen read past covers all of them."""
    unruled = set()
    for path in _every_log():
        for stage in eventlog.profile(path).stages:
            unruled.update(model.coverage(stage).unruled)
    assert unruled == set(), sorted(unruled)
    assert set(model.STAGE_TOTAL_FIELDS) & set(model.DROPPED) == set(), "a name is both"
    assert len(model.DROPPED) == 19, len(model.DROPPED)
    for name, reason in model.DROPPED.items():
        assert reason.strip(), name


def check_the_unruled_class_catches_a_task_metric_nobody_ruled_on():
    """Otherwise the check above passes on a log holding nothing to rule on."""
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    invented = model.Total(acc_id=-1, name=model.TASK_METRIC + "notAThingSparkWrites",
                           value=7)
    damaged = dataclasses.replace(stage, totals=stage.totals + (invented,))
    cover = model.coverage(damaged)
    assert cover.unruled == (invented.name,), cover
    assert model.classify(invented.name) == model.UNRULED
    # `distinct` adds four lengths and the unruled one is zero on every committed log, so
    # a mutant subtracting it instead of adding it survived the whole suite. One stage
    # with an unruled name in it is what makes that term count.
    clean = model.coverage(stage)
    assert cover.distinct == clean.distinct + 1, (cover.distinct, clean.distinct)
    assert cover.rows == clean.rows + 1, (cover.rows, clean.rows)


def check_the_grouping_stage_coverage_is_the_split_the_readme_publishes():
    """Thirty seven rows is not twelve plus twenty five, which is what it used to say.

    Ten of the twelve are on this stage and two never moved, so the readable number is a
    property of the log. The twenty seven it does not keep split two ways, into task
    metrics it reads past and plan metrics that were never candidates for the map.
    """
    cover = model.coverage(_app(SKEWED).stage(GROUPING_STAGE))
    assert cover.rows == 37, cover.rows
    assert cover.distinct == 36, cover.distinct
    assert len(cover.kept) == 10, cover.kept
    assert len(cover.read_past) == 17, cover.read_past
    assert len(cover.plan) == 9, cover.plan
    assert cover.unruled == (), cover.unruled
    assert cover.repeated == ("number of output rows",), cover.repeated


def check_the_row_count_and_the_name_count_differ_by_the_repeat():
    """Which is the whole reason the key is the id rather than the name."""
    for path in _every_log():
        for stage in eventlog.profile(path).stages:
            cover = model.coverage(stage)
            extra = cover.rows - cover.distinct
            assert extra >= 0, (cover.rows, cover.distinct)
            assert extra == 0 or cover.repeated, cover
    stage = _app(SKEWED).stage(0)
    assert model.coverage(stage).rows == model.coverage(stage).distinct


def check_one_leaf_spelling_is_carried_by_two_task_metrics():
    """`recordsread` names two of them, which is why a leaf cannot be an address.

    The first version of the probe keyed a dict on the leaf spelling and so reported one
    twin for a plan metric that has two. That is the same mistake this day is about, made
    in the code written to describe it, and it is the reason the grouping is a list.
    """
    grouped = {}
    for name in sorted(set(model.STAGE_TOTAL_FIELDS) | set(model.DROPPED)):
        grouped.setdefault(_leaf(name), []).append(name)
    shared = {spelling: names for spelling, names in grouped.items() if len(names) > 1}
    assert sorted(shared) == ["recordsread"], sorted(shared)
    assert shared["recordsread"] == [model.TASK_METRIC + "input.recordsRead",
                                     model.TASK_METRIC + "shuffle.read.recordsRead"], shared
    assert model.classify(shared["recordsread"][0]) == model.READ_PAST
    assert model.classify(shared["recordsread"][1]) == model.KEPT


def check_the_accumulable_probe_runs_every_control_and_reaches_both_sides():
    """The probe is what publishes the day's numbers, so the suite grades it too."""
    sys.path.insert(0, os.path.join(ROOT, "scripts"))
    import accumulable_probe

    apps = {job: _app(os.path.join(ROOT, "tests", "fixtures", "eventlogs", job))
            for job in ("skewed", "balanced")}
    results = accumulable_probe.controls(apps)
    assert len(results) == 5, results
    assert [ok for _label, ok, _detail in results] == [True] * 5, results

    ok, detail = accumulable_probe.a_clean_log_has_nothing_unruled(apps)
    assert (ok, detail) == (True, ""), detail
    stage = apps["skewed"].stage(GROUPING_STAGE)
    invented = model.Total(acc_id=-1, name=model.TASK_METRIC + "nope", value=1)
    damaged = dataclasses.replace(apps["skewed"], stages=(
        dataclasses.replace(stage, totals=stage.totals + (invented,)),))
    broken, detail = accumulable_probe.a_clean_log_has_nothing_unruled({"x": damaged})
    assert broken is False, detail
    assert "unruled" in detail, detail


def check_the_probe_agrees_with_the_model_on_both_derivation_counts():
    """The two numbers in the docstring of `measured`, recomputed rather than quoted."""
    sys.path.insert(0, os.path.join(ROOT, "scripts"))
    import accumulable_probe

    rows = accumulable_probe.derivation_rows()
    assert len(rows) == 12, len(rows)
    assert sum(1 for _f, _g, _r, ok in rows if ok) == 1, rows

    apps = {"skewed": _app(SKEWED)}
    hits, total, _real = accumulable_probe.leaf_derivation(apps, _only_log(SKEWED))
    assert (hits, total) == (14, 34), (hits, total)


def _probe():
    sys.path.insert(0, os.path.join(ROOT, "scripts"))
    import accumulable_probe
    return accumulable_probe


def _all_apps():
    root = os.path.join(ROOT, "tests", "fixtures", "eventlogs")
    return {job: _app(os.path.join(root, job)) for job in sorted(os.listdir(root))
            if os.path.isdir(os.path.join(root, job))}


def check_the_probe_counts_are_computed_rather_than_written_into_a_print():
    """The two subtractions the report leads with, and the log list it walks."""
    probe = _probe()
    apps = _all_apps()
    counts = probe.summary(apps)
    assert counts == {"with_a_total": 12, "without_one": 8, "read_past": 19,
                      "has_moved": 7, "never_moved": 12}, counts
    assert counts["with_a_total"] + counts["without_one"] == len(
        dataclasses.fields(model.Task))
    assert counts["has_moved"] + counts["never_moved"] == counts["read_past"]

    paths = probe.logs()
    assert len(paths) == 8, paths
    assert not any(path.endswith(".md") for path in paths), paths
    assert all(probe.job_of(path) in apps for path in paths), paths


def check_the_probe_names_every_metric_that_has_moved():
    """Driven both ways, because a list of seven is also what a broken filter returns."""
    probe = _probe()
    apps = _all_apps()
    moved = probe.moved(apps)
    assert len(moved) == 7, sorted(moved)
    assert model.TASK_METRIC + "resultSize" in moved, sorted(moved)
    assert all(model.classify(name) == model.READ_PAST for name in moved), sorted(moved)

    one = apps["skewed"]
    stage = one.stage(GROUPING_STAGE)
    kept = tuple(total for total in stage.totals
                 if model.classify(total.name) != model.READ_PAST)
    stripped = dataclasses.replace(one, stages=(dataclasses.replace(stage, totals=kept),))
    assert probe.moved({"x": stripped}) == set(), probe.moved({"x": stripped})


def check_the_probe_groups_a_leaf_spelling_rather_than_picking_one_name():
    """The shape of the answer is the finding. One leaf, two task metrics."""
    probe = _probe()
    grouped = probe.task_metrics_by_leaf()
    assert grouped["recordsread"] == [model.TASK_METRIC + "input.recordsRead",
                                      model.TASK_METRIC + "shuffle.read.recordsRead"], grouped
    assert grouped["peakexecutionmemory"] == [model.TASK_METRIC + "peakExecutionMemory"]

    collisions = probe.spelling_collisions(_all_apps())
    names = [name for name, _twins in collisions]
    assert names == ["fetch wait time", "local bytes read", "records read"], names
    twins = dict(collisions)
    assert len(twins["records read"]) == 2, twins["records read"]
    assert len(twins["local bytes read"]) == 1, twins["local bytes read"]


def check_the_leaf_path_derivation_gets_one_right_and_these_two_wrong():
    """Pin the rule's answers, so a mutant inside it moves an asserted string.

    `leaf_derivation` only asserts a count, and a count is reachable by more than one
    wrong rule. These three are the shapes that decided the rule cannot be the map.
    """
    probe = _probe()
    hit = ("Shuffle Read Metrics", "Local Bytes Read")
    assert probe.from_leaf_path(hit) == model.TASK_METRIC + "shuffle.read.localBytesRead"
    assert probe.from_leaf_path(("JVM GC Time",)) == model.TASK_METRIC + "jVMGCTime"
    assert probe.from_leaf_path(("Shuffle Read Metrics", "Total Records Read")) == (
        model.TASK_METRIC + "shuffle.read.totalRecordsRead")
    assert probe.from_field_name("gc_time") == model.TASK_METRIC + "gcTime"
    assert probe.from_field_name("executor_run_time") == (
        model.TASK_METRIC + "executorRunTime")
    assert probe.leaf("internal.metrics.shuffle.read.localBytesRead") == "localbytesread"


def check_seven_of_the_metrics_read_past_have_moved_in_the_committed_logs():
    """Twelve of nineteen are zero everywhere here, and that is not evidence either way.

    The nine push based shuffle names cannot move outside a cluster running it, and three
    remote fetch names cannot move on one machine. Counting those as a gap in the map
    would be counting a property of where this ran.
    """
    moved = set()
    for path in _every_log():
        for stage in eventlog.profile(path).stages:
            for total in stage.totals:
                if model.classify(total.name) != model.READ_PAST:
                    continue
                try:
                    value = int(total.value)
                except (TypeError, ValueError):
                    continue
                if value:
                    moved.add(total.name)
    assert len(moved) == 7, sorted(moved)
    still = sorted(set(model.DROPPED) - moved)
    assert len(still) == 12, still
    assert all("push" in name or "remote" in name for name in still), still

def check_a_total_name_that_appears_once_still_answers():
    """Otherwise the refusal above could be refusing everything."""
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    assert stage.reported_total("internal.metrics.executorRunTime") == 4560, stage.totals

def check_a_total_value_is_not_always_a_number():
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    rows = [total for total in stage.totals if total.name == "number of output rows"]
    assert [total.value for total in rows] == ["200", "200"], rows
    assert len({total.acc_id for total in rows}) == 2, rows

def check_the_stage_peak_memory_total_is_a_sum_of_peaks_and_not_a_peak():
    """The measured reason `Stage.peak_memory` reads the tasks instead."""
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    assert stage.reported_total(PEAK) == stage.total("peak_memory"), stage.totals
    assert stage.peak_memory == 377486768, stage.peak_memory
    assert stage.reported_total(PEAK) == 562035856, stage.reported_total(PEAK)
    assert stage.peak_memory < stage.reported_total(PEAK), stage.peak_memory

def check_the_summed_peak_ranks_the_job_that_did_not_spill_as_the_worse_one():
    """Which is the whole argument for not using it."""
    skewed = _app(SKEWED).stage(GROUPING_STAGE)
    balanced = _app(BALANCED).stage(GROUPING_STAGE)
    assert balanced.reported_total(PEAK) > skewed.reported_total(PEAK), (
        balanced.reported_total(PEAK), skewed.reported_total(PEAK))
    assert balanced.peak_memory < skewed.peak_memory, (balanced.peak_memory,
                                                       skewed.peak_memory)
    assert balanced.total("memory_spilled") == 0, balanced.total("memory_spilled")
    assert skewed.total("memory_spilled") == 620755808, skewed.total("memory_spilled")

def check_the_records_read_the_two_jobs_did_are_the_same():
    """The jobs differ in the key expression. They do not differ in how much they move."""
    skewed = _app(SKEWED).stage(GROUPING_STAGE)
    balanced = _app(BALANCED).stage(GROUPING_STAGE)
    assert skewed.total("records_read") == balanced.total("records_read") == 8000000, (
        skewed.total("records_read"), balanced.total("records_read"))

def check_the_records_are_a_stage_apart_from_the_bytes():
    stage = _app(SKEWED).stage(1)
    assert stage.total("records_written") == 8000000, stage.total("records_written")
    assert stage.total("bytes_written") == 130788590, stage.total("bytes_written")
    assert stage.total("remote_bytes_read") == 0, stage.total("remote_bytes_read")
    assert stage.total("local_bytes_read") == 42048255, stage.total("local_bytes_read")

def check_the_records_are_the_records_the_profiler_needs_for_a_partition_count():
    """Shuffle volume and cores are the two halves of a partition recommendation."""
    app = _app(SKEWED)
    assert app.cores == 2, app.cores
    assert app.stage(1).total("bytes_written") > 0, app.stage(1)

def check_the_records_of_a_failed_task_are_marked_as_such():
    """No fixture has one, so this is built rather than read."""
    app = model.build([
        {"Event": "SparkListenerApplicationStart", "App ID": "x", "Timestamp": 1},
        _task_event(failed=True),
    ])
    assert app.stage(7).tasks[0].failed is True, app.stage(7).tasks
    assert model.build([
        {"Event": "SparkListenerApplicationStart", "App ID": "x", "Timestamp": 1},
        _task_event(failed=False),
    ]).stage(7).tasks[0].failed is False

def _task_event(failed):
    zero = {"Total Records Read": 0, "Local Bytes Read": 0, "Remote Bytes Read": 0}
    return {
        "Event": "SparkListenerTaskEnd", "Stage ID": 7, "Stage Attempt ID": 0,
        "Task Info": {"Index": 0, "Executor ID": "driver", "Launch Time": 10,
                      "Finish Time": 30, "Failed": failed},
        "Task Metrics": {"Executor Run Time": 5, "Executor Deserialize Time": 1,
                         "Result Serialization Time": 0, "JVM GC Time": 0,
                         "Peak Execution Memory": 0, "Memory Bytes Spilled": 0,
                         "Disk Bytes Spilled": 0, "Shuffle Read Metrics": zero,
                         "Shuffle Write Metrics": {"Shuffle Records Written": 0,
                                                   "Shuffle Bytes Written": 0}},
    }

def check_a_stage_wall_time_is_the_submission_subtracted_from_the_completion():
    stage = _app(SKEWED).stage(GROUPING_STAGE)
    assert stage.wall_time == stage.completion_time - stage.submission_time, stage
    assert stage.wall_time == 3879, stage.wall_time

def check_a_stage_seen_only_through_its_task_events_declares_no_tasks():
    """The submitted event is where the planned count lives and this log has none."""
    app = model.build([
        {"Event": "SparkListenerApplicationStart", "App ID": "x", "Timestamp": 1},
        _task_event(failed=False),
    ])
    stage = app.stage(7)
    assert stage.declared_tasks == 0, stage.declared_tasks
    assert len(stage.tasks) == 1, stage.tasks
    assert stage.totals == (), stage.totals

def check_a_task_that_does_not_say_whether_it_failed_did_not():
    """Spark writes the flag. A log that leaves it out should not invent a failure."""
    event = _task_event(failed=False)
    del event["Task Info"]["Failed"]
    app = model.build([
        {"Event": "SparkListenerApplicationStart", "App ID": "x", "Timestamp": 1},
        event,
    ])
    assert app.stage(7).tasks[0].failed is False, app.stage(7).tasks[0]

def check_an_executor_that_reports_no_core_count_adds_none():
    app = model.build([
        {"Event": "SparkListenerApplicationStart", "App ID": "x", "Timestamp": 1},
        {"Event": "SparkListenerExecutorAdded", "Executor ID": "1", "Executor Info": {}},
        _task_event(failed=False),
    ])
    assert app.cores == 0, app.cores

    with_cores = model.build([
        {"Event": "SparkListenerApplicationStart", "App ID": "x", "Timestamp": 1},
        {"Event": "SparkListenerExecutorAdded", "Executor ID": "1",
         "Executor Info": {"Total Cores": 4}},
        {"Event": "SparkListenerExecutorAdded", "Executor ID": "2",
         "Executor Info": {"Total Cores": 4}},
        _task_event(failed=False),
    ])
    assert with_cores.cores == 8, with_cores.cores

def check_a_task_with_no_partition_id_falls_back_to_its_index():
    app = model.build([
        {"Event": "SparkListenerApplicationStart", "App ID": "x", "Timestamp": 1},
        _task_event(failed=False),
    ])
    assert app.stage(7).tasks[0].partition == 0, app.stage(7).tasks[0]

def _dataclasses_in(holder):
    """Every dataclass a module or a class exposes, with whether it is frozen."""
    found = {}
    for name in sorted(dir(holder)):
        value = getattr(holder, name)
        if isinstance(value, type) and dataclasses.is_dataclass(value):
            found[name] = value.__dataclass_params__.frozen
    return found


def check_every_record_in_the_model_is_frozen():
    """Driven off the module rather than off a list of records somebody remembered.

    The behavioural check below builds five records by hand and asserts each refuses an
    edit. `Coverage` arrived as the sixth and that list did not, so a mutant turning its
    frozen flag off survived a pass over the whole module. A list of names kept beside
    the thing it describes is what this file spent the day removing, and it was sitting
    inside the test written to prevent exactly this.
    """
    found = _dataclasses_in(model)
    assert len(found) >= 6, found
    assert "Coverage" in found, sorted(found)
    unfrozen = sorted(name for name, frozen in found.items() if not frozen)
    assert unfrozen == [], unfrozen


def check_the_frozen_walk_would_catch_a_record_that_was_not():
    """Otherwise the walk above is a loop over records that all happen to be fine."""
    @dataclasses.dataclass(frozen=False)
    class Loose:
        a: int = 0

    class Holder:
        pass

    Holder.Loose = Loose
    Holder.Coverage = model.Coverage
    found = _dataclasses_in(Holder)
    assert found == {"Coverage": True, "Loose": False}, found


def check_the_records_of_the_model_cannot_be_written_to():
    app = _app(SKEWED)
    stage = app.stage(GROUPING_STAGE)
    for record, name, value in ((app, "cores", 99), (stage, "stage_id", 99),
                                (stage.tasks[0], "index", 99), (app.jobs[0], "job_id", 99),
                                (stage.totals[0], "value", 99)):
        try:
            setattr(record, name, value)
        except dataclasses.FrozenInstanceError:
            continue
        raise AssertionError("{} let {} be written to".format(type(record).__name__, name))

def check_a_stage_submitted_but_never_completed_still_builds():
    """A log from a job that died mid stage is a real thing to be handed."""
    root = tempfile.mkdtemp(prefix="sjp-partial-")
    try:
        path = os.path.join(root, "partial")
        with open(path, "w") as handle:
            handle.write('{"Event":"SparkListenerApplicationStart","App ID":"x",'
                         '"Timestamp":1}\n')
            handle.write('{"Event":"SparkListenerStageSubmitted","Stage Info":'
                         '{"Stage ID":4,"Stage Attempt ID":0,"Number of Tasks":9,'
                         '"Submission Time":100}}\n')
        app = eventlog.profile(path)
        stage = app.stage(4)
        assert stage.declared_tasks == 9, stage
        assert stage.tasks == (), stage
        assert stage.completion_time is None, stage
    finally:
        shutil.rmtree(root)

# the kinds declared on the task record

def check_every_task_field_declares_a_kind():
    """The check that makes the declaration a single place rather than a habit.

    A field added without a kind fails here, which is the whole reason the kind lives on the
    field instead of in a tuple somewhere that a detector reads.
    """
    for entry in dataclasses.fields(model.Task):
        assert "kind" in entry.metadata, entry.name
        assert entry.metadata["kind"] in model.KINDS, (entry.name, entry.metadata)

def check_every_kind_is_used_by_something():
    """A kind nothing declares is a category that was imagined rather than found."""
    declared = {entry.metadata["kind"] for entry in dataclasses.fields(model.Task)}
    declared.update(model.DERIVED.values())
    assert declared == set(model.KINDS), declared

def check_every_numeric_property_of_a_task_is_a_declared_derived_quantity():
    """A property cannot carry field metadata, so this is what keeps DERIVED honest.

    Adding a numeric property and forgetting to declare it fails here rather than leaving a
    quantity the detector never judges.
    """
    task = _app(SKEWED).stages[-1].tasks[0]
    for name in dir(model.Task):
        if name.startswith("_") or not isinstance(getattr(model.Task, name), property):
            continue
        if isinstance(getattr(task, name), (int, float)):
            assert name in model.DERIVED, name
            assert model.DERIVED[name] in model.KINDS, name

def check_quantities_holds_every_measured_field_and_nothing_else():
    measured = [entry.name for entry in dataclasses.fields(model.Task)
                if entry.metadata["kind"] in model.MEASURED]
    assert set(model.QUANTITIES) == set(measured) | set(model.DERIVED), model.QUANTITIES
    assert len(model.QUANTITIES) == len(set(model.QUANTITIES)), model.QUANTITIES
    for name in ("stage_id", "index", "partition", "executor_id", "launch_time",
                 "finish_time", "failed"):
        assert name not in model.QUANTITIES, name

def check_the_order_of_quantities_is_the_order_the_record_declares():
    """So a report's rows do not move when somebody sorts something."""
    measured = [entry.name for entry in dataclasses.fields(model.Task)
                if entry.metadata["kind"] in model.MEASURED]
    assert list(model.QUANTITIES[:len(measured)]) == measured, model.QUANTITIES

def check_kind_of_answers_for_a_field_and_for_a_derived_value():
    assert model.kind_of("memory_spilled") == model.BYTES
    assert model.kind_of("records_read") == model.COUNT
    assert model.kind_of("gc_time") == model.MILLIS
    assert model.kind_of("launch_time") == model.INSTANT
    assert model.kind_of("executor_id") == model.IDENTITY
    assert model.kind_of("failed") == model.FLAG
    assert model.kind_of("duration") == model.MILLIS
    assert model.kind_of("outside_run_time") == model.MILLIS

def check_kind_of_refuses_a_name_the_record_does_not_declare():
    """Rather than guessing, because a caller asking is about to treat it as a number."""
    try:
        model.kind_of("shuffle_read_wait")
    except model.UnexpectedLog as problem:
        assert "not a task quantity" in str(problem), problem
        return
    raise AssertionError("an undeclared name was given a kind")

def check_a_measured_kind_is_not_an_identity_or_an_instant():
    assert model.IDENTITY not in model.MEASURED
    assert model.INSTANT not in model.MEASURED
    assert model.FLAG not in model.MEASURED
    assert set(model.MEASURED) == {model.COUNT, model.BYTES, model.MILLIS}
