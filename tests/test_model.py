"""The stage and task model, and the one list this repo is allowed to keep by hand.

The event names the model reads are registered by its handlers. The first check here
walks the module's syntax tree and refuses any other Spark event name written into it,
because an inline comparison against a name nobody registered is how the list and the
parser come apart again.
"""
import ast
import dataclasses
import os
import shutil
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


def check_a_repeated_total_name_is_refused_rather_than_answered():
    """`number of output rows` is in there twice under two ids, in both logs."""
    for directory in (SKEWED, BALANCED):
        stage = _app(directory).stage(GROUPING_STAGE)
        names = [total.name for total in stage.totals]
        assert names.count("number of output rows") == 2, names
        try:
            stage.reported_total("number of output rows")
        except model.UnexpectedLog as problem:
            assert "2 times" in str(problem), problem
        else:
            raise AssertionError("a name appearing twice answered with one value")


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
