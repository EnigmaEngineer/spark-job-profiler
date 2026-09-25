"""Grade the committed fixtures on the pathologies they were captured for.

The detector is not built yet. What this pins is the fixture. If somebody regenerates
these logs at a smaller row count the skew and the spill quietly leave, and every later
check would be grading a detector against a log with nothing in it.

The arithmetic is `sjp.model` now. What is checked here is `scripts/fixture_probe.py`,
which drives it and prints, and which the suite imports so a mutation pass over it is
graded rather than invisible.
"""
import contextlib
import io
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import fixture_probe  # noqa: E402

from sjp import eventlog  # noqa: E402

FIXTURES = os.path.join(ROOT, "tests", "fixtures", "eventlogs")

# Measured off these exact bytes. A record count reproduces or the fixture moved.
SKEWED_RECORDS = {0: (0, 0), 1: (1000000, 1000000), 2: (156784, 6932663)}
BALANCED_RECORDS = {0: (0, 0), 1: (1000000, 1000000), 2: (984924.5, 1206030)}
FIRST_STAGE_SPILL = (117440288, 61304287)
GROUPING_STAGE = 2


def _app(job):
    return eventlog.profile(fixture_probe.only_log(os.path.join(FIXTURES, job)))


def check_the_skewed_fixture_has_the_shuffle_volume_it_was_captured_with():
    app = _app("skewed")
    for stage_id, expected in SKEWED_RECORDS.items():
        stage = app.stage(stage_id)
        got = (stage.median("records_read"), stage.largest("records_read"))
        assert got == expected, (stage_id, got, expected)


def check_the_balanced_fixture_has_the_shuffle_volume_it_was_captured_with():
    app = _app("balanced")
    for stage_id, expected in BALANCED_RECORDS.items():
        stage = app.stage(stage_id)
        got = (stage.median("records_read"), stage.largest("records_read"))
        assert got == expected, (stage_id, got, expected)


def check_one_task_in_the_skewed_grouping_stage_reads_far_more_than_the_median():
    stage = _app("skewed").stage(GROUPING_STAGE)
    assert stage.spread("records_read") > 40, stage.spread("records_read")


def check_the_balanced_grouping_stage_is_nearly_even():
    stage = _app("balanced").stage(GROUPING_STAGE)
    assert stage.spread("records_read") < 1.5, stage.spread("records_read")


def check_the_skewed_grouping_stage_also_takes_far_longer_on_one_task():
    """A duration is a timing, so this is a floor rather than a pinned value."""
    stage = _app("skewed").stage(GROUPING_STAGE)
    assert stage.spread("duration") > 5, stage.spread("duration")


def check_the_balanced_grouping_stage_does_not():
    stage = _app("balanced").stage(GROUPING_STAGE)
    assert stage.spread("duration") < 3, stage.spread("duration")


def check_only_the_skewed_fixture_spills_in_the_grouping_stage():
    skewed = _app("skewed").stage(GROUPING_STAGE)
    balanced = _app("balanced").stage(GROUPING_STAGE)
    assert skewed.total("memory_spilled") == 620755808, skewed.total("memory_spilled")
    assert skewed.total("disk_spilled") == 88088795, skewed.total("disk_spilled")
    assert balanced.total("memory_spilled") == 0, balanced.total("memory_spilled")
    assert balanced.total("disk_spilled") == 0, balanced.total("disk_spilled")


def check_both_fixtures_spill_identically_in_the_first_stage():
    """Not evidence of skew, and still a cost.

    Stage 0 is the same expression in both jobs and it spills the same bytes in both. A
    detector that sums spill over an application says the same thing about both. The two
    totals being equal to the byte is also the evidence that the two jobs differ in one
    expression and nothing else.
    """
    for job in ("skewed", "balanced"):
        stage = _app(job).stage(0)
        got = (stage.total("memory_spilled"), stage.total("disk_spilled"))
        assert got == FIRST_STAGE_SPILL, (job, got)


def check_both_fixtures_were_captured_under_the_settings_the_job_module_still_declares():
    for job in ("skewed", "balanced"):
        properties = _app(job).properties
        assert properties["spark.sql.shuffle.partitions"] == "8", properties
        assert properties["spark.sql.adaptive.enabled"] == "false", properties


def check_only_one_log_per_fixture_directory_is_accepted():
    import shutil
    import tempfile

    root = tempfile.mkdtemp(prefix="sjp-two-logs-")
    try:
        for name in ("a", "b"):
            open(os.path.join(root, name), "w").close()
        try:
            fixture_probe.only_log(root)
        except ValueError:
            pass
        else:
            raise AssertionError("two logs in one fixture directory were accepted")
    finally:
        shutil.rmtree(root)


def check_the_probe_runs_and_its_controls_pass():
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = fixture_probe.main()
    text = out.getvalue()
    assert code == 0, (code, text)
    assert "skewed" in text and "balanced" in text, text
    assert "0 disagree with the task sums" in text, text
    assert "controls: 0 of 2 failed" in text, text
    assert "NO" not in text, text


def check_dropping_a_task_moves_the_totals_by_exactly_that_task():
    """The control is arithmetic and not just a difference, so the numbers are pinned.

    The last task by index in the skewed grouping stage is the hot one. Take it out and
    the stage still reports the run time of all eight while the tasks add up to seven.
    """
    stage = _app("skewed").stage(GROUPING_STAGE)
    dropped = stage.tasks[-1]
    rows = dict((name, (reported, summed)) for name, reported, summed
                in fixture_probe.disagreement_from_dropping_a_task(stage))
    assert rows["internal.metrics.executorRunTime"] == (4560, 4560 - dropped.executor_run_time), rows
    assert rows["internal.metrics.memoryBytesSpilled"] == (620755808, 0), rows
    assert dropped.memory_spilled == 620755808, dropped


def check_a_stage_whose_totals_are_whole_produces_no_disagreement():
    """Otherwise the control above could be reporting a difference that is always there."""
    stage = _app("balanced").stage(1)
    assert model_disagreements(stage) == [], model_disagreements(stage)


def check_the_repeated_name_control_answers_both_ways():
    stage = _app("skewed").stage(GROUPING_STAGE)
    assert fixture_probe.refuses_a_repeated_name(stage) is True

    import dataclasses
    kept = tuple(total for total in stage.totals
                 if total.name != fixture_probe.REPEATED_NAME)
    assert fixture_probe.refuses_a_repeated_name(
        dataclasses.replace(stage, totals=kept)) is False


def check_the_probe_counts_the_controls_that_failed():
    """A count rather than a flag, because only the truthiness used to be read."""
    assert fixture_probe.failures_in([("a", True, ""), ("b", True, "")]) == 0
    assert fixture_probe.failures_in([("a", False, ""), ("b", True, "")]) == 1
    assert fixture_probe.failures_in([("a", False, ""), ("b", False, "")]) == 2


def check_the_probe_runs_both_controls_on_the_stage_that_spilled():
    """Naming the stage matters. Only the grouping stage has a spill total to come apart."""
    results = fixture_probe.controls(_app("skewed"))
    assert len(results) == 2, results
    assert fixture_probe.failures_in(results) == 0, results
    assert "internal.metrics.memoryBytesSpilled" in results[0][2], results[0]


def check_the_probe_exit_code_is_reachable_from_both_sides():
    assert fixture_probe.exit_code(0) == 0
    assert fixture_probe.exit_code(1) == 1
    assert fixture_probe.exit_code(2) == 1


def check_the_probe_counts_the_totals_the_two_fixtures_actually_carry():
    """Same code, different key sets, because a total that stayed at zero is left out."""
    assert fixture_probe.agreement(_app("skewed"))[:2] == (24, 12)
    assert fixture_probe.agreement(_app("balanced"))[:2] == (22, 14)


def check_the_probe_reports_a_disagreement_it_is_given():
    import dataclasses

    app = _app("skewed")
    stage = app.stage(GROUPING_STAGE)
    damaged = dataclasses.replace(app, stages=(dataclasses.replace(
        stage, tasks=stage.tasks[:-1]),))
    _present, _absent, off = fixture_probe.agreement(damaged)
    assert off, off
    assert all(row[0] == GROUPING_STAGE for row in off), off


def model_disagreements(stage):
    from sjp import model
    return model.disagreements(stage)
