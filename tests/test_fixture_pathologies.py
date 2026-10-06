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

# The two cycle 2 fixtures, captured 2026-10-02. Neither carries a pathology in the query.
# Each carries one the profiler's own output has.
#
# `wide` is 48 stages, and 48 times the 14 metric default set is the 672 verdicts the scan
# produces. The line count is those plus the header, the tally, the contract line and one
# worst line per unit. Pinned because the whole reason the fixture exists is the size of
# that number.
#
# 675 and 4 until 2026-10-06. Day 5 put the build and the metric set id under the tally,
# which is one line on every run of this command. Both figures are re-measured here rather
# than incremented, and the day 2 post quoting 675 was accurate the day it was written.
WIDE_STAGES = 48
WIDE_VERDICTS = 672
WIDE_LINES = 676
# Measured 2026-10-06 with `--only skewed`. The header and the nothing skewed line, then
# the tally and the contract line, then the line saying the body is empty.
WIDE_FILTERED_LINES = 5

# `small` at 600 rows. These two came off the deterministic half of the log and reproduced
# across two separate captures to the record and to the byte, which is why they can be
# pinned at all. The millis metrics moved between those captures and are not pinned here.
SMALL_GROUPING_STAGE = 2
SMALL_RECORDS = (12.0, 518)
SMALL_LOCAL_BYTES = (793.0, 7449)


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


def check_the_wide_fixture_still_carries_the_stage_count_it_was_captured_for():
    """The fixture is the measurement. If somebody recaptures it with fewer steps the
    output problem it exists to show goes away and nothing here would notice."""
    app = _app("wide")
    assert len(app.stages) == WIDE_STAGES, len(app.stages)


def check_the_wide_fixture_still_prints_every_verdict_and_answers_in_the_first_three():
    """This is `ot-095` after day 2, and the line count is deliberately unchanged.

    675 lines for a job with nothing wrong with it was never a length problem. The two
    lines carrying the answer were 674 and 675, so a reader scrolled a screen of noise to
    reach them. Day 2 moved them to 2 and 3 and left the body where it was.

    The five cycle 1 logs all print between 45 and 61 lines, which is why the seven days
    of work that produced them never ran into this.
    """
    from sjp import commands, skew

    app = _app("wide")
    verdicts = skew.scan(app)
    assert len(verdicts) == WIDE_VERDICTS, len(verdicts)
    lines = commands.skew_lines(app, verdicts, skew.DEFAULT_THRESHOLD,
                                skew.DEFAULT_METRICS)
    assert len(lines) == WIDE_LINES, len(lines)
    assert "nothing skewed" in lines[1], lines[1]
    assert "skewed," in lines[2], lines[2]
    # The check this replaces asserted the opposite, that the tally was second from last.
    # It was right about yesterday's tree.
    assert "skewed," not in lines[-2], lines[-2]


def check_filtering_the_wide_fixture_answers_in_five_lines():
    """`--only skewed` is what makes the wide fixture readable, and the figure is measured.

    676 lines to 5 on a log where the answer is that nothing is skewed. The body goes to
    zero rows and the last line says so, because a reader who filters still has to be told
    what was left out.
    """
    from sjp import commands, skew

    app = _app("wide")
    lines = commands.skew_lines(app, skew.scan(app), skew.DEFAULT_THRESHOLD,
                                skew.DEFAULT_METRICS, only=skew.SKEWED)
    assert len(lines) == WIDE_FILTERED_LINES, len(lines)
    assert lines[-1].strip() == "showing 0 of 672, skewed only", lines[-1]


def check_the_small_fixture_reports_byte_and_record_skew_nobody_would_act_on():
    """This is the `ot-093` evidence. `skew.FLOORS` leaves bytes and counts at None, and
    None means no floor, so these two are reported skewed on a few kilobytes.

    Neither of these is a bug in the arithmetic. 7,449 bytes really is 9.4 times 793. The
    point is that the verdict is true and useless, and until this fixture existed there was
    no log in the repo where the small end of the byte and count ranges was populated.
    """
    stage = _app("small").stage(SMALL_GROUPING_STAGE)
    assert (stage.median("records_read"), stage.largest("records_read")) == SMALL_RECORDS
    got = (stage.median("local_bytes_read"), stage.largest("local_bytes_read"))
    assert got == SMALL_LOCAL_BYTES, got


def check_the_small_fixture_byte_skew_is_below_anything_the_cycle_one_logs_carry():
    """The floors cannot be placed from one end of a range.

    The skewed join's key exchange writes 88,788,038 bytes. This stage's largest task reads
    7,449. Four orders of magnitude between them, in the same repo, judged by the same rule
    with no floor in it.
    """
    small = _app("small").stage(SMALL_GROUPING_STAGE).largest("local_bytes_read")
    assert small < 10_000, small
    skewed = _app("skewed").stage(GROUPING_STAGE).largest("local_bytes_read")
    assert skewed > 1_000_000, skewed
    assert skewed / small > 1_000, skewed / small
