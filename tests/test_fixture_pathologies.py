"""Grade the committed fixtures on the pathologies they were captured for.

The detector is not built yet. What this pins is the fixture. If somebody regenerates
these logs at a smaller row count the skew and the spill quietly leave, and every later
check would be grading a detector against a log with nothing in it.

The arithmetic comes from `scripts/fixture_probe.py` so that there is one of it, and so a
mutation pass over that file is graded here rather than being invisible.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import fixture_probe  # noqa: E402

FIXTURES = os.path.join(ROOT, "tests", "fixtures", "eventlogs")

# Measured off these exact bytes. A record count reproduces or the fixture moved.
SKEWED_RECORDS = {0: (0, 0), 1: (1000000, 1000000), 2: (156784, 6932663)}
BALANCED_RECORDS = {0: (0, 0), 1: (1000000, 1000000), 2: (984924.5, 1206030)}
FIRST_STAGE_SPILL = (117440288, 61304287)
GROUPING_STAGE = 2


def _summary(job):
    return fixture_probe.per_stage(fixture_probe.only_log(os.path.join(FIXTURES, job)))


def check_the_skewed_fixture_has_the_shuffle_volume_it_was_captured_with():
    summary = _summary("skewed")
    for stage, expected in SKEWED_RECORDS.items():
        got = (summary[stage]["records_median"], summary[stage]["records_max"])
        assert got == expected, (stage, got, expected)


def check_the_balanced_fixture_has_the_shuffle_volume_it_was_captured_with():
    summary = _summary("balanced")
    for stage, expected in BALANCED_RECORDS.items():
        got = (summary[stage]["records_median"], summary[stage]["records_max"])
        assert got == expected, (stage, got, expected)


def check_one_task_in_the_skewed_grouping_stage_reads_far_more_than_the_median():
    row = _summary("skewed")[GROUPING_STAGE]
    assert fixture_probe.ratio(row["records_median"], row["records_max"]) > 40, row


def check_the_balanced_grouping_stage_is_nearly_even():
    row = _summary("balanced")[GROUPING_STAGE]
    assert fixture_probe.ratio(row["records_median"], row["records_max"]) < 1.5, row


def check_the_skewed_grouping_stage_also_takes_far_longer_on_one_task():
    """A duration is a timing, so this is a floor rather than a pinned value."""
    row = _summary("skewed")[GROUPING_STAGE]
    assert fixture_probe.ratio(row["duration_median"], row["duration_max"]) > 5, row


def check_the_balanced_grouping_stage_does_not():
    row = _summary("balanced")[GROUPING_STAGE]
    assert fixture_probe.ratio(row["duration_median"], row["duration_max"]) < 3, row


def check_only_the_skewed_fixture_spills_in_the_grouping_stage():
    skewed = _summary("skewed")[GROUPING_STAGE]
    balanced = _summary("balanced")[GROUPING_STAGE]
    assert skewed["memory_spilled"] == 620755808, skewed
    assert skewed["disk_spilled"] == 88088795, skewed
    assert balanced["memory_spilled"] == 0, balanced
    assert balanced["disk_spilled"] == 0, balanced


def check_both_fixtures_spill_identically_in_the_first_stage():
    """The spill nobody should report.

    Stage 0 is the same expression in both jobs and it spills the same bytes in both. A
    detector that sums spill over an application would call the healthy job broken. The
    two totals being equal to the byte is also the evidence that the two jobs differ in
    one expression and nothing else.
    """
    for job in ("skewed", "balanced"):
        row = _summary(job)[0]
        assert (row["memory_spilled"], row["disk_spilled"]) == FIRST_STAGE_SPILL, (job, row)


def check_a_stage_that_shuffles_nothing_reads_as_zero_rather_than_raising():
    """Stage 0 reads no shuffle records, so the guard in `ratio` is on a real path."""
    row = _summary("skewed")[0]
    assert row["records_median"] == 0, row
    assert fixture_probe.ratio(row["records_median"], row["records_max"]) == 0.0, row


def check_the_probe_refuses_a_log_with_no_tasks_in_it():
    import shutil
    import tempfile

    from sjp import eventlog

    root = tempfile.mkdtemp(prefix="sjp-notasks-")
    try:
        path = os.path.join(root, "thin")
        with open(path, "w") as handle:
            handle.write('{"Event":"SparkListenerJobStart"}\n')
        try:
            fixture_probe.per_stage(path)
        except eventlog.NotAnEventLog:
            pass
        else:
            raise AssertionError("a log with no task events was summarised anyway")
    finally:
        shutil.rmtree(root)


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


def check_the_probe_runs_and_reports_success():
    import contextlib
    import io

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = fixture_probe.main()
    assert code == 0, code
    assert "skewed" in out.getvalue() and "balanced" in out.getvalue(), out.getvalue()


def check_a_duration_and_a_record_count_are_not_the_same_number():
    """Both are sorted out of the same task tuple and a swap would go unnoticed.

    Every task in the second stage reads exactly 1,000,000 records in both jobs. No task
    took anything like that many milliseconds.
    """
    for job in ("skewed", "balanced"):
        row = _summary(job)[1]
        assert row["records_median"] == 1000000, (job, row)
        assert row["duration_median"] < 10000, (job, row)
        assert row["duration_max"] < 10000, (job, row)
