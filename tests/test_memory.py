"""The spill numbers, and the three metrics this module does not have because of them.

Every figure asserted here was measured off the two committed logs before the module was
written. The order matters. Two of these checks exist because a metric was planned and the
measurement removed it.

The control fixture spills. That is the check nobody expected to need.
"""
import dataclasses
import os

from sjp import eventlog, memory, model

HERE = os.path.dirname(os.path.abspath(__file__))
SKEWED_DIR = os.path.join(HERE, "fixtures", "eventlogs", "skewed")
BALANCED_DIR = os.path.join(HERE, "fixtures", "eventlogs", "balanced")
GROUPING_STAGE = 2
READ_STAGE = 0

def _only_log(directory):
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])

def _app(directory):
    return eventlog.profile(_only_log(directory))

def _stage(memory_values, disk_values, peaks=None, stage_id=3):
    """A stage whose tasks carry only the spill and peak numbers named."""
    peaks = [0] * len(memory_values) if peaks is None else peaks
    blank = {name: 0 for name in model.Task.__dataclass_fields__}
    tasks = []
    for index, (spilled, disk, peak) in enumerate(zip(memory_values, disk_values, peaks)):
        fields = dict(blank)
        fields.update(stage_id=stage_id, stage_attempt=0, index=index, partition=index,
                      executor_id="1", failed=False, memory_spilled=spilled,
                      disk_spilled=disk, peak_memory=peak)
        tasks.append(model.Task(**fields))
    return model.Stage(stage_id=stage_id, attempt=0, name="made up",
                       declared_tasks=len(tasks), parent_ids=(), submission_time=0,
                       completion_time=0, failure=None, totals=(), tasks=tuple(tasks))

# the control spills, which is the finding the rest of this module is arranged around

def check_the_healthy_log_spills_the_same_bytes_as_the_pathological_one():
    """Stage 0 of both logs is identical to the byte, so a spill is not the pathology.

    A detector reporting that a stage spilled would flag the fixture whose whole job is to
    have nothing wrong with it. Which spill is pathological is a question about how few
    tasks did it.
    """
    guilty = _app(SKEWED_DIR).stage(READ_STAGE)
    clean = _app(BALANCED_DIR).stage(READ_STAGE)
    assert guilty.values("memory_spilled") == clean.values("memory_spilled")
    assert guilty.values("disk_spilled") == clean.values("disk_spilled")
    assert guilty.total("memory_spilled") == 117440288, guilty.total("memory_spilled")
    assert memory.examine(guilty).spilling
    assert memory.examine(clean).spilling

def check_concentration_is_what_separates_the_two_jobs_and_not_the_spill():
    reports = {r.stage_id: r for r in memory.scan(_app(SKEWED_DIR))}
    clean = {r.stage_id: r for r in memory.scan(_app(BALANCED_DIR))}
    assert reports[READ_STAGE].concentration == 0.5, reports[READ_STAGE]
    assert clean[READ_STAGE].concentration == 0.5, clean[READ_STAGE]
    assert reports[GROUPING_STAGE].concentration == 1.0, reports[GROUPING_STAGE]
    assert clean[GROUPING_STAGE].concentration is None, clean[GROUPING_STAGE]

def check_one_task_of_eight_spilled_all_of_it():
    report = memory.examine(_app(SKEWED_DIR).stage(GROUPING_STAGE))
    assert report.tasks == 8 and report.tasks_spilling == 1, report
    assert report.memory_bytes == 620755808, report
    assert "1 of 8 tasks spilled all 620755808 bytes" in report.why, report.why

# the two spill numbers are one event

def check_the_two_spill_numbers_are_the_same_bytes_and_are_never_added():
    """The sum is a number describing nothing, and here is what it would have said."""
    stage = _app(SKEWED_DIR).stage(GROUPING_STAGE)
    real = stage.total("memory_spilled")
    added = real + stage.total("disk_spilled")
    assert real == 620755808, real
    assert added == 708844603, added
    assert added > real

def check_the_factor_between_them_is_not_a_constant():
    """So there is nothing to convert one into the other with.

    1.9157 on the read stage of both logs and 7.0469 on the skewed grouping stage. A tool
    holding one factor is wrong by this much inside one file.
    """
    app = _app(SKEWED_DIR)
    read = memory.inflation(app.stage(READ_STAGE))
    grouping = memory.inflation(app.stage(GROUPING_STAGE))
    assert round(read, 4) == 1.9157, read
    assert round(grouping, 4) == 7.0469, grouping
    assert round(grouping / read, 2) == 3.68, grouping / read
    balanced = memory.inflation(_app(BALANCED_DIR).stage(READ_STAGE))
    assert balanced == read, (balanced, read)

def check_a_stage_that_reached_no_disk_has_no_factor():
    assert memory.inflation(_stage([5, 5], [0, 0])) is None
    assert memory.inflation(_stage([0, 0], [0, 0])) is None

def check_a_stage_that_spilled_nothing_has_no_concentration():
    assert memory.concentration(_stage([0, 0], [0, 0])) is None

def check_an_even_spill_across_eight_tasks_reads_one_eighth():
    """The other end of the concentration range, which no committed stage reaches."""
    even = _stage([10] * 8, [5] * 8)
    assert memory.concentration(even) == 0.125, memory.concentration(even)
    assert memory.examine(even).why == "8 of 8 tasks spilled"

# peak execution memory, and why it is not the pressure signal

def check_peak_memory_is_zero_on_the_stage_that_spilled_the_most_often():
    """Both tasks spilled 58,720,144 bytes each and both report a peak of zero.

    A pressure metric built as peak over a budget reads no pressure at all here. So peak is
    evidence when it is present and it is not the signal.
    """
    for directory in (SKEWED_DIR, BALANCED_DIR):
        stage = _app(directory).stage(READ_STAGE)
        assert stage.total("memory_spilled") == 117440288, directory
        assert stage.values("peak_memory") == [0, 0], directory
        assert memory.peak_reported(stage) is False, directory

def check_peak_memory_is_reported_on_the_grouping_stage_of_both_logs():
    """The one place it is present, so the check above is about the log and not the tool."""
    for directory in (SKEWED_DIR, BALANCED_DIR):
        assert memory.peak_reported(_app(directory).stage(GROUPING_STAGE)) is True, directory

def check_the_higher_peak_belongs_to_the_job_that_did_not_spill():
    """So peak execution memory does not order two jobs by whether they spill.

    The balanced grouping stage peaks at 167,771,904 on one task and spills nothing. Seven
    of the skewed stage's eight tasks peak far below that and also spill nothing. The eighth
    peaks at 377,486,768 and spills all of it.
    """
    guilty = _app(SKEWED_DIR).stage(GROUPING_STAGE)
    clean = _app(BALANCED_DIR).stage(GROUPING_STAGE)
    assert clean.peak_memory == 167771904, clean.peak_memory
    assert guilty.peak_memory == 377486768, guilty.peak_memory
    quiet = [peak for peak, spilled in zip(guilty.values("peak_memory"),
                                           guilty.values("memory_spilled")) if not spilled]
    assert len(quiet) == 7, quiet
    assert max(quiet) == 33554384, quiet
    assert clean.peak_memory > max(quiet) * 4, (clean.peak_memory, max(quiet))
    assert clean.total("memory_spilled") == 0

# the budget the log does not hold

def check_neither_log_records_the_memory_a_task_had():
    for directory in (SKEWED_DIR, BALANCED_DIR):
        money = memory.budget(_app(directory).properties)
        assert money.known is False, directory
        assert money.absent == memory.BUDGET_PROPERTIES, money.absent
        assert money.recorded == {}, money.recorded

def check_the_budget_is_known_when_every_property_is_there():
    """The other side of the refusal, so the refusal is about the input."""
    money = memory.budget({name: "1g" for name in memory.BUDGET_PROPERTIES})
    assert money.known is True, money
    assert money.absent == (), money
    assert set(money.recorded) == set(memory.BUDGET_PROPERTIES), money

def check_one_missing_property_is_enough_to_refuse():
    partial = {name: "1g" for name in memory.BUDGET_PROPERTIES[1:]}
    money = memory.budget(partial)
    assert money.known is False, money
    assert money.absent == (memory.BUDGET_PROPERTIES[0],), money.absent
    assert len(money.recorded) == len(memory.BUDGET_PROPERTIES) - 1, money.recorded

def check_the_driver_memory_the_log_does_record_is_not_taken_as_the_budget():
    """Both logs set spark.driver.memory and it is not on the list on purpose.

    Local mode makes the driver the executor. Reading that as the executor budget would be
    right for these two logs and wrong for every log captured on a cluster.
    """
    for directory in (SKEWED_DIR, BALANCED_DIR):
        properties = _app(directory).properties
        assert properties["spark.driver.memory"] == "1g", directory
        assert "spark.driver.memory" not in memory.BUDGET_PROPERTIES

def check_the_report_puts_the_missing_budget_before_any_number():
    """A reader who learns it at the bottom has already read the numbers as absolute."""
    lines = memory.pressure_lines(_app(SKEWED_DIR))
    assert lines[0].startswith("no execution memory budget"), lines[0]
    for name in memory.BUDGET_PROPERTIES:
        assert name in lines[0], name
    first_number = next(i for i, line in enumerate(lines) if "117440288" in line)
    assert first_number > 0, lines

def check_the_report_names_a_known_budget_rather_than_staying_silent():
    app = dataclasses.replace(
        _app(SKEWED_DIR),
        properties={name: "1g" for name in memory.BUDGET_PROPERTIES})
    lines = memory.pressure_lines(app)
    assert lines[0].startswith("execution memory budget readable from"), lines[0]

def check_a_spill_report_cannot_be_edited_after_it_is_made():
    report = memory.examine(_app(SKEWED_DIR).stage(GROUPING_STAGE))
    for field, value in (("memory_bytes", 1), ("outcome", memory.CLEAN)):
        try:
            setattr(report, field, value)
        except dataclasses.FrozenInstanceError:
            continue
        raise AssertionError("{} could be written to".format(field))

def check_every_outcome_is_one_of_the_two():
    for directory in (SKEWED_DIR, BALANCED_DIR):
        for report in memory.scan(_app(directory)):
            assert report.outcome in memory.OUTCOMES, report
            assert report.spilling == (report.outcome == memory.SPILLING), report

def check_scan_covers_every_stage_in_order():
    app = _app(SKEWED_DIR)
    reports = memory.scan(app)
    assert [r.stage_id for r in reports] == [s.stage_id for s in app.stages], reports

# the boundaries a mutation pass found unguarded

def check_a_budget_cannot_be_edited_after_it_is_made():
    money = memory.budget({})
    for field, value in (("known", True), ("absent", ())):
        try:
            setattr(money, field, value)
        except dataclasses.FrozenInstanceError:
            continue
        raise AssertionError("{} could be written to".format(field))

def check_a_stage_that_spilled_to_memory_but_not_to_disk_is_still_spilling():
    """Clean means both columns are zero. Either one moving is a spill.

    No committed stage separates these, because spilling to memory and reaching disk happen
    together in both logs. The rule is still the rule.
    """
    memory_only = memory.examine(_stage([5, 5], [0, 0]))
    disk_only = memory.examine(_stage([0, 0], [5, 5]))
    assert memory_only.outcome == memory.SPILLING, memory_only
    assert disk_only.outcome == memory.SPILLING, disk_only
    assert memory.examine(_stage([0, 0], [0, 0])).outcome == memory.CLEAN

def check_one_task_of_two_spilling_still_reads_as_all_of_it():
    """The smallest stage the concentrated wording applies to.

    The committed two task stage has both tasks spilling, so nothing here reached this
    branch at its own boundary.
    """
    report = memory.examine(_stage([0, 7], [0, 3]))
    assert report.tasks_spilling == 1 and report.tasks == 2, report
    assert report.why == "1 of 2 tasks spilled all 7 bytes of it", report.why
    assert report.concentration == 1.0, report

def check_a_single_task_stage_is_not_described_as_concentrated():
    """One task cannot concentrate anything, so the wording has to change.

    The concentration is still 1.0 arithmetically and the sentence would be a claim about
    skew that a stage of one task cannot support.
    """
    report = memory.examine(_stage([7], [3]))
    assert report.tasks == 1 and report.tasks_spilling == 1, report
    assert report.why == "1 of 1 tasks spilled", report.why
    assert "all 7 bytes" not in report.why, report.why

def check_a_peak_of_one_byte_counts_as_reported():
    """Above zero is the test, because zero is what Spark writes when it tracked nothing."""
    assert memory.peak_reported(_stage([0], [0], peaks=[1])) is True
    assert memory.peak_reported(_stage([0], [0], peaks=[0])) is False
