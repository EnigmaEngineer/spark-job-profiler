"""The skew detector, and the two claims about thresholds that had to be measured.

The first is that a median relative ratio is unreachable below three tasks. That is checked
by searching for the largest ratio such a stage can produce rather than by asserting the
algebra, because the algebra is what would be wrong if the reasoning were wrong.

The second is that a zero median is not evenness. The control for that one is a real stage
out of a committed log, which is better than a constructed one and was not expected. The
skewed job's grouping stage has a median spill of zero and one task that spilled.
"""
import dataclasses
import itertools
import math
import os
import statistics

from sjp import eventlog, model, skew

HERE = os.path.dirname(os.path.abspath(__file__))
SKEWED_DIR = os.path.join(HERE, "fixtures", "eventlogs", "skewed")
BALANCED_DIR = os.path.join(HERE, "fixtures", "eventlogs", "balanced")
GROUPING_STAGE = 2
METRICS = ("records_read", "duration", "executor_run_time")

def _only_log(directory):
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])

def _app(directory):
    return eventlog.profile(_only_log(directory))

def _task(index, **fields):
    """A task carrying zero everywhere except the fields named."""
    blank = {name: 0 for name in model.Task.__dataclass_fields__}
    blank.update(stage_id=0, stage_attempt=0, index=index, partition=index,
                 executor_id="1", failed=False)
    blank.update(fields)
    return model.Task(**blank)

def _stage(values, field="records_read", stage_id=7):
    """A stage whose tasks carry `values` for one field and nothing else."""
    tasks = tuple(_task(i, **{field: v}) for i, v in enumerate(values))
    return model.Stage(stage_id=stage_id, attempt=0, name="made up", declared_tasks=len(tasks),
                       parent_ids=(), submission_time=0, completion_time=0, failure=None,
                       totals=(), tasks=tasks)

# the reachability claim behind MIN_TASKS

def check_the_ratio_a_stage_can_reach_is_capped_below_three_tasks():
    """Search for the largest ratio rather than trusting the formula that predicts it.

    One task can only ever read 1.0, and two tasks approach 2 without arriving because the
    largest value is one of the two the median averages.
    """
    def best_for(n):
        found = 0.0
        for values in itertools.combinations_with_replacement([1, 2, 5, 37, 10 ** 6], n):
            middle = statistics.median(values)
            if middle:
                found = max(found, max(values) / middle)
        return found

    assert best_for(1) == 1.0, best_for(1)
    assert best_for(2) < 2.0, best_for(2)
    assert best_for(3) > 100.0, best_for(3)

def check_two_tasks_stay_under_two_however_extreme_the_pair():
    """The cap is a property of the count and not of the size of the numbers."""
    for big in (10, 10 ** 6, 10 ** 12):
        stage = _stage([1, big])
        ratio = stage.largest("records_read") / stage.median("records_read")
        assert ratio < 2.0, (big, ratio)

def check_a_stage_below_the_task_floor_is_undecided_and_not_even():
    for values in ([], [500], [1, 10 ** 9]):
        verdict = skew.judge(_stage(values), "records_read")
        assert verdict.outcome == skew.UNDECIDED, (values, verdict)
        assert verdict.ratio is None, verdict

def check_the_task_floor_is_the_number_the_search_supports():
    """A floor of 2 would admit the two task case the search shows cannot fire."""
    assert skew.MIN_TASKS == 3, skew.MIN_TASKS

def check_a_zero_median_with_a_non_zero_maximum_is_skewed():
    verdict = skew.judge(_stage([0, 0, 0, 0, 0, 0, 0, 5000000]), "records_read")
    assert verdict.outcome == skew.SKEWED, verdict
    assert verdict.unbounded, verdict
    assert "5000000" in verdict.why, verdict.why

def check_the_real_grouping_stage_spill_is_the_zero_median_case():
    """The control is captured data rather than a constructed stage.

    One task of eight spilled and the other seven did not, so the median is zero and the
    ratio has no denominator. This is the metric the whole project is about.
    """
    stage = _app(SKEWED_DIR).stage(GROUPING_STAGE)
    assert stage.median("memory_spilled") == 0, stage.median("memory_spilled")
    assert stage.largest("memory_spilled") == 620755808, stage.largest("memory_spilled")
    verdict = skew.judge(stage, "memory_spilled")
    assert verdict.outcome == skew.SKEWED, verdict
    assert verdict.unbounded, verdict

def check_spread_refuses_to_summarise_a_zero_median():
    """`Stage.spread` answered 0.0 here once, which read as perfectly even."""
    stage = _app(SKEWED_DIR).stage(GROUPING_STAGE)
    assert stage.spread("memory_spilled") is None, stage.spread("memory_spilled")

def check_comparing_that_refusal_against_a_threshold_raises():
    """The reason None was chosen over 0.0. A caller that forgets gets a stack trace."""
    stage = _app(SKEWED_DIR).stage(GROUPING_STAGE)
    try:
        stage.spread("memory_spilled") > skew.DEFAULT_THRESHOLD
    except TypeError:
        pass
    else:
        raise AssertionError("a missing spread compared against a threshold without raising")

def check_a_metric_no_task_moved_is_undecided_rather_than_even():
    """Nothing happened is not the same answer as it happened evenly."""
    stage = _app(BALANCED_DIR).stage(GROUPING_STAGE)
    assert stage.largest("memory_spilled") == 0, stage.largest("memory_spilled")
    verdict = skew.judge(stage, "memory_spilled")
    assert verdict.outcome == skew.UNDECIDED, verdict
    assert "zero" in verdict.why, verdict.why

# the threshold, and the range the fixtures put around it

def check_the_default_threshold_sits_inside_the_measured_separating_range():
    """The justification for 4.0 is a check rather than a sentence in a docstring.

    Every stage of the balanced log is healthy and so are stages 0 and 1 of the skewed
    one. The separating range is the gap between the largest healthy ratio and the
    smallest ratio on the skewed grouping stage, over all three metrics.
    """
    healthy, guilty = [], []
    for directory in (BALANCED_DIR, SKEWED_DIR):
        app = _app(directory)
        for stage in app.stages:
            if len(stage.tasks) < skew.MIN_TASKS:
                continue
            for metric in METRICS:
                middle = stage.median(metric)
                if not middle:
                    continue
                ratio = stage.largest(metric) / middle
                guilty.append(ratio) if (directory == SKEWED_DIR
                                         and stage.stage_id == GROUPING_STAGE) else healthy.append(ratio)

    assert round(max(healthy), 4) == 1.7407, max(healthy)
    assert round(min(guilty), 4) == 14.7246, min(guilty)
    assert max(healthy) < skew.DEFAULT_THRESHOLD < min(guilty), skew.DEFAULT_THRESHOLD

def check_a_threshold_at_or_below_an_even_stage_is_refused():
    """1.0 is what a perfectly even stage reads, so a threshold there calls it skewed."""
    for bad in (1.0, 0.5, 0.0, -3.0):
        try:
            skew.judge(_stage([1, 1, 1]), "records_read", bad)
        except ValueError:
            continue
        raise AssertionError("threshold {} was accepted".format(bad))

def check_the_threshold_is_the_thing_that_decides():
    """Same stage, two thresholds, two answers. Otherwise the argument is decorative."""
    stage = _stage([100, 100, 100, 500])
    assert skew.judge(stage, "records_read", 3.0).outcome == skew.SKEWED
    assert skew.judge(stage, "records_read", 9.0).outcome == skew.EVEN

# the two fixtures, end to end

def check_the_detector_separates_the_two_jobs():
    """Over every quantity now, which is what the floor had to be added for.

    Over the three metrics this started with the skewed log answered 3 and the balanced one
    answered 0. Over all fourteen the balanced log answered 1 before the floor existed, on a
    result serialization time of 8 milliseconds against a median of zero.
    """
    skewed = skew.counts(skew.scan(_app(SKEWED_DIR)))
    balanced = skew.counts(skew.scan(_app(BALANCED_DIR)))
    assert skewed[skew.SKEWED] == 8, skewed
    assert balanced[skew.SKEWED] == 0, balanced

def check_every_skewed_verdict_is_on_the_grouping_stage():
    """The skewed job differs from the balanced one in one expression, in that stage."""
    for verdict in skew.scan(_app(SKEWED_DIR)):
        if verdict.outcome == skew.SKEWED:
            assert verdict.stage_id == GROUPING_STAGE, verdict

def check_scan_covers_every_stage_and_every_metric():
    app = _app(SKEWED_DIR)
    verdicts = skew.scan(app, METRICS)
    assert len(verdicts) == len(app.stages) * len(METRICS), len(verdicts)
    assert {(v.stage_id, v.metric) for v in verdicts} == {
        (s.stage_id, m) for s in app.stages for m in METRICS}

def check_every_outcome_is_one_of_the_three():
    for directory in (SKEWED_DIR, BALANCED_DIR):
        for verdict in skew.scan(_app(directory)):
            assert verdict.outcome in skew.OUTCOMES, verdict

def check_counts_reports_an_outcome_that_did_not_happen():
    tally = skew.counts(skew.scan(_app(BALANCED_DIR)))
    assert set(tally) == set(skew.OUTCOMES), tally
    assert tally[skew.SKEWED] == 0, tally
    app = _app(BALANCED_DIR)
    assert sum(tally.values()) == len(app.stages) * len(skew.DEFAULT_METRICS), tally

def check_worst_puts_an_unbounded_ratio_above_every_finite_one():
    verdicts = skew.scan(_app(SKEWED_DIR))
    first = skew.worst(verdicts, model.BYTES)
    assert first.metric == "memory_spilled", first
    assert first.unbounded, first
    assert max(v.ratio for v in verdicts if v.outcome == skew.SKEWED
               and not v.unbounded) > 40, verdicts

def check_worst_is_none_when_nothing_skewed():
    for kind in model.MEASURED:
        assert skew.worst(skew.scan(_app(BALANCED_DIR)), kind) is None, kind

def check_worst_breaks_a_tie_on_the_stage_id_rather_than_on_argument_order():
    low = skew.judge(_stage([1, 1, 1, 100], stage_id=1), "records_read")
    high = skew.judge(_stage([1, 1, 1, 100], stage_id=5), "records_read")
    assert low.ratio == high.ratio, (low, high)
    pair = [low, high]
    assert skew.worst(pair, model.COUNT) is low, skew.worst(pair, model.COUNT)
    assert skew.worst(pair[::-1], model.COUNT) is low, skew.worst(pair[::-1], model.COUNT)

# the metric set, and the floor that widening it made necessary

def check_the_default_metric_set_is_the_records_declared_quantities():
    """Not a list this module keeps. That list was three names and it was wrong."""
    assert skew.DEFAULT_METRICS == model.QUANTITIES, skew.DEFAULT_METRICS
    assert len(skew.DEFAULT_METRICS) > 3, skew.DEFAULT_METRICS
    for metric in skew.DEFAULT_METRICS:
        assert model.kind_of(metric) in model.MEASURED, metric

def check_no_identity_or_instant_field_is_judged():
    """A ratio over a launch time or an executor id is arithmetic on a label."""
    for name in ("stage_id", "index", "partition", "executor_id", "launch_time",
                 "finish_time", "failed"):
        assert name not in skew.DEFAULT_METRICS, name

def check_the_balanced_log_would_be_called_skewed_without_the_floor():
    """The control for the floor is the control fixture itself.

    Run the balanced log with every floor removed and it reports a skewed stage. That is
    the case the floor exists for and it is measured rather than constructed.
    """
    none_at_all = {kind: None for kind in model.MEASURED}
    without = skew.scan(_app(BALANCED_DIR), floors=none_at_all)
    guilty = [v for v in without if v.outcome == skew.SKEWED]
    assert len(guilty) == 1, guilty
    assert guilty[0].metric == "serialize_time", guilty[0]
    assert guilty[0].largest == 8, guilty[0]
    assert guilty[0].unbounded, guilty[0]
    assert skew.counts(skew.scan(_app(BALANCED_DIR)))[skew.SKEWED] == 0

def _millis_skewed_values(floors):
    """Every millisecond value that produces a skewed verdict, over both logs."""
    found = []
    for directory in (SKEWED_DIR, BALANCED_DIR):
        for verdict in skew.scan(_app(directory), floors=floors):
            if verdict.outcome == skew.SKEWED and model.kind_of(verdict.metric) == model.MILLIS:
                found.append(verdict.largest)
    return sorted(found)

def check_the_millis_floor_sits_in_the_gap_the_two_logs_leave():
    """What the data gives is a gap. Where in it the floor goes is a judgement.

    An earlier version of this check tried to separate noise from real by asking whether
    the stage had spilled. That is not a separator. The skewed log's grouping stage spilled
    and its result serialization time is 3 milliseconds, so the proxy called 3 real.

    So this asserts the honest thing instead. These are the values, this is the widest gap
    in the low end of them, and the floor is inside it. A fixture that puts a value in the
    gap fails this rather than quietly moving the answer.
    """
    values = _millis_skewed_values({kind: None for kind in model.MEASURED})
    assert values == [3, 8, 24, 29, 71, 3040, 3048], values
    below = [v for v in values if v < skew.FLOORS[model.MILLIS]]
    above = [v for v in values if v >= skew.FLOORS[model.MILLIS]]
    assert below == [3, 8, 24, 29], below
    assert max(below) == 29 and min(above) == 71, (below, above)

def check_the_skewed_count_is_swept_rather_than_quoted_at_one_floor():
    """The headline is a fact about the floor until somebody moves the floor.

    Published in the README beside the default so that the number is not read as a
    property of the job.
    """
    sweep = {}
    for floor in (0, 30, 50, 100, 3041):
        floors = dict(skew.FLOORS)
        floors[model.MILLIS] = floor
        sweep[floor] = skew.counts(skew.scan(_app(SKEWED_DIR), floors=floors))[skew.SKEWED]
    assert sweep == {0: 11, 30: 8, 50: 8, 100: 7, 3041: 6}, sweep
    # Every floor from 30 to 71 gives the same answer, which is what the gap means. The
    # first draft of this check guessed 9 at a floor of 30 and the measurement said 8.
    assert sweep[30] == sweep[50], sweep

def check_a_value_under_the_floor_is_undecided_and_says_which_floor():
    verdict = skew.judge(_stage([0, 0, 0, 8], field="serialize_time"), "serialize_time")
    assert verdict.outcome == skew.UNDECIDED, verdict
    assert "under the millis floor of 50" in verdict.why, verdict
    assert verdict.ratio is None, verdict

def check_exactly_the_floor_gets_an_answer():
    """Both sides of the limit, because a floor is a comparison like any other."""
    at = skew.judge(_stage([0, 0, 0, 50], field="gc_time"), "gc_time")
    under = skew.judge(_stage([0, 0, 0, 49], field="gc_time"), "gc_time")
    assert at.outcome == skew.SKEWED, at
    assert under.outcome == skew.UNDECIDED, under

def check_a_kind_with_no_floor_is_judged_at_any_size():
    """Bytes and counts have no measured floor, so nothing silences a small one."""
    assert skew.FLOORS[model.BYTES] is None
    assert skew.FLOORS[model.COUNT] is None
    tiny = skew.judge(_stage([0, 0, 0, 1], field="memory_spilled"), "memory_spilled")
    assert tiny.outcome == skew.SKEWED, tiny

def check_worst_by_kind_answers_once_per_unit_that_skewed():
    found = skew.worst_by_kind(skew.scan(_app(SKEWED_DIR)))
    assert set(found) == {model.COUNT, model.BYTES, model.MILLIS}, found
    assert found[model.BYTES].metric == "memory_spilled", found
    assert found[model.MILLIS].metric == "gc_time", found
    for kind, verdict in found.items():
        assert model.kind_of(verdict.metric) == kind, (kind, verdict)
    assert skew.worst_by_kind(skew.scan(_app(BALANCED_DIR))) == {}

def check_ranking_across_units_is_what_worst_by_kind_refuses_to_do():
    """Three unbounded verdicts on one stage, and a ratio cannot order them.

    The garbage collection and the spill are both unbounded and both fully concentrated in
    one task, so neither the ratio nor the share separates a 71 millisecond pause from a
    620,755,808 byte spill. The unit is the only thing that does.
    """
    verdicts = skew.scan(_app(SKEWED_DIR))
    unbounded = [v for v in verdicts if v.outcome == skew.SKEWED and v.unbounded]
    assert len(unbounded) == 3, unbounded
    assert len({v.ratio for v in unbounded}) == 1, unbounded
    assert len({v.stage_id for v in unbounded}) == 1, unbounded
    assert len({model.kind_of(v.metric) for v in unbounded}) == 2, unbounded

# the boundaries, every one of which a mutation pass found unguarded

def check_a_verdict_cannot_be_edited_after_it_is_made():
    """A verdict is evidence. Something that can be rewritten after the fact is not."""
    verdict = skew.judge(_stage([1, 1, 1, 100]), "records_read")
    try:
        verdict.outcome = skew.EVEN
    except dataclasses.FrozenInstanceError:
        pass
    else:
        raise AssertionError("a verdict was edited after it was made")

def check_a_threshold_just_above_an_even_stage_is_accepted():
    """The refusal is meant to catch 1.0 and below and nothing more.

    2.0 is the interesting value to pin, because it is the ceiling a two task stage
    cannot pass, so somebody will eventually set it on purpose.
    """
    verdict = skew.judge(_stage([1, 1, 1, 100]), "records_read", 2.0)
    assert verdict.outcome == skew.SKEWED, verdict
    assert skew.judge(_stage([1, 1, 1, 1]), "records_read", 1.0001).outcome == skew.EVEN

def check_a_stage_with_no_tasks_reports_zero_for_what_it_could_not_measure():
    """The verdict carries the numbers it was decided on, so they have to be honest.

    There is no median and no maximum here, and inventing either would put a figure in
    front of a reader that came from nothing.
    """
    verdict = skew.judge(_stage([]), "records_read")
    assert verdict.tasks == 0, verdict
    assert verdict.median == 0, verdict
    assert verdict.largest == 0, verdict

def check_exactly_the_task_floor_gets_an_answer():
    """Three tasks is the first count where a ratio can reach a threshold, so it answers."""
    verdict = skew.judge(_stage([1, 1, 100]), "records_read")
    assert verdict.tasks == skew.MIN_TASKS, verdict
    assert verdict.outcome == skew.SKEWED, verdict
    assert verdict.ratio == 100.0, verdict

def check_a_ratio_exactly_at_the_threshold_is_even():
    """The rule is above the threshold and not at it. A boundary needs one of the two."""
    stage = _stage([1, 1, 1, 4])
    assert stage.largest("records_read") / stage.median("records_read") == 4.0
    assert skew.judge(stage, "records_read", 4.0).outcome == skew.EVEN
    assert skew.judge(stage, "records_read", 3.9999).outcome == skew.SKEWED

# the number formatting, which a published figure depends on

def check_plain_keeps_every_digit_of_a_large_measurement():
    assert skew.plain(6932663) == "6932663"
    assert skew.plain(620755808) == "620755808"
    assert skew.plain(1000000.0) == "1000000"

def check_plain_keeps_the_half_a_median_can_end_in():
    assert skew.plain(984924.5) == "984924.5"

def check_a_verdict_reason_carries_both_numbers_it_was_decided_on():
    stage = _app(SKEWED_DIR).stage(GROUPING_STAGE)
    why = skew.judge(stage, "records_read").why
    assert "6932663" in why and "156784" in why, why
