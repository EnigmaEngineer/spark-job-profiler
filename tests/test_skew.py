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

import sjp
from sjp import eventlog, model, skew

HERE = os.path.dirname(os.path.abspath(__file__))
SKEWED_DIR = os.path.join(HERE, "fixtures", "eventlogs", "skewed")
BALANCED_DIR = os.path.join(HERE, "fixtures", "eventlogs", "balanced")
SMALL_DIR = os.path.join(HERE, "fixtures", "eventlogs", "small")
SKEWED_JOIN_DIR = os.path.join(HERE, "fixtures", "eventlogs", "skewed_join")
# Every log here, so a claim about what the floors change is a claim about all of them.
ALL_DIRS = tuple(sorted(
    os.path.join(HERE, "fixtures", "eventlogs", name)
    for name in os.listdir(os.path.join(HERE, "fixtures", "eventlogs"))
    if os.path.isdir(os.path.join(HERE, "fixtures", "eventlogs", name))))
NO_FLOORS = {model.MILLIS: None, model.BYTES: None, model.COUNT: None}
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
    """Same stage, two thresholds, two answers. Otherwise the argument is decorative.

    The values are above the count floor on purpose. A ratio is scale free and the floor is
    not, so a fixture built to exercise the threshold has to clear the floor or it never
    reaches the division being tested.
    """
    stage = _stage([100000, 100000, 100000, 500000])
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
    low = skew.judge(_stage([1000, 1000, 1000, 100000], stage_id=1), "records_read")
    high = skew.judge(_stage([1000, 1000, 1000, 100000], stage_id=5), "records_read")
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

def check_every_measured_kind_now_carries_a_floor():
    """This check used to assert the opposite and that is the point of keeping it here.

    Bytes and counts were `None` until a log populated their small end, and `None` meant a
    one byte spill came back skewed. The replacement asserts the case that used to pass.
    """
    for kind in model.MEASURED:
        assert skew.FLOORS[kind] is not None, kind
    tiny = skew.judge(_stage([0, 0, 0, 1], field="memory_spilled"), "memory_spilled")
    assert tiny.outcome == skew.UNDECIDED, tiny
    assert "under the bytes floor of 700000" in tiny.why, tiny


def check_a_caller_can_still_turn_a_floor_off_and_the_small_verdict_comes_back():
    """The probe that measured the brackets had to see what the floors hide.

    So `floors` is not decoration. A caller passing a kind mapped to None gets the old
    behaviour for that kind, which is how both ends of every bracket above were read.
    """
    off = {model.MILLIS: None, model.BYTES: None, model.COUNT: None}
    tiny = skew.judge(_stage([0, 0, 0, 1], field="memory_spilled"), "memory_spilled",
                      floors=off)
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
    verdict = skew.judge(_stage([1000, 1000, 1000, 100000]), "records_read", 2.0)
    assert verdict.outcome == skew.SKEWED, verdict
    even = _stage([100000, 100000, 100000, 100000])
    assert skew.judge(even, "records_read", 1.0001).outcome == skew.EVEN

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
    verdict = skew.judge(_stage([1000, 1000, 100000]), "records_read")
    assert verdict.tasks == skew.MIN_TASKS, verdict
    assert verdict.outcome == skew.SKEWED, verdict
    assert verdict.ratio == 100.0, verdict

def check_a_ratio_exactly_at_the_threshold_is_even():
    """The rule is above the threshold and not at it. A boundary needs one of the two."""
    stage = _stage([25000, 25000, 25000, 100000])
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

# ranking and filtering, added day 2 for ot-095

def check_ranking_is_a_permutation_and_drops_nothing():
    """A reordering that loses a row is the failure mode the tally would not catch."""
    verdicts = skew.scan(_app(SKEWED_DIR))
    ranked = skew.ranked(verdicts)
    assert len(ranked) == len(verdicts), (len(ranked), len(verdicts))
    assert sorted(map(id, ranked)) == sorted(map(id, verdicts))


def check_ranking_puts_every_skewed_verdict_above_every_other_one():
    verdicts = skew.ranked(skew.scan(_app(SKEWED_DIR)))
    outcomes = [verdict.outcome for verdict in verdicts]
    assert outcomes == sorted(outcomes, key=skew.OUTCOMES.index), outcomes


def check_ranking_keeps_an_unbounded_ratio_inside_its_own_unit():
    """The reason `rank_key` sorts by kind before ratio, measured on the skewed log.

    Three of its eight skewed verdicts divide by zero, and they span two kinds. gc_time's
    largest task is 71 milliseconds and memory_spilled's is 620,755,808 bytes. Both ratios
    are `inf`, so a flat sort by ratio orders those two by nothing at all and whichever
    comes first is a fact about the metric order.
    """
    skewed = skew.only(skew.ranked(skew.scan(_app(SKEWED_DIR))), skew.SKEWED)
    unbounded = [verdict.metric for verdict in skewed if verdict.unbounded]
    assert unbounded == ["disk_spilled", "memory_spilled", "gc_time"], unbounded
    kinds = [model.kind_of(verdict.metric) for verdict in skewed]
    assert kinds == sorted(kinds, key=model.KINDS.index), kinds
    # gc_time leads the millis block on a value four orders below the byte verdicts above
    # it, which is the whole argument for not ranking the two against each other.
    millis = [verdict for verdict in skewed if model.kind_of(verdict.metric) == model.MILLIS]
    assert millis[0].metric == "gc_time", [v.metric for v in millis]
    assert millis[0].largest == 71, millis[0].largest


def check_ranking_does_not_depend_on_the_order_the_metrics_were_asked_for():
    app = _app(SKEWED_DIR)
    forward = skew.ranked(skew.scan(app, skew.DEFAULT_METRICS))
    backward = skew.ranked(skew.scan(app, tuple(reversed(skew.DEFAULT_METRICS))))
    assert [(v.stage_id, v.metric) for v in forward] == \
           [(v.stage_id, v.metric) for v in backward]


def check_an_undecided_verdict_sorts_last_inside_its_kind_rather_than_crashing():
    """`ratio` is None on every undecided verdict, so the sort needs a value for it."""
    verdicts = skew.ranked(skew.scan(_app(SKEWED_DIR)))
    undecided = skew.only(verdicts, skew.UNDECIDED)
    assert undecided, "the skewed log has 28 of these"
    assert all(verdict.ratio is None for verdict in undecided)
    assert verdicts[-len(undecided):] == undecided


def check_a_scan_refuses_every_metric_a_ratio_cannot_measure():
    """The replacement for the day 2 check that asserted the opposite.

    That one pinned `scan` judging `launch_time` and `rank_key` sorting the verdict after
    the measured kinds. It was right about the sort and it was describing a defect, which
    is why it is gone rather than relaxed.
    """
    for metric, word in (("launch_time", "instant"),
                         ("executor_id", "identity"),
                         ("failed", "flag")):
        try:
            skew.scan(_app(SKEWED_DIR), (metric, "records_read"))
        except skew.MetricRefused as refusal:
            assert metric in str(refusal), str(refusal)
            assert word in str(refusal), str(refusal)
        else:
            raise AssertionError("{} was judged".format(metric))


def check_a_refusal_names_every_bad_metric_and_not_only_the_first():
    try:
        skew.scan(_app(SKEWED_DIR), ("launch_time", "records_read", "executor_id", "nope"))
    except skew.MetricRefused as refusal:
        lines = str(refusal).splitlines()
        assert len(lines) == 3, lines
        assert "not a task quantity" in lines[2], lines
    else:
        raise AssertionError("three bad metrics were judged")


def check_a_single_stage_cannot_bypass_the_refusal_by_calling_judge():
    """`scan` is not the only door. A caller holding one stage reaches `judge` directly."""
    stage = _app(SKEWED_DIR).stages[0]
    try:
        skew.judge(stage, "launch_time")
    except skew.MetricRefused:
        pass
    else:
        raise AssertionError("judge answered on a clock reading")


def check_a_verdict_on_a_non_measured_kind_cannot_be_built():
    """What `rank_key` relies on now that it indexes `model.MEASURED` again.

    `rank_key` would raise a bare ValueError on such a verdict. Nothing should be able to
    hand it one, and this is the check that sentence points at.
    """
    app = _app(SKEWED_DIR)
    for name in model.QUANTITIES:
        assert model.kind_of(name) in model.MEASURED, name
    verdicts = skew.scan(app)
    assert verdicts, "the skewed log has stages"
    for verdict in verdicts:
        assert model.kind_of(verdict.metric) in model.MEASURED, verdict.metric
    assert len(skew.ranked(verdicts)) == len(verdicts)


def check_the_metric_set_id_is_the_count_and_a_digest_of_the_names():
    one = skew.metric_set_id(("records_read", "duration"))
    assert one.startswith("2:"), one
    assert len(one) == len("2:") + 8, one
    # Order independent. The same two metrics asked for backwards is the same question.
    assert skew.metric_set_id(("duration", "records_read")) == one
    # And a different set is a different id, which is the only thing it has to do.
    assert skew.metric_set_id(("records_read",)) != one


def check_the_live_metric_set_is_the_last_entry_in_the_history():
    """The reason the version digit means anything.

    `DEFAULT_METRICS` is derived from the kinds declared on `model.Task`, so a field added
    there widens the set with nobody editing `sjp.skew`. This is what fails when that
    happens and the history was not updated.
    """
    version, set_id = skew.METRIC_SET_HISTORY[-1]
    assert skew.metric_set_id(skew.DEFAULT_METRICS) == set_id, (
        "the default set is now {} and the history ends at {}".format(
            skew.metric_set_id(skew.DEFAULT_METRICS), set_id))
    assert sjp.__version__ == version, (sjp.__version__, version)


def check_the_history_records_the_three_names_the_tool_started_with():
    """Read out of commit 4a5d5c0 rather than out of memory, then hashed here."""
    first_version, first_id = skew.METRIC_SET_HISTORY[0]
    assert first_version == "0.1.0", first_version
    assert skew.metric_set_id(("records_read", "duration", "memory_spilled")) == first_id
    assert first_id != skew.METRIC_SET_HISTORY[-1][1], "the set never changed"


def check_the_label_says_default_only_when_the_set_is_the_default_one():
    assert skew.metric_set_label(skew.DEFAULT_METRICS) == "default"
    # Named one at a time, which is what fourteen --metric flags produce.
    assert skew.metric_set_label(tuple(skew.DEFAULT_METRICS)) == "default"
    assert skew.metric_set_label(("duration",)) == "selected"


def check_the_exit_status_is_the_whole_scan_and_nothing_else():
    assert skew.exit_status(skew.scan(_app(SKEWED_DIR))) == 1
    assert skew.exit_status(skew.scan(_app(BALANCED_DIR))) == 0
    assert skew.exit_status([]) == 0


def check_only_narrows_the_body_and_leaves_the_tally_alone():
    verdicts = skew.scan(_app(SKEWED_DIR))
    tally = skew.counts(verdicts)
    for outcome in skew.OUTCOMES:
        subset = skew.only(verdicts, outcome)
        assert len(subset) == tally[outcome], (outcome, len(subset), tally[outcome])
        assert all(verdict.outcome == outcome for verdict in subset)
    assert skew.counts(verdicts) == tally


def check_only_refuses_an_outcome_that_is_not_one():
    try:
        skew.only(skew.scan(_app(SKEWED_DIR)), "bad")
    except ValueError as error:
        assert "is not an outcome" in str(error), str(error)
    else:
        raise AssertionError("accepted an outcome that does not exist")


# the floors, and the bracket each one was derived from

def _byte_and_count_verdicts(directory, floors=None):
    """Every skewed byte or count verdict in one log.

    Each one comes back as its kind beside its metric name and its largest task value.
    """
    found = []
    for verdict in skew.scan(_app(directory), floors=floors):
        if verdict.outcome != skew.SKEWED:
            continue
        kind = model.kind_of(verdict.metric)
        if kind in (model.BYTES, model.COUNT):
            found.append((kind, verdict.metric, verdict.largest))
    return sorted(found)


def check_the_derivation_reproduces_the_millisecond_floor_that_shipped_before_it():
    """The only independent case the rule has, so it is the one worth pinning.

    50 was placed by hand inside a measured gap of 29 to 71 weeks before `floor_from`
    existed. The rule returns it. That is one agreement rather than a law and
    `docs/adr-0006` says so, but a rule that disagreed with the number already shipped
    would have been a rule invented to produce the two new ones.
    """
    assert skew.floor_from((29, 71)) == 50, skew.floor_from((29, 71))
    assert skew.FLOORS[model.MILLIS] == 50, skew.FLOORS


def check_each_derived_floor_is_the_geometric_mean_of_its_own_bracket():
    """Derived and not typed, so a floor cannot drift from the bracket it came from.

    Two kinds are derived and one is not, and the set is asserted rather than the pair,
    because a third bracket arriving unnoticed is how the millisecond claim got made.
    """
    assert set(skew.BRACKETS) == {model.BYTES, model.COUNT}, skew.BRACKETS
    assert set(skew.FLOORS) == set(model.MEASURED), skew.FLOORS
    for kind, bracket in skew.BRACKETS.items():
        assert skew.FLOORS[kind] == skew.floor_from(bracket), (kind, bracket)
    assert skew.FLOORS[model.MILLIS] == skew.MILLIS_FLOOR, skew.FLOORS


def check_every_floor_sits_strictly_inside_its_bracket():
    """A floor at either end is a floor deciding against the evidence that placed it."""
    for kind, (noise, real) in skew.BRACKETS.items():
        floor = skew.FLOORS[kind]
        assert noise < floor < real, (kind, noise, floor, real)


def check_the_slack_each_floor_leaves_is_computed_rather_than_claimed():
    """The README publishes these multiples and arithmetic over a table goes stale.

    Both sides are reported because a floor far from the middle of its bracket is the
    thing to argue with, and a reader cannot see that from the floor alone.
    """
    expected = {model.BYTES: (93, 84), model.COUNT: (115, 115)}
    for kind, (noise, real) in skew.BRACKETS.items():
        floor = skew.FLOORS[kind]
        above = int(floor // noise)
        below = int(real // floor)
        assert (above, below) == expected[kind], (kind, above, below)


def check_a_bracket_whose_ends_are_the_wrong_way_round_is_refused():
    """A noise end above a real end is a measurement error and not a narrow bracket."""
    for bad in ((71, 29), (50, 50), (0, 10), (-1, 10)):
        try:
            skew.floor_from(bad)
        except ValueError:
            continue
        raise AssertionError("bracket {} was accepted".format(bad))


def check_the_noise_end_of_both_brackets_is_what_the_small_log_really_reads():
    """Re-derived from the log rather than trusted, because both ends are measurements.

    The small log is the skewed distribution over 600 rows. Its largest byte verdict and
    its largest count verdict are the two noise ends, and they are what the brackets say.
    """
    found = _byte_and_count_verdicts(SMALL_DIR, floors=NO_FLOORS)
    largest = {}
    for kind, _metric, value in found:
        largest[kind] = max(value, largest.get(kind, 0))
    assert largest[model.BYTES] == skew.BRACKETS[model.BYTES][0], largest
    assert largest[model.COUNT] == skew.BRACKETS[model.COUNT][0], largest


def check_the_real_end_of_both_brackets_is_the_smallest_a_pathology_log_reads():
    """The other end, over every committed log, so nothing smaller is sitting unnoticed.

    A bracket's real end has to be the smallest real value anywhere rather than the
    smallest one in the log the author happened to open. The two logs captured at eight
    million rows are the only ones that reach a byte or count verdict at all.
    """
    smallest = {}
    for directory in ALL_DIRS:
        if directory == SMALL_DIR:
            continue
        for kind, _metric, value in _byte_and_count_verdicts(directory, floors=NO_FLOORS):
            smallest[kind] = min(value, smallest.get(kind, value))
    assert smallest[model.BYTES] == skew.BRACKETS[model.BYTES][1], smallest
    assert smallest[model.COUNT] == skew.BRACKETS[model.COUNT][1], smallest


def check_the_floors_remove_every_byte_and_count_verdict_from_the_small_log():
    """What the day was for. Two verdicts, both arithmetically right, neither actionable."""
    before = _byte_and_count_verdicts(SMALL_DIR, floors=NO_FLOORS)
    after = _byte_and_count_verdicts(SMALL_DIR)
    assert len(before) == 2, before
    assert after == [], after


def check_the_floors_remove_nothing_from_either_pathology_log():
    """The control. A floor that quietened the fixtures this tool exists for is a bug.

    Eight verdicts across the two logs, four bytes and one count each, and the floors
    leave all eight standing.
    """
    for directory in (SKEWED_DIR, SKEWED_JOIN_DIR):
        before = _byte_and_count_verdicts(directory, floors=NO_FLOORS)
        after = _byte_and_count_verdicts(directory)
        assert len(before) == 5, (directory, before)
        assert after == before, (directory, before, after)


def check_the_floors_change_nothing_on_a_log_that_never_reached_a_verdict():
    """Four logs produce no byte or count verdict at all and the floors must not invent one."""
    quiet = [d for d in ALL_DIRS
             if os.path.basename(d) in ("balanced", "join", "by_column", "wide")]
    assert len(quiet) == 4, quiet
    for directory in quiet:
        assert _byte_and_count_verdicts(directory, floors=NO_FLOORS) == [], directory
        assert _byte_and_count_verdicts(directory) == [], directory


def check_exactly_the_byte_floor_gets_an_answer():
    """Both sides of the new limit, which is where a mutant goes when nothing sits on it.

    The values are written out rather than read off `FLOORS`, so a change to either floor
    fails here and has to be argued for. The check above is what ties them to the brackets.
    """
    assert skew.FLOORS[model.BYTES] == 700000, skew.FLOORS
    at = skew.judge(_stage([0, 0, 0, 700000], field="disk_spilled"), "disk_spilled")
    under = skew.judge(_stage([0, 0, 0, 699999], field="disk_spilled"), "disk_spilled")
    assert at.outcome == skew.SKEWED, at
    assert under.outcome == skew.UNDECIDED, under
    assert "under the bytes floor of 700000" in under.why, under


def check_exactly_the_count_floor_gets_an_answer():
    assert skew.FLOORS[model.COUNT] == 60000, skew.FLOORS
    at = skew.judge(_stage([0, 0, 0, 60000], field="records_written"), "records_written")
    under = skew.judge(_stage([0, 0, 0, 59999], field="records_written"), "records_written")
    assert at.outcome == skew.SKEWED, at
    assert under.outcome == skew.UNDECIDED, under
    assert "under the count floor of 60000" in under.why, under


def check_the_small_log_falsifies_the_millisecond_bracket_that_shipped():
    """The sharpest thing the new fixture did, and it was not what it was captured for.

    The millisecond floor was placed inside a bracket of 29 to 71 read off the two logs
    captured at eight million rows. A 600 row job runs a task for 556 milliseconds, so the
    largest noise value is eight times the supposed smallest real one. Re-derived here off
    the log rather than quoted, because a falsification carried as a literal is a claim.
    """
    noisiest = 0
    for verdict in skew.scan(_app(SMALL_DIR), floors=NO_FLOORS):
        if verdict.outcome == skew.SKEWED and model.kind_of(verdict.metric) == model.MILLIS:
            noisiest = max(noisiest, verdict.largest)
    assert noisiest == skew.MILLIS_BRACKET_AS_MEASURED[0], noisiest
    real_end = skew.MILLIS_BRACKET_AS_MEASURED[1]
    assert noisiest > real_end, (noisiest, real_end)
    assert noisiest > skew.FLOORS[model.MILLIS], noisiest


def check_the_measured_millisecond_bracket_is_refused_by_the_derivation():
    """So the floor that is hand placed is the one the rule cannot place.

    Without this the module reads as though two floors were derived and the third was an
    oversight. It was measured and it came back unplaceable.
    """
    try:
        skew.floor_from(skew.MILLIS_BRACKET_AS_MEASURED)
    except ValueError as problem:
        assert "556" in str(problem), problem
    else:
        raise AssertionError("the inverted millisecond bracket produced a floor")


def check_the_millisecond_floor_still_leaves_the_small_log_two_verdicts():
    """What the day did not fix, asserted rather than left in prose nothing reads.

    Both are executor run time on a 600 row job and neither is worth acting on. The byte
    and count verdicts are gone and these two are the residue, which is the honest state
    of this fixture and the reason `ot-117` is open.
    """
    left = [v for v in skew.scan(_app(SMALL_DIR))
            if v.outcome == skew.SKEWED]
    assert len(left) == 2, left
    for verdict in left:
        assert verdict.metric == "executor_run_time", verdict
        assert model.kind_of(verdict.metric) == model.MILLIS, verdict


def check_a_bracket_whose_noise_end_is_one_is_still_accepted():
    """A case exactly on the guard, because that is where a mutant goes.

    The guard refuses a bracket that is not a positive noise end below a real end, and
    nothing here had a noise end of 1, so widening the test to refuse that as well changed
    no answer and survived. These numbers are not a bracket anything would really measure.
    The function's contract is about the ordering rather than about the magnitude.
    """
    assert skew.floor_from((1, 10)) == 3, skew.floor_from((1, 10))
    assert skew.floor_from((1, 1000000)) == 1000, skew.floor_from((1, 1000000))


def check_the_real_end_of_the_millisecond_bracket_is_read_off_the_skewed_log():
    """So the inverted bracket is two measured numbers rather than one and a literal.

    71 is the smallest millisecond value on the skewed log that clears the floor already in
    place, which is what makes it the smallest one anything here treats as real. The
    published sweep in the README rests on the same reading.
    """
    floor = skew.FLOORS[model.MILLIS]
    above = [v.largest for v in skew.scan(_app(SKEWED_DIR), floors=NO_FLOORS)
             if v.outcome == skew.SKEWED
             and model.kind_of(v.metric) == model.MILLIS
             and v.largest >= floor]
    assert min(above) == skew.MILLIS_BRACKET_AS_MEASURED[1], (sorted(above),
                                                              skew.MILLIS_BRACKET_AS_MEASURED)


def check_the_published_millisecond_value_list_is_what_the_two_logs_really_produce():
    """The README prints this list and nothing had ever re-derived it.

    Over the two logs captured at eight million rows, which is the pair the millisecond
    floor was placed on. The 600 row log is deliberately not in it, because adding it is
    what inverts the bracket and that is argued separately.
    """
    values = set()
    for directory in (SKEWED_DIR, BALANCED_DIR):
        for verdict in skew.scan(_app(directory), floors=NO_FLOORS):
            if verdict.outcome == skew.SKEWED and model.kind_of(verdict.metric) == model.MILLIS:
                values.add(verdict.largest)
    assert sorted(values) == [3, 8, 24, 29, 71, 3040, 3048], sorted(values)
