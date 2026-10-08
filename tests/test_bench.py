"""Comparing two runs, which is the one thing here that reads two logs at once.

Every other module in this repo answers a question about one log. This one decides whether
a difference between two of them is real, and that is a different kind of wrong. A parser
that misreads a number produces a number nobody can reproduce. A comparison that separates
two arms it should not have produces a sentence somebody puts in a README.

So the checks below spend most of their effort on the refusals. That an exact permutation
test returns a small p on four against four is arithmetic. That it refuses to return one at
all on three against three is the part that stops a result being published.

The fixtures are deliberately lopsided. A pair of arms split evenly around the same mean
survives an inverted comparison, because the difference of means does not move.
"""
import math
import os

from sjp import bench, eventlog

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "fixtures", "eventlogs")

def _only_log(job):
    directory = os.path.join(LOGS, job)
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])

def _stage(stage_id=1, tasks=8, wall=100, median=10.0, largest=20, records=1000):
    return bench.StageTime(stage_id=stage_id, tasks=tasks, wall=wall,
                           median_duration=median, largest_duration=largest,
                           records_read=records)

def _pass(arm="a", stages=None, app_wall=500):
    return bench.Pass(arm=arm, path="/nowhere", app_wall=app_wall,
                      stages=tuple(stages if stages is not None else [_stage()]))

# the floor, which is computed before anything runs

def check_three_passes_an_arm_cannot_reach_five_percent():
    assert bench.p_floor(3, 3) > 0.05

def check_four_passes_an_arm_can():
    assert bench.p_floor(4, 4) <= 0.05

def check_the_floor_is_two_over_the_number_of_splits():
    # 8 choose 4 is 70, so the two extreme splits are 2/70.
    assert bench.p_floor(4, 4) == 2.0 / 70

def check_the_floor_refuses_a_count_it_cannot_permute():
    for left, right in ((0, 4), (4, 0)):
        try:
            bench.p_floor(left, right)
        except ValueError:
            continue
        raise AssertionError("{} and {} was accepted".format(left, right))

def check_a_fully_separated_pair_lands_on_the_floor():
    found = bench.permutation_p([1, 2, 3, 4], [10, 11, 12, 13])
    assert found == bench.p_floor(4, 4), found

def check_two_identical_arms_return_one():
    assert bench.permutation_p([5, 5, 5, 5], [5, 5, 5, 5]) == 1.0

def check_the_test_does_not_care_which_arm_is_named_first():
    left, right = [1, 2, 3, 4], [10, 11, 12, 13]
    assert bench.permutation_p(left, right) == bench.permutation_p(right, left)

def check_an_evenly_straddled_pair_is_not_separated():
    """The control for the lopsided fixture rule.

    These two arms interleave around one mean, so no split is more extreme than the
    observed one and the test has to come back at its largest value.
    """
    assert bench.permutation_p([1, 4, 5, 8], [2, 3, 6, 7]) > 0.05

def check_the_observed_split_counts_itself():
    """Floating point must not let the real split miss its own comparison.

    Means that are not exactly representable are the case. Without the tolerance the
    observed arrangement can fail its own `>=` and the count comes back one short, which
    shows up as a p below the floor.
    """
    found = bench.permutation_p([0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8])
    assert found >= bench.p_floor(4, 4), found

# the verdict, which is what a report prints

def check_an_underpowered_comparison_offers_no_p_at_all():
    found = bench.verdict([1, 2, 3], [10, 11, 12])
    assert found.p is None
    assert found.decided is False
    assert "smallest reachable p" in found.why

def check_a_separated_comparison_is_decided():
    found = bench.verdict([1, 2, 3, 4], [10, 11, 12, 13])
    assert found.decided is True
    assert found.p == bench.p_floor(4, 4)

def check_an_unseparated_comparison_says_undecided_rather_than_equal():
    found = bench.verdict([1, 4, 5, 8], [2, 3, 6, 7])
    assert found.decided is False
    assert "undecided rather than equal" in found.why

def check_a_separated_verdict_says_its_p_is_the_floor():
    """The word on its own was the whole of `ot-104`.

    A reader who sees "separated" takes it as a small p. What it means at this size is
    that the arms do not interleave, and the sentence has to say so.
    """
    found = bench.verdict([1, 2, 3, 4], [10, 11, 12, 13])
    assert found.on_floor is True
    assert "is the floor" in found.why, found.why
    assert "no run of this size could report less" in found.why, found.why

def check_a_verdict_above_the_floor_does_not_claim_to_be_on_it():
    """The control for the check above. Six passes an arm can land here and four cannot."""
    left = [4996, 5393, 5421, 5783, 5371, 5453]
    right = [5622, 5885, 5905, 5652, 6401, 6038]
    found = bench.verdict(left, right)
    assert found.decided is True
    assert found.on_floor is False, found.p
    assert found.p > found.floor
    assert "against a floor of" in found.why, found.why

def check_an_underpowered_comparison_is_not_on_the_floor_either():
    """No p means no reading of the floor. `on_floor` must not be true by default."""
    found = bench.verdict([1, 2, 3], [10, 11, 12])
    assert found.p is None
    assert found.on_floor is False

def check_a_p_sitting_exactly_on_alpha_is_decided():
    """The boundary `ot-099` called unreachable, reached through the alpha argument.

    These eight values give an exact p of 4/70. The default alpha cannot be hit this way,
    which the check below proves, but alpha is an argument and a caller can pass one.
    """
    found = bench.verdict([1, 2, 3, 5], [4, 6, 7, 8], alpha=4 / 70)
    assert found.p == 4 / 70, found.p
    assert found.decided is True, found.why
    # A verdict that separated must not describe itself as undecided. The outcome and the
    # sentence are computed from two different comparisons and they can disagree.
    assert "undecided" not in found.why, found.why

def check_no_decided_verdict_ever_calls_itself_undecided():
    """The outcome and the sentence come from separate comparisons of p against alpha."""
    cases = ((([1, 2, 3, 4], [10, 11, 12, 13]), 0.05),
             (([1, 2, 3, 5], [4, 6, 7, 8]), 4 / 70),
             (([4996, 5393, 5421, 5783, 5371, 5453],
               [5622, 5885, 5905, 5652, 6401, 6038]), 0.05))
    for (left, right), alpha in cases:
        found = bench.verdict(left, right, alpha=alpha)
        assert found.decided is True, (left, right, alpha, found.why)
        assert "undecided" not in found.why, found.why

def check_a_floor_sitting_exactly_on_alpha_is_not_refused():
    """A floor equal to alpha is reachable, so the refusal has to be strictly above it."""
    found = bench.verdict([1, 2, 3, 4], [10, 11, 12, 13], alpha=bench.p_floor(4, 4))
    assert found.floor == bench.p_floor(4, 4)
    assert found.p is not None, found.why
    assert found.decided is True

def check_equal_arms_cannot_land_exactly_on_five_percent():
    """Why the two alpha boundaries above need a hand picked alpha to reach.

    A split and its complement are both enumerated and both score the same gap, so they
    are extreme together and the count is always even. An exact p of 0.05 would need
    half the split total divided by ten, and that is odd at the two pass counts where it
    is a whole number at all.
    """
    for passes in range(3, 13):
        splits = math.comb(2 * passes, passes)
        reachable = splits * 0.05
        assert not (reachable.is_integer() and int(reachable) % 2 == 0), passes

def check_the_extreme_count_is_even_whenever_the_arms_are_the_same_size():
    """The parity the check above rests on, measured rather than argued."""
    for left, right in (([1, 2, 3, 4], [10, 11, 12, 13]),
                        ([1, 4, 5, 8], [2, 3, 6, 7]),
                        ([5, 5, 5, 5], [5, 5, 5, 6])):
        splits = math.comb(len(left) + len(right), len(left))
        assert round(bench.permutation_p(left, right) * splits) % 2 == 0

def check_a_tied_split_is_counted_rather_than_absorbed_by_a_tolerance():
    """What replaced the epsilon.

    Integer milliseconds produce splits that tie with the observed one. The old code
    compared floats against `observed - 1e-9`, which counted a tie and would have counted
    it just as well with the comparison written the other way. Exact arithmetic makes the
    tie a real equality, so it is the comparison that decides it.
    """
    left, right = [1, 2, 3, 4], [1, 2, 3, 4]
    assert bench.permutation_p(left, right) == 1.0
    one_apart = bench.permutation_p([1, 2, 3, 4], [2, 3, 4, 5])
    assert one_apart > bench.p_floor(4, 4), one_apart

def check_the_exact_rewrite_moved_no_p_on_a_real_pair():
    """Readings off the 2026-10-08 schedule, where the float version gave the same p.

    Kept as a fixture rather than a note, because the rewrite's whole claim is that it
    changed the arithmetic and not the answer.
    """
    current = [3924, 4140, 4209, 4367, 4161, 4189]
    advised = [5323, 5837, 5279, 5473, 6121, 5796]
    assert bench.permutation_p(current, advised) == bench.p_floor(6, 6)

def check_the_ratio_is_the_second_arm_over_the_first():
    found = bench.verdict([10, 10, 10, 10], [20, 20, 20, 20])
    assert found.ratio == 2.0

def check_a_zero_baseline_has_no_ratio_rather_than_a_crash():
    found = bench.verdict([0, 0, 0, 0], [20, 20, 20, 20])
    assert found.ratio is None

def check_every_arm_is_first_exactly_once_over_a_full_rotation():
    _warm, order = bench.schedule(["a", "b", "c", "d"], 4)
    firsts = [order[index * 4] for index in range(4)]
    assert sorted(firsts) == ["a", "b", "c", "d"], firsts

def check_every_arm_runs_the_same_number_of_times():
    _warm, order = bench.schedule(["a", "b", "c"], 4)
    assert {name: order.count(name) for name in "abc"} == {"a": 4, "b": 4, "c": 4}

def check_a_single_arm_schedule_is_not_a_rotation_and_still_works():
    warm, order = bench.schedule(["only"], 3)
    assert warm == "only"
    assert order == ["only", "only", "only"]

def check_a_schedule_with_no_passes_is_refused():
    for passes in (0, -1):
        try:
            bench.schedule(["a"], passes)
        except ValueError:
            continue
        raise AssertionError("{} passes was accepted".format(passes))

def check_a_schedule_with_no_arms_is_refused():
    try:
        bench.schedule([], 4)
    except ValueError:
        return
    raise AssertionError("an empty arm list was accepted")

# what makes two arms comparable, and what deliberately does not

def check_arms_that_moved_different_rows_are_refused():
    one = _pass("a", [_stage(records=1000)])
    other = _pass("b", [_stage(records=999)])
    try:
        bench.check_comparable([one, other])
    except bench.NotComparable as problem:
        assert "nothing to compare" in str(problem)
        return
    raise AssertionError("two different row counts were compared")

def check_arms_with_different_stage_counts_are_refused():
    one = _pass("a", [_stage()])
    other = _pass("b", [_stage(), _stage(stage_id=2)])
    try:
        bench.check_comparable([one, other])
    except bench.NotComparable:
        return
    raise AssertionError("a different number of stages was compared")

def check_a_different_task_count_is_not_a_reason_to_refuse():
    """The control for the rule above, and the whole point of the module.

    The partition count is the thing being changed, so the task counts differ by design.
    A shape check that included them would refuse every comparison this module exists for.
    """
    one = _pass("a", [_stage(tasks=8)])
    other = _pass("b", [_stage(tasks=2)])
    assert bench.check_comparable([one, other]) == (1, (1000,))

def check_the_same_stages_in_a_different_order_still_compare():
    """A join gives its two sides whichever stage id the scheduler reaches first.

    So one job run twice puts the same row counts at different positions. Reading them
    positionally refused sixteen passes of one job, and the arms were identical.
    """
    one = _pass("a", [_stage(stage_id=1, records=0), _stage(stage_id=2, records=8000000),
                      _stage(stage_id=3, records=0), _stage(stage_id=4, records=8000199)])
    other = _pass("b", [_stage(stage_id=1, records=0), _stage(stage_id=2, records=0),
                        _stage(stage_id=3, records=8000000),
                        _stage(stage_id=4, records=8000199)])
    assert bench.check_comparable([one, other]) == (4, (0, 0, 8000000, 8000199))

def check_reordering_does_not_hide_a_row_count_that_really_moved():
    """The control for the sort. It must not swallow a real difference.

    Same number of stages, same positions rearranged, and one count genuinely different.
    A sort that compared only the totals would let this through.
    """
    one = _pass("a", [_stage(stage_id=1, records=0), _stage(stage_id=2, records=8000000)])
    other = _pass("b", [_stage(stage_id=1, records=8000001), _stage(stage_id=2, records=0)])
    try:
        bench.check_comparable([one, other])
    except bench.NotComparable:
        return
    raise AssertionError("a row count that moved was reordered away")

def check_a_different_wall_time_is_not_a_reason_to_refuse():
    one = _pass("a", [_stage(wall=100)])
    other = _pass("b", [_stage(wall=9999)])
    bench.check_comparable([one, other])

# the spread, and the stage a skew report would name

def check_a_stage_whose_median_task_did_nothing_has_no_spread():
    assert _stage(median=0).spread is None

def check_the_spread_is_the_largest_over_the_median():
    assert _stage(median=10.0, largest=95).spread == 9.5

def check_the_worst_stage_is_the_one_with_the_largest_spread():
    one = _pass("a", [_stage(stage_id=1, median=10.0, largest=20),
                      _stage(stage_id=2, median=10.0, largest=90),
                      _stage(stage_id=3, median=10.0, largest=50)])
    assert bench.worst_stage(one).stage_id == 2

def check_a_run_where_no_stage_has_a_spread_has_no_worst_stage():
    one = _pass("a", [_stage(median=0), _stage(stage_id=2, median=0)])
    assert bench.worst_stage(one) is None

def check_a_stage_with_no_spread_does_not_hide_one_that_has_it():
    one = _pass("a", [_stage(stage_id=1, median=0, largest=9999),
                      _stage(stage_id=2, median=10.0, largest=30)])
    assert bench.worst_stage(one).stage_id == 2

# against a real log rather than a hand built one

def check_a_pass_read_from_a_committed_log_carries_every_stage():
    path = _only_log("skewed")
    one = bench.read_pass("current", path)
    app = eventlog.profile(path)
    assert len(one.stages) == len(app.stages)
    assert [s.stage_id for s in one.stages] == [s.stage_id for s in app.stages]

def check_the_stage_total_and_the_application_wall_are_different_numbers():
    """A run's stages do not add up to its wall time, and both are reported.

    The gap is session start, session stop and whatever sat between stages. A benchmark
    quoting only one of them cannot tell a change in the work from a change around it.
    """
    one = bench.read_pass("current", _only_log("skewed"))
    assert one.stage_wall_total != one.app_wall
    assert one.stage_wall_total < one.app_wall

def check_the_skewed_log_worst_stage_is_the_one_that_spilled():
    """The stage the committed skewed log is skewed on is the one this picks.

    Pinned against the log rather than against a number written here, so an edit to the
    ranking rule that picks a different stage fails rather than being believed.
    """
    one = bench.read_pass("current", _only_log("skewed"))
    app = eventlog.profile(_only_log("skewed"))
    spilled = max(app.stages, key=lambda stage: stage.total("disk_spilled"))
    assert bench.worst_stage(one).stage_id == spilled.stage_id

def check_the_balanced_log_is_less_spread_than_the_skewed_one():
    skewed = bench.worst_stage(bench.read_pass("a", _only_log("skewed")))
    balanced = bench.worst_stage(bench.read_pass("b", _only_log("balanced")))
    assert skewed.spread > balanced.spread, (skewed.spread, balanced.spread)

def check_two_readings_of_one_log_are_the_same_pass():
    path = _only_log("balanced")
    assert bench.read_pass("a", path).stages == bench.read_pass("a", path).stages

def check_readings_come_back_in_the_order_the_passes_ran():
    passes = [_pass("a", app_wall=1), _pass("b", app_wall=2), _pass("a", app_wall=3)]
    assert bench.readings(passes, "a", lambda one: one.app_wall) == [1, 3]

def check_readings_for_an_arm_that_never_ran_are_empty():
    assert bench.readings([_pass("a")], "b", lambda one: one.app_wall) == []

# the stage a benchmark is about, which is not the stage a spread ranking picks

def check_the_carrying_stage_is_the_one_that_read_the_most_rows():
    one = _pass("a", [_stage(stage_id=1, records=0), _stage(stage_id=2, records=8000000),
                      _stage(stage_id=3, records=8000199)])
    assert bench.carrying_stage(one).stage_id == 3

def check_a_run_where_nothing_read_a_row_has_no_carrying_stage():
    one = _pass("a", [_stage(records=0), _stage(stage_id=2, records=0)])
    assert bench.carrying_stage(one) is None

def check_the_carrying_stage_does_not_move_when_the_stage_ids_do():
    """The reason it exists. A join's two sides race for the lower id between runs."""
    one = _pass("a", [_stage(stage_id=2, records=8000000, wall=50),
                      _stage(stage_id=3, records=8000199, wall=99)])
    other = _pass("b", [_stage(stage_id=1, records=8000199, wall=99),
                        _stage(stage_id=3, records=8000000, wall=50)])
    assert bench.carrying_stage(one).wall == bench.carrying_stage(other).wall

def check_the_spread_ranking_and_the_carrying_stage_can_disagree():
    """Measured, not invented. Cutting the count of a skewed job does this.

    The hot partition and the median partition become the same partition, so the ratio
    collapses on the stage doing all the work and a stage doing none out-ranks it. Both
    functions are right and reading either alone is what goes wrong.
    """
    one = _pass("a", [_stage(stage_id=1, records=0, median=10.0, largest=90),
                      _stage(stage_id=2, records=8000199, median=6804.0, largest=10916)])
    assert bench.worst_stage(one).stage_id == 1
    assert bench.carrying_stage(one).stage_id == 2

def check_the_two_agree_on_a_job_that_was_not_cut():
    """The control for the check above. They must not always disagree."""
    one = _pass("a", [_stage(stage_id=1, records=0, median=10.0, largest=11),
                      _stage(stage_id=2, records=8000199, median=868.0, largest=7418)])
    assert bench.worst_stage(one).stage_id == bench.carrying_stage(one).stage_id == 2

# the boundaries themselves, which the refusals above stand next to
#
# Every refusal here was checked against a value it must reject and none against the
# smallest value it must accept, so widening any of the guards by one changed nothing the
# suite could see. Six mutants of four lines survived on that.

def check_one_pass_is_a_schedule_rather_than_a_refusal():
    warm, order = bench.schedule(["a", "b"], 1)
    assert warm == "a"
    assert order == ["a", "b"]

def check_one_observation_an_arm_has_a_floor_rather_than_a_refusal():
    assert bench.p_floor(1, 1) == 1.0

def check_the_floor_accepts_one_on_either_side():
    assert bench.p_floor(1, 5) == 2.0 / 6
    assert bench.p_floor(5, 1) == 2.0 / 6

# the records are frozen, and nothing else here would notice if they stopped being

def check_a_pass_cannot_be_edited_after_it_is_read():
    """A comparison holds every pass while it runs.

    Nothing in the suite writes to one, so the four records dropping their frozen flag was
    invisible to all of it. A benchmark whose readings can be edited between the run and
    the report is a benchmark with no chain of custody.
    """
    for record in (_pass(), _stage(),
                   bench.Arm(name="a", job="j", partitions=1),
                   bench.verdict([1, 2, 3, 4], [10, 11, 12, 13])):
        try:
            setattr(record, next(iter(vars(record))), "edited")
        except Exception:
            continue
        raise AssertionError("{} accepted an edit".format(type(record).__name__))
