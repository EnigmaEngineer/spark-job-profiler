"""The two sizes, and every number this repo publishes about them.

The measurements came first and one of them changed the design. `data size` looked like
the size of a relation until it read identically on two jobs whose shuffles differ by
twenty percent, at which point it stopped being a measurement and became an estimate with
a name that hides it.

Everything asserted here was read off the three committed logs before `sjp.layout` was
written, and the arithmetic is recomputed rather than transcribed. A check that asserts a
figure agrees with whoever typed it. A check that computes one disagrees.
"""
import dataclasses
import math
import os

from sjp import eventlog, layout, plan, skew

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "fixtures", "eventlogs")
SKEWED, BALANCED, JOIN = "skewed", "balanced", "join"
SKEWED_JOIN = "skewed_join"
BY_COLUMN = "by_column"

# Rows going through the exchange in every committed log. The per row widths below are
# divisions by this, so it is named once rather than written into each of them.
ROWS = 8000000
DIM_ROWS = 199

def _only_log(job):
    directory = os.path.join(LOGS, job)
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])

def _app(job):
    return eventlog.profile(_only_log(job))

def _shuffles(job):
    app = _app(job)
    return app, layout.shuffles(app, plan.read(app.plans[0]))

def _hash_exchange(job):
    """The exchange that partitions by key. The one every figure here is about."""
    _app_, found = _shuffles(job)
    by_key = [s for s in found if s.scheme == "hashpartitioning"]
    assert by_key, job
    return by_key[0]

# data size is an estimate, and this is how the repo found out

def check_the_estimate_is_identical_on_two_jobs_whose_measured_shuffle_differs():
    """The finding the module is built around.

    One expression separates the two jobs. The estimate cannot see it and the measurement
    can, so a recommender reading the estimate gives both jobs the same advice.
    """
    skewed, balanced = _hash_exchange(SKEWED), _hash_exchange(BALANCED)
    assert skewed.estimated == balanced.estimated == 832000000
    assert skewed.written == 130788590
    assert balanced.written == 163272682
    assert skewed.written != balanced.written

def check_the_estimate_is_a_whole_number_of_bytes_per_row_on_every_exchange():
    """What makes it an estimate rather than a measurement.

    Rows multiplied by a width taken from the schema. A measured size would not divide
    evenly, and the measured one below does not.
    """
    widths = {}
    for job in (SKEWED, BALANCED, JOIN):
        _app_, found = _shuffles(job)
        for shuffle in found:
            rows = DIM_ROWS if shuffle.estimated < 100000 else ROWS
            width = shuffle.estimated / rows
            assert width == int(width), (job, shuffle.scheme, shuffle.estimated, rows)
            widths[int(width)] = widths.get(int(width), 0) + 1
    assert sorted(widths) == [16, 24, 104]

def check_the_measured_size_does_not_divide_evenly_by_the_row_count():
    """The control for the check above. If both divided evenly the test would be about
    arithmetic rather than about the difference between the two numbers."""
    for job, expected in ((SKEWED, 130788590), (BALANCED, 163272682)):
        written = _hash_exchange(job).written
        assert written == expected
        assert written / ROWS != int(written / ROWS)

def check_the_factor_between_the_two_sizes_is_not_a_constant():
    """So there is no converting one into the other, which is why both are carried.

    Computed over every exchange in the repo rather than asserted as a pair, because a
    pair of numbers typed here would agree with whoever typed them.
    """
    factors = []
    for job in (SKEWED, BALANCED, JOIN):
        _app_, found = _shuffles(job)
        for shuffle in found:
            factors.append(shuffle.estimated / shuffle.written)
    assert len(factors) == 7
    assert min(factors) < 2.0
    assert max(factors) > 6.3
    assert max(factors) / min(factors) > 3.1

# slots, which are cores only while a task asks for one cpu

def check_slots_come_from_the_cores_and_the_cpus_a_task_asks_for():
    for job in (SKEWED, BALANCED, JOIN):
        app = _app(job)
        assert app.cores == 2
        assert app.cpus_per_task == 1.0
        assert app.slots == 2

def check_a_task_asking_for_two_cpus_halves_the_slots():
    """Nothing in this repo runs that way, so the case is built rather than captured.

    A profiler reading cores alone answers the same number as this one on every log here
    and the wrong number on the first log where a task asks for more.
    """
    app = _app(JOIN)
    doubled = type(app)(**dict(app.__dict__, cpus_per_task=2.0))
    assert doubled.slots == 1

def check_a_log_reporting_no_cpus_per_task_falls_back_to_the_cores():
    app = _app(JOIN)
    silent = type(app)(**dict(app.__dict__, cpus_per_task=0.0))
    assert silent.slots == silent.cores == 2

def check_eight_partitions_on_two_slots_is_four_full_rounds():
    assert layout.waves(8, 2) == (4, 0)

def check_a_count_that_does_not_divide_leaves_a_part_filled_last_round():
    assert layout.waves(9, 2) == (5, 1)
    assert layout.waves(1, 2) == (1, 1)

def check_waves_refuses_a_slot_count_of_zero_rather_than_dividing_by_it():
    for bad in (0, -1):
        try:
            layout.waves(8, bad)
        except ValueError:
            continue
        raise AssertionError("{} slots was accepted".format(bad))

def check_waves_refuses_a_partition_count_of_zero():
    try:
        layout.waves(0, 2)
    except ValueError:
        return
    raise AssertionError("zero partitions was accepted")

# sizing, and the refusal that covers both committed aggregate logs

def check_a_count_written_into_the_query_gets_no_recommendation():
    for job in (SKEWED, BALANCED):
        app, found = _shuffles(job)
        for shuffle in found:
            sizing = layout.size(shuffle, app.slots)
            assert sizing.target is None, (job, shuffle.scheme)
            assert plan.CHOSEN_BY_HAND in sizing.why

def check_the_join_log_is_the_only_one_where_anything_can_be_recommended():
    app, found = _shuffles(JOIN)
    targets = [layout.size(s, app.slots).target for s in found]
    assert targets.count(None) == 1
    assert sorted(t for t in targets if t is not None) == [2, 4]

def check_the_recommended_count_is_the_volume_rounded_up_to_whole_rounds():
    """Recomputed from the advisory size rather than asserted, so the check disagrees
    with the module when the module changes and the docstring does not."""
    app, found = _shuffles(JOIN)
    big = [s for s in found if s.changeable and s.written > 1000000][0]
    sizing = layout.size(big, app.slots)
    from_volume = math.ceil(big.written / layout.ADVISORY_BYTES)
    assert sizing.from_volume == from_volume == 3
    assert sizing.target == int(math.ceil(max(from_volume, app.slots) / app.slots) * app.slots)
    assert sizing.target == 4
    assert layout.waves(sizing.target, app.slots) == (2, 0)

def check_the_slot_count_is_a_floor_under_the_volume():
    """A shuffle small enough to want one partition still gets the slot count, because
    fewer partitions than slots leaves cores idle for the whole stage."""
    app, found = _shuffles(JOIN)
    small = [s for s in found if s.changeable and s.written < 1000000][0]
    sizing = layout.size(small, app.slots)
    assert sizing.from_volume == 1
    assert sizing.target == app.slots == 2

def check_a_shuffle_already_inside_the_advisory_size_says_so_rather_than_repeating_itself():
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=2, partitions=2,
                             estimated=100, written=100)
    sizing = layout.size(shuffle, 2)
    assert sizing.target == sizing.current == 2
    assert "already inside the advisory size" in sizing.why

# the two partition counts, which are two readings of one number

def check_the_plan_text_and_the_driver_metric_report_the_same_partition_count():
    """Two independent readings. The count is in the partitioning expression and the
    driver reports it as a metric, and nothing in the log forces them to agree."""
    for job in (SKEWED, BALANCED, JOIN):
        _app_, found = _shuffles(job)
        for shuffle in found:
            assert shuffle.declared == 8
            assert shuffle.partitions == 8
            assert shuffle.counts_agree

def check_the_partition_count_is_only_ever_reported_by_the_driver():
    """So a lookup that searched the stages alone would answer None for a number that is
    in the file. This is what the driver accumulator handler is for."""
    app = _app(JOIN)
    root = plan.read(app.plans[0])
    for node in plan.exchanges(root):
        acc_id = node.metrics[layout.PARTITION_COUNT]
        on_a_stage = any(total.acc_id == acc_id
                         for stage in app.stages for total in stage.totals)
        assert not on_a_stage
        assert app.accumulator(acc_id) == 8

def check_counts_that_disagree_are_reported_as_disagreeing():
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=8, partitions=4,
                             estimated=100, written=100)
    assert not shuffle.counts_agree

def check_per_partition_is_none_when_the_log_reported_no_count():
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=8, partitions=None,
                             estimated=100, written=100)
    assert shuffle.per_partition is None

def check_the_two_aggregate_logs_produce_no_broadcast_verdict_at_all():
    for job in (SKEWED, BALANCED):
        app, _found = _shuffles(job)
        assert layout.candidates(app, plan.read(app.plans[0])) == []

def check_the_small_side_of_the_join_is_under_sparks_own_threshold():
    app = _app(JOIN)
    found = layout.candidates(app, plan.read(app.plans[0]))
    assert len(found) == 2
    small = [c for c in found if c.worth_it]
    assert len(small) == 1
    assert small[0].estimated == 4776
    assert small[0].estimated < layout.BROADCAST_BYTES

def check_the_saving_reported_is_the_other_sides_measured_shuffle():
    """The argument for a broadcast is never the size of the thing broadcast."""
    app = _app(JOIN)
    small = [c for c in layout.candidates(app, plan.read(app.plans[0])) if c.worth_it][0]
    assert small.saved == 163272682
    assert small.saved == _hash_exchange(JOIN).written
    assert small.saved / small.estimated > 34000

def check_the_large_side_is_reported_with_its_verdict_rather_than_dropped():
    """A summary that keeps only the qualifying side cannot report the absence of one."""
    app = _app(JOIN)
    found = layout.candidates(app, plan.read(app.plans[0]))
    large = [c for c in found if not c.worth_it]
    assert len(large) == 1
    assert large[0].estimated == 832000000
    assert "over the" in large[0].why

def check_a_threshold_above_both_sides_still_leaves_the_large_side_unbroadcastable():
    """The control for the one above. Moving the threshold past the large side has to
    change the verdict, or the verdict was not being decided by the threshold."""
    app = _app(JOIN)
    found = layout.candidates(app, plan.read(app.plans[0]), threshold=10 ** 12)
    assert all(c.worth_it for c in found)

def check_a_side_that_was_not_shuffled_is_not_a_candidate():
    """A broadcast join's build side has no exchange, so there is nothing to save. That
    is the log of a job that already took this advice."""
    exchange = plan.Node(name=plan.EXCHANGE, simple="Exchange SinglePartition, ENSURE_REQUIREMENTS",
                         metrics={layout.ESTIMATED_SIZE: 1, layout.MEASURED_SIZE: 2},
                         children=())
    join = plan.Node(name="BroadcastHashJoin", simple="", metrics={},
                     children=(plan.Node(name="Sort", simple="", metrics={}, children=()),
                               plan.Node(name="Sort", simple="", metrics={},
                                         children=(exchange,))))
    app = _app(JOIN)
    unshuffled = type(app)(**dict(app.__dict__, accumulators={1: "50", 2: "40"}))
    found = layout.candidates(unshuffled, join)
    assert len(found) == 1
    assert found[0].estimated == 50

def check_a_candidate_with_no_measurement_of_the_other_side_is_not_worth_it():
    """Under the threshold is half the answer. Without the other side's shuffle there is
    no saving to report, and a recommendation with no saving is a number with no argument."""
    exchange = plan.Node(name=plan.EXCHANGE, simple="Exchange SinglePartition, ENSURE_REQUIREMENTS",
                         metrics={layout.ESTIMATED_SIZE: 1}, children=())
    side = plan.Node(name="Sort", simple="", metrics={}, children=(exchange,))
    join = plan.Node(name="SortMergeJoin", simple="", metrics={}, children=(side, side))
    app = _app(JOIN)
    quiet = type(app)(**dict(app.__dict__, accumulators={1: "50"}))
    found = layout.candidates(quiet, join)
    assert len(found) == 2
    assert not any(c.worth_it for c in found)
    assert all("nothing measures" in c.why for c in found)

# the lines the command prints

def check_the_lines_name_the_origin_of_every_exchange():
    app = _app(JOIN)
    lines = layout.layout_lines(app)
    assert sum(1 for line in lines if plan.CHOSEN_BY_SPARK in line) == 2
    assert sum(1 for line in lines if plan.CHOSEN_BY_HAND in line) == 2

def check_the_lines_say_there_is_no_join_when_there_is_none():
    for job in (SKEWED, BALANCED):
        lines = layout.layout_lines(_app(job))
        assert any("no join in this plan" in line for line in lines)

def check_a_log_with_no_plan_says_so_rather_than_printing_an_empty_report():
    app = _app(JOIN)
    planless = type(app)(**dict(app.__dict__, plans=()))
    lines = layout.layout_lines(planless)
    assert any("no physical plan" in line for line in lines)
    assert len(lines) == 2

# what the mutation pass asked for

def check_sparks_two_documented_defaults_are_pinned_as_the_numbers_they_are():
    """Asserted as literals and not through their own names.

    A constant checked only as `layout.ADVISORY_BYTES` is a constant nothing pins, and
    these two are Spark's numbers rather than this repo's. When Spark changes one of them
    this check is supposed to fail.
    """
    assert layout.ADVISORY_BYTES == 67108864
    assert layout.ADVISORY_BYTES == 64 * 1024 * 1024
    assert layout.BROADCAST_BYTES == 10485760
    assert layout.BROADCAST_BYTES == 10 * 1024 * 1024

def check_the_layout_records_are_frozen():
    records = (
        layout.Shuffle(scheme="s", origin="o", changeable=True, declared=1, partitions=1,
                       estimated=1, written=1),
        layout.Sizing(current=1, target=1, from_volume=1,
                      current_waves=(1, 0), target_waves=(1, 0), why="w"),
        layout.Candidate(join="j", estimated=1, threshold=1, saved=1, worth_it=True,
                         why="w"),
    )
    for record in records:
        try:
            setattr(record, "why", "changed")
        except dataclasses.FrozenInstanceError:
            continue
        raise AssertionError("{} accepted an edit".format(type(record).__name__))

def check_per_partition_divides_rather_than_multiplying():
    shuffle = layout.Shuffle(scheme="s", origin="o", changeable=True, declared=4,
                             partitions=4, estimated=0, written=1000)
    assert shuffle.per_partition == 250.0

def check_one_slot_is_schedulable_and_is_not_refused_with_the_zero():
    """The boundary from both sides. A floor written one off refuses the smallest real
    machine and nothing else in the suite runs one."""
    assert layout.waves(8, 1) == (8, 0)
    shuffle = layout.Shuffle(scheme="s", origin=plan.CHOSEN_BY_SPARK, changeable=True,
                             declared=1, partitions=1, estimated=0, written=10)
    assert layout.size(shuffle, 1).target == 1

def check_one_partition_is_schedulable_too():
    assert layout.waves(1, 1) == (1, 0)

def check_the_current_count_is_the_driver_metric_and_not_the_plan_text():
    """The two disagree only when something is wrong, so a rule that reads either one
    looks correct on every log here. The driver metric is the measured one."""
    shuffle = layout.Shuffle(scheme="s", origin=plan.CHOSEN_BY_SPARK, changeable=True,
                             declared=8, partitions=4, estimated=0, written=10)
    assert layout.size(shuffle, 2).current == 4

def check_a_missing_driver_count_falls_back_to_the_plan_text():
    shuffle = layout.Shuffle(scheme="s", origin=plan.CHOSEN_BY_SPARK, changeable=True,
                             declared=8, partitions=None, estimated=0, written=10)
    assert layout.size(shuffle, 2).current == 8

def check_a_side_with_an_exchange_but_no_estimate_is_skipped():
    """Either half missing is a side this cannot judge. Both halves have to be tested
    separately, because a rule requiring both to be absent skips neither case."""
    exchange = plan.Node(name=plan.EXCHANGE, simple="Exchange SinglePartition, ENSURE_REQUIREMENTS",
                         metrics={layout.MEASURED_SIZE: 9}, children=())
    side = plan.Node(name="Sort", simple="", metrics={}, children=(exchange,))
    join = plan.Node(name="SortMergeJoin", simple="", metrics={}, children=(side, side))
    app = _app(JOIN)
    quiet = type(app)(**dict(app.__dict__, accumulators={9: "40"}))
    assert layout.candidates(quiet, join) == []

def check_a_relation_exactly_on_the_threshold_is_not_under_it():
    """The boundary. A comparison written one off broadcasts the first relation Spark
    would refuse to."""
    exchange = plan.Node(name=plan.EXCHANGE, simple="Exchange SinglePartition, ENSURE_REQUIREMENTS",
                         metrics={layout.ESTIMATED_SIZE: 1, layout.MEASURED_SIZE: 2},
                         children=())
    side = plan.Node(name="Sort", simple="", metrics={}, children=(exchange,))
    join = plan.Node(name="SortMergeJoin", simple="", metrics={}, children=(side, side))
    app = _app(JOIN)
    on_the_line = type(app)(**dict(app.__dict__,
                                   accumulators={1: str(layout.BROADCAST_BYTES), 2: "5"}))
    assert not any(c.worth_it for c in layout.candidates(on_the_line, join))
    under = type(app)(**dict(app.__dict__,
                             accumulators={1: str(layout.BROADCAST_BYTES - 1), 2: "5"}))
    assert all(c.worth_it for c in layout.candidates(under, join))

def check_the_schedule_line_prints_the_round_count_and_then_the_last_rounds_width():
    """Both numbers come out of one pair and every log here has an empty tail, so a line
    reading the wrong end of it looks right on all three."""
    lines = layout.layout_lines(_app(JOIN))
    schedule = [line for line in lines if "rounds of" in line]
    assert len(schedule) == 3
    assert all("4 rounds of 2 slots, 2 in the last" in line for line in schedule)

def check_a_part_filled_last_round_is_printed_as_its_real_width():
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_HAND,
                             changeable=False, declared=9, partitions=9,
                             estimated=0, written=10)
    sizing = layout.size(shuffle, 2)
    assert sizing.current_waves == (5, 1)

def check_the_partitions_line_names_a_target_only_when_there_is_one():
    """The two branches print opposite things, so a condition read backwards swaps the
    refusal and the recommendation and both still read as sentences."""
    join_lines = layout.layout_lines(_app(JOIN))
    assert any("partitions    4 rather than 8." in line for line in join_lines)
    assert any("partitions    none. " + plan.CHOSEN_BY_HAND in line for line in join_lines)
    for job in (SKEWED, BALANCED):
        lines = layout.layout_lines(_app(job))
        assert all("rather than" not in line for line in lines)
        assert sum(1 for line in lines if "partitions    none." in line) == 2

# the count the exit status is computed from

def check_the_two_aggregate_logs_have_nothing_worth_changing():
    """Both of them size their partitions badly against the advisory number and neither
    is advice, because neither count is the config's to move."""
    for job in (SKEWED, BALANCED):
        app = _app(job)
        assert layout.actionable(app) == 0

def check_the_join_log_counts_two_partition_targets_and_one_broadcast():
    """Pinned as an arithmetic total rather than as a truthy flag. An exit status cannot
    tell three from one, so a rule that double counted would look correct from outside."""
    app = _app(JOIN)
    assert layout.actionable(app) == 3

def check_an_advisory_the_current_count_already_satisfies_drops_that_exchange():
    """Moving the advisory number has to move the count, or the count is not computed
    from it. 21000000 bytes a partition is what 163272682 over 8 asks for, so the large
    exchange stops being advice and the small one and the broadcast stay."""
    app = _app(JOIN)
    assert layout.actionable(app, advisory=21000000) == 2

def check_an_advisory_above_the_whole_shuffle_still_leaves_the_slot_floor():
    """The volume stops arguing and the slots do not. Eight partitions on two slots is
    four rounds for a shuffle that wants one partition, so both counts are still advice."""
    app = _app(JOIN)
    assert layout.actionable(app, advisory=10 ** 12) == 3

def check_a_threshold_below_the_small_side_leaves_only_the_partition_targets():
    app = _app(JOIN)
    assert layout.actionable(app, threshold=1) == 2

def check_the_count_is_printed_rather_than_left_as_an_exit_status():
    assert any("3 worth changing" in line for line in layout.layout_lines(_app(JOIN)))
    for job in (SKEWED, BALANCED):
        assert any("0 worth changing" in line for line in layout.layout_lines(_app(job)))

# what the report does when a count or a volume is missing
#
# None of these are reachable from the three committed logs. Every exchange in all three
# declares its count in the plan text and the driver reports it too. They are here because
# the shapes are ordinary in real Spark work and both used to raise a TypeError out of
# divmod rather than answer.

def check_no_count_in_either_place_is_a_refusal_and_not_a_crash():
    """SinglePartition carries no count in the plan text. A log where the driver reported
    none either leaves nothing to divide or to schedule, which is an answer this has to be
    able to give."""
    shuffle = layout.Shuffle(scheme="SinglePartition", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=None, partitions=None,
                             estimated=100, written=1000)
    sizing = layout.size(shuffle, 2)
    assert sizing.current is None
    assert sizing.target is None
    assert sizing.current_waves is None
    assert sizing.target_waves is None
    assert "nothing to size" in sizing.why

def check_a_shuffle_with_no_measured_bytes_is_a_refusal_and_not_a_crash():
    """The count is known and the volume is not, so the schedule is answerable and the
    target is not. Both halves are asserted, because a refusal that drops the half it
    could have answered is a different defect."""
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=8, partitions=8,
                             estimated=100, written=None)
    sizing = layout.size(shuffle, 2)
    assert sizing.current == 8
    assert sizing.current_waves == (4, 0)
    assert sizing.target is None
    assert sizing.from_volume is None
    assert "no shuffle bytes" in sizing.why

def _without_the_driver_partition_count(job):
    """The join log with the accumulator carrying `number of partitions` removed."""
    app = _app(job)
    root = plan.read(app.plans[0])
    gone = {node.metrics.get(layout.PARTITION_COUNT) for node in plan.exchanges(root)}
    kept = {key: value for key, value in app.accumulators.items() if key not in gone}
    return type(app)(**dict(app.__dict__, accumulators=kept))

def check_the_report_renders_when_the_driver_never_reported_a_count():
    """This is the one that was a crash. The library answered None correctly in three
    places and the one function that prints them formatted it."""
    quiet = _without_the_driver_partition_count(JOIN)
    assert [s.partitions for s in layout.shuffles(quiet, plan.read(quiet.plans[0]))] == [None] * 3
    lines = layout.layout_lines(quiet)
    assert any("with no partition count reported" in line for line in lines)
    assert any("no count to divide the bytes by" in line for line in lines)
    assert not any("None" in line for line in lines)

def check_the_plan_text_count_is_reported_against_the_driver_count():
    """Two independent readings of one number. Quoting one without the other throws away
    a control that was sitting next to it."""
    lines = layout.layout_lines(_app(JOIN))
    assert sum(1 for line in lines if "plan text     agrees at 8" in line) == 3

def check_a_disagreement_between_the_two_counts_is_said_out_loud():
    """The agreeing case is the only one the logs reach, so the disagreeing branch is
    built rather than assumed. A check that only ever sees agreement grades nothing."""
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=8, partitions=4,
                             estimated=100, written=100)
    assert not shuffle.counts_agree
    app = _app(JOIN)
    lines = layout._shuffle_lines(
        shuffle, layout.Advice(sizing=layout.size(shuffle, app.slots), withheld=False,
                               why=""), app.slots)
    assert any("says 8 against the driver's 4" in line for line in lines)

def check_the_plan_text_carrying_no_count_checks_nothing():
    shuffle = layout.Shuffle(scheme="SinglePartition", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=None, partitions=4,
                             estimated=100, written=100)
    app = _app(JOIN)
    lines = layout._shuffle_lines(
        shuffle, layout.Advice(sizing=layout.size(shuffle, app.slots), withheld=False,
                               why=""), app.slots)
    assert any("carries no count, so it checks nothing" in line for line in lines)

def check_the_target_count_is_reported_with_the_rounds_it_buys():
    """The count is the advice and the rounds are what it buys. Both targets on the join
    log save rounds, and a reader given the count alone does this arithmetic themselves."""
    lines = layout.layout_lines(_app(JOIN))
    assert any("schedule after 2 rounds rather than 4 rounds" in line for line in lines)
    assert any("schedule after 1 round rather than 4 rounds" in line for line in lines)

def check_a_single_round_is_not_printed_as_a_plural():
    assert layout._rounds(1) == "1 round"
    assert layout._rounds(2) == "2 rounds"
    assert layout._rounds(0) == "0 rounds"

def check_no_target_line_means_no_rounds_line():
    """The two logs whose counts are written into the query get a refusal and no target,
    so there is nothing for a rounds line to compare."""
    for job in (SKEWED, BALANCED):
        lines = layout.layout_lines(_app(job))
        assert not any("schedule after" in line for line in lines)

# The guard that carries the benchmark. Everything below is about the one shape where the
# volume arithmetic was measured and lost, and the control for it is the join log, whose
# query is the same shape and whose key distribution is not.

def check_an_exchange_names_exactly_one_writing_stage_on_every_log():
    """The measured bytes and the stage's total of its tasks' write bytes are two readings
    of one event, so the number has to pick out one stage and no more."""
    for job in (SKEWED, BALANCED, JOIN, SKEWED_JOIN):
        app, found = _shuffles(job)
        for shuffle in found:
            stage = layout.writing_stage(app, shuffle)
            assert stage is not None, (job, shuffle.scheme)
            assert stage.total("bytes_written") == shuffle.written

def check_two_stages_writing_the_same_bytes_names_neither():
    """A coincidence this cannot tell from a match. Built, because no committed log has
    two stages agreeing to the byte and a refusal nothing reaches is a refusal nothing
    grades."""
    app = _app(JOIN)
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=8, partitions=8,
                             estimated=100, written=0)
    zeroes = [s for s in app.stages if s.total("bytes_written") == 0]
    assert len(zeroes) == 1, "the join log should have exactly one stage writing nothing"
    doubled = dataclasses.replace(app, stages=app.stages + (zeroes[0],))
    assert layout.writing_stage(doubled, shuffle) is None

def check_an_exchange_with_no_measured_bytes_names_no_stage():
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=8, partitions=8,
                             estimated=100, written=None)
    assert layout.writing_stage(_app(JOIN), shuffle) is None

def check_the_reading_stage_is_the_writing_stage_s_child():
    for job in (SKEWED, BALANCED, JOIN, SKEWED_JOIN):
        app, found = _shuffles(job)
        for shuffle in found:
            upstream = layout.writing_stage(app, shuffle)
            reader = layout.reading_stage(app, shuffle)
            assert reader is not None, (job, shuffle.scheme)
            assert upstream.stage_id in reader.parent_ids

def check_a_writing_stage_with_no_child_names_no_reading_stage():
    """The terminal stage of any log writes nothing downstream. Reached by handing the
    function a stage nothing claims as a parent."""
    app = _app(JOIN)
    last = max(app.stages, key=lambda s: s.stage_id)
    assert not any(last.stage_id in s.parent_ids for s in app.stages)
    shuffle = layout.Shuffle(scheme="hashpartitioning", origin=plan.CHOSEN_BY_SPARK,
                             changeable=True, declared=8, partitions=8, estimated=100,
                             written=last.total("bytes_written"))
    assert layout.writing_stage(app, shuffle) is last
    assert layout.reading_stage(app, shuffle) is None

def check_the_hot_key_verdict_separates_the_two_join_logs():
    """The whole guard rests on this. Same query shape, same two hash exchanges, same cut
    arithmetic. The only thing that moves is the key distribution."""
    hot = layout.hot_key(_app(SKEWED_JOIN), _hash_exchange(SKEWED_JOIN))
    assert hot.outcome == skew.SKEWED, hot
    assert hot.ratio > 40, hot.ratio
    cool = layout.hot_key(_app(JOIN), _hash_exchange(JOIN))
    assert cool.outcome == skew.EVEN, cool

def check_the_cut_is_withheld_on_the_skewed_join_and_not_on_the_join():
    app, found = _shuffles(SKEWED_JOIN)
    hashed = [s for s in found if s.scheme == layout.KEY_HASH]
    assert hashed, "the skewed join log should carry a hash exchange"
    for shuffle in hashed:
        advice = layout.advise(app, shuffle)
        assert advice.withheld, shuffle
        assert advice.sizing.target < advice.sizing.current
        assert "records_read" in advice.why

    app, found = _shuffles(JOIN)
    for shuffle in [s for s in found if s.scheme == layout.KEY_HASH]:
        advice = layout.advise(app, shuffle)
        assert not advice.withheld, shuffle

def check_a_target_equal_to_the_current_count_is_not_withheld():
    """The boundary between a cut and no cut, on the side that is not a cut.

    `advise` returns early when the target is not below the current count. A guard written
    with a strict comparison there falls through and withholds on a stage nothing was asking
    to change, and the report then prints that the volume asks for 2 rather than 2. No
    committed log lands on this, so the count is moved rather than the volume.

    The volume is what has to stay fixed. `writing_stage` finds the stage by matching the
    exchange's measured bytes against a stage total, so editing `written` here would stop any
    stage matching and the guard would go quiet for an unrelated reason.
    """
    app = _app(SKEWED_JOIN)
    hashed = _hash_exchange(SKEWED_JOIN)
    assert layout.size(hashed, app.slots).target == 2
    level = dataclasses.replace(hashed, partitions=2, declared=2)
    sizing = layout.size(level, app.slots)
    assert sizing.target == sizing.current == 2, sizing
    advice = layout.advise(app, level)
    assert not advice.withheld, advice
    assert advice.why == ""
    # The control. The same exchange at the count the log really used is a cut, the hot key
    # is found, and it is withheld. So the comparison is what separates the two cases.
    assert layout.advise(app, hashed).withheld

def check_the_guard_goes_quiet_when_no_stage_carries_the_exchange_bytes():
    """A property worth knowing, because it is a refusal that looks like a verdict.

    The hot key is found through the stage whose write bytes equal the exchange's. An
    exchange whose bytes match no stage has no reading stage, so the guard has nothing to
    say and the cut is recommended. Advice is given unless this repo can show the cut hurts,
    which is the right default and is not the same as having checked.
    """
    app = _app(SKEWED_JOIN)
    orphan = dataclasses.replace(_hash_exchange(SKEWED_JOIN), written=400000000)
    assert layout.writing_stage(app, orphan) is None
    assert layout.hot_key(app, orphan) is None
    advice = layout.advise(app, orphan)
    assert advice.sizing.target < advice.sizing.current
    assert not advice.withheld

def check_a_raise_is_never_withheld():
    """Adding partitions splits the cold keys further and the benchmark measured that
    helping, so the guard has no argument against it. Built, because no committed log
    asks for more partitions than it used."""
    app = _app(SKEWED_JOIN)
    raised = dataclasses.replace(_hash_exchange(SKEWED_JOIN), partitions=1, declared=1)
    advice = layout.advise(app, raised)
    assert advice.sizing.target > advice.sizing.current
    assert not advice.withheld

def check_round_robin_is_never_withheld_however_hot_the_next_stage_is():
    """Round robin spreads rows without reading any key, so cutting its count does not
    leave a hot key anywhere."""
    app = _app(SKEWED_JOIN)
    hashed = _hash_exchange(SKEWED_JOIN)
    robin = dataclasses.replace(hashed, scheme="RoundRobinPartitioning")
    assert layout.advise(app, hashed).withheld
    assert not layout.advise(app, robin).withheld

def check_withholding_takes_the_count_out_of_the_actionable_total():
    """Three on the skewed join log before the guard, and the two that went are the cuts.
    What is left is the broadcast candidate."""
    assert layout.actionable(_app(SKEWED_JOIN)) == 1
    assert layout.actionable(_app(JOIN)) == 3

def check_the_report_prints_the_arithmetic_it_is_refusing_to_recommend():
    """Hiding the number would read as the tool having nothing to say, and what it has to
    say is that it measured this."""
    lines = layout.layout_lines(_app(SKEWED_JOIN))
    withheld = [line for line in lines if "40 to 52 percent slower" in line]
    assert len(withheld) == 2, withheld
    assert all("asks for 2 rather than 8" in line for line in withheld)
    assert any("one key is most of the rows" in line for line in lines)
    assert not any("schedule after" in line for line in lines)

def check_the_skewed_join_key_exchange_still_measures_what_the_benchmark_measured():
    """Captured on Java 21 against a reading first taken on Java 11, five weeks apart, and
    the key exchange reproduces to the byte. A figure that survives a major runtime version
    is the strongest evidence available that the runtime is not in it.

    `python -m sjp capture --job skewed_join --out <dir> --rows 8000000` is what produced
    the log this reads."""
    shuffle = _hash_exchange(SKEWED_JOIN)
    assert shuffle.written == 88788038
    assert shuffle.partitions == 8
    assert shuffle.origin == plan.CHOSEN_BY_SPARK

def check_a_hash_the_query_left_uncounted_gets_the_advice():
    """The positive control for the guard. A hash exchange, a cut, and no hot key behind it.

    Without a log of this shape the guard could be withholding on every hash it sees and
    every check here would still pass, because the only hash exchanges in the other logs
    are either behind a hot key or already the right size.
    """
    app, found = _shuffles(BY_COLUMN)
    hashed = [s for s in found if s.scheme == layout.KEY_HASH]
    assert len(hashed) == 1, hashed
    advice = layout.advise(app, hashed[0])
    assert not advice.withheld, advice
    assert advice.sizing.target == 2
    assert advice.sizing.current == 8
    assert advice.why == ""
    assert layout.actionable(app) == 1

def check_the_uncounted_hash_would_have_been_refused_by_the_old_origin_rule():
    """What the fix changed, stated as the arithmetic rather than as the origin's name.

    The exchange reads 119303250 bytes over 8 partitions against a 67108864 advisory, so
    the volume has an opinion. Reading this origin as a number somebody typed threw that
    opinion away and printed that the config does not decide the count.
    """
    app, found = _shuffles(BY_COLUMN)
    hashed = [s for s in found if s.scheme == layout.KEY_HASH][0]
    assert hashed.origin == plan.ASKED_BY_KEY
    assert hashed.changeable is True
    assert hashed.written == 119303250
    assert hashed.partitions == 8
    # `changeable` rather than `origin`, because the flag is a field on the exchange that
    # `shuffles` fills in from the plan. Replacing the origin here would leave the flag
    # alone and the check would pass while testing nothing.
    refused = dataclasses.replace(hashed, changeable=False)
    assert layout.size(refused, app.slots).target is None
    assert "does not decide it" in layout.size(refused, app.slots).why
    # And the flag does come from the origin, which is the join the fix travels through.
    assert hashed.changeable is plan.partitioning_is_config_decided(hashed.origin)

def check_advice_is_frozen():
    advice = layout.Advice(sizing=None, withheld=False, why="")
    try:
        advice.withheld = True
    except dataclasses.FrozenInstanceError:
        return
    raise AssertionError("Advice accepted an assignment")
