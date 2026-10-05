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

def check_the_lines_name_the_origin_of_every_decision():
    """Once per decision rather than once per exchange.

    The join log carries three exchanges and two decisions. `ENSURE_REQUIREMENTS` is
    named once because one count covers both of its exchanges, and the block says how
    many it covers so the reader is not left to count byte lines.
    """
    app = _app(JOIN)
    lines = layout.layout_lines(app)
    assert sum(1 for line in lines if plan.CHOSEN_BY_SPARK in line) == 1
    assert sum(1 for line in lines if plan.CHOSEN_BY_HAND in line) == 2
    assert any("exchanges     2 into stage 3" in line for line in lines)

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
    assert len(schedule) == 2
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

def check_the_join_log_counts_one_partition_target_and_one_broadcast():
    """Pinned as an arithmetic total rather than as a truthy flag. An exit status cannot
    tell two from one, so a rule that double counted would look correct from outside.

    It did double count. Both hash exchanges are the same `spark.sql.shuffle.partitions`
    and each was sized and counted on its own, so this read 3.
    """
    app = _app(JOIN)
    assert layout.actionable(app) == 2

def check_an_advisory_the_current_count_already_satisfies_drops_that_decision():
    """Moving the advisory number has to move the count, or the count is not computed
    from it. 21000000 bytes a partition over 8 partitions is 168000000, which is above the
    163275083 the join's two exchanges wrote between them, so the one partition target
    stops being advice and the broadcast is what is left."""
    app = _app(JOIN)
    assert layout.actionable(app, advisory=21000000) == 1

def check_an_advisory_above_the_whole_shuffle_still_leaves_the_slot_floor():
    """The volume stops arguing and the slots do not. Eight partitions on two slots is
    four rounds for a shuffle that wants one partition, so the count is still advice and
    so is the broadcast."""
    app = _app(JOIN)
    assert layout.actionable(app, advisory=10 ** 12) == 2

def check_a_threshold_below_the_small_side_leaves_only_the_partition_target():
    app = _app(JOIN)
    assert layout.actionable(app, threshold=1) == 1

def check_the_count_is_printed_rather_than_left_as_an_exit_status():
    assert any("2 worth changing" in line for line in layout.layout_lines(_app(JOIN)))
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
    assert sum(1 for line in lines if "plan text     agrees at 8" in line) == 2

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
    """The count is the advice and the rounds are what it buys, and a reader given the
    count alone does this arithmetic themselves.

    One target on the join log now, and the round count it saves is the pair's. The second
    line this used to assert read `1 round rather than 4 rounds`, which came off sizing the
    2401 byte side of the join on its own. Nobody could ever have set that count without
    unsetting the other, so the rounds it promised were not available either.
    """
    lines = layout.layout_lines(_app(JOIN))
    assert any("schedule after 2 rounds rather than 4 rounds" in line for line in lines)
    assert sum(1 for line in lines if "schedule after" in line) == 1

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
    assert layout.actionable(_app(JOIN)) == 2

def check_the_report_prints_the_arithmetic_it_is_refusing_to_recommend():
    """Hiding the number would read as the tool having nothing to say, and what it has to
    say is that it measured this."""
    lines = layout.layout_lines(_app(SKEWED_JOIN))
    withheld = [line for line in lines if "40 to 52 percent slower" in line]
    assert len(withheld) == 1, withheld
    assert all("asks for 2 rather than 8" in line for line in withheld)
    assert any("one key is most of the rows" in line for line in lines)
    assert not any("schedule after" in line for line in lines)

def check_the_skewed_join_key_exchange_still_measures_what_the_benchmark_measured():
    """Two benchmark schedules two days apart, and the key exchange reproduces to the byte.

    This log was captured on Java 21 and never on Java 11, so it is not evidence about a
    runtime version. The log that carries that evidence is `skewed`, whose key exchange
    writes 130,788,590 bytes under Java 11 and the same number re-measured under Java 21.

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
    refused = dataclasses.replace(hashed, changeable=False,
                                  origin=plan.CHOSEN_BY_HAND)
    assert refused.origin_kind == plan.QUERY
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

# One partition count, however many exchanges it controls
#
# `spark.sql.shuffle.partitions` is one number and a sort merge join shuffles both sides
# by the same key into the same number of pieces. Sizing the sides apart gave the join log
# two targets for that one number, and they were not the same target.

ALL_JOBS = ("skewed", "balanced", "join", "skewed_join", "by_column",
            "by_column_at_5", "small", "wide")

def _decisions(job):
    app = _app(job)
    return app, [d for info in app.plans
                 for d in layout.decisions(app, plan.read(info))]

def _with_one_count_changed(job, value):
    """The join log with one hash exchange's driver reported partition count altered.

    Built rather than captured. Two exchanges the config decides that feed one stage
    cannot really disagree about the count, which is the premise the grouping rests on,
    so the only way to exercise the premise failing is to break it on purpose.
    """
    app = _app(job)
    root = plan.read(app.plans[0])
    hashed = [node for node in plan.exchanges(root)
              if plan.partitioning(node).scheme == layout.KEY_HASH]
    assert len(hashed) == 2, hashed
    acc_id = hashed[1].metrics[layout.PARTITION_COUNT]
    changed = dict(app.accumulators)
    changed[acc_id] = str(value)
    return type(app)(**dict(app.__dict__, accumulators=changed))

def check_the_two_sides_of_one_join_are_one_decision():
    for job in ("join", "skewed_join"):
        app, found = _decisions(job)
        assert len(found) == 2, (job, found)
        shared = [d for d in found if len(d.shuffles) > 1]
        assert len(shared) == 1, (job, shared)
        assert shared[0].stage_id == 3, (job, shared[0])
        assert all(s.scheme == layout.KEY_HASH for s in shared[0].shuffles)
        assert all(s.origin == plan.CHOSEN_BY_SPARK for s in shared[0].shuffles)

def check_sizing_each_side_apart_gives_two_targets_for_one_number():
    """The defect, as the two numbers rather than as the grouping.

    Both exchanges come from `spark.sql.shuffle.partitions` and feed stage 3, so the
    report was asking for 4 and for 2 at once. Taking the second would undo the first.
    """
    app, found = _shuffles("join")
    hashed = [s for s in found if s.scheme == layout.KEY_HASH]
    assert len(hashed) == 2
    apart = sorted(layout.size(s, app.slots).target for s in hashed)
    assert apart == [2, 4], apart
    together = layout.size_of(tuple(hashed), app.slots)
    assert together.target == 4, together

def check_a_decision_sizes_the_sum_of_the_exchanges_it_controls():
    """Recomputed from the two byte counts rather than asserted as one figure."""
    app, found = _decisions("join")
    shared = [d for d in found if len(d.shuffles) > 1][0]
    written = sorted(s.written for s in shared.shuffles)
    assert written == [2401, 163272682], written
    assert layout.volume(shared.shuffles) == sum(written) == 163275083
    sizing = layout.size_of(shared.shuffles, app.slots)
    assert sizing.from_volume == math.ceil(sum(written) / layout.ADVISORY_BYTES) == 3
    assert str(sum(written)) in sizing.why

def check_a_count_the_query_names_never_shares_a_decision():
    """The control on what makes two exchanges one count.

    Two `repartition` arguments feeding one stage are two numbers typed in two places, so
    they stay apart however the plan arranges them. Asserted by moving the origin on an
    exchange that does group, because the discriminator is the origin and nothing else.
    """
    app = _app("join")
    hashed = _hash_exchange("join")
    assert layout._decides_with(app, hashed) == 3
    for origin in (plan.CHOSEN_BY_HAND, "REPARTITION_BY_SOMETHING_NEW", ""):
        moved = dataclasses.replace(hashed, origin=origin)
        assert layout._decides_with(app, moved) is None, origin

def check_an_exchange_whose_reading_stage_is_unknown_stands_alone():
    """Grouping on a stage nothing identified would be grouping on a guess, and the two
    exchanges it merged would lose their separate byte counts for no checkable reason."""
    app = _app("join")
    orphan = dataclasses.replace(_hash_exchange("join"), written=400000000)
    assert layout.reading_stage(app, orphan) is None
    assert layout._decides_with(app, orphan) is None

def check_a_decision_whose_members_disagree_on_the_count_comes_back_apart():
    """The premise is that the count is shared. A disagreement falsifies it, so the two
    exchanges print separately rather than being sized against a number neither has."""
    app = _with_one_count_changed("join", 4)
    found = layout.decisions(app, plan.read(app.plans[0]))
    assert len(found) == 3, found
    assert all(len(d.shuffles) == 1 for d in found), found
    assert all(d.stage_id is None for d in found), found
    # The control. The same application with the count left alone does group.
    assert any(len(d.shuffles) == 2 for d in _decisions("join")[1])

def check_the_aggregate_logs_group_nothing():
    """Six of the eight committed logs have no join, and grouping must not touch them.

    Without this the fix could be collapsing exchanges all over the tree and every check
    above would still pass, because the two join logs are the only ones it reads.
    """
    for job in ("skewed", "balanced", "by_column", "by_column_at_5", "small", "wide"):
        app, found = _decisions(job)
        assert all(len(d.shuffles) == 1 for d in found), job
        assert all(d.stage_id is None for d in found), job

def check_every_exchange_keeps_its_own_byte_line():
    """The report is a reading of the log as well as a recommendation. One count is one
    decision and two exchanges are still two measurements."""
    lines = layout.layout_lines(_app("join"))
    assert any("bytes         163272682 measured" in line for line in lines)
    assert any("bytes         2401 measured" in line for line in lines)
    assert any("bytes         163275083 measured in total" in line for line in lines)

def check_the_withheld_cut_is_one_line_and_names_its_stage_once():
    """What the duplication actually looked like.

    The reason names the hot key on the stage the count divides. Both sides feed that one
    stage, so printing it per exchange stated one finding twice and hung the large side's
    44.2 ratio next to a 2401 byte exchange that has no key distribution to speak of.
    """
    lines = layout.layout_lines(_app("skewed_join"))
    reasons = [line for line in lines if "one key is most of the rows" in line]
    assert len(reasons) == 1, reasons
    assert "stage 3 reads 44.2 on records_read" in reasons[0]
    assert sum(1 for line in lines if "40 to 52 percent slower" in line) == 1

def check_per_partition_divides_the_whole_decision():
    app, found = _decisions("skewed_join")
    shared = [d for d in found if len(d.shuffles) > 1][0]
    assert layout._per_partition(shared.shuffles) == 88790439 / 8
    lines = layout.layout_lines(app)
    assert any("per partition 11098805 measured bytes each" in line for line in lines)

def check_a_decision_missing_one_exchange_s_bytes_refuses_rather_than_summing_the_rest():
    """A sum that drops a missing term is a smaller number and not an unknown one, and
    small is the direction that reads as a confident answer."""
    present = _hash_exchange("join")
    absent = dataclasses.replace(present, written=None)
    assert layout.volume((present, absent)) is None
    sizing = layout.size_of((present, absent), 2)
    assert sizing.target is None
    assert sizing.from_volume is None
    assert "no shuffle bytes" in sizing.why
    assert layout._per_partition((present, absent)) is None

# The origin list, and the fourth origin nobody has seen
#
# `plan.CONFIG_DECIDED` is two names and membership used to be the whole test, so an
# origin on neither list came back as a count the config does not decide. That sentence is
# a claim about the query, and it is what `REPARTITION_BY_COL` got for two days.

def check_every_origin_in_every_committed_log_is_one_this_repo_reads():
    """The completeness report, which is the most a log can give.

    Nothing in an event log states which origins Spark can write, so there is no check to
    run against the data. What there is instead is this: every origin the committed logs
    carry is named, and the count is published rather than implied.
    """
    seen = {}
    for job in ALL_JOBS:
        _app_, found = _decisions(job)
        for decision in found:
            for shuffle in decision.shuffles:
                seen[shuffle.origin] = seen.get(shuffle.origin, 0) + 1
    assert sorted(seen) == sorted(plan.CONFIG_DECIDED + plan.QUERY_DECIDED), seen
    assert set(seen) == set(plan.KNOWN_ORIGINS), seen
    assert all(plan.origin_kind(origin) != plan.UNRECOGNISED for origin in seen)
    # Three names over the eight logs, and the one the fix added is the rarest of them.
    assert seen[plan.ASKED_BY_KEY] == 2, seen
    assert seen[plan.CHOSEN_BY_SPARK] == 4, seen

def check_an_unrecognised_origin_is_not_reported_as_a_query_argument():
    """The sentence this split exists to stop.

    A gap in this repo printed as a fact about somebody's query. The refusal still refuses,
    which is the right default, and it no longer says why in words that are not true.
    """
    unknown = dataclasses.replace(_hash_exchange("join"),
                                  origin="REBALANCE_PARTITIONS_BY_NONE", changeable=False)
    assert unknown.origin_kind == plan.UNRECOGNISED
    why = layout.size(unknown, 2).why
    assert "whether the config decides this count is unknown" in why, why
    assert "argument in the query" not in why, why
    assert layout.size(unknown, 2).target is None

def check_an_exchange_with_no_origin_in_the_plan_text_says_that():
    """`partitioning` leaves the origin empty when the node's simple string carries one
    field. Naming the empty string back at the reader is not an explanation."""
    blank = dataclasses.replace(_hash_exchange("join"), origin="", changeable=False)
    assert blank.origin_kind == plan.UNRECOGNISED
    why = layout.size(blank, 2).why
    assert why.startswith("the plan text names no origin"), why

def check_the_flag_and_the_origin_disagreeing_is_said_out_loud():
    """`changeable` is what the branch is taken on and `origin` is what it came from.

    They are separate fields because a copy is where two readings of one fact drift apart.
    A message built from the origin alone reports the drift as a fact about the query.
    """
    drifted = dataclasses.replace(_hash_exchange("join"), changeable=False)
    assert drifted.origin_kind == plan.CONFIG
    why = layout.size(drifted, 2).why
    assert "the two readings of one fact disagree" in why, why
    assert "argument in the query" not in why, why

def check_an_unrecognised_origin_is_not_config_decided():
    """The quiet direction is the dangerous one here. An origin nobody recognises must not
    fall into the list that produces advice either."""
    for origin in ("REBALANCE_PARTITIONS_BY_COL", "", "ensure_requirements"):
        assert not plan.partitioning_is_config_decided(origin), origin
        assert plan.origin_kind(origin) == plan.UNRECOGNISED, origin

def check_a_decision_is_frozen():
    """`Advice` has this and `Decision` did not, and a mutant turning it off survived."""
    decision = layout.Decision(shuffles=(), stage_id=None)
    try:
        decision.stage_id = 3
    except dataclasses.FrozenInstanceError:
        return
    raise AssertionError("Decision accepted an assignment")

def check_a_lone_exchange_prints_one_byte_line_and_no_total():
    """A total over one number is the number, and restating it reads as a second
    measurement.

    This is the check that was missing. A mutant widening the guard on the total line put
    a `measured in total` line under every lone exchange in six of the eight logs, and all
    380 checks passed with it, because nothing here asserted the shape of the block rather
    than the presence of its parts.
    """
    for job in ("balanced", "skewed", "small", "by_column", "by_column_at_5"):
        lines = layout.layout_lines(_app(job))
        assert not any("measured in total" in line for line in lines), job
        assert sum(1 for line in lines if "      bytes " in line) == 2, job
    # Three byte lines on the join log. One per exchange and one for the pair.
    for job in ("join", "skewed_join"):
        lines = layout.layout_lines(_app(job))
        assert sum(1 for line in lines if "      bytes " in line) == 4, job
        assert sum(1 for line in lines if "measured in total" in line) == 1, job

def check_a_group_of_one_never_reaches_the_disagreement_branch():
    """Why the length guard in front of it was deleted rather than kept.

    One exchange carries one count, so the set of counts holds one element whatever that
    count is, including None. Both readings of a length comparison in front of this are
    the same function, which is a site nothing can grade.
    """
    for partitions in (8, None, 0):
        lone = dataclasses.replace(_hash_exchange("join"), partitions=partitions)
        assert len({s.partitions for s in (lone,)}) == 1, partitions
