"""Reading the physical plan, which is the one part of a log that is not structured data.

Spark writes a partitioning as a string. `hashpartitioning(key#4, 8), ENSURE_REQUIREMENTS`
is the whole record of who chose a partition count and how many ways it went, and nothing
in the log restates it as fields. That makes every function in `sjp.plan` a text parser
standing under a recommendation, so each one is checked against the three committed logs
rather than against an example written here.

The control for the splitting rule is the comma inside the brackets. A parser that splits
on a plain comma reads the partition count as the origin and keeps working, because both
halves are strings.
"""
import dataclasses
import os

from sjp import eventlog, plan

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "fixtures", "eventlogs")
SKEWED, BALANCED, JOIN = "skewed", "balanced", "join"


def _only_log(job):
    directory = os.path.join(LOGS, job)
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])


def _root(job):
    app = eventlog.profile(_only_log(job))
    assert app.plans, "{} carries no plan".format(job)
    return plan.read(app.plans[0])


# --- the field splitter, and the comma that breaks the naive version ---

def check_fields_split_at_bracket_depth_zero_and_not_on_every_comma():
    text = "Exchange hashpartitioning(key#4, 8), ENSURE_REQUIREMENTS"
    assert plan._fields(text) == ["Exchange hashpartitioning(key#4, 8)",
                                 "ENSURE_REQUIREMENTS"]


def check_splitting_on_every_comma_would_read_the_partition_count_as_the_origin():
    """The control. Without the depth rule the second field is ` 8)` and looks like a name.

    This is the failure the splitter exists to stop and it is silent, because a wrong
    origin is still a string and every caller downstream keeps working.
    """
    text = "Exchange hashpartitioning(key#4, 8), ENSURE_REQUIREMENTS"
    naive = [part.strip() for part in text.split(",")]
    assert naive[1] == "8)"
    assert plan._fields(text)[1] == "ENSURE_REQUIREMENTS"


def check_a_nested_bracket_does_not_end_a_field_early():
    assert plan._fields("a(b(c, d), e), f") == ["a(b(c, d), e)", "f"]


# --- partitioning, read off all three committed logs ---

def check_every_exchange_in_every_committed_log_parses_to_a_declared_count():
    for job in (SKEWED, BALANCED, JOIN):
        for node in plan.exchanges(_root(job)):
            how = plan.partitioning(node)
            assert how.declared == 8, (job, node.simple, how)
            assert how.scheme in ("hashpartitioning", "RoundRobinPartitioning"), how


def check_the_committed_logs_carry_both_origins_so_the_rule_is_exercised_both_ways():
    """Neither origin is a case only one fixture reaches.

    A changeable flag tested against logs that all answer the same way is a flag nothing
    grades. The two aggregate logs write their counts by hand and the join log lets Spark
    choose two of its three.
    """
    origins = {}
    for job in (SKEWED, BALANCED, JOIN):
        origins[job] = sorted({plan.partitioning(node).origin
                               for node in plan.exchanges(_root(job))})
    assert origins[SKEWED] == [plan.CHOSEN_BY_HAND]
    assert origins[BALANCED] == [plan.CHOSEN_BY_HAND]
    assert origins[JOIN] == [plan.CHOSEN_BY_SPARK, plan.CHOSEN_BY_HAND]


def check_only_an_ensure_requirements_exchange_reads_as_changeable():
    changeable = [plan.partitioning(node).changeable for node in plan.exchanges(_root(JOIN))]
    assert changeable.count(True) == 2
    assert changeable.count(False) == 1


def check_a_scheme_with_no_count_declares_none_rather_than_one():
    """`SinglePartition` carries no number, so filling one in would be inventing a reading.

    Nothing in the committed logs reaches this, which is why the node is built here.
    """
    node = plan.Node(name=plan.EXCHANGE, simple="Exchange SinglePartition, ENSURE_REQUIREMENTS",
                     metrics={}, children=())
    how = plan.partitioning(node)
    assert how.declared is None
    assert how.scheme == "SinglePartition"
    assert how.changeable is True


def check_the_plan_id_suffix_is_stripped_rather_than_read_as_an_origin():
    node = plan.Node(name=plan.EXCHANGE, metrics={}, children=(),
                     simple="Exchange RoundRobinPartitioning(8), REPARTITION_BY_NUM, [plan_id=26]")
    assert plan.partitioning(node).origin == plan.CHOSEN_BY_HAND


def check_partitioning_refuses_a_node_that_is_not_an_exchange():
    node = plan.Node(name="Sort", simple="Sort [key#4 ASC]", metrics={}, children=())
    try:
        plan.partitioning(node)
    except plan.UnreadablePlan:
        return
    raise AssertionError("a sort was read as an exchange")


# --- finding the joins, and the two logs that have none ---

def check_the_two_aggregate_logs_hold_no_join_at_all():
    """The reason a third log was captured. A broadcast recommender graded on these two
    would be graded on a question neither of them asks."""
    for job in (SKEWED, BALANCED):
        assert plan.joins(_root(job)) == []


def check_the_join_log_holds_one_sort_merge_join():
    found = plan.joins(_root(JOIN))
    assert [node.name for node in found] == ["SortMergeJoin"]


def check_a_join_has_two_sides_and_each_one_has_an_exchange_under_it():
    join = plan.joins(_root(JOIN))[0]
    pairs = plan.sides(join)
    assert len(pairs) == 2
    assert all(exchange is not None for _child, exchange in pairs)
    assert all(exchange.is_exchange for _child, exchange in pairs)


def check_the_two_sides_of_the_join_reach_two_different_exchanges():
    """Both sides walking down to the same node would make every size comparison equal.

    The walk takes the first exchange downward, so a defect that returned the outermost
    one would give the two sides one answer and it would look like a symmetric join.
    """
    join = plan.joins(_root(JOIN))[0]
    left, right = [exchange for _child, exchange in plan.sides(join)]
    assert left.metrics != right.metrics


def check_a_side_carrying_no_exchange_answers_none_rather_than_raising():
    """A broadcast join's build side is not shuffled at all, so this is the shape a log
    that already took the advice has."""
    node = plan.Node(name="Sort", simple="Sort", metrics={}, children=())
    assert plan.feeding_exchange(node) is None


def check_sides_refuses_a_node_that_does_not_have_two_of_them():
    node = plan.Node(name="BroadcastHashJoin", simple="", metrics={}, children=())
    try:
        plan.sides(node)
    except plan.UnreadablePlan:
        return
    raise AssertionError("a join with no children was read as having two sides")


# --- metrics are addressed by id, because the names are not unique ---

def check_a_node_reporting_one_metric_name_twice_is_refused():
    info = {"nodeName": "Exchange", "simpleString": "", "children": [],
            "metrics": [{"name": "data size", "accumulatorId": 1, "metricType": "size"},
                        {"name": "data size", "accumulatorId": 2, "metricType": "size"}]}
    try:
        plan.read(info)
    except plan.UnreadablePlan:
        return
    raise AssertionError("two ids under one name were read as one metric")


def check_walk_reaches_every_node_and_not_only_the_first_child():
    root = _root(JOIN)
    names = [node.name for node in plan.walk(root)]
    assert names.count("Exchange") == 3
    assert "SortMergeJoin" in names
    assert names[0] == root.name


# --- what the mutation pass asked for ---

def _refuses_an_edit(record, attribute):
    try:
        setattr(record, attribute, "changed")
    except dataclasses.FrozenInstanceError:
        return True
    return False


def check_the_plan_records_are_frozen():
    """Read once and passed around. A caller that could edit a partitioning would be
    editing the log's own account of what Spark did."""
    assert _refuses_an_edit(plan.Node(name="x", simple="", metrics={}, children=()), "name")
    assert _refuses_an_edit(plan.Partitioning(scheme="x", declared=1, origin="y"), "scheme")


def check_an_exchange_with_no_origin_field_reads_as_an_empty_origin():
    """Spark truncates a long simple string, so a node carrying only its partitioning is
    a shape the reader has to survive rather than index past the end of."""
    node = plan.Node(name=plan.EXCHANGE, simple="Exchange SinglePartition",
                     metrics={}, children=())
    how = plan.partitioning(node)
    assert how.origin == ""
    assert how.changeable is False
    assert how.scheme == "SinglePartition"


def check_a_truncated_partitioning_does_not_read_as_a_count():
    """An opening bracket with no closing one. The two tests have to both hold, because
    either one alone reads a cut off string as a declared number."""
    node = plan.Node(name=plan.EXCHANGE, metrics={}, children=(),
                     simple="Exchange hashpartitioning(key#4, 8, ENSURE_REQUIREMENTS")
    how = plan.partitioning(node)
    assert how.declared is None
    # The whole unparsed text is the scheme on purpose. Reading `hashpartitioning` out of
    # a string that was cut off would be a name this could not see the end of, and it
    # reads identically to a name it did.
    assert how.scheme == "hashpartitioning(key#4, 8, ENSURE_REQUIREMENTS"


def check_an_empty_argument_list_declares_no_count_rather_than_raising():
    node = plan.Node(name=plan.EXCHANGE, metrics={}, children=(),
                     simple="Exchange somepartitioning(), ENSURE_REQUIREMENTS")
    how = plan.partitioning(node)
    assert how.scheme == "somepartitioning"
    assert how.declared is None
