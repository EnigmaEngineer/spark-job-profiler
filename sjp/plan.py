"""The physical plan, which is where a join and an exchange are named.

Everything else this profiler reads is a measurement off a stage or a task. The plan is
different. It is the only record of what Spark decided to do, and two questions this
module exists for cannot be answered without it.

Which shuffles are joins. A stage boundary looks the same whether it feeds an aggregate or
a join, so nothing in the stage model can tell a shuffle that a broadcast would remove from
one that nothing would.

Where a partition count came from. An exchange carries its origin, and the two origins in
this repo mean opposite things to a recommendation. `REPARTITION_BY_NUM` is a number
somebody typed into a `repartition` call. `ENSURE_REQUIREMENTS` is a number Spark took from
`spark.sql.shuffle.partitions`. Advising a config change on the first one is advice that
does nothing.

A plan node names its metrics by accumulator id and carries no values. The values are on
the stages, except for the three that are only ever reported by the driver. `Application`
holds both in one map, so reading a metric here is a lookup by id and never a match by
name. That matters because names repeat: `number of output rows` appears twice on one
stage of both committed logs.
"""
import re
from dataclasses import dataclass

# Spark writes `Exchange <partitioning>, <origin>, [plan_id=57]`. The origin is the second
# comma separated field at bracket depth zero, and the partitioning is the first.
_PLAN_ID = re.compile(r",\s*\[plan_id=\d+\]\s*$")

# A join is an operator whose name ends in Join. `CartesianProduct` is Spark's one join
# that does not, and it is deliberately not on this list. A cartesian product is not a
# join a broadcast fixes, so a recommender finding one would have nothing to say about it.
JOIN_SUFFIX = "Join"

EXCHANGE = "Exchange"

# What the exchange's origin means for whether a count can be changed at all.
CHOSEN_BY_SPARK = "ENSURE_REQUIREMENTS"
CHOSEN_BY_HAND = "REPARTITION_BY_NUM"


class UnreadablePlan(Exception):
    """Raised when a plan node does not hold what this module needs to read it."""


@dataclass(frozen=True)
class Node:
    """One operator. `metrics` maps a metric name to the accumulator carrying its value."""
    name: str
    simple: str
    metrics: dict
    children: tuple

    @property
    def is_exchange(self):
        return self.name == EXCHANGE

    @property
    def is_join(self):
        return self.name.endswith(JOIN_SUFFIX)


def read(info):
    """One `sparkPlanInfo` object as a tree.

    A metric name appearing twice on one node raises rather than keeping whichever came
    last. Nothing in either committed log does it, and a node where it happened would
    hand a caller one of two ids with no way to know which.
    """
    metrics = {}
    for metric in info.get("metrics", []):
        name = metric["name"]
        if name in metrics:
            raise UnreadablePlan("{} reports {} twice".format(
                info.get("nodeName"), name))
        metrics[name] = metric["accumulatorId"]
    return Node(
        name=info.get("nodeName", ""),
        simple=info.get("simpleString", ""),
        metrics=metrics,
        children=tuple(read(child) for child in info.get("children", [])),
    )


def walk(node):
    """Every node, parents before children."""
    yield node
    for child in node.children:
        for found in walk(child):
            yield found


def _fields(text):
    """The comma separated fields of a node's simple string, at bracket depth zero.

    Splitting on a plain comma is wrong here. `hashpartitioning(key#4, 8)` carries one
    inside its own brackets, and a parser that splits on it reads the partition count as
    the origin.
    """
    fields = []
    depth = 0
    current = ""
    for character in text:
        if character in "([":
            depth += 1
        elif character in ")]":
            depth -= 1
        if character == "," and depth == 0:
            fields.append(current.strip())
            current = ""
            continue
        current += character
    fields.append(current.strip())
    return [field for field in fields if field]


@dataclass(frozen=True)
class Partitioning:
    """How an exchange partitioned, how many ways, and who chose the number.

    `declared` is None when the scheme carries no count. `SinglePartition` is the case,
    and it is one partition by definition rather than by a number in the text, so filling
    it in would be this module inventing a reading of a string that does not have one.
    """
    scheme: str
    declared: int
    origin: str

    @property
    def changeable(self):
        """Whether `spark.sql.shuffle.partitions` is what decides this count."""
        return self.origin == CHOSEN_BY_SPARK


def partitioning(node):
    """The partitioning of one exchange.

    Reads the text rather than a field, because Spark does not write the partitioning as
    structured data anywhere in the log. The fragility of that is the reason every part of
    it is checked against the committed logs in `tests/test_plan.py` rather than trusted.
    """
    if not node.is_exchange:
        raise UnreadablePlan("{} is not an exchange".format(node.name))
    text = _PLAN_ID.sub("", node.simple)
    fields = _fields(text)
    if not fields:
        raise UnreadablePlan("exchange with no partitioning in {!r}".format(node.simple))
    head = fields[0]
    head = head[len(EXCHANGE):].strip() if head.startswith(EXCHANGE) else head
    origin = fields[1] if len(fields) > 1 else ""

    scheme, declared = head, None
    if head.endswith(")") and "(" in head:
        scheme = head[:head.index("(")]
        arguments = _fields(head[head.index("(") + 1:-1])
        if arguments and arguments[-1].isdigit():
            declared = int(arguments[-1])
    return Partitioning(scheme=scheme, declared=declared, origin=origin)


def exchanges(node):
    """Every exchange in the plan, parents before children."""
    return [found for found in walk(node) if found.is_exchange]


def joins(node):
    """Every join in the plan, parents before children."""
    return [found for found in walk(node) if found.is_join]


def feeding_exchange(node):
    """The first exchange at or under `node`, or None.

    A join side is a `Sort` over an `InputAdapter` over the exchange that shuffled it, and
    the depth of that chain is a property of whether the operators fused. Taking the first
    exchange downward rather than a fixed number of hops is what stops this breaking the
    moment codegen groups the stages differently.
    """
    for found in walk(node):
        if found.is_exchange:
            return found
    return None


def sides(join):
    """The two sides of a join, each with the exchange that shuffled it.

    Returns a list of pairs rather than a left and a right, because the words left and
    right are about the query and this module is about which side is smaller.
    """
    if len(join.children) != 2:
        raise UnreadablePlan("{} has {} children".format(join.name, len(join.children)))
    return [(child, feeding_exchange(child)) for child in join.children]
