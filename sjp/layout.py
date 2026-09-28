"""Partition counts and broadcast candidates, and which number answers which question.

An event log holds two numbers that both read like the size of a relation, and the whole
of this module turns on them being different measurements.

`data size` is an estimate. It is the row count multiplied by a width taken from the
schema. Every exchange in this repo divides to a whole number of bytes per row. The range
gives 16. The payload column takes it to 104 and the small side of the join is 24. So it
cannot see the data. The skewed job and the balanced job differ in one expression and both
report 832,000,000 on the same exchange.

`shuffle bytes written` is measured. It is what went to disk, serialized and compressed,
and it moves with the data. The same exchange writes 130,788,590 bytes on the skewed job
and 163,272,682 on the balanced one.

The factor between the two runs 1.9892, 3.0441, 5.0958 and 6.3614 across the four
exchanges this repo has captured. So there is no converting one into the other, and using
the wrong one is not a rounding error.

**Partition sizing takes the measured number.** Adaptive execution compares its advisory
size against the map output sizes, which are the compressed bytes.

**A broadcast decision takes the estimate.** That is what Spark's own planner compares
against `spark.sql.autoBroadcastJoinThreshold`, so a tool asking whether a join could have
been a broadcast has to ask the question the planner asked.

The second thing this module refuses to do is recommend a count nobody can change. An
exchange records who chose its partition count. `ENSURE_REQUIREMENTS` means Spark took it
from `spark.sql.shuffle.partitions`. `REPARTITION_BY_NUM` means it is an argument in the
query. Both committed logs are the second kind, so on both of them the recommendation this
module exists for correctly has nothing to say.
"""
import math
from dataclasses import dataclass

from sjp import plan

# Spark's documented default for spark.sql.adaptive.advisoryPartitionSizeInBytes. It is
# Spark's number rather than one chosen here, which is the only reason a default is
# defensible at all. A caller who knows their cluster passes their own.
ADVISORY_BYTES = 64 * 1024 * 1024

# Spark's documented default for spark.sql.autoBroadcastJoinThreshold.
BROADCAST_BYTES = 10 * 1024 * 1024

MEASURED_SIZE = "shuffle bytes written"
ESTIMATED_SIZE = "data size"
PARTITION_COUNT = "number of partitions"


@dataclass(frozen=True)
class Shuffle:
    """One exchange, with both of its sizes and who chose its partition count.

    `estimated` and `written` are kept as separate fields with separate names rather than
    one size, because the two get confused precisely when they are close enough for the
    confusion to survive.
    """
    scheme: str
    origin: str
    changeable: bool
    declared: int
    partitions: int
    estimated: int
    written: int

    @property
    def per_partition(self):
        """Measured bytes per partition, or None when the log did not report a count."""
        if not self.partitions:
            return None
        return self.written / self.partitions

    @property
    def counts_agree(self):
        """Whether the count in the plan text and the count the driver reported match.

        Two independent readings of one number. The plan writes it into the partitioning
        expression and the driver reports it as a metric. Nothing forces them to agree, so
        a report quoting one of them should have compared it against the other.
        """
        return self.declared == self.partitions


def waves(partitions, slots):
    """How many rounds the tasks run in, and how full the last one is.

    A partition count is not judged only on the size of a partition. Eight partitions on
    two slots is four full rounds. Nine is five rounds whose last one leaves half the
    slots idle for the length of one task, and the tail is invisible in any figure about
    bytes.
    """
    if slots <= 0:
        raise ValueError("{} slots, so nothing can be scheduled".format(slots))
    if partitions <= 0:
        raise ValueError("{} partitions, so there is nothing to schedule".format(partitions))
    full, tail = divmod(partitions, slots)
    return (full + (1 if tail else 0), tail)


@dataclass(frozen=True)
class Sizing:
    """What a partition count would be if it came from the measured volume.

    `target` is None when the count is not the config's to change. That is a refusal and
    not a missing number, which is why it is not filled in with the count already in use.
    """
    current: int
    target: int
    from_volume: int
    current_waves: tuple
    target_waves: tuple
    why: str


def size(shuffle, slots, advisory=ADVISORY_BYTES):
    """The partition count the measured volume and the slot count imply.

    The floor is the slot count. A shuffle split into fewer partitions than there are
    slots leaves cores idle for the whole stage however well sized each partition is, and
    no amount of volume argues for that.

    Two inputs can be absent and each one is a refusal rather than a number. `SinglePartition`
    carries no count in the plan text, so a log where the driver also reported none leaves
    nothing to divide or schedule. An exchange whose measured bytes never reached the driver
    leaves nothing to size against the advisory. Both raised a `TypeError` out of `divmod`
    before, on a shape no committed log reaches and every real job does.
    """
    current = shuffle.partitions or shuffle.declared
    if current is None:
        return Sizing(current=None, target=None, from_volume=None, current_waves=None,
                      target_waves=None,
                      why="no count in the plan text and none from the driver, so there "
                          "is nothing to size")
    if shuffle.written is None:
        return Sizing(current=current, target=None, from_volume=None,
                      current_waves=waves(current, slots), target_waves=None,
                      why="the driver reported no shuffle bytes, so the volume this would "
                          "be sized against is missing")
    from_volume = max(1, math.ceil(shuffle.written / advisory))
    if not shuffle.changeable:
        return Sizing(current=current, target=None, from_volume=from_volume,
                      current_waves=waves(current, slots), target_waves=None,
                      why="{} is an argument in the query, so the config does not "
                          "decide it".format(shuffle.origin))

    target = max(from_volume, slots)
    # Rounded up to a whole number of rounds. A count that leaves a part filled last wave
    # pays a whole task's wall time for whatever fraction of the slots it uses.
    target = int(math.ceil(target / slots) * slots)
    if target == current:
        why = "{} bytes over {} partitions is already inside the advisory size".format(
            shuffle.written, current)
    else:
        why = "{} bytes at an advisory {} wants {}, and the {} slots round it to {}".format(
            shuffle.written, advisory, from_volume, slots, target)
    return Sizing(current=current, target=target, from_volume=from_volume,
                  current_waves=waves(current, slots), target_waves=waves(target, slots),
                  why=why)


@dataclass(frozen=True)
class Candidate:
    """One side of one join, and what broadcasting it would have removed.

    `saved` is the other side's measured shuffle. That is the number worth reporting,
    because the argument for a broadcast is never the size of the thing broadcast. It is
    the shuffle on the other side that stops happening.
    """
    join: str
    estimated: int
    threshold: int
    saved: int
    worth_it: bool
    why: str


def _metric(app, node, name):
    """One of a plan node's metrics by accumulator id, or None.

    By id and never by name, because a name is not unique inside one log.
    """
    if node is None:
        return None
    acc_id = node.metrics.get(name)
    if acc_id is None:
        return None
    value = app.accumulator(acc_id)
    return None if value is None else int(value)


def shuffles(app, root):
    """Every exchange in one plan as a `Shuffle`, in plan order."""
    found = []
    for node in plan.exchanges(root):
        how = plan.partitioning(node)
        found.append(Shuffle(
            scheme=how.scheme, origin=how.origin, changeable=how.changeable,
            declared=how.declared, partitions=_metric(app, node, PARTITION_COUNT),
            estimated=_metric(app, node, ESTIMATED_SIZE),
            written=_metric(app, node, MEASURED_SIZE)))
    return found


def candidates(app, root, threshold=BROADCAST_BYTES):
    """Every join side small enough to broadcast, with the shuffle it would remove.

    A side with no exchange under it was not shuffled, so there is nothing to save and it
    is not a candidate whatever its size. That is the case a broadcast join is already in,
    and reporting it would be recommending what already happened.
    """
    found = []
    for join in plan.joins(root):
        pairs = plan.sides(join)
        sizes = [_metric(app, exchange, ESTIMATED_SIZE) for _child, exchange in pairs]
        written = [_metric(app, exchange, MEASURED_SIZE) for _child, exchange in pairs]
        for index, (_child, exchange) in enumerate(pairs):
            estimated = sizes[index]
            if exchange is None or estimated is None:
                continue
            other = written[1 - index]
            under = estimated < threshold
            if under and other:
                why = "{} estimated against a {} threshold, and the other side shuffled {}".format(
                    estimated, threshold, other)
            elif under:
                why = "{} estimated against a {} threshold, and nothing measures what the " \
                      "other side shuffled".format(estimated, threshold)
            else:
                why = "{} estimated, which is over the {} threshold".format(
                    estimated, threshold)
            found.append(Candidate(join=join.name, estimated=estimated,
                                   threshold=threshold, saved=other,
                                   worth_it=bool(under and other), why=why))
    return found


def actionable(app, advisory=ADVISORY_BYTES, threshold=BROADCAST_BYTES):
    """How many things in this log this command has an opinion about.

    Lives here rather than in the command that exits on it. A rule that decides a status
    inside a report function cannot be reached by a mutation pass pointed at the library,
    so it is a decision nothing grades.

    A broadcast candidate counts. So does a partition count the config decides and the
    measured volume disagrees with. An exchange whose count is written into the query
    counts for nothing however far from the advisory size it sits, because there is no
    advice to give about it.
    """
    found = 0
    for info in app.plans:
        root = plan.read(info)
        found += sum(1 for candidate in candidates(app, root, threshold)
                     if candidate.worth_it)
        for shuffle in shuffles(app, root):
            sizing = size(shuffle, app.slots, advisory)
            if sizing.target is not None and sizing.target != sizing.current:
                found += 1
    return found


def _shuffle_lines(shuffle, sizing, slots):
    """The block one exchange prints.

    Its own function because most of the branches below are about a shape no
    committed log reaches. Left inline they could only be reached through a whole
    application, which is how a branch ends up graded on nothing.
    """
    lines = []
    if shuffle.partitions is None:
        lines.append("  {} with no partition count reported  chosen by {}".format(
            shuffle.scheme, shuffle.origin))
    else:
        lines.append("  {} into {} partitions  chosen by {}".format(
            shuffle.scheme, shuffle.partitions, shuffle.origin))
    lines.append("      {:<13} {} measured  {} estimated".format(
        "bytes", shuffle.written, shuffle.estimated))
    # The plan text and the driver both carry the count and nothing makes them
    # agree. Reading one without the other is quoting a number that had a control
    # sitting next to it.
    if shuffle.declared is None:
        lines.append("      {:<13} carries no count, so it checks nothing".format(
            "plan text"))
    elif shuffle.partitions is None:
        lines.append("      {:<13} says {}, and the driver reported nothing to "
                     "check it against".format("plan text", shuffle.declared))
    elif shuffle.counts_agree:
        lines.append("      {:<13} agrees at {}".format("plan text", shuffle.declared))
    else:
        lines.append("      {:<13} says {} against the driver's {}".format(
            "plan text", shuffle.declared, shuffle.partitions))
    if shuffle.per_partition is None:
        lines.append("      {:<13} unknown. no count to divide the bytes by".format(
            "per partition"))
    else:
        lines.append("      {:<13} {:.0f} measured bytes each".format(
            "per partition", shuffle.per_partition))
    if sizing.current_waves is None:
        lines.append("      {:<13} unknown. {}".format("schedule", sizing.why))
    else:
        lines.append("      {:<13} {} rounds of {} slots, {} in the last".format(
            "schedule", sizing.current_waves[0], slots,
            sizing.current_waves[1] or slots))
    if sizing.target is None:
        lines.append("      {:<13} none. {}".format("partitions", sizing.why))
    else:
        lines.append("      {:<13} {} rather than {}. {}".format(
            "partitions", sizing.target, sizing.current, sizing.why))
        # The count is the advice and the rounds are what it buys. A reader given
        # the count alone has to do this arithmetic to know whether it is worth it.
        lines.append("      {:<13} {} rather than {}".format(
            "schedule after", _rounds(sizing.target_waves[0]),
            _rounds(sizing.current_waves[0])))
    return lines


def _rounds(count):
    """A round count with its noun. One round is not "1 rounds"."""
    return "{} round{}".format(count, "" if count == 1 else "s")


def layout_lines(app, advisory=ADVISORY_BYTES, threshold=BROADCAST_BYTES):
    """The exchanges, their sizing and any broadcast candidate, for one application."""
    lines = ["{}  {}  {} cores  {} cpus a task  {} slots".format(
        app.app_id, app.name, app.cores, app.cpus_per_task, app.slots)]
    if not app.plans:
        lines.append("  no physical plan in this log, so no exchange can be named")
        return lines

    for info in app.plans:
        root = plan.read(info)
        for shuffle in shuffles(app, root):
            lines.extend(_shuffle_lines(
                shuffle, size(shuffle, app.slots, advisory), app.slots))
        for found in candidates(app, root, threshold):
            verdict = "broadcast it" if found.worth_it else "leave it shuffled"
            lines.append("  {} side of {}  {}".format(
                "small" if found.worth_it else "large", found.join, verdict))
            lines.append("      {:<13} {}".format("why", found.why))
        if not plan.joins(root):
            lines.append("  no join in this plan, so no broadcast has anything to replace")
    # Said out loud rather than left as an exit status, because a count a reader can see
    # is a count a reader can disagree with.
    lines.append("  {} worth changing".format(actionable(app, advisory, threshold)))
    return lines
