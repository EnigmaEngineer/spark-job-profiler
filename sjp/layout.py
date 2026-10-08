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

from sjp import plan, skew

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
    def origin_kind(self):
        """Which of plan's three readings this exchange's origin gets.

        `changeable` is a boolean and has nowhere to put an origin nobody here has seen,
        so it collapses that case into the query one. The refusal printed beside it then
        reads as a statement about the query, which is the shape of the defect that cost
        `REPARTITION_BY_COL` two days.
        """
        return plan.origin_kind(self.origin)

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


def volume(group):
    """The measured bytes one partition count divides.

    The sum over every exchange that count controls. A sort merge join shuffles both sides
    into the same stage and each partition of that stage reads its slice of both, so the
    volume a count is sized against is the pair rather than either side. Sizing the small
    side on its own is what produced a second target for a number that can only hold one.

    None when any member's bytes never reached the driver. A sum that silently drops a
    missing term is a smaller number rather than an unknown one, and small is the
    direction that reads as a confident answer.
    """
    if any(shuffle.written is None for shuffle in group):
        return None
    return sum(shuffle.written for shuffle in group)


def _refusal(shuffle):
    """Why a count is not this config's to change, from each thing that can make it so.

    An origin on neither of plan's lists is a gap in this repo rather than a number
    somebody typed, so it does not get the sentence about the query. Reading the second as
    the first is the whole of the defect this split exists for.

    The last case is the two fields disagreeing. `changeable` is what the branch above was
    taken on and `origin` is what it was derived from, and they are separate fields because
    a copy is where two readings of one fact drift apart. A message built from the origin
    alone states the drift as a fact about the query, which is a confident wrong sentence
    where the honest one is short.
    """
    if shuffle.origin_kind == plan.QUERY:
        return "{} is an argument in the query, so the config does not decide it".format(
            shuffle.origin)
    if shuffle.origin_kind == plan.UNRECOGNISED:
        if shuffle.origin:
            return ("{} is not an origin this tool has a reading for, so whether the "
                    "config decides this count is unknown".format(shuffle.origin))
        return ("the plan text names no origin, so whether the config decides this "
                "count is unknown")
    return ("{} is an origin the config decides and this exchange is flagged otherwise, "
            "so the two readings of one fact disagree".format(shuffle.origin))


def size_of(group, slots, advisory=ADVISORY_BYTES):
    """The partition count the measured volume and the slot count imply.

    The floor is the slot count. A shuffle split into fewer partitions than there are
    slots leaves cores idle for the whole stage however well sized each partition is, and
    no amount of volume argues for that.

    Two inputs can be absent and each one is a refusal rather than a number. `SinglePartition`
    carries no count in the plan text, so a log where the driver also reported none leaves
    nothing to divide or schedule. An exchange whose measured bytes never reached the driver
    leaves nothing to size against the advisory. Both raised a `TypeError` out of `divmod`
    before, on a shape no committed log reaches and every real job does.

    Takes a group because a count is a property of the decision and not of one exchange.
    The members agree on the count by the time they are grouped, so the first one carries
    it for all of them.
    """
    first = group[0]
    current = first.partitions or first.declared
    if current is None:
        return Sizing(current=None, target=None, from_volume=None, current_waves=None,
                      target_waves=None,
                      why="no count in the plan text and none from the driver, so there "
                          "is nothing to size")
    written = volume(group)
    if written is None:
        return Sizing(current=current, target=None, from_volume=None,
                      current_waves=waves(current, slots), target_waves=None,
                      why="the driver reported no shuffle bytes, so the volume this would "
                          "be sized against is missing")
    from_volume = max(1, math.ceil(written / advisory))
    if not first.changeable:
        return Sizing(current=current, target=None, from_volume=from_volume,
                      current_waves=waves(current, slots), target_waves=None,
                      why=_refusal(first))

    target = max(from_volume, slots)
    # Rounded up to a whole number of rounds. The argument for this used to be that a
    # part filled last round pays a whole task's wall time for a fraction of the slots.
    # That was never measured and it did not survive being measured. Running one job at 3
    # and at 4 on two slots, both counts take two rounds, so the rounding removed no round
    # at all and the stage wall did not move.
    #
    # The replacement argument was that the extra partition splits the keys one more way
    # and cuts the largest task. That was separated on one schedule at 0.8470 and
    # undecided on a second at 0.9102, so it is a direction rather than an established
    # result. The rounding is kept because nothing measured argues for removing it,
    # which is a weaker reason than this comment used to give.
    target = int(math.ceil(target / slots) * slots)
    if target == current:
        why = "{} bytes over {} partitions is already inside the advisory size".format(
            written, current)
    else:
        why = "{} bytes at an advisory {} wants {}, and the {} slots round it to {}".format(
            written, advisory, from_volume, slots, target)
    return Sizing(current=current, target=target, from_volume=from_volume,
                  current_waves=waves(current, slots), target_waves=waves(target, slots),
                  why=why)


def size(shuffle, slots, advisory=ADVISORY_BYTES):
    """One exchange sized on its own, which is the one member case of `size_of`.

    Kept because most exchanges are their own decision and because sizing a side in
    isolation is the comparison that shows what grouping changed.
    """
    return size_of((shuffle,), slots, advisory)


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


# What Spark calls the partitioning when it hashes a key expression. Round robin is the
# other scheme these logs carry and it spreads rows without looking at any key, so the
# guard below has nothing to say about it.
KEY_HASH = "hashpartitioning"

# The one quantity read for a hot key. A hash decides which rows land in which partition,
# so a stage whose tasks read very different row counts is reporting the key distribution
# and nothing else. A duration spread would answer the same question through a slow
# executor as well as through a hot key.
KEY_METRIC = "records_read"


@dataclass(frozen=True)
class Advice:
    """A sizing, and whether this repo has measured that taking it makes things worse."""
    sizing: object
    withheld: bool
    why: str


def writing_stage(app, shuffle):
    """The stage that wrote this exchange, found by its measured bytes.

    An exchange's measured bytes and the sum of its stage's per task shuffle write bytes
    are two readings of one event, so the number identifies the stage. Matched on the
    number rather than on the stage id because a join submits both sides at once and which
    side gets the lower id depends on which one the scheduler took first.

    None when the number does not pick out exactly one stage. Two stages writing the same
    byte count is a coincidence this cannot tell from a match, and a guess there is worse
    than a refusal.
    """
    if shuffle.written is None:
        return None
    found = [stage for stage in app.stages
             if stage.total("bytes_written") == shuffle.written]
    return found[0] if len(found) == 1 else None


def reading_stage(app, shuffle):
    """The stage whose work this exchange's partition count divides.

    The count on an exchange decides how many pieces the next stage runs in, so that is
    the stage a recommendation about the count is about. It is the writing stage's child.
    None when the writing stage is unknown or does not have exactly one child.
    """
    upstream = writing_stage(app, shuffle)
    if upstream is None:
        return None
    found = [stage for stage in app.stages if upstream.stage_id in stage.parent_ids]
    return found[0] if len(found) == 1 else None


def hot_key(app, shuffle, threshold=skew.DEFAULT_THRESHOLD, floors=None):
    """The row count verdict on the stage this exchange feeds, or None.

    None means the stage could not be named, which is different from a stage that was
    named and came back even.
    """
    stage = reading_stage(app, shuffle)
    if stage is None:
        return None
    return skew.judge(stage, KEY_METRIC, threshold, floors)


def advise(app, shuffle, advisory=ADVISORY_BYTES, threshold=skew.DEFAULT_THRESHOLD,
           floors=None):
    """The sizing, plus the one case where this repo measured the sizing to be wrong.

    `size` is arithmetic over a total and a total cannot see a distribution. There is one
    shape where that arithmetic was benchmarked and it lost, on four separate schedules
    now. Cutting the count on a hash partitioning whose next stage is carrying a hot key
    left the stage wall total undecided on every one of them. It made the largest task
    slower every time and separated every time. The two four pass schedules read 1.3979
    and 1.5170 against a floor of 0.0286. The two six pass schedules of 2026-10-08 read
    1.3887 and 1.3537 against a floor of 0.0022. A hash sends one key to one partition at
    any count, so a cut moves every other key onto fewer partitions and the hot task
    carries more of them.

    This is the only comparison in the benchmark that has separated on every schedule,
    which is the reason the guard rests on it rather than on any of the others. The two
    wall clock totals have each separated on one schedule and not on the next.

    Only a cut is withheld. Raising the count on such a stage splits the cold keys further
    and the same benchmark measured that helping, so the guard has no reason to block it.

    Withheld rather than reversed. Nothing here measures what the right count is on a
    stage like that. The honest answer is a different key or a salt and an event log
    carries no key distribution to propose one from.
    """
    return advise_decision(app, Decision(shuffles=(shuffle,), stage_id=None),
                           advisory, threshold, floors)


@dataclass(frozen=True)
class Decision:
    """The exchanges one partition count controls, and the stage they feed.

    `stage_id` is None when nothing grouped this exchange with another. That is every
    exchange on every committed log except the two under each sort merge join.
    """
    shuffles: tuple
    stage_id: int


def _decides_with(app, shuffle):
    """The stage that says which count this exchange belongs to, or None for its own.

    Only a config decided origin can share a count. Two `repartition` arguments feeding
    one stage are two numbers typed in two places, so they are two decisions however the
    plan arranges them, and an origin nobody here recognises is not known to be either.

    None also when the reading stage cannot be named. Grouping on a stage nothing
    identified would be grouping on a guess, and the two exchanges it merged would lose
    their separate byte counts for no reason anybody could check.
    """
    if shuffle.origin_kind != plan.CONFIG:
        return None
    stage = reading_stage(app, shuffle)
    return None if stage is None else stage.stage_id


def decisions(app, root):
    """One plan's exchanges, grouped into the partition counts they really are.

    `spark.sql.shuffle.partitions` is one number. A sort merge join partitions both sides
    by the same key into the same number of pieces, so the two exchanges under it are one
    decision, and sizing them apart gave the join log two targets for a number that can
    only hold one. The large side asked for 4 and the small side asked for 2.

    A group whose members disagree on the count comes back apart. The count being shared
    is the premise the grouping rests on, so a disagreement falsifies it, and printing
    both separately is the answer that does not hide the contradiction.
    """
    groups = []
    where = {}
    for shuffle in shuffles(app, root):
        key = _decides_with(app, shuffle)
        if key is not None and key in where:
            groups[where[key]][1].append(shuffle)
            continue
        if key is not None:
            where[key] = len(groups)
        groups.append((key, [shuffle]))
    found = []
    for key, group in groups:
        # No length guard. A group of one holds one count, so the set below holds one
        # element and this is never true of it. A `len(group) > 1` in front of it was a
        # mutation site nothing could grade, because both readings of it are the same
        # function.
        if len({shuffle.partitions for shuffle in group}) != 1:
            found.extend(Decision(shuffles=(shuffle,), stage_id=None)
                         for shuffle in group)
            continue
        found.append(Decision(shuffles=tuple(group),
                              stage_id=key if len(group) > 1 else None))
    return found


def advise_decision(app, decision, advisory=ADVISORY_BYTES,
                    threshold=skew.DEFAULT_THRESHOLD, floors=None):
    """`advise` over a whole decision, which is where the guard belongs.

    Withholding once per decision rather than once per exchange. The reason the guard
    prints names the hot key on the stage the count divides, and both sides of a join feed
    one stage, so printing it twice stated one finding as two and attached the large
    side's hot key to a 2,401 byte exchange that has no keys to speak of.

    Every member has to be a hash for the guard to apply. Its mechanism is that a hash
    sends one key to one partition at any count, which says nothing about a round robin
    that never looks at a key.
    """
    group = decision.shuffles
    sizing = size_of(group, app.slots, advisory)
    if sizing.target is None or sizing.target >= sizing.current:
        return Advice(sizing=sizing, withheld=False, why="")
    if any(shuffle.scheme != KEY_HASH for shuffle in group):
        return Advice(sizing=sizing, withheld=False, why="")
    verdict = hot_key(app, group[0], threshold, floors)
    if verdict is None or verdict.outcome != skew.SKEWED:
        return Advice(sizing=sizing, withheld=False, why="")
    return Advice(
        sizing=sizing, withheld=True,
        why="stage {} reads {} on {}, so one key is most of the rows and a hash keeps it "
            "on one partition at any count".format(
                verdict.stage_id, skew.plain(verdict.ratio), KEY_METRIC))


def actionable(app, advisory=ADVISORY_BYTES, threshold=BROADCAST_BYTES):
    """How many things in this log this command has an opinion about.

    Lives here rather than in the command that exits on it. A rule that decides a status
    inside a report function cannot be reached by a mutation pass pointed at the library,
    so it is a decision nothing grades.

    A broadcast candidate counts. So does a partition count the config decides and the
    measured volume disagrees with. An exchange whose count is written into the query
    counts for nothing however far from the advisory size it sits, because there is no
    advice to give about it. Neither does a cut `advise` withheld, because a number the
    report is arguing against is not a thing it is asking anybody to change.
    """
    found = 0
    for info in app.plans:
        root = plan.read(info)
        found += sum(1 for candidate in candidates(app, root, threshold)
                     if candidate.worth_it)
        for decision in decisions(app, root):
            advice = advise_decision(app, decision, advisory)
            sizing = advice.sizing
            if advice.withheld:
                continue
            if sizing.target is not None and sizing.target != sizing.current:
                found += 1
    return found


def _per_partition(group):
    """Measured bytes a partition of this decision carries, or None.

    The decision's whole volume over its count. `Shuffle.per_partition` is the one
    exchange reading of the same thing and it divides a byte count that may not be there,
    so this answers None where that raises.
    """
    if not group[0].partitions:
        return None
    written = volume(group)
    return None if written is None else written / group[0].partitions


def _decision_lines(decision, advice, slots):
    """The block one partition count prints.

    Its own function because most of the branches below are about a shape no
    committed log reaches. Left inline they could only be reached through a whole
    application, which is how a branch ends up graded on nothing.

    A decision controlling more than one exchange keeps a byte line for each, because
    those are separate measurements and the report is a reading of the log as well as a
    recommendation. What it prints once is the count, the schedule and the advice, which
    are the parts there is only one of.
    """
    group = decision.shuffles
    shuffle = group[0]
    sizing = advice.sizing
    lines = []
    if shuffle.partitions is None:
        lines.append("  {} with no partition count reported  chosen by {}".format(
            shuffle.scheme, shuffle.origin))
    else:
        lines.append("  {} into {} partitions  chosen by {}".format(
            shuffle.scheme, shuffle.partitions, shuffle.origin))
    if len(group) > 1:
        lines.append("      {:<13} {} into stage {}, and one count decides them".format(
            "exchanges", len(group), decision.stage_id))
    for member in group:
        lines.append("      {:<13} {} measured  {} estimated".format(
            "bytes", member.written, member.estimated))
    if len(group) > 1:
        lines.append("      {:<13} {} measured in total, which is what the count "
                     "divides".format("bytes", volume(group)))
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
    each = _per_partition(group)
    if each is None:
        lines.append("      {:<13} unknown. no count to divide the bytes by".format(
            "per partition"))
    else:
        lines.append("      {:<13} {:.0f} measured bytes each".format(
            "per partition", each))
    if sizing.current_waves is None:
        lines.append("      {:<13} unknown. {}".format("schedule", sizing.why))
    else:
        lines.append("      {:<13} {} rounds of {} slots, {} in the last".format(
            "schedule", sizing.current_waves[0], slots,
            sizing.current_waves[1] or slots))
    if sizing.target is None:
        lines.append("      {:<13} none. {}".format("partitions", sizing.why))
    elif advice.withheld:
        # The arithmetic is still shown. Hiding it would make the refusal look like the
        # tool had nothing to say, and what it has to say is that it measured this.
        lines.append("      {:<13} none. the volume asks for {} rather than {}, and "
                     "taking that cut was measured here to leave the stage wall alone "
                     "and make the largest task 40 to 52 percent slower".format(
                         "partitions", sizing.target, sizing.current))
        lines.append("      {:<13} {}".format("why", advice.why))
    else:
        lines.append("      {:<13} {} rather than {}. {}".format(
            "partitions", sizing.target, sizing.current, sizing.why))
        # The count is the advice and the rounds are what it buys. A reader given
        # the count alone has to do this arithmetic to know whether it is worth it.
        lines.append("      {:<13} {} rather than {}".format(
            "schedule after", _rounds(sizing.target_waves[0]),
            _rounds(sizing.current_waves[0])))
    return lines


def _shuffle_lines(shuffle, advice, slots):
    """The block one lone exchange prints, which is the one member case."""
    return _decision_lines(Decision(shuffles=(shuffle,), stage_id=None), advice, slots)


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
        for decision in decisions(app, root):
            lines.extend(_decision_lines(
                decision, advise_decision(app, decision, advisory), app.slots))
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
