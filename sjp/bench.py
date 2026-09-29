"""Running one job at two partition counts and saying whether the difference is real.

Everything else in this profiler reads a log somebody else produced. This module is the
one place that compares two logs, and comparing is where the arithmetic goes wrong in ways
a single reading never does.

Three rules are built into it rather than left to whoever runs it.

The arms do not run in a fixed order. A process pays for its own start, and whichever arm
goes first in every pass is charged for whatever the machine had not warmed up yet. The
schedule rotates, so each arm is first exactly once.

The first pass is thrown away. It is the one that finds the files cold.

A comparison this size cannot reach a small p value however the numbers land, so the floor
is computed from the pass counts before any run happens. Four passes an arm is the
smallest schedule that can return anything under 0.05, and three passes an arm cannot, so
a three pass run is refused rather than reported.

What it does not do is decide that two arms are the same. Failing to separate four
readings is not evidence that a difference is absent, and `Verdict.decided` is false in
both cases for that reason. The word the report uses is undecided.
"""
import itertools
import math
import statistics
from dataclasses import dataclass

from sjp import eventlog


class NotComparable(Exception):
    """Raised when two arms did not do the same work, so no comparison means anything."""


@dataclass(frozen=True)
class Arm:
    """One configuration under test. `partitions` is what the session is told."""
    name: str
    job: str
    partitions: int


@dataclass(frozen=True)
class StageTime:
    """What one stage of one run cost, and enough of its shape to check the comparison."""
    stage_id: int
    tasks: int
    wall: int
    median_duration: float
    largest_duration: int
    records_read: int

    @property
    def spread(self):
        """Largest task over median task, or None when the median is zero.

        Same convention as `sjp.model.Stage.spread` and for the same reason. A stage whose
        median task did nothing has no ratio, and zero is the wrong answer in the worst
        available direction.
        """
        if not self.median_duration:
            return None
        return self.largest_duration / self.median_duration


@dataclass(frozen=True)
class Pass:
    """One run of one arm."""
    arm: str
    path: str
    app_wall: int
    stages: tuple

    @property
    def stage_wall_total(self):
        """The stages added up.

        Not the same number as `app_wall`, which carries session start and stop and the
        gaps between stages. Both are reported because a change that moves one and not the
        other is a change outside the work.
        """
        return sum(stage.wall for stage in self.stages)


def read_pass(arm, path):
    """Build a `Pass` from one event log."""
    app = eventlog.profile(path)
    stages = tuple(
        StageTime(stage_id=stage.stage_id, tasks=len(stage.tasks), wall=stage.wall_time,
                  median_duration=stage.median("duration"),
                  largest_duration=stage.largest("duration"),
                  records_read=stage.total("records_read"))
        for stage in app.stages)
    return Pass(arm=arm, path=path, app_wall=app.wall_time, stages=stages)


def work_shape(one):
    """What has to match between two arms for their times to be about the same work.

    Deliberately not the task counts. Those differ between arms by design, since the
    partition count is the thing being changed. What must not differ is the number of
    stages and the rows that crossed them. An arm that moved fewer rows got a different
    query, and its being faster would say nothing.

    Sorted, and that is not tidiness. A join submits both of its sides at once and
    whichever the scheduler takes first gets the lower stage id, so two runs of one job
    put the same stages in a different order. Reading the row counts positionally makes
    that look like two different queries. It refused sixteen passes of one job before the
    sort went in, which is the check finding a defect in itself rather than in the data.

    The cost is that two genuinely different plans sharing one multiset of row counts
    would compare. Nothing here distinguishes them, and the stage count is the only other
    guard. A shape that needs more than this needs the plan, not the stage list.
    """
    return (len(one.stages), tuple(sorted(stage.records_read for stage in one.stages)))


def check_comparable(passes):
    """Raise unless every pass did the same work. Returns the shape they agreed on."""
    shapes = {work_shape(one) for one in passes}
    if len(shapes) != 1:
        raise NotComparable(
            "{} different work shapes across {} passes, so there is nothing to "
            "compare".format(len(shapes), len(passes)))
    return shapes.pop()


def schedule(arms, passes):
    """The order the runs happen in, rotating so no arm is always first.

    Returns the warmup run separately from the measured ones. The warmup is one run of the
    first arm, and its number is thrown away rather than folded in.
    """
    if passes < 1:
        raise ValueError("{} passes, so there is nothing to measure".format(passes))
    if not arms:
        raise ValueError("no arms, so there is nothing to compare")
    order = []
    for index in range(passes):
        rotated = arms[index % len(arms):] + arms[:index % len(arms)]
        order.extend(rotated)
    return arms[0], order


def p_floor(left, right):
    """The smallest two sided p an exact permutation test on these counts could return.

    Computed before the runs rather than after, because a comparison that cannot reach the
    threshold it will be read against is a comparison not worth paying for.
    """
    if left < 1 or right < 1:
        raise ValueError("{} and {} observations, so there is nothing to permute".format(
            left, right))
    return 2.0 / math.comb(left + right, left)


def permutation_p(left, right):
    """Exact two sided permutation p on the difference of means.

    Every split of the pooled values is enumerated, so this is the real answer for these
    counts rather than a sample of it. Eight observations is seventy splits.
    """
    pooled = list(left) + list(right)
    size = len(left)
    observed = abs(statistics.fmean(left) - statistics.fmean(right))
    total = extreme = 0
    for combination in itertools.combinations(range(len(pooled)), size):
        chosen = set(combination)
        a = [pooled[index] for index in combination]
        b = [pooled[index] for index in range(len(pooled)) if index not in chosen]
        total += 1
        # The tolerance is there because the observed split is one of the enumerated ones
        # and floating point subtraction must not let it miss itself.
        if abs(statistics.fmean(a) - statistics.fmean(b)) >= observed - 1e-9:
            extreme += 1
    return extreme / total


@dataclass(frozen=True)
class Verdict:
    """Whether a difference between two arms was separated, and by how much."""
    decided: bool
    p: float
    floor: float
    left_mean: float
    right_mean: float
    why: str

    @property
    def ratio(self):
        """Right over left, or None when the left arm measured zero."""
        if not self.left_mean:
            return None
        return self.right_mean / self.left_mean


def verdict(left, right, alpha=0.05):
    """Compare two arms' readings.

    The underpowered case comes first and returns no p at all. Printing a p beside a note
    that it could never have been small enough invites a reader to take whichever half
    suits them, which is a thing this program has already shipped once.
    """
    floor = p_floor(len(left), len(right))
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    if floor > alpha:
        return Verdict(decided=False, p=None, floor=floor, left_mean=left_mean,
                       right_mean=right_mean,
                       why="{} and {} passes put the smallest reachable p at {:.4f}, "
                           "which is above {}. No result here could be significant, so "
                           "none is offered".format(len(left), len(right), floor, alpha))
    found = permutation_p(left, right)
    if found <= alpha:
        why = "p {:.4f} against a floor of {:.4f}".format(found, floor)
    else:
        why = "p {:.4f} against a floor of {:.4f}, so undecided rather than equal".format(
            found, floor)
    return Verdict(decided=found <= alpha, p=found, floor=floor, left_mean=left_mean,
                   right_mean=right_mean, why=why)


def readings(passes, arm, pick):
    """One number per pass for one arm, in the order the passes ran."""
    return [pick(one) for one in passes if one.arm == arm]


def worst_stage(one):
    """The stage of a run with the largest task duration spread, or None.

    The stage a skew report would name. Read it beside `carrying_stage` and not on its
    own. Cutting the partition count of a job whose rows sit on one key drives this
    ranking onto a stage that does no work, because the hot partition and the median
    partition become the same partition and the ratio collapses while the task it is
    about gets slower. Measured at 8 partitions and at 2 on one job: the spread fell from
    8.54 to 1.60 and the largest task rose from 7,418 ms to 10,916 ms.
    """
    ranked = [stage for stage in one.stages if stage.spread is not None]
    return max(ranked, key=lambda stage: stage.spread) if ranked else None


def carrying_stage(one):
    """The stage that read the most rows, or None when nothing read any.

    A stable handle on the stage a benchmark is about, which the stage id is not. A join
    submits both sides at once and whichever the scheduler takes first gets the lower id,
    so addressing the stage by its number picks a different stage between two runs of one
    job. The rows crossing a stage do not move when the partition count does.
    """
    ranked = [stage for stage in one.stages if stage.records_read]
    return max(ranked, key=lambda stage: stage.records_read) if ranked else None
