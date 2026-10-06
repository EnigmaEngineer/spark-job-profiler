"""Deciding whether a stage skewed, from the largest task measured against the median.

The arithmetic is in `sjp.model`. This module holds the only threshold in the repo, and
more of it is about when a threshold means nothing than about the threshold itself.

Three things were measured before the first one was written. Two changed its shape.

**A median relative ratio is unreachable below three tasks.** With one task the largest
value is the median, so the ratio is exactly 1.0 whatever the task did. With two tasks the
largest value is one of the two the median averages, which makes the ratio `2b / (a + b)`
and caps it below 2 however extreme the pair. Both committed logs have a two task stage.
So a stage with fewer than three tasks gets no answer here rather than a comfortable one.

**A zero median is not evenness.** When more than half the tasks moved nothing and one
moved everything there is no denominator, and that is the most extreme skew a stage can
reach rather than the least. It is not hypothetical. On the skewed log's grouping stage the
median spill is 0 and one task spilled 620,755,808 bytes, so the metric this whole project
exists to find is the one with no denominator.

**The separating range was measured.** Over both logs the largest ratio on a healthy stage
is 1.7407 and the smallest on the skewed stage is 14.7246, across records read and duration
and executor run time. Any threshold between them behaves identically on every stage this
repo has, so the default below is a round number with slack on both sides rather than a
value tuned until the two fixtures came out right.

The 1.7407 is worth one more line. The first version of this docstring said 1.7378, which is
the largest duration ratio, because that was the column being read at the time. Executor run
time on the same stage is higher. The check in `tests/test_skew.py` searches all three
metrics and it is what found the wrong number.

**The metric set is not a choice this module gets to make.** It was three names for one day
and that was long enough to be wrong. `model.QUANTITIES` is the set now, derived from the
kind declared on every field of the task record. The separating range above is still quoted
over the three metrics it was measured on, because widening it is a measurement and not a
rename. `tests/test_skew.py` recomputes it.
"""
import hashlib
import math
from dataclasses import dataclass

from sjp import model

SKEWED = "skewed"
EVEN = "even"
UNDECIDED = "undecided"
OUTCOMES = (SKEWED, EVEN, UNDECIDED)

# Inside the measured separating range of 1.7407 to 14.7246 and not at either end of it.
DEFAULT_THRESHOLD = 4.0

# Below this a median relative ratio cannot reach any threshold worth setting. The reason
# is in the module docstring and `tests/test_skew.py` measures it rather than trusting it.
MIN_TASKS = 3

# How large the biggest task has to be before a ratio over it is worth printing, per unit.
#
# This exists because widening the metric set broke the control. The balanced log, which is
# the fixture whose job is to have nothing wrong with it, reported one skewed stage. It was
# a result serialization time of 8 milliseconds against a median of zero. The zero median
# rule cannot tell that from the 620,755,808 byte spill it was written for, because the
# ratio is unbounded in both cases and a ratio carries no unit.
#
# The bracket for each kind whose two ends a committed log has really populated. The
# largest value that is noise and the smallest that is real. `tests/test_skew.py`
# re-derives both ends off the logs rather than trusting the pair written here.
#
# The small end needed a second kind of fixture. A pathology captured at one row count only
# ever populates one end, so these were `None` for nine days and `None` meant no floor. The
# small log is the same skewed distribution over 600 rows, where the hot key still takes 85
# percent of the rows and the biggest task reads 7,449 bytes and 518 records.
BRACKETS = {
    model.BYTES: (7449, 59191004),
    model.COUNT: (518, 6932663),
}


def floor_from(bracket):
    """The geometric mean of a bracket, to one significant figure.

    A geometric mean rather than an arithmetic one because these are quantities people
    compare by ratio. The byte bracket spans 7,946 times and its arithmetic middle sits
    within a factor of two of the real end, which is a floor that would start deciding
    against real evidence the first time a job came in slightly smaller.

    One significant figure because the precision is not there. Rounding to it leaves
    roughly equal multiplicative slack on each side, 93 times the noise end against an
    84th of the real end for bytes, and 115 times either way for counts.
    """
    low, high = bracket
    if not 0 < low < high:
        raise ValueError("bracket {} is not a noise end below a real end".format(bracket))
    middle = math.sqrt(low * high)
    return int(round(middle, -math.floor(math.log10(middle))))


# What the millisecond bracket really is once the 600 row log is read with the other seven.
# A noise end ABOVE a real end, so `floor_from` refuses it and there is no number to place.
#
# 556 is the small log's grouping stage, one task running 556 milliseconds against a median
# of 114.5 over 600 rows. 71 is the skewed log's garbage collection pause at eight million
# rows, which is the smallest value anything here calls real.
MILLIS_BRACKET_AS_MEASURED = (556, 71)

# So the millisecond floor stays the hand placed number it has always been.
#
# It was put inside a bracket of 29 to 71 read off the two logs captured at eight million
# rows, and `floor_from` returns 50 for that pair, which is the number that shipped weeks
# before any of this was written. That agreement is worth one line and it is not evidence
# the bracket was right. The small log falsifies it.
#
# The reason the method works for two kinds and not the third is that bytes and records
# scale with the data and a task's wall time does not. A 600 row task still pays for a JVM,
# a launch and a serialisation, so the millisecond floor under a trivial job is set by fixed
# cost rather than by the work. A magnitude floor cannot separate that from a real pause and
# the fix is a different mechanism rather than a different number. See `docs/adr-0006`.
MILLIS_FLOOR = 50

# Derived where the evidence allows it, so a floor cannot drift from the bracket it came
# from, and hand placed where it does not.
FLOORS = {kind: floor_from(bracket) for kind, bracket in BRACKETS.items()}
FLOORS[model.MILLIS] = MILLIS_FLOOR

# Every quantity a task carries, because the alternative was a list of three I chose.
#
# It used to be records read and wall time and memory spill. Those are the three a person
# names first and that is the reason they were wrong. A stage skewed only on disk spill or
# on remote bytes read got no verdict, and the command's exit status is computed over
# whatever was judged, so a real skew outside the three read as a clean run. The set now
# comes from the kind declared on each field of `model.Task`. Judging a metric nobody
# cares about costs one line of output. Skipping one costs the answer.
DEFAULT_METRICS = model.QUANTITIES


class MetricRefused(Exception):
    """A caller asked for a verdict on something a median relative ratio cannot measure."""


def refuse_unmeasured(metrics):
    """Raise unless every name is a quantity a ratio means something for.

    This lives in the module that decides rather than in the command that prints, because
    the command printing a refusal while `scan` still answered would leave the defect in
    place for anything importing this.

    `--metric launch_time` was accepted for eleven days. Measured on 2026-10-03 it read
    1.0000 on stage 1 and stage 2 of the small log, off values near 1790970484000, which
    is what a ratio over two clock readings is by construction. `--metric executor_id`
    was worse. It reached `statistics.median` and raised a TypeError.

    Every refused name is reported rather than the first, because a caller who fixes one
    and runs again to find a second has been told half the answer twice.
    """
    refusals = []
    for metric in metrics:
        try:
            kind = model.kind_of(metric)
        except model.UnexpectedLog:
            refusals.append("refused {}. not a task quantity".format(metric))
            continue
        if kind not in model.MEASURED:
            refusals.append("refused {}. a declared {} rather than a measured quantity".format(
                metric, kind))
    if refusals:
        raise MetricRefused("\n".join(refusals))


def metric_set_id(metrics):
    """A short stable name for exactly the set of metrics a scan judged.

    `sjp skew` exits 1 when anything skewed and the skew is counted over whatever was
    judged, so the status cannot be read without knowing the set. A hand written version
    cannot carry that. The set is derived from the kind declared on each field of
    `model.Task`, which means a field added there widens it with nobody touching this
    module, and a hand written number would still read the same afterwards. A digest of
    the names cannot drift from the names.

    Sorted before hashing. Judging the same fourteen metrics in a different order asks the
    same question and should not read as a different contract.

    Eight hex characters, which separates the handful of sets this tool will ever have.
    It is an identity rather than a defence against somebody building a collision.
    """
    joined = "\n".join(sorted(metrics))
    return "{}:{}".format(len(metrics),
                          hashlib.sha256(joined.encode("utf-8")).hexdigest()[:8])


def metric_set_label(metrics):
    """Whether a scan judged the set this tool chose or a set the caller named.

    Derived by comparison rather than passed down as a flag, so fourteen `--metric` flags
    naming the default set are reported as the default set. The id is the same either way
    and the label would otherwise disagree with it.
    """
    return "default" if tuple(metrics) == tuple(DEFAULT_METRICS) else "selected"


# Every default metric set this tool has shipped, oldest first, against the version that
# shipped it.
#
# `3:5f2a4f77` is records read and duration and memory spilled, the three names I chose.
# Read out of commit 4a5d5c0 rather than out of memory. `14:e575855f` is the derived set.
# The same log hands a shell a different status under the two, which is the only reason
# this list is worth keeping.
#
# `tests/test_skew.py` asserts the live set is the last entry here and that its version is
# `sjp.__version__`. Widening the set and not recording it fails the suite.
METRIC_SET_HISTORY = (
    ("0.1.0", "3:5f2a4f77"),
    ("0.2.0", "14:e575855f"),
)


def exit_status(verdicts):
    """The status `sjp skew` returns to a shell.

    Here rather than in the command so the line that prints the status and the value the
    command returns cannot be computed twice and disagree. Same reason `skew_lines` takes
    the verdicts instead of scanning a second time.

    The two values are `cli.OK` and `cli.FOUND` and a check asserts that. This module does
    not import `sjp.cli`, because the arithmetic here should not depend on an entry point.
    """
    return 1 if counts(verdicts)[SKEWED] else 0


def plain(value):
    """A measurement as digits a person can read back against the log.

    `{:g}` turns 6932663 into 6.93266e+06, which drops three digits of a number whose
    whole point is that it is larger than the others. A median is a float that is usually
    whole and sometimes ends in .5, so it gets a decimal only when it needs one.
    """
    if float(value).is_integer():
        return str(int(value))
    return "{:.1f}".format(value)


@dataclass(frozen=True)
class Verdict:
    """One answer about one stage and one metric, carrying what it was decided on.

    `ratio` is None when nothing was divided and `math.inf` when the divisor was zero and
    the numerator was not. Neither is a number a caller should compare against a
    threshold, which is why `outcome` is the field to read and `ratio` is evidence.
    """
    stage_id: int
    metric: str
    outcome: str
    tasks: int
    median: float
    largest: int
    ratio: float
    why: str

    @property
    def unbounded(self):
        return self.ratio is not None and math.isinf(self.ratio)


def floor_for(metric, floors=None):
    """The magnitude below which `metric` is not worth a verdict, or None for no floor."""
    floors = FLOORS if floors is None else floors
    return floors.get(model.kind_of(metric))


def judge(stage, metric, threshold=DEFAULT_THRESHOLD, floors=None):
    """Whether `stage` skewed on `metric`, or why that question has no answer here.

    Deliberately does not call `Stage.spread`. That property answers None on a zero median
    so nothing can compare it to a threshold by accident, and the case it refuses to
    summarise is a case this function has to name.
    """
    if threshold <= 1:
        # A ratio of 1.0 is what a single task and a perfectly even stage both read, so a
        # threshold at or below it calls everything skewed and means nothing.
        raise ValueError("threshold {} is at or below an even stage's ratio".format(threshold))
    # Again here rather than only in `scan`, because a caller reaching one stage directly
    # is the caller least likely to have checked.
    #
    # `model.kind_of` walks twenty fields and the wide log calls this 672 times. Measured
    # 2026-10-06 over 20 runs, the scan goes from a median of 2.9 ms to 4.2 ms, so the
    # duplicate guard is 1.3 ms on the widest log in the repo. Paid.
    refuse_unmeasured([metric])

    values = stage.values(metric)
    tasks = len(values)
    median = stage.median(metric) if values else 0
    largest = max(values) if values else 0

    def answer(outcome, ratio, why):
        return Verdict(stage_id=stage.stage_id, metric=metric, outcome=outcome, tasks=tasks,
                       median=median, largest=largest, ratio=ratio, why=why)

    if not tasks:
        return answer(UNDECIDED, None, "the log carries no task events for this stage")
    if tasks < MIN_TASKS:
        return answer(UNDECIDED, None,
                      "{} tasks, and below {} the ratio cannot pass 2".format(tasks, MIN_TASKS))
    if largest == 0:
        return answer(UNDECIDED, None, "every task reads zero, so there is no spread")
    floor = floor_for(metric, floors)
    if floor is not None and largest < floor:
        # Before the ratio, because an unbounded ratio over a small number is the case this
        # is here for and it would otherwise be answered before anything measured it.
        return answer(UNDECIDED, None,
                      "largest task {} is under the {} floor of {}".format(
                          plain(largest), model.kind_of(metric), plain(floor)))
    if median == 0:
        return answer(SKEWED, math.inf,
                      "more than half the tasks read zero and one reads {}".format(largest))

    ratio = largest / median
    outcome = SKEWED if ratio > threshold else EVEN
    # The ratio is already a field. The reason says what it was computed from, because a
    # reader arguing with the verdict wants the two numbers rather than the division again.
    return answer(outcome, ratio, "largest task {} against a median of {}".format(
        plain(largest), plain(median)))


def scan(app, metrics=DEFAULT_METRICS, threshold=DEFAULT_THRESHOLD, floors=None):
    """Every stage against every metric, in stage order.

    The whole metric list is refused up front rather than one stage at a time, so a caller
    naming three bad metrics is told about three rather than about the first one.
    """
    refuse_unmeasured(metrics)
    return [judge(stage, metric, threshold, floors)
            for stage in app.stages for metric in metrics]


def counts(verdicts):
    """How many of each outcome, with every outcome present even at zero.

    An absent key would let a caller print a summary that quietly omits the thing it was
    asked about, which is the shape this repo keeps finding.
    """
    tally = {outcome: 0 for outcome in OUTCOMES}
    for verdict in verdicts:
        tally[verdict.outcome] += 1
    return tally


def rank_key(verdict):
    """Where one verdict sits when the body is ordered for reading.

    Outcome first, then the unit, then the ratio inside the unit.

    Sorting the whole list by ratio is the comparison `worst` below refuses to make. On
    the skewed log that sort puts gc_time's 71 millisecond unbounded ratio level with a
    620,755,808 byte spill, because both divide by zero and a ratio carries no unit. Three
    of that log's eight skewed verdicts are unbounded and they span two kinds. So the sort
    stays inside a kind, where it is a comparison.

    The cost is that the body cannot say which single verdict is worst. The worst lines
    answer that per unit and there is no cross unit answer to give.

    Kinds come from `model.MEASURED`. They came from `model.KINDS` for two days, so that a
    verdict on `launch_time` would sort after the measured ones rather than raise, which
    was a sort working around a defect that was not the sort's. `judge` refuses a
    non measured metric now, so no verdict carrying one can reach here, and
    `check_a_verdict_on_a_non_measured_kind_cannot_be_built` is what says so.
    """
    ratio = verdict.ratio
    return (OUTCOMES.index(verdict.outcome),
            model.MEASURED.index(model.kind_of(verdict.metric)),
            # Descending, so the worst is first. An unbounded ratio goes to -inf and leads
            # its kind. None has nothing to order by and goes last.
            -ratio if ratio is not None else math.inf,
            verdict.stage_id,
            verdict.metric)


def ranked(verdicts):
    """Every verdict in reading order. Skewed first, then even, then undecided."""
    return sorted(verdicts, key=rank_key)


def only(verdicts, outcome):
    """The verdicts carrying one outcome, for a reader who wants the problems alone.

    This narrows what gets printed and never what was judged. `counts`, `worst_by_kind`
    and the command's exit status all read the whole scan, because a flag that changes the
    answer is the thing `sjp.cli` exists to prevent. A tally computed over this would read
    0 even and 0 undecided on every log and be a false statement about the job.
    """
    if outcome not in OUTCOMES:
        raise ValueError("{!r} is not an outcome. Expected one of {}".format(
            outcome, OUTCOMES))
    return [verdict for verdict in verdicts if verdict.outcome == outcome]


def worst(verdicts, kind):
    """The skewed verdict to act on first within one unit, or None when nothing skewed.

    Takes a kind because a ratio carries no unit and a single answer across units is a
    comparison this module cannot make. Ranking every skewed verdict together put a
    seventy one millisecond garbage collection level with a 620,755,808 byte spill. Both
    are unbounded and both are on the same stage, so the answer was whichever the metric
    order reached first. Concentration does not separate them either. One task did all of
    both.

    An unbounded ratio sorts above every finite one, because a stage where most tasks did
    nothing is worse than one that is merely lopsided. Ties break on the stage id so the
    answer does not depend on the order the metrics were asked for.
    """
    skewed = [v for v in verdicts
              if v.outcome == SKEWED and model.kind_of(v.metric) == kind]
    if not skewed:
        return None
    return max(skewed, key=lambda v: (v.ratio, -v.stage_id))


def worst_by_kind(verdicts):
    """One worst verdict per unit that has a skewed verdict, in declared kind order.

    The unit is part of the answer rather than something a reader supplies. A report
    naming one worst stage across every metric is naming the metric order.
    """
    found = {}
    for kind in model.MEASURED:
        first = worst(verdicts, kind)
        if first is not None:
            found[kind] = first
    return found
