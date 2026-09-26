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
"""
import math
from dataclasses import dataclass

SKEWED = "skewed"
EVEN = "even"
UNDECIDED = "undecided"
OUTCOMES = (SKEWED, EVEN, UNDECIDED)

# Inside the measured separating range of 1.7407 to 14.7246 and not at either end of it.
DEFAULT_THRESHOLD = 4.0

# Below this a median relative ratio cannot reach any threshold worth setting. The reason
# is in the module docstring and `tests/test_skew.py` measures it rather than trusting it.
MIN_TASKS = 3

# The metrics `sjp skew` looks at unless told otherwise. Shuffle records is the classic
# skew, wall time is what a person actually noticed, and spill is the one whose median is
# zero on the skewed log.
DEFAULT_METRICS = ("records_read", "duration", "memory_spilled")


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


def judge(stage, metric, threshold=DEFAULT_THRESHOLD):
    """Whether `stage` skewed on `metric`, or why that question has no answer here.

    Deliberately does not call `Stage.spread`. That property answers None on a zero median
    so nothing can compare it to a threshold by accident, and the case it refuses to
    summarise is a case this function has to name.
    """
    if threshold <= 1:
        # A ratio of 1.0 is what a single task and a perfectly even stage both read, so a
        # threshold at or below it calls everything skewed and means nothing.
        raise ValueError("threshold {} is at or below an even stage's ratio".format(threshold))

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
    if median == 0:
        return answer(SKEWED, math.inf,
                      "more than half the tasks read zero and one reads {}".format(largest))

    ratio = largest / median
    outcome = SKEWED if ratio > threshold else EVEN
    # The ratio is already a field. The reason says what it was computed from, because a
    # reader arguing with the verdict wants the two numbers rather than the division again.
    return answer(outcome, ratio, "largest task {} against a median of {}".format(
        plain(largest), plain(median)))


def scan(app, metrics=DEFAULT_METRICS, threshold=DEFAULT_THRESHOLD):
    """Every stage against every metric, in stage order."""
    return [judge(stage, metric, threshold)
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


def worst(verdicts):
    """The skewed verdict to act on first, or None when nothing skewed.

    An unbounded ratio sorts above every finite one, because a stage where most tasks did
    nothing is worse than one that is merely lopsided. Ties break on the stage id so the
    answer does not depend on the order the metrics were asked for.
    """
    skewed = [v for v in verdicts if v.outcome == SKEWED]
    if not skewed:
        return None
    return max(skewed, key=lambda v: (v.ratio, -v.stage_id))
