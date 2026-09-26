# What a median relative threshold cannot say

Skew detection against a median relative threshold. Most of the work went into the second
half of that phrase rather than the first.

## The decision

A stage's skew verdict is one of three values and not two.

```
skewed      the largest task is above the threshold times the median
even        it is at or below
undecided   nothing here can be measured against a median
```

`undecided` is the addition. Before today the model answered a spread of 0.0 whenever the
median was zero, and that number reads as perfectly even. It is the opposite of even.

`Stage.spread` now returns None in that case. None cannot be compared against a threshold
without raising, which is the point of choosing it over 0.0 or over an exception. A caller
that forgets gets a stack trace rather than a wrong answer. The case gets a real answer in
`sjp/skew.py` and only there.

## Three measurements, taken before the threshold was written

### A ratio is unreachable below three tasks

With one task the largest value is the median, so the ratio is exactly 1.0 whatever
happened. With two tasks the largest value is one of the two the median averages, so the
ratio is `2b / (a + b)` and it approaches 2 without arriving. Searched rather than derived,
over vectors drawn from a small set of values.

```
n=1  best=1.0000  at (1,)
n=2  best=1.7778  at (1, 8)
n=3  best=8.0000  at (1, 1, 8)
```

Pushing one task to a million shows the cap is a property of the count.

```
n=1   values [1]*0 + [1e6] -> spread 1.0000
n=2   values [1]*1 + [1e6] -> spread 2.0000
n=3   values [1]*2 + [1e6] -> spread 1000000.0000
```

Both committed logs have a two task stage. So `MIN_TASKS` is 3 and a stage below it is
answered `undecided` with the reason printed. A threshold above 2 can never fire on two
tasks, and reporting such a stage as even would be a claim the arithmetic cannot support.

### A zero median is the worst case rather than the mildest

This was expected to need a constructed fixture. It does not. It fires on captured data,
on the one metric this project exists to find.

```
memory_spilled   sorted=[0, 0, 0, 0, 0, 0, 0, 620755808]  median=0.0  max=620755808  1 of 8 tasks non zero
disk_spilled     sorted=[0, 0, 0, 0, 0, 0, 0, 88088795]  median=0.0  max=88088795  1 of 8 tasks non zero
gc_time          sorted=[0, 0, 0, 0, 0, 0, 0, 71]  median=0.0  max=71  1 of 8 tasks non zero
```

One task of eight spilled 620,755,808 bytes to memory and the other seven spilled nothing.
The old spread for that stage was 0.0. A detector reading it would have reported the most
lopsided stage in the repo as the flattest thing in the file.

So when the median is zero and the maximum is not, the verdict is `skewed` and the ratio is
recorded as unbounded. More than half the tasks did nothing while one did everything, and
there is no finite number that describes that better than the words do.

### The separating range, and the figure that was wrong on the first pass

Over both logs, across records read and duration and executor run time, on every stage with
enough tasks to answer.

```
largest healthy: ['1.7407 skewed stage 1 executor_run_time', '1.7378 skewed stage 1 duration', '1.5747 balanced stage 1 executor_run_time']
smallest guilty: ['14.7246 skewed stage 2 duration', '16.0422 skewed stage 2 executor_run_time', '44.2179 skewed stage 2 records_read']
```

Any threshold between 1.7407 and 14.7246 behaves identically on every stage this repo has.
The default is 4.0, which sits inside that range with room on both sides rather than at an
edge of it.

The first version of this document said the range started at 1.7378. That is the largest
duration ratio and it was the column being read at the time. Executor run time on the same
stage is higher. A check searching all three metrics is what found it, before the number
reached anything published.

## What the detector does on the two logs

```
  3 skewed, 2 even, 4 undecided
  worst  stage 2 on memory_spilled at unbounded
```

against

```
  0 skewed, 4 even, 5 undecided
  nothing skewed at this threshold
```

The two jobs differ in one expression. Five of nine verdicts on the healthy job are
`undecided`, which is a high proportion and it is honest. Four of them are the two task
stage and the fifth is a spill metric nothing moved.

## What was rejected

**Keeping 0.0 and documenting it.** A number that is wrong in a README is still wrong in
the code, and the next caller does not read the README.

**Raising on a zero median.** A profiler handed a real log should report on the stages it
can and name the ones it cannot. Refusing the whole file over one absent denominator makes
the tool useless on exactly the logs worth profiling.

**Tuning the threshold until the two fixtures came out right.** Two logs cannot justify a
constant to four decimal places. They can bound it, and the bound is wide, so the honest
statement is the range and a round number inside it.

**A fourth outcome for the unbounded case.** It was tempting, because unbounded skew and
merely lopsided skew are different situations. They call for the same action though, which
is to look at that stage, so the ratio field carries the distinction and the outcome does
not multiply.

## Still open

`STAGE_TOTAL_FIELDS` maps twelve accumulables of the thirty seven the grouping stage
carries. The detector reads task fields rather than stage totals, so nothing today depends
on that map. It stays open.

The threshold is one number for every metric. A spill ratio and a duration ratio probably
do not deserve the same constant, and nothing measured today says what the difference should
be. Per metric thresholds wait for a log that argues for them.
