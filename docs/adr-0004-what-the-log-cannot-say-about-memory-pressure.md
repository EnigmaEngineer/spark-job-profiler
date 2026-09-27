# ADR 0004: what the log cannot say about memory pressure

Date: 2026-09-27. Status: accepted.

## Context

The profiler is meant to say where a job spilled and what memory pressure caused it. Spill is
in the event log. Pressure, in the sense a person means it, is the relationship between what an operator
needed and what it was allowed. Three metrics were planned to express that and the
measurements removed all three.

## The measurements, in one place

Every figure below is read off the two committed logs by the checks in
`tests/test_memory.py`. None of them is typed.

```
memory spilled on the grouping stage      620755808
disk spilled on the same stage            88088795
the sum a total would have published      708844603
inflation on stage 0 of both logs         1.9157
inflation on the grouping stage           7.0469
how far apart those two factors are       3.68
stage 0 spills this and peaks at zero     117440288 bytes
balanced grouping stage peak, no spill    167771904
largest skewed task peak that did not spill 33554384
the task that did spill peaked at         377486768
```

## What was planned, and what each measurement did to it

### Total bytes spilled, per stage

Dropped. `Memory Bytes Spilled` is the size of the records in memory before they went out.
`Disk Bytes Spilled` is the size of the same records serialized and compressed on the way to
disk. They are one event counted twice, so a sum is a number about nothing. On the skewed
log's grouping stage the sum reads 708,844,603 against 620,755,808 of data.

The fallback was to convert one into the other with a factor. There is no factor. It is
1.9157 on stage 0 of both logs and 7.0469 on the skewed grouping stage, 3.68 times apart
inside one file. The ratio is reported as `inflation` so a reader can see it move, and nothing
computes with it.

### Memory pressure as peak execution memory over the task's budget

Dropped, for two separate reasons, either of which is enough.

The numerator is absent where it matters. Stage 0 of both logs spills 117,440,288 bytes to
memory and reports a peak execution memory of zero on every task. A ratio with that numerator
reads no pressure on the one stage that ran out of room. `peak_reported` is a separate
function so that a caller checking it is making a claim about the log rather than about the
job.

The denominator is not in the file. `spark.executor.memory`, `spark.executor.cores`,
`spark.memory.fraction` and `spark.memory.storageFraction` are the four properties that decide
it and none of the four appears in either log. Spark's environment update records properties
that were set, not properties that took their default, and both logs were captured in local
mode where the driver is the executor.

Substituting Spark's documented defaults was considered and refused. A figure computed that
way is a measurement of the defaults presented as a measurement of the run. `budget` returns
`known` false and the list of absent names.

Reading `spark.driver.memory` as the budget was also considered and refused. Both logs set it
to `1g`, so it would work here and be wrong on every log from a cluster. A rule that is
correct only on the fixtures is the failure this repo keeps finding.

### Peak execution memory as the signal that predicts a spill

Dropped. It does not order the two jobs the right way round. The balanced grouping stage peaks
at 167,771,904 bytes on one task and spills nothing. Seven of the skewed grouping stage's eight
tasks peak at 33,554,384 or below and also spill nothing. The eighth peaks at 377,486,768 and
spills all 620,755,808 bytes. The balanced job's peak is more than four times the skewed job's
ordinary task and it is the one that stayed inside memory.

## Decision

Report the spill as measured. Report how few tasks did it, which is the number that separates
the two jobs. Report the inflation factor so nobody sums the two columns. Say on every stage
whether peak execution memory was present. Say once, before any number, that the budget is
absent and name what is missing.

## Consequences

The analysis is smaller than the heading promises and it is honest about why.

A spill is not a finding on its own. Stage 0 of the balanced log is identical to stage 0 of the
skewed log to the byte, so `sjp spill` exits 1 on both. The exit status means the job spilled.
Which spill is pathological is the `memory_spilled` verdict in `sjp skew`, where concentration
is what differs.

Nothing here has run against a log from a cluster. Such a log would carry the four budget
properties and the `known` branch of `budget` has only ever been exercised by a constructed
dictionary in `tests/test_memory.py`. That is a real gap and it is not a gap a fixture
captured in local mode can close.

`peak_reported` answering false is currently a fact about local mode as much as about the
stage. Whether a cluster log reports a peak on a shuffle write stage is unknown here.
