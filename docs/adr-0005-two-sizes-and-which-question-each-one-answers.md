# Two sizes in one log, and which question each one answers

## Context

A partition count recommendation and a broadcast recommendation are both arguments about
the size of a relation. The event log holds two numbers that read like that size and they
are not the same measurement.

`data size` sits on every exchange. `shuffle bytes written` sits beside it. Both are
reported in bytes, both are about the same operator, and nothing in the log says they
answer different questions.

## What was measured

Every exchange in the three committed logs, read through the accumulator ids the plan
hands out.

```
skewed    hashpartitioning         832000000 estimated   130788590 measured
balanced  hashpartitioning         832000000 estimated   163272682 measured
join      hashpartitioning         832000000 estimated   163272682 measured
join      hashpartitioning              4776 estimated        2401 measured
all three RoundRobinPartitioning   128000000 estimated    42048255 measured
```

Two things fall out of that table.

**The estimate cannot see the data.** The skewed job and the balanced job differ in one
expression. Their measured shuffles differ by 32,484,092 bytes and their estimates are the
same number. A recommender reading the estimate gives both jobs identical advice, and the
advice it gives the sick one is the advice for the healthy one.

**The estimate is arithmetic over the schema.** It divides to a whole number of bytes per
row every time.

```
832000000 / 8000000 = 104 bytes a row
128000000 / 8000000 = 16 bytes a row
     4776 /     199 = 24 bytes a row
```

The measured number does not. 130788590 over 8000000 rows is 16.34857375.

**The factor between them is not a constant.** Over the four distinct exchanges it reads
1.9892, 3.0441, 5.0958 and 6.3614. That is a spread of 3.2 times inside one repo. So there
is no converting one into the other and a tool has to carry both.

## Decision

**Partition sizing reads `shuffle bytes written`.** Adaptive execution compares its
advisory partition size against the map output sizes, which are the compressed bytes on
disk. Sizing a partition by an estimate that ignores compression would be sizing it by a
number Spark does not use for that.

**A broadcast decision reads `data size`.** That is the statistic Spark's own planner
compares against `spark.sql.autoBroadcastJoinThreshold`. A tool asking whether a join could
have been a broadcast has to ask the question the planner asked, or it disagrees with the
planner for reasons that have nothing to do with the job.

The two fields are named `estimated` and `written` on `layout.Shuffle` rather than folded
into one size. They get confused exactly when they are close enough for the confusion to
survive, so the names carry the difference.

## The second refusal

An exchange records who chose its partition count.

```
skewed    REPARTITION_BY_NUM  REPARTITION_BY_NUM
balanced  REPARTITION_BY_NUM  REPARTITION_BY_NUM
join      ENSURE_REQUIREMENTS  REPARTITION_BY_NUM  ENSURE_REQUIREMENTS
```

`ENSURE_REQUIREMENTS` is a count Spark took from `spark.sql.shuffle.partitions`.
`REPARTITION_BY_NUM` is an argument somebody wrote into the query. Advising a config change
on the second kind is advice that does nothing, so `sjp layout` prints no target for one
and names the origin instead.

Both aggregate logs are the second kind throughout. The recommendation this work exists for
correctly has nothing to say on either of them, and that is why a third job was captured.

## Why a third fixture

Neither aggregate log contains a join of any operator kind. A broadcast recommender graded
against them is graded on a question they do not ask, which is the shape this repo calls a
metric whose subject the repo does not have.

`jobs/sample.py` grew a join job with `spark.sql.autoBroadcastJoinThreshold` set to -1.
Left alone Spark sees a relation of a few kilobytes and broadcasts it, which produces a log
holding the answer rather than the question.

## What the measurement says about that join

```
small side   4776 estimated, against a 10485760 threshold
large side   163272682 measured bytes shuffled to meet it
```

The argument for the broadcast is the second number and never the first. `Candidate.saved`
carries the other side's measured shuffle for that reason, and a side with no measurement
of what the other one shuffled is not reported as worth it however small it is.

## A stage id is not stable across two runs of this job

The join job was captured twice while settling the log directory name. Same code, same row
count, same configuration.

```
first capture   spill on stage 1   small side on stage 0
second capture  spill on stage 0   small side on stage 2
```

Every stage moved. The two sides of a join are submitted together and whichever is
scheduled first takes the lower id, so on a plan that is not a straight line an id is a
property of one run.

Nothing in this module addresses a stage by id. A plan node names its metrics by
accumulator id and `Application.accumulator` resolves them, so every figure above is
addressed by something the log guarantees. The earlier documents here do quote stage ids,
and they are safe only because both aggregate plans are linear.

## What this does not do

The partition target is arithmetic over one shuffle's volume and the slot count. It does
not know whether the skew in a stage is key borne, and hash partitioning puts one key in
one partition however many partitions there are. So a target computed here is not advice
for a stage skewed on its key, and nothing in the log carries the key distribution needed
to tell one case from the other.

`number of partitions` is reported only by the driver and never by a stage. A lookup that
searched the stage accumulables alone would answer that the count is absent from a file
that contains it.
