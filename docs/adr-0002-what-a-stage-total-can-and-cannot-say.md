# What a stage total can and cannot say

## Status

Accepted. The measurements below decided what the stage and task model reads.

## Context

A Spark event log offers the same quantity twice. `SparkListenerStageCompleted` carries a
list of accumulables holding stage totals, and `SparkListenerTaskEnd` carries the same
metrics one task at a time. Reading the totals is less work and less code. The question is
what that costs.

Everything here was measured on the two committed logs, which are one skewed job and one
balanced job over the same 8,000,000 rows.

## The totals are exactly right

Twelve accumulables map onto a field the model keeps per task. Over six stages, every one
of them that the log reported equals the sum of that field over the stage's tasks. Not
close. Equal.

```
skewed    stage totals: 24 present, 12 absent, 0 disagree with the task sums
balanced  stage totals: 22 present, 14 absent, 0 disagree with the task sums
```

A clean line like that is also what a comparison of nothing against nothing prints, so the
probe takes one task out of a stage and fails if the disagreement does not appear.

```
control, a stage missing one task disagrees: yes
control, a repeated total name is refused: yes
controls: 0 of 2 failed
```

## And that is the problem

A sum over the tasks cannot see the spread across them. The skewed grouping stage reads a
median of 156,784 shuffle records and a maximum of 6,932,663. Both jobs read 8,000,000
records in that stage. The stage total is the same number for a job with a hot key and a
job without one, which is the entire question the profiler exists to answer.

So the model reads the task events. The totals are kept because they are a second reading
of the same quantity, and two readings that have to agree is a check that costs nothing.

## Three things about the list that a parser has to know

**A metric that stayed at zero is absent rather than zero.** The key set is a property of
the data. The grouping stage of the skewed job carries 37 accumulables and the same stage
of the balanced job carries 35, on code that differs in one expression. A reader treating
a missing key as unknown reports unknown for every metric that behaved.

**The names are not unique.** `number of output rows` appears twice in the grouping stage
of both logs, under two different ids, so reading the list into a dictionary keyed on the
name silently drops one. The model keeps each accumulable as a record with its id and
refuses to answer when a name it is asked for appears more than once.

**The values are not all numbers.** The SQL metrics write theirs as strings. The two
`number of output rows` entries read `"200"` in the skewed job and `"199"` in the balanced
one, which are the distinct key counts the two jobs were built with.

## The total called peak is a sum of peaks

`internal.metrics.peakExecutionMemory` is the one that would have caused real damage. It
is a sum over the tasks like every other total, and summing peaks is not a peak of
anything.

```
skewed    stage 2  tasks 8 of 8
    peak      377486768 largest task  562035856 summed by the stage
balanced  stage 2  tasks 8 of 8
    peak      167771904 largest task  1166014800 summed by the stage
```

Read the stage total and the balanced job looks worse by a factor of two. Read the tasks
and the skewed job holds more than twice as much in one place. The balanced job spilled
nothing at all in that stage and the skewed one spilled 620,755,808 bytes to memory, so
the task reading is the one that matches what happened.

A stage total is the wrong number for a question about memory pressure, and it is wrong in
the direction that makes the healthy job look like the problem.

## Decision

The model builds from task events. `Stage.peak_memory` is the largest peak any one task
reached and never the reported total. Accumulables are kept as records rather than a
dictionary. A total that is absent reads as zero, and a total whose name appears twice
raises rather than answering.

## Consequences

Stage totals become a control rather than a source. Anything the profiler publishes that
could come from either has to come from the tasks, and the totals say whether the reading
was complete.

The event names the model reads are registered by the handlers that read them, so there is
no separate list to keep in step. A check walks the module and refuses any Spark event
name in it that no handler claimed.
