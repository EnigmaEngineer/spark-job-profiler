# What a Spark event log actually holds, read off a log rather than off the docs

## Status

Accepted. This is the record of what was measured before the parser was written.

## Context

The profiler reads Spark event logs. Before building a model of stages and tasks it is
worth knowing what is really in the file, because a model built from documentation tends
to assume fields that a given Spark version does not write.

Two jobs were run on this machine under pyspark 3.5.6 and OpenJDK 11.0.32.1, on
`local[2]` with a 1g driver, 8 shuffle partitions and adaptive execution off. They do the
same work over 8,000,000 rows. The only thing that differs is the expression that builds
the key.

## What the format is

One JSON object per line. Every object carries an `Event` key naming the listener event it
came from. Nothing else is on every line.

A run of the shape above produced 55 lines and 16 distinct event names.

```
tests/fixtures/eventlogs/skewed/local-1790267120237  55 lines
       18  SparkListenerTaskEnd
       18  SparkListenerTaskStart
        3  SparkListenerStageCompleted
        3  SparkListenerStageSubmitted
        2  org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
        1  SparkListenerApplicationEnd
        1  SparkListenerApplicationStart
        1  SparkListenerBlockManagerAdded
        1  SparkListenerEnvironmentUpdate
        1  SparkListenerExecutorAdded
        1  SparkListenerJobEnd
        1  SparkListenerJobStart
        1  SparkListenerLogStart
        1  SparkListenerResourceProfileAdded
        1  org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
        1  org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
  every event the profiler needs is present
```

Two of those names are fully qualified class names rather than short ones. Anything keying
on a short name misses them.

## Where the numbers the profiler needs actually live

`SparkListenerTaskEnd` carries the whole per task picture.

```
Task Info      Launch Time, Finish Time, Executor ID, Index, Partition ID
Task Metrics   Executor Run Time, JVM GC Time, Peak Execution Memory
               Memory Bytes Spilled, Disk Bytes Spilled
               Shuffle Read Metrics and Shuffle Write Metrics, nested
```

A task duration is a subtraction and not a field. `Finish Time` minus `Launch Time` is
wall time for the task including scheduling. `Executor Run Time` is the compute part and
the two are not the same number.

`SparkListenerEnvironmentUpdate` holds the config the run really used, including
`spark.sql.shuffle.partitions` and `spark.driver.memory`. Reading the partition count
from there rather than from a recommendation is what lets the tool say the count was
wrong.

`SparkListenerExecutorAdded` holds `Total Cores`, which is the other half of a partition
count recommendation.

`SparkListenerStageCompleted` repeats most of the task metrics as 37 accumulables under
names like `internal.metrics.memoryBytesSpilled`. Those are stage totals. The per task
spread, which is the whole point of skew detection, is only in the task events.

## What the fixtures carry, measured

```
skewed    stage 0  tasks 2
    records   median 0.0  max 0  ratio 0.00
    duration  median 3886.0 ms  max 3895 ms  ratio 1.00
    spilled   117440288 memory  61304287 disk
skewed    stage 1  tasks 8
    records   median 1000000.0  max 1000000  ratio 1.00
    duration  median 492.0 ms  max 855 ms  ratio 1.74
    spilled   0 memory  0 disk
skewed    stage 2  tasks 8
    records   median 156784.0  max 6932663  ratio 44.22
    duration  median 207.0 ms  max 3048 ms  ratio 14.72
    spilled   620755808 memory  88088795 disk
balanced  stage 0  tasks 2
    records   median 0.0  max 0  ratio 0.00
    duration  median 3671.5 ms  max 3686 ms  ratio 1.00
    spilled   117440288 memory  61304287 disk
balanced  stage 1  tasks 8
    records   median 1000000.0  max 1000000  ratio 1.00
    duration  median 542.0 ms  max 849 ms  ratio 1.57
    spilled   0 memory  0 disk
balanced  stage 2  tasks 8
    records   median 984924.5  max 1206030  ratio 1.22
    duration  median 938.0 ms  max 1258 ms  ratio 1.34
    spilled   0 memory  0 disk
```

Three things fall out of that table.

The skew is real and the balanced job is the control that says so. Without the second job
a 44x ratio is a number with nothing to compare it to.

The spill in stage 2 is caused by the skew. The spill in stage 0 is not. Stage 0 is the
same expression in both jobs and it spills the same bytes in both, to the byte.

That is a statement about cause and not about cost. Stage 0 spilling 117 MB across two
tasks is a real cost in both jobs. The point is that a detector summing spill over an
application cannot separate the two, so it says the same thing about a job with a 44x hot
key and a job without one.

Those stage 0 totals being equal is also the evidence that the two jobs differ in one
expression and in nothing else. It was not built as a control and it works as one.

## Why the fixtures are 8,000,000 rows

The first pair was captured at 2,000,000 rows. The skew was there at a shuffle record
ratio of 44.23. Nothing spilled anywhere, in either job, at any stage.

So the row count decides which pathologies are in the log. A fixture at 2,000,000 rows
would have left every later spill check grading a detector against a log with no spill in
it.

The record ratio is the part that does not care. It is 44.23 at 2,000,000 rows and 44.22
at 8,000,000, because it is a property of the key expression rather than of the volume.
Spill is the opposite. It is absent at one size and 620,755,808 bytes at the other, with
the same key distribution both times. A skew number and a spill number are not two
readings of one thing.

Rebuild either pair with

```
python -m sjp capture --job skewed   --out <dir> --rows <n>
python -m sjp capture --job balanced --out <dir> --rows <n>
```

## Why the checks pin counts and not durations

The 8,000,000 row pair was captured twice, in two separate Spark runs. Every shuffle
record count came back identical and every spill byte count came back identical. The task
durations moved, and the skewed stage duration ratio moved from 13.02 to 14.72. The
2,000,000 row pair was captured twice as well and its duration ratio moved further, from
8.04 to 5.61, on a record ratio that did not move at all.

So a count gets pinned to the value and a duration gets a floor. Giving a duration a
tolerance band would be the same as giving a count one, and a count with a tolerance band
stops catching anything.

## Consequences

The stage and task model reads `SparkListenerTaskEnd` and derives duration rather than
looking for it. Stage totals are available and are not enough. Spill has to be attributed
to a stage before it means anything.

An event log also discloses more than its numbers. These two hold the driver's working
directory, the operating system user who ran the job, and the full classpath of every jar
on it. Anyone pointing this tool at a log from somewhere else should know that.
