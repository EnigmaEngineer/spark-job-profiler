# Where these came from

Three real event logs, unedited. All were produced on this machine by

```
python -m sjp capture --job skewed   --out <dir> --rows 8000000
python -m sjp capture --job balanced --out <dir> --rows 8000000
python -m sjp capture --job join     --out <dir> --rows 8000000
```

running pyspark 3.5.6 on Java 11. The session was `local[2]` with a 1g driver and 8
shuffle partitions. Adaptive execution was off. The first two differ in one expression,
which is the key. Everything else is held.

The join log also sets `spark.sql.autoBroadcastJoinThreshold` to -1. Without that Spark
sees a relation of a few kilobytes and broadcasts it, which produces a log holding the
answer rather than the question.

They are committed rather than regenerated because a fixture that has to be rebuilt by
anyone who clones the repo is a fixture most people never run.

Two things about them are worth knowing before reading a number out of one.

An event log discloses the driver's working directory, the operating system user and the
full classpath. These are unedited and carry all three. Editing them would make them
something other than real logs.

A stage id is not stable across two runs of the join job. Its plan is not a straight line,
the two sides are submitted together, and whichever is scheduled first takes the lower id.
Address a stage by something the log guarantees rather than by its number.
