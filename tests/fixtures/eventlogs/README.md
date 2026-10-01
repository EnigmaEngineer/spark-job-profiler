# Where these came from

Six real event logs, unedited. All were produced on this machine by

```
python -m sjp capture --job skewed      --out <dir> --rows 8000000
python -m sjp capture --job balanced    --out <dir> --rows 8000000
python -m sjp capture --job join        --out <dir> --rows 8000000
python -m sjp capture --job skewed_join --out <dir> --rows 8000000
python -m sjp capture --job by_column   --out <dir> --rows 8000000
python -m sjp capture --job by_column   --out <dir> --rows 8000000 --partitions 5
```

running pyspark 3.5.6. The session was `local[2]` with a 1g driver and 8 shuffle
partitions, except for the one log the command above asks for 5. Adaptive execution was off
on all of them. The first two differ in one expression, which is the key. Everything else is
held.

The first three ran on Java 11 and the last three on Java 21, five weeks later. Worth saying
out loud because it is a reason to trust the numbers rather than a caveat on them. The key
exchange on the skewed join writes 88,788,038 bytes and that figure was first measured on
Java 11. It reproduced to the byte on Java 21 and again on a second benchmark schedule.

Both join logs set `spark.sql.autoBroadcastJoinThreshold` to -1. Without that Spark sees a
relation of a few kilobytes and broadcasts it, which produces a log holding the answer
rather than the question.

`skewed_join` was the first log here whose key exchange takes its partition count from the
session rather than from the query. It is the log the before and after benchmark was run on.
Without it the repo published a measurement with no fixture behind it.

The two `by_column` logs are the same query at two session counts, 8 and 5. A `repartition`
naming a column and no number is a third origin, and one log cannot show that its count came
from the config rather than from the query. Two can. The pair is the only evidence here that
the origin means what the code now says it means, so neither log is redundant.

They are committed rather than regenerated because a fixture that has to be rebuilt by
anyone who clones the repo is a fixture most people never run.

Two things about them are worth knowing before reading a number out of one.

An event log discloses the driver's working directory, the operating system user and the
full classpath. These are unedited and carry all three. Editing them would make them
something other than real logs.

A stage id is not stable across two runs of the join job. Its plan is not a straight line,
the two sides are submitted together, and whichever is scheduled first takes the lower id.
Address a stage by something the log guarantees rather than by its number.
