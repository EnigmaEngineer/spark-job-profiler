# Where these came from

Eight real event logs, unedited. All were produced on this machine by

```
python -m sjp capture --job skewed      --out <dir> --rows 8000000
python -m sjp capture --job balanced    --out <dir> --rows 8000000
python -m sjp capture --job join        --out <dir> --rows 8000000
python -m sjp capture --job skewed_join --out <dir> --rows 8000000
python -m sjp capture --job by_column   --out <dir> --rows 8000000
python -m sjp capture --job by_column   --out <dir> --rows 8000000 --partitions 5
python -m sjp capture --job wide        --out <dir> --rows 0
python -m sjp capture --job small       --out <dir> --rows 0
```

The last two were captured 2026-10-02 and they are a different kind of fixture. The first six
carry a pathology in the query. These two carry one in the profiler's own output, so `--rows`
does nothing on either. `wide` takes its size from `WIDE_STEPS` and `WIDE_ROWS_PER_STEP`, and
`small` from `SMALL_ROWS`.

running pyspark 3.5.6. The session was `local[2]` with a 1g driver and 8 shuffle
partitions, except for the one log the command above asks for 5. Adaptive execution was off
on all of them. The first two differ in one expression, which is the key. Everything else is
held.

Three of them ran on Java 11 and five on Java 21, eight days apart at the widest. Read out
of the logs themselves rather than remembered. No command prints this, so it is a table.

| Java version | logs |
|---|---|
| 11.0.32.1 | `skewed`, `balanced`, `join` |
| 21.0.10 | `skewed_join`, `by_column`, `by_column_at_5` |
| 21.0.12.1 | `wide`, `small` |

That split is a reason to trust one figure rather than a caveat on all of them. The skewed
job's key exchange writes 130,788,590 bytes. Recapturing the same job eleven days later
returned the same number to the byte, four occurrences in the log either way.

The skewed join's key exchange writes 88,788,038 bytes and that one says nothing about a
runtime, because its log was captured on Java 21 and never on Java 11. What it survived is
two separate benchmark schedules two days apart.

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

## The two cycle 2 logs, and why a row count would not have produced them

`wide` runs 24 small aggregates in one application rather than one large one, so the
application carries **48 stages**. Chaining them into a single plan would have given one job
with a long lineage, which is not the shape that breaks anything. Collecting each step is what
makes the stage count grow, and an application that runs a sequence of steps is the ordinary
case rather than a contrived one.

Nothing in it is skewed. That is deliberate. `sjp skew` prints one line per metric per stage,
and 48 stages against the 14 metric default set is 672 verdicts and **675 lines of output to
say that nothing is wrong**. The tally and the worst line per unit are the last four. The six
logs above print between 45 and 61 lines, which is why the first eight days of work on this
tool never ran into it.

`small` is the skewed distribution over **600 rows**. Its grouping stage reported
`records_read` skewed at 43.17, which is a largest task of 518 records against a median of
12. It also reported `local_bytes_read` skewed at 9.39, which is 7,449 bytes against 793.
Both verdicts were arithmetically correct and neither was worth acting on.

It exists because a floor cannot be placed from one end of a range. The skewed join's key
exchange writes 88,788,038 bytes. This stage's largest task reads 7,449. Four orders of
magnitude apart, in the same repo, judged by the same rule with nothing in it.

Those two numbers are now the noise end of the byte and count brackets, and both floors are
derived from them. The log reports neither verdict any more. What it still reports is two
`executor_run_time` verdicts, and that is the part this capture settled without being asked.
One of its tasks runs 556 milliseconds on 600 rows, which is eight times the smallest
millisecond value any log here calls real, so the millisecond bracket is inverted and no
magnitude floor can close it. See `docs/adr-0006`.

Both record and byte numbers above came off the deterministic half of the log and reproduced
across three separate captures, to the record and to the byte. The third was 2026-10-05 on
Java 11, against the first two on Java 21. The millisecond metrics moved every time, so
nothing pins them.
