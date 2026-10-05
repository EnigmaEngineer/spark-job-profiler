"""Seven jobs, each carrying one pathology that is known before the log is read.

The first two do the same work over the same number of rows. One has a key distribution
that puts most of the rows on a single reducer. The other spreads them. Everything else is
held the same, including the shuffle partition count, because a comparison that moves two
things at once credits the whole difference to whichever one the author is writing about.

The third joins a large relation to a small one with broadcasting switched off, so Spark
plans a sort merge join and shuffles both sides. It exists because neither of the other two
contains a join of any kind, and a tool that recommends a broadcast needs a log where a
join it could have replaced actually happened.

The fourth is the skewed distribution going through that same join. It exists because the
first three all name their own partition count, and a count written into the query is not
a count a config change can move. So the profiler correctly refuses to advise on any of
them, and a recommendation nothing can be applied to cannot be benchmarked. Here the join
is what demands the partitioning, so Spark takes the count from the session and the
recommendation becomes a thing you can act on and time.

Putting the hot key through a join rather than through a grouped aggregate is not a
decoration either. Spark puts a partial aggregate in front of the shuffle whenever the
query lets it, and a partial aggregate over two hundred keys empties the shuffle of the
very rows the skew is made of. The join has to carry every row across the boundary.

The fifth is not a pathology. It is a shape the profiler was reading wrongly. A
`repartition` call naming a column and no number asks Spark for a hash and says
nothing about how many ways, so the count comes from the session exactly as it does
when a join demands the partitioning. The exchange records a different origin, and
reading that origin as a number somebody typed is what this job exists to catch.

The sixth and seventh are cycle 2 additions and neither of them is a pathology in the
query. They are pathologies in the profiler's own output.

The sixth runs many small aggregates in one application instead of one large one. Nothing
about any single aggregate is interesting. The point is the stage count, because `sjp skew`
prints one line per metric per stage and the five logs above are all small enough that the
output fits on a screen. A real application doing twenty steps does not, and the three
lines that matter end up at the bottom. Reproducing that needs a log rather than an
argument.

The seventh runs the skewed distribution over very few rows. Every byte and record verdict
it produces is small enough that nobody would act on it, which is the one thing none of the
five logs above can say. Placing a magnitude floor needs a log where the small end is
populated.

The point of these is not that they are realistic. It is that the pathology is known
before the profiler is pointed at the log, so the profiler can be graded rather than
believed.

**Every job name has to reach its own branch of `run` and nothing here needs a session to
check that.** `tests/test_sample_jobs.py` drives the dispatch against recorded helpers,
because this module went three days with a mutant in it that sent four job names down one
branch and crashed two more. Nothing noticed, because the only part of `run` a test had
ever reached was the refusal on the first line.
"""
import os

HOT_SHARE = 85       # percent of rows landing on one key in the skewed job
COLD_KEYS = 199      # how many keys the rest are spread over
SHUFFLE_PARTITIONS = 8

# One row per key on the small side of the join. Small enough that Spark would broadcast
# it if it were allowed to, which is the whole point of switching that off below.
DIM_ROWS = COLD_KEYS

# The wide job's shape. Steps rather than rows, because the stage count is what it is for
# and a row count moves neither.
WIDE_STEPS = 24
WIDE_ROWS_PER_STEP = 40000

# Few enough rows that every byte and record verdict off this job is noise. The number is
# the fixture's identity rather than a tuning knob, so `--rows` does not reach it.
SMALL_ROWS = 600

JOBS = ("skewed", "balanced", "join", "skewed_join", "by_column", "wide", "small")

# The jobs whose key exchange Spark sizes from the session rather than from the query.
CONFIG_SIZED = ("skewed_join", "by_column")

# The jobs that must not be allowed to broadcast the small side.
NO_BROADCAST = ("join", "skewed_join")

# The jobs that build their own frame inside the dispatch, so `run` has none to hand them
# and `--rows` does nothing. Named here rather than tested inline, because the dispatch
# reading one list is what lets a check assert the list against the branches.
OWN_FRAME = ("wide", "small")


def _session(app_name, event_log_dir, extra=(), partitions=None):
    from pyspark.sql import SparkSession

    os.makedirs(event_log_dir, exist_ok=True)
    # The session's count, which only reaches a job that has not named one itself. The
    # default is the same number the query writing jobs use, so a run that passes nothing
    # is the run the committed logs came from.
    session_partitions = SHUFFLE_PARTITIONS if partitions is None else partitions
    builder = (SparkSession.builder
               .master("local[2]")
               .appName(app_name)
               .config("spark.driver.memory", "1g")
               .config("spark.sql.shuffle.partitions", str(session_partitions))
               .config("spark.sql.adaptive.enabled", "false")
               .config("spark.ui.enabled", "false")
               .config("spark.eventLog.enabled", "true")
               .config("spark.eventLog.dir", "file://" + os.path.abspath(event_log_dir)))
    for name, value in extra:
        builder = builder.config(name, value)
    return builder.getOrCreate()


def _keyed(spark, rows, skewed):
    """One row per id, with a key column whose distribution is the only thing that moves.

    The key is derived from the id by arithmetic rather than by a random draw, so two runs
    at the same row count produce the same distribution.
    """
    from pyspark.sql import functions as F

    base = spark.range(0, rows).repartition(SHUFFLE_PARTITIONS)
    cold = F.concat(F.lit("k"), (F.col("id") % COLD_KEYS).cast("string"))
    if skewed:
        key = F.when(F.col("id") % 100 < HOT_SHARE, F.lit("hot")).otherwise(cold)
    else:
        key = cold
    return base.select(
        key.alias("key"),
        (F.col("id") % 977).alias("value"),
        # A payload wide enough that a partition of these is worth spilling.
        F.concat(F.lit("payload-"), F.col("id").cast("string"),
                 F.lit("-"), F.repeat(F.lit("x"), 48)).alias("payload"),
    )


def _labels(spark):
    """The small side of the join. One row per key the balanced distribution uses.

    Deliberately narrow. A wide small side would make the shuffle it costs look like the
    reason to broadcast it, and the reason to broadcast it is the shuffle it saves on the
    other side.
    """
    from pyspark.sql import functions as F

    return spark.range(0, DIM_ROWS).select(
        F.concat(F.lit("k"), F.col("id").cast("string")).alias("key"),
        F.concat(F.lit("label-"), F.col("id").cast("string")).alias("label"),
    )


def _joined_aggregate(frame):
    """The same aggregate, over a frame the join has already partitioned.

    No `repartition` call, which is the whole point. The join is what demands the
    partitioning, so the count comes from `spark.sql.shuffle.partitions` and the exchange
    records `ENSURE_REQUIREMENTS` rather than a number somebody typed.

    No `sortWithinPartitions` either. The sort merge join sorts both sides by the key
    already, so adding one would be asking for work that is being done anyway.
    """
    from pyspark.sql import functions as F

    return (frame
            .groupBy("key")
            .agg(F.count(F.lit(1)).alias("n"),
                 F.max("payload").alias("widest")))


def _grouped(frame):
    """The aggregate the query sized jobs end on, so the shape after the shuffle is held.

    Four job names reach it. The two that differ only in their key distribution, the inner
    join, and the small row count job.
    """
    from pyspark.sql import functions as F

    # sortWithinPartitions is here to make the hot partition do work proportional to
    # its size rather than only receive rows. Without it the skew shows up as a long
    # task and nothing else.
    return (frame
            .repartition(SHUFFLE_PARTITIONS, F.col("key"))
            .sortWithinPartitions("payload")
            .groupBy("key")
            .agg(F.count(F.lit(1)).alias("n"),
                 F.sum("value").alias("total"),
                 F.max("payload").alias("widest")))


def _wide(spark, steps, rows_per_step):
    """Many small aggregates in one application, each one its own Spark job.

    Chaining them into a single plan would give one job with a long lineage, which is not
    the shape that breaks the output. Collecting each one is what makes the application
    carry many stages, and an application running a sequence of steps is the ordinary case
    rather than a contrived one.

    Nothing here is skewed on purpose. A wide log full of even stages is the harder test of
    the output problem, because the lines that say nothing are the ones burying the lines
    that do.
    """
    from pyspark.sql import functions as F

    seen = 0
    for step in range(steps):
        start = step * rows_per_step
        frame = spark.range(start, start + rows_per_step).select(
            F.concat(F.lit("k"), (F.col("id") % COLD_KEYS).cast("string")).alias("key"),
            (F.col("id") % 977).alias("value"))
        rows = (frame
                .repartition(SHUFFLE_PARTITIONS, F.col("key"))
                .groupBy("key")
                .agg(F.count(F.lit(1)).alias("n"),
                     F.sum("value").alias("total"))
                .collect())
        seen += len(rows)
    return seen


def run(job, out_dir, rows, partitions=None):
    """Run one job with event logging on and return the log file it produced.

    `partitions` sets `spark.sql.shuffle.partitions` for the session. It moves nothing on
    the three jobs that write their own count into the query, which is not a limitation of
    this function. It is the thing the fourth job exists to show.

    The branch order matters and it is the reason this function has a test. Every arm names
    one job, so a reader can see which arm a name takes without holding the earlier
    conditions in their head. An arm written as "not one of the others" is what put four
    names on one branch for three days.
    """
    if job not in JOBS:
        raise ValueError("unknown job {!r}".format(job))

    before = set(os.listdir(out_dir)) if os.path.isdir(out_dir) else set()
    # A sort merge join is what these logs are for. Left on its own Spark sees a relation
    # of a few kilobytes and broadcasts it, which produces a log holding the answer rather
    # than the question.
    extra = (("spark.sql.autoBroadcastJoinThreshold", "-1"),) if job in NO_BROADCAST else ()
    spark = _session("sjp-" + job, out_dir, extra, partitions)
    try:
        # The two output shaped jobs build their own frame at their own size, so neither
        # takes the row count and neither gets one built for it.
        frame = None if job in OWN_FRAME else _keyed(
            spark, rows, skewed=job.startswith("skewed"))
        if job == "join":
            _grouped(frame.join(_labels(spark), on="key", how="inner")).collect()
        elif job == "by_column":
            from pyspark.sql import functions as F

            # repartition(col) with no number. The aggregate that follows needs the rows
            # grouped by key and they already are, so Spark adds no second exchange and
            # the only hash in the plan is this one.
            _joined_aggregate(frame.repartition(F.col("key"))).collect()
        elif job == "skewed_join":
            # Left, because the hot key is not in the small side and an inner join would
            # drop the eighty five percent of rows that are the whole pathology.
            _joined_aggregate(
                frame.join(_labels(spark), on="key", how="left")).collect()
        elif job == "wide":
            _wide(spark, WIDE_STEPS, WIDE_ROWS_PER_STEP)
        elif job == "small":
            _grouped(_keyed(spark, SMALL_ROWS, skewed=True)).collect()
        else:
            _grouped(frame).collect()
    finally:
        spark.stop()

    after = set(os.listdir(out_dir))
    fresh = sorted(after - before)
    if not fresh:
        raise RuntimeError("event logging produced no file in {}".format(out_dir))
    return os.path.join(out_dir, fresh[-1])
