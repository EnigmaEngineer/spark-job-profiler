"""Three jobs, each carrying one pathology that is known before the log is read.

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

The point of these is not that they are realistic. It is that the pathology is known
before the profiler is pointed at the log, so the profiler can be graded rather than
believed.
"""
import os

HOT_SHARE = 85       # percent of rows landing on one key in the skewed job
COLD_KEYS = 199      # how many keys the rest are spread over
SHUFFLE_PARTITIONS = 8

# One row per key on the small side of the join. Small enough that Spark would broadcast
# it if it were allowed to, which is the whole point of switching that off below.
DIM_ROWS = COLD_KEYS

JOBS = ("skewed", "balanced", "join", "skewed_join")

# The jobs whose key exchange Spark sizes from the session rather than from the query.
CONFIG_SIZED = ("skewed_join",)

# The jobs that must not be allowed to broadcast the small side.
NO_BROADCAST = ("join", "skewed_join")


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
    """The aggregate all three jobs end on, so the shape after the shuffle is held fixed."""
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


def run(job, out_dir, rows, partitions=None):
    """Run one job with event logging on and return the log file it produced.

    `partitions` sets `spark.sql.shuffle.partitions` for the session. It moves nothing on
    the three jobs that write their own count into the query, which is not a limitation of
    this function. It is the thing the fourth job exists to show.
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
        frame = _keyed(spark, rows, skewed=job.startswith("skewed"))
        if job == "join":
            _grouped(frame.join(_labels(spark), on="key", how="inner")).collect()
        elif job == "skewed_join":
            # Left, because the hot key is not in the small side and an inner join would
            # drop the eighty five percent of rows that are the whole pathology.
            _joined_aggregate(
                frame.join(_labels(spark), on="key", how="left")).collect()
        else:
            _grouped(frame).collect()
    finally:
        spark.stop()

    after = set(os.listdir(out_dir))
    fresh = sorted(after - before)
    if not fresh:
        raise RuntimeError("event logging produced no file in {}".format(out_dir))
    return os.path.join(out_dir, fresh[-1])
