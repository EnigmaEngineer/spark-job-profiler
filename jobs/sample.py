"""Three jobs, each carrying one pathology that is known before the log is read.

The first two do the same work over the same number of rows. One has a key distribution
that puts most of the rows on a single reducer. The other spreads them. Everything else is
held the same, including the shuffle partition count, because a comparison that moves two
things at once credits the whole difference to whichever one the author is writing about.

The third joins a large relation to a small one with broadcasting switched off, so Spark
plans a sort merge join and shuffles both sides. It exists because neither of the other two
contains a join of any kind, and a tool that recommends a broadcast needs a log where a
join it could have replaced actually happened.

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

JOBS = ("skewed", "balanced", "join")


def _session(app_name, event_log_dir, extra=()):
    from pyspark.sql import SparkSession

    os.makedirs(event_log_dir, exist_ok=True)
    builder = (SparkSession.builder
               .master("local[2]")
               .appName(app_name)
               .config("spark.driver.memory", "1g")
               .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
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


def run(job, out_dir, rows):
    """Run one job with event logging on and return the log file it produced."""
    if job not in JOBS:
        raise ValueError("unknown job {!r}".format(job))

    before = set(os.listdir(out_dir)) if os.path.isdir(out_dir) else set()
    # A sort merge join is what this log is for. Left on its own Spark sees a relation of
    # a few kilobytes and broadcasts it, which produces a log holding the answer rather
    # than the question.
    extra = (("spark.sql.autoBroadcastJoinThreshold", "-1"),) if job == "join" else ()
    spark = _session("sjp-" + job, out_dir, extra)
    try:
        frame = _keyed(spark, rows, skewed=(job == "skewed"))
        if job == "join":
            frame = frame.join(_labels(spark), on="key", how="inner")
        _grouped(frame).collect()
    finally:
        spark.stop()

    after = set(os.listdir(out_dir))
    fresh = sorted(after - before)
    if not fresh:
        raise RuntimeError("event logging produced no file in {}".format(out_dir))
    return os.path.join(out_dir, fresh[-1])
