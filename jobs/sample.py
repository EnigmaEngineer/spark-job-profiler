"""Two jobs that do the same work over the same number of rows.

One of them has a key distribution that puts most of the rows on a single reducer. The
other spreads them. Everything else is held the same, including the shuffle partition
count, because a comparison that moves two things at once credits the whole difference to
whichever one the author is writing about.

The point of these is not that they are realistic. It is that the pathology is known
before the profiler is pointed at the log, so the profiler can be graded rather than
believed.
"""
import os

HOT_SHARE = 85       # percent of rows landing on one key in the skewed job
COLD_KEYS = 199      # how many keys the rest are spread over
SHUFFLE_PARTITIONS = 8


def _session(app_name, event_log_dir):
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


def run(job, out_dir, rows):
    """Run one job with event logging on and return the log file it produced."""
    if job not in ("skewed", "balanced"):
        raise ValueError("unknown job {!r}".format(job))

    before = set(os.listdir(out_dir)) if os.path.isdir(out_dir) else set()
    spark = _session("sjp-" + job, out_dir)
    try:
        from pyspark.sql import functions as F

        frame = _keyed(spark, rows, skewed=(job == "skewed"))
        # sortWithinPartitions is here to make the hot partition do work proportional to
        # its size rather than only receive rows. Without it the skew shows up as a long
        # task and nothing else.
        grouped = (frame
                   .repartition(SHUFFLE_PARTITIONS, F.col("key"))
                   .sortWithinPartitions("payload")
                   .groupBy("key")
                   .agg(F.count(F.lit(1)).alias("n"),
                        F.sum("value").alias("total"),
                        F.max("payload").alias("widest")))
        grouped.collect()
    finally:
        spark.stop()

    after = set(os.listdir(out_dir))
    fresh = sorted(after - before)
    if not fresh:
        raise RuntimeError("event logging produced no file in {}".format(out_dir))
    return os.path.join(out_dir, fresh[-1])
