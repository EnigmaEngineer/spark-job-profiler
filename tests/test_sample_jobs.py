"""What can be checked about the sample jobs without a Spark session.

Almost nothing runs, and that is worth stating rather than leaving as a gap somebody
notices later. `jobs/sample.py` needs a real driver to do anything, so the only reachable
part of it is the refusal at the top of `run`.

The settings are checked against the committed logs rather than against themselves. A
check reading `sample.SHUFFLE_PARTITIONS == 8` asserts the module against a copy of its
own constant and goes stale the moment somebody changes both. Reading the number back out
of the event log asks a different question, which is whether the fixtures were captured
under the settings this module still declares.
"""
import os

from jobs import sample
from sjp import eventlog

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "eventlogs")


def _only_log(job):
    directory = os.path.join(FIXTURES, job)
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])


def check_an_unknown_job_name_is_refused_before_a_session_is_started():
    try:
        sample.run("nonsense", "/nowhere", 10)
    except ValueError as problem:
        assert "nonsense" in str(problem), problem
    else:
        raise AssertionError("an unknown job name started a Spark session")


def check_the_fixtures_were_captured_at_the_partition_count_this_module_declares():
    for job in ("skewed", "balanced"):
        properties = eventlog.spark_properties(_only_log(job))
        got = properties["spark.sql.shuffle.partitions"]
        assert got == str(sample.SHUFFLE_PARTITIONS), (job, got, sample.SHUFFLE_PARTITIONS)


def check_the_two_fixtures_were_captured_under_the_same_settings():
    """The comparison is worth something only if one thing differed between the jobs."""
    watched = ("spark.sql.shuffle.partitions", "spark.driver.memory", "spark.master",
               "spark.sql.adaptive.enabled")
    skewed = eventlog.spark_properties(_only_log("skewed"))
    balanced = eventlog.spark_properties(_only_log("balanced"))
    for name in watched:
        assert skewed[name] == balanced[name], (name, skewed[name], balanced[name])


def check_adaptive_execution_was_off_when_the_fixtures_were_captured():
    """Adaptive execution splits a skewed partition, which is the thing being demonstrated."""
    for job in ("skewed", "balanced"):
        properties = eventlog.spark_properties(_only_log(job))
        assert properties["spark.sql.adaptive.enabled"] == "false", (job, properties)


def check_a_log_recording_no_environment_is_refused():
    import shutil
    import tempfile

    root = tempfile.mkdtemp(prefix="sjp-noenv-")
    try:
        path = os.path.join(root, "thin")
        with open(path, "w") as handle:
            handle.write('{"Event":"SparkListenerJobStart"}\n')
        try:
            eventlog.spark_properties(path)
        except eventlog.NotAnEventLog:
            pass
        else:
            raise AssertionError("a log with no environment reported properties anyway")
    finally:
        shutil.rmtree(root)
