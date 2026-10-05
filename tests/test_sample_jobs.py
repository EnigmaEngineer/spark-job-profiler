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


# the dispatch, which nothing reached until a mutant sat in it for three days

def _recording_run(job, want="route", rows=12345):
    """Drive `sample.run` for one job name with every helper replaced by a recorder.

    With `want="route"` it returns the list of helper names the dispatch reached, in order.
    With `want="keyed"` it returns the arguments `_keyed` was called with, with
    `want="handed"` whether the branch was handed a frame rather than None, and with
    `want="extra"` the session settings the job asked for on top of the defaults.

    The three readings exist because the route alone does not pin the dispatch. A mutant
    flipping `job in OWN_FRAME` to `not in` leaves every route unchanged, since the two jobs
    that build their own frame never read the one `run` would have made for them.

    This needs no Spark session and no pyspark install, which is the whole reason it can
    exist. The module's own docstring used to say the dispatch was unreachable without a
    driver and that was true of running the queries. It was never true of the branch
    choice, and believing it is what left `run` with one check on it for eleven days.
    """
    import sys
    import types

    taken = []

    class Frame:
        def repartition(self, *a, **k):
            taken.append("repartition")
            return self

        def sortWithinPartitions(self, *a, **k):
            return self

        def groupBy(self, *a, **k):
            return self

        def agg(self, *a, **k):
            return self

        def join(self, *a, **k):
            taken.append("join")
            return self

        def select(self, *a, **k):
            return self

        def collect(self):
            return []

    def session(app_name, event_log_dir, extra=(), partitions=None):
        session_extra.extend(extra)
        os.makedirs(event_log_dir, exist_ok=True)
        # run() reports the file event logging produced, so the stub has to produce one or
        # the refusal at the bottom fires and hides whichever branch ran.
        with open(os.path.join(event_log_dir, "written-by-the-recorder"), "w"):
            pass
        return types.SimpleNamespace(stop=lambda: None)

    handed = []
    keyed_calls = []
    session_extra = []
    fake = types.ModuleType("pyspark")
    fake.sql = types.ModuleType("pyspark.sql")
    fake.sql.functions = types.SimpleNamespace(col=lambda *a: "col", lit=lambda *a: "lit")
    fake.sql.SparkSession = object
    saved_modules = {name: sys.modules.get(name)
                     for name in ("pyspark", "pyspark.sql", "pyspark.sql.functions")}
    sys.modules["pyspark"] = fake
    sys.modules["pyspark.sql"] = fake.sql
    sys.modules["pyspark.sql.functions"] = fake.sql.functions

    saved = {name: getattr(sample, name)
             for name in ("_session", "_keyed", "_labels", "_grouped",
                          "_joined_aggregate", "_wide")}
    def keyed(spark, rows, skewed):
        keyed_calls.append({"rows": rows, "skewed": skewed})
        return Frame()

    def record(name):
        def helper(frame):
            taken.append(name)
            handed.append(frame is not None)
            return frame
        return helper

    sample._session = session
    sample._keyed = keyed
    sample._labels = lambda *a: Frame()
    sample._grouped = record("_grouped")
    sample._joined_aggregate = record("_joined_aggregate")
    sample._wide = lambda *a: taken.append("_wide") or 0
    try:
        import shutil
        import tempfile
        root = tempfile.mkdtemp(prefix="sjp-dispatch-")
        try:
            sample.run(job, os.path.join(root, job), rows)
        finally:
            shutil.rmtree(root, ignore_errors=True)
    finally:
        for name, value in saved.items():
            setattr(sample, name, value)
        for name, value in saved_modules.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value
    if want == "extra":
        return session_extra
    if want == "keyed":
        return keyed_calls
    if want == "handed":
        return handed
    return taken


# What each job name is supposed to do, written down so the dispatch is graded against a
# claim rather than against itself. A branch that changes has to change this line too.
DISPATCH = {
    "skewed": ["_grouped"],
    "balanced": ["_grouped"],
    "join": ["join", "_grouped"],
    "skewed_join": ["join", "_joined_aggregate"],
    "by_column": ["repartition", "_joined_aggregate"],
    "wide": ["_wide"],
    "small": ["_grouped"],
}


def check_the_dispatch_claim_covers_every_job_name():
    """Driven off JOBS rather than off the map, so a new job cannot arrive unclaimed."""
    assert set(DISPATCH) == set(sample.JOBS), (sorted(DISPATCH), sorted(sample.JOBS))


def check_every_job_name_reaches_the_branch_it_claims():
    """The check that was missing.

    A mutant turned `job == "by_column"` into `job != "by_column"` and committed. Four job
    names then went down one branch, two crashed on a frame they were never handed, and
    the suite stayed green because nothing here had ever called `run` past its first line.
    """
    for job in sample.JOBS:
        assert _recording_run(job) == DISPATCH[job], (job, _recording_run(job))


def check_no_two_job_names_reach_the_same_branch_by_the_same_route():
    """A collapsed arm is the failure shape, so the distinctness is the thing to assert.

    The two distribution jobs are the deliberate exception. They differ in the key and in
    nothing else, which is what makes them comparable, so they share a route by design and
    are compared by their logs instead.
    """
    routes = {}
    for job in sample.JOBS:
        routes.setdefault(tuple(_recording_run(job)), []).append(job)
    shared = {route: names for route, names in routes.items() if len(names) > 1}
    assert shared == {("_grouped",): ["skewed", "balanced", "small"]}, shared


def check_the_jobs_that_build_their_own_frame_are_handed_none():
    """`--rows` reaches five jobs and the other two take their size from a constant.

    Asserted on what `_keyed` was really called with rather than on the route, because the
    route cannot see this. Flipping the membership test leaves all seven routes identical
    and hands the other five jobs a None they then pass straight to a helper.
    """
    assert set(sample.OWN_FRAME) == {"wide", "small"}, sample.OWN_FRAME
    for job in sample.JOBS:
        handed = _recording_run(job, want="handed")
        for got in handed:
            assert got is True, (job, handed)


def check_the_row_count_reaches_the_five_jobs_that_take_one_and_no_others():
    """Which is the claim OWN_FRAME makes, read off the calls rather than off the list."""
    for job in ("skewed", "balanced", "join", "skewed_join", "by_column"):
        calls = _recording_run(job, want="keyed")
        assert len(calls) == 1, (job, calls)
        assert calls[0]["rows"] == 12345, (job, calls)
    assert _recording_run("wide", want="keyed") == [], "wide took a row count"


def check_the_small_job_is_the_skewed_distribution_at_its_own_size():
    """The whole reason the fixture is usable as the noise end of two brackets.

    An unskewed 600 row job has nothing lopsided in it and would place both floors from a
    log with no pathology in it at all. A mutant flipping that argument is invisible to the
    route and to the handed check, so it is asserted on the call.
    """
    calls = _recording_run("small", want="keyed", rows=12345)
    assert len(calls) == 1, calls
    assert calls[0]["skewed"] is True, calls
    # Driven with a row count that is not SMALL_ROWS, because the two agree at 600 and a
    # fixture where they agree cannot tell which one the branch read.
    assert calls[0]["rows"] == sample.SMALL_ROWS, calls
    assert calls[0]["rows"] != 12345, calls


def check_the_recorder_would_fail_against_the_dispatch_that_shipped():
    """The control, and it is the exact code that was committed rather than a rewrite.

    Without this the check above is a check whose own correctness nobody measured. The
    arm below is the mutant, so a recorder that cannot tell the two apart fails here.
    """
    import types

    taken = []

    class Frame:
        def repartition(self, *a, **k):
            taken.append("repartition")
            return self

        def join(self, *a, **k):
            taken.append("join")
            return self

    def damaged(job, frame, labels, grouped, joined_aggregate):
        """The committed arm order, with `!=` where `==` belongs."""
        if job == "join":
            grouped(frame.join(labels))
        elif job != "by_column":
            joined_aggregate(frame.repartition("key"))
        elif job == "skewed_join":
            joined_aggregate(frame.join(labels))
        else:
            grouped(frame)

    for job in ("skewed", "balanced", "skewed_join"):
        taken.clear()
        damaged(job, Frame(), Frame(),
                lambda f: taken.append("_grouped"),
                lambda f: taken.append("_joined_aggregate"))
        assert taken != DISPATCH[job], (job, taken)
    for job in sample.OWN_FRAME:
        try:
            damaged(job, None, Frame(),
                    lambda f: taken.append("_grouped"),
                    lambda f: taken.append("_joined_aggregate"))
        except AttributeError:
            continue
        raise AssertionError("{} did not crash on the frame it was never handed".format(job))


def check_only_the_two_join_jobs_switch_broadcasting_off():
    """The setting both join logs were captured under, and it decides what they hold.

    Left on, Spark sees a small side of a few kilobytes and broadcasts it, so the log
    records a plan with no shuffle on that side and the fixture holds the answer rather
    than the question. Read off the session call rather than off NO_BROADCAST, because the
    list and the branch that reads it are two places to remember.
    """
    assert set(sample.NO_BROADCAST) == {"join", "skewed_join"}, sample.NO_BROADCAST
    for job in sample.JOBS:
        extra = _recording_run(job, want="extra")
        names = [name for name, _value in extra]
        if job in ("join", "skewed_join"):
            assert names == ["spark.sql.autoBroadcastJoinThreshold"], (job, extra)
            assert extra[0][1] == "-1", (job, extra)
        else:
            assert extra == [], (job, extra)
