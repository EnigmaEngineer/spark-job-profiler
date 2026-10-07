"""The stage and task model, built only from events the model declares it reads.

`CONSUMES` is not a list somebody maintains. A handler registers itself against one event
name and says what the model takes from it, so the set of names this module reads is the
set of names registered. `tests/test_model.py` walks this file and refuses any other
Spark event name appearing in it, which is the only way a second place to remember gets
back in.

Three things about the file decided the shape of this model, and all three were measured
off the committed logs rather than read off the documentation.

A task duration is a subtraction. `Finish Time` minus `Launch Time` is wall time for the
task. `Executor Run Time` is the compute part and it is a smaller number.

A stage's accumulable list is sparse and its names repeat. A metric that stayed at zero is
absent rather than zero, so the key set is a property of the data. `number of output rows`
appears twice in the grouping stage of both logs under two different ids, so reading the
list into a dict keyed on the name silently drops one of them. The key is the id.

A stage total is a sum over the tasks, including the one called peak. That makes the
per task spread unreachable from the totals, which is the whole reason this model reads
the task events.
"""
import statistics
from dataclasses import dataclass, field, fields

CONSUMES = {}
HANDLERS = {}


class UnexpectedLog(Exception):
    """Raised when a log does not hold what the model needs to build anything."""


def handles(name, why):
    """Register a handler for one event name and record what the model wants from it."""
    if name in CONSUMES:
        raise UnexpectedLog("{} is handled twice".format(name))

    def register(fn):
        CONSUMES[name] = why
        HANDLERS[name] = fn
        return fn
    return register


@dataclass(frozen=True)
class Total:
    """One accumulable off a stage.

    Kept as a record rather than folded into a dict because the names are not unique and
    the values are not all numbers. A SQL metric writes its value as a string.
    """
    acc_id: int
    name: str
    value: object


# What a task field is, decided once on the field and never again anywhere else.
#
# A detector that ranks stages by a ratio needs to know which fields a ratio means
# something for. Before this, the answer was a tuple of three names in `sjp.skew` that I
# picked, so a stage skewed on disk spill got no verdict at all. The kind is declared on
# the field because a new field then cannot be added without answering the question, and
# `tests/test_model.py` refuses a field carrying no kind.
COUNT = "count"
BYTES = "bytes"
MILLIS = "millis"
# Names a thing rather than measuring an amount of it. A ratio over an executor id or a
# partition number is arithmetic on a label.
IDENTITY = "identity"
# A point on the clock. Differences between instants are quantities and the instants are
# not, so `launch_time` is here and `duration` is a declared derived quantity.
INSTANT = "instant"
# Whether something happened. Two states, so a median relative ratio has nothing to say.
FLAG = "flag"

MEASURED = (COUNT, BYTES, MILLIS)
KINDS = MEASURED + (IDENTITY, INSTANT, FLAG)

# Every accumulable Spark writes for a task metric sits under this prefix. A name without
# it belongs to one plan node, which is a different quantity reached by a different
# address. `classify` is where that split gets stated rather than assumed.
TASK_METRIC = "internal.metrics."


def measured(kind, total=None):
    """A field, the kind of quantity it holds, and the stage accumulable for the same thing.

    `total` is declared on the field rather than worked out from the field name, because
    the two spellings are not related by a rule. Camel casing the field name reproduces
    exactly one of the twelve real names. Building the name out of the task metrics leaf
    path instead reaches fourteen of thirty four. `scripts/accumulable_probe.py` measures
    both, so the reason this is a declaration is a number rather than a preference.
    """
    return field(metadata={"kind": kind, "total": total})


@dataclass(frozen=True)
class Task:
    stage_id: int = measured(IDENTITY)
    stage_attempt: int = measured(IDENTITY)
    index: int = measured(IDENTITY)
    partition: int = measured(IDENTITY)
    executor_id: str = measured(IDENTITY)
    launch_time: int = measured(INSTANT)
    finish_time: int = measured(INSTANT)
    executor_run_time: int = measured(MILLIS, TASK_METRIC + "executorRunTime")
    deserialize_time: int = measured(MILLIS, TASK_METRIC + "executorDeserializeTime")
    serialize_time: int = measured(MILLIS, TASK_METRIC + "resultSerializationTime")
    gc_time: int = measured(MILLIS, TASK_METRIC + "jvmGCTime")
    peak_memory: int = measured(BYTES, TASK_METRIC + "peakExecutionMemory")
    memory_spilled: int = measured(BYTES, TASK_METRIC + "memoryBytesSpilled")
    disk_spilled: int = measured(BYTES, TASK_METRIC + "diskBytesSpilled")
    records_read: int = measured(COUNT, TASK_METRIC + "shuffle.read.recordsRead")
    local_bytes_read: int = measured(BYTES, TASK_METRIC + "shuffle.read.localBytesRead")
    remote_bytes_read: int = measured(BYTES, TASK_METRIC + "shuffle.read.remoteBytesRead")
    records_written: int = measured(COUNT, TASK_METRIC + "shuffle.write.recordsWritten")
    bytes_written: int = measured(BYTES, TASK_METRIC + "shuffle.write.bytesWritten")
    failed: bool = measured(FLAG)

    @property
    def duration(self):
        """Wall time from launch to finish. Not a field in the log."""
        return self.finish_time - self.launch_time

    @property
    def outside_run_time(self):
        """Wall time the executor did not spend running the task body.

        Deserialising the task, serialising the result and whatever the scheduler took.
        Worth having as its own number because a task that looks slow can be slow before
        any of its work starts.
        """
        return self.duration - self.executor_run_time


# A quantity a task carries that is not a field on it. Kept as its own map because a
# property cannot hold dataclass metadata. `tests/test_model.py` walks Task for any
# property returning a number and refuses one that is absent here, so this is checked
# rather than remembered.
DERIVED = {
    "duration": MILLIS,
    "outside_run_time": MILLIS,
}


def kind_of(name):
    """The kind declared for one task quantity, field or derived.

    Raises rather than guessing, because a caller asking for the kind of something the
    record does not declare is a caller about to treat it as a number by default.
    """
    for entry in fields(Task):
        if entry.name == name:
            return entry.metadata["kind"]
    if name in DERIVED:
        return DERIVED[name]
    raise UnexpectedLog("{} is not a task quantity".format(name))


def quantities():
    """Every task field and derived value a ratio means something for, in declared order.

    Fields first in the order the log fills them, then the derived ones. `sjp.skew` reads
    this instead of holding a list, which is the whole point of the kinds above.
    """
    declared = [entry.name for entry in fields(Task)
                if entry.metadata["kind"] in MEASURED]
    return tuple(declared + sorted(DERIVED))


QUANTITIES = quantities()


@dataclass(frozen=True)
class Stage:
    stage_id: int
    attempt: int
    name: str
    declared_tasks: int
    parent_ids: tuple
    submission_time: int
    completion_time: int
    failure: str
    totals: tuple
    tasks: tuple

    @property
    def wall_time(self):
        return self.completion_time - self.submission_time

    def values(self, name):
        """Every task's value for one field, in task index order."""
        return [getattr(task, name) for task in self.tasks]

    def total(self, name):
        return sum(self.values(name))

    def median(self, name):
        return statistics.median(self.values(name))

    def largest(self, name):
        return max(self.values(name))

    def spread(self, name):
        """Largest over median, or None when the median is zero.

        It answered 0.0 before the detector was written and that was wrong in the worst
        available direction. On the skewed log's grouping stage the median spill is 0 and
        one task of eight spilled 620,755,808 bytes, so the number read as perfectly even
        for the most lopsided stage in the repo. None cannot be compared against a
        threshold without raising, which is the point of it. `sjp.skew` is where the case
        gets an answer instead of a summary.
        """
        middle = self.median(name)
        return None if middle == 0 else self.largest(name) / middle

    @property
    def peak_memory(self):
        """The largest peak any one task reached.

        Not `internal.metrics.peakExecutionMemory`, which is the sum of the per task
        peaks. Summing peaks gives a bigger answer for a stage that spread its work out,
        which is backwards for a question about memory pressure.
        """
        return self.largest("peak_memory")

    def reported_total(self, name):
        """The stage's own total for one task metric.

        Absent means zero, because Spark drops an accumulable that never moved. Raises
        when the name appears more than once, since a caller asking for one number should
        be told the question was ambiguous rather than handed whichever came first.

        A name outside the task metric namespace is refused rather than answered. Spark
        writes a plan node's own metrics into the same list under names like
        `local bytes read` and `peak memory`, one word away from a stage total and
        measuring one node rather than the whole stage. Six such names answered with a
        plan node's value on 36 stages or more before this refusal existed, and two of
        them never got refused at all, so the ambiguity was live rather than theoretical.
        `peak memory` is the one that would have cost the most, because the stage total a
        word away from it is the one metric in this file known to rank the wrong job as
        the memory problem. A plan metric is addressed by accumulator id through
        `Application.accumulator`, which is the address a plan node actually hands out.
        """
        if not name.startswith(TASK_METRIC):
            raise UnexpectedLog(
                "{} is a plan metric rather than a task metric, so it has no stage total."
                " read it by accumulator id".format(name))
        found = [total.value for total in self.totals if total.name == name]
        if len(found) > 1:
            raise UnexpectedLog("{} appears {} times on stage {}".format(
                name, len(found), self.stage_id))
        return found[0] if found else 0


def spread_text(stage, name):
    """A stage's spread for printing, and the reason when there is not one.

    Lives here rather than with the two scripts that print it. The words are a statement
    about `Stage.spread` returning None, so anyone changing that convention should have to
    walk past them. Formatting None as a float raises, which is the whole point of None,
    and a line of output is the one place the absence has to be spelled out because a
    reader who sees a blank will supply their own explanation.
    """
    ratio = stage.spread(name)
    return "no median to divide by" if ratio is None else "{:.2f}".format(ratio)


@dataclass(frozen=True)
class Job:
    job_id: int
    stage_ids: tuple
    submission_time: int
    completion_time: int
    result: str


@dataclass(frozen=True)
class Application:
    app_id: str
    name: str
    user: str
    start_time: int
    end_time: int
    properties: dict
    cores: int
    cpus_per_task: float
    jobs: tuple
    stages: tuple
    plans: tuple
    accumulators: dict
    events_used: frozenset

    @property
    def slots(self):
        """How many tasks can run at once.

        Cores alone is the wrong answer whenever a task asks for more than one cpu, and
        the amount it asks for is recorded in its own event rather than in the executor
        one. Nothing in this repo runs with a task cpus above 1, so this returns the same
        number as cores here, and a log where it does not is the case it exists for.
        """
        if not self.cpus_per_task:
            return self.cores
        return int(self.cores // self.cpus_per_task)

    @property
    def wall_time(self):
        return self.end_time - self.start_time

    def stage(self, stage_id):
        for stage in self.stages:
            if stage.stage_id == stage_id:
                return stage
        raise UnexpectedLog("no stage {} in {}".format(stage_id, self.app_id))

    def accumulator(self, acc_id):
        """One accumulator's value by id, or None when the log never reported it.

        The id is the only address a plan node hands out for its metrics, and the value
        may be on a stage or it may be on the driver. Three of the metrics this repo reads
        are driver side and never appear on a stage at all, so a lookup that searched only
        the stages would answer None for a number that is in the file.
        """
        return self.accumulators.get(acc_id)


def stage_total_fields(record=Task):
    """Accumulable name to the task field holding the same quantity, read off the fields.

    This used to be a dict kept here with twelve entries in it, which made the question
    of what the other accumulables are a judgement nobody could fail. Now a field cannot
    arrive without answering whether it has a stage total, and the map is whatever the
    fields say it is.

    Two fields claiming one accumulable is refused. The map is read in both directions,
    name to field and field to name, so a collision would make one of those readings
    quietly answer about the wrong field. `record` is an argument so that a check can
    hand it a record with the collision in it, because a refusal nothing has ever
    triggered is a refusal nobody has tested.
    """
    found = {}
    for entry in fields(record):
        name = entry.metadata["total"]
        if not name:
            continue
        if name in found:
            raise UnexpectedLog("{} is claimed by {} and by {}".format(
                name, found[name], entry.name))
        found[name] = entry.name
    return found


STAGE_TOTAL_FIELDS = stage_total_fields()

# The task metrics this model reads past, and the reason for each one. Every name here is
# in the log per task and the model does not keep it.
#
# The list is not here to be read. It is here so the twelve kept plus these cover every
# task metric the committed logs carry, which turns the coverage of the map from a
# judgement into something a check can fail. `tests/test_model.py` is where it fails.
#
# Seven of the nineteen have been nonzero somewhere in the committed logs and twelve are
# zero on every task of every one of them. A metric that has never moved here is not
# evidence that keeping it would be useless. It is the absence of evidence either way.
_LOCAL_ONLY = "remote fetch, which a run on one machine never does"
_PUSH = "push based shuffle, which is off outside a cluster that has it"
_CPU = "cpu nanoseconds rather than wall milliseconds. the wall pair is kept"
_UNRANKED = "a real quantity nothing here ranks yet"
DROPPED = {
    TASK_METRIC + "executorCpuTime": _CPU,
    TASK_METRIC + "executorDeserializeCpuTime": _CPU,
    TASK_METRIC + "resultSize": "bytes handed back to the driver. " + _UNRANKED,
    TASK_METRIC + "input.recordsRead":
        "rows off the source rather than off a shuffle. every job here reads a frame it built",
    TASK_METRIC + "shuffle.read.fetchWaitTime":
        "time blocked on a fetch. nonzero on two stage rows in the whole fixture set",
    TASK_METRIC + "shuffle.read.localBlocksFetched":
        "a block count. the bytes those blocks carried are kept",
    TASK_METRIC + "shuffle.read.remoteBlocksFetched": _LOCAL_ONLY,
    TASK_METRIC + "shuffle.read.remoteBytesReadToDisk": _LOCAL_ONLY,
    TASK_METRIC + "shuffle.read.remoteReqsDuration": _LOCAL_ONLY,
    TASK_METRIC + "shuffle.write.writeTime":
        "nanoseconds spent writing shuffle. " + _UNRANKED,
    TASK_METRIC + "shuffle.push.read.corruptMergedBlockChunks": _PUSH,
    TASK_METRIC + "shuffle.push.read.localMergedBlocksFetched": _PUSH,
    TASK_METRIC + "shuffle.push.read.localMergedBytesRead": _PUSH,
    TASK_METRIC + "shuffle.push.read.localMergedChunksFetched": _PUSH,
    TASK_METRIC + "shuffle.push.read.mergedFetchFallbackCount": _PUSH,
    TASK_METRIC + "shuffle.push.read.remoteMergedBlocksFetched": _PUSH,
    TASK_METRIC + "shuffle.push.read.remoteMergedBytesRead": _PUSH,
    TASK_METRIC + "shuffle.push.read.remoteMergedChunksFetched": _PUSH,
    TASK_METRIC + "shuffle.push.read.remoteMergedReqsDuration": _PUSH,
}

# What this model does with one accumulable name.
KEPT = "kept"
READ_PAST = "read past"
PLAN = "plan metric"
UNRULED = "unruled"


def classify(name):
    """Which of four things an accumulable name is to this model.

    A task metric that is neither kept nor recorded as read past comes back `UNRULED`
    rather than raising. A log written by a Spark this repo has never met will carry one,
    and a profiler that refuses such a log is worse than one that says what it skipped.
    The committed logs are held to zero of them by a check, so the tolerant answer here
    does not buy silence there.
    """
    if name in STAGE_TOTAL_FIELDS:
        return KEPT
    if name in DROPPED:
        return READ_PAST
    if name.startswith(TASK_METRIC):
        return UNRULED
    return PLAN


@dataclass(frozen=True)
class Coverage:
    """One stage's accumulable list, split by what the model does with each name.

    `rows` is how many entries the stage carries and `distinct` is how many names. They
    differ because a plan metric name repeats, which is the reason the key is the id.
    """
    rows: int
    kept: tuple
    read_past: tuple
    plan: tuple
    unruled: tuple
    repeated: tuple

    @property
    def distinct(self):
        return len(self.kept) + len(self.read_past) + len(self.plan) + len(self.unruled)


def coverage(stage):
    """Every accumulable one stage carries, counted rather than implied.

    The twelve the map covers is a property of the model. How much of a stage that is, is
    a property of the log, and it moves from three stages to the next because Spark drops
    an accumulable that never moved.
    """
    seen = {}
    for total in stage.totals:
        seen[total.name] = seen.get(total.name, 0) + 1
    buckets = {KEPT: [], READ_PAST: [], PLAN: [], UNRULED: []}
    for name in sorted(seen):
        buckets[classify(name)].append(name)
    return Coverage(rows=len(stage.totals),
                    kept=tuple(buckets[KEPT]),
                    read_past=tuple(buckets[READ_PAST]),
                    plan=tuple(buckets[PLAN]),
                    unruled=tuple(buckets[UNRULED]),
                    repeated=tuple(name for name in sorted(seen) if seen[name] > 1))


def totals_against_tasks(stage):
    """Each mapped metric as the stage reported it, beside the sum over its tasks."""
    return [(name, stage.reported_total(name), stage.total(field))
            for name, field in sorted(STAGE_TOTAL_FIELDS.items())]


def disagreements(stage):
    """The mapped metrics where the stage total and the task sum are different numbers."""
    return [row for row in totals_against_tasks(stage) if row[1] != row[2]]


class _Build:
    """Mutable state while the events go past. Everything it holds is frozen by `finish`."""

    def __init__(self):
        self.app = {}
        self.properties = {}
        self.cores = 0
        self.jobs = []
        self.submitted = {}
        self.completed = {}
        self.tasks = {}
        self.cpus_per_task = 0.0
        self.plans = []
        self.driver_accums = {}


@handles("SparkListenerApplicationStart", "the application id, its name and when it began")
def _application_start(state, event):
    state.app["app_id"] = event.get("App ID")
    state.app["name"] = event.get("App Name")
    state.app["user"] = event.get("User")
    state.app["start_time"] = event.get("Timestamp")


@handles("SparkListenerApplicationEnd", "when it stopped, so wall time is a subtraction")
def _application_end(state, event):
    state.app["end_time"] = event.get("Timestamp")


@handles("SparkListenerEnvironmentUpdate", "the configuration the run really used")
def _environment(state, event):
    # Spark writes this section as a list of pairs rather than as an object.
    state.properties = dict(event.get("Spark Properties", []))


@handles("SparkListenerExecutorAdded", "the cores an executor brought to the run")
def _executor_added(state, event):
    state.cores += event.get("Executor Info", {}).get("Total Cores", 0)


@handles("SparkListenerResourceProfileAdded", "how many cpus one task asks for")
def _resource_profile(state, event):
    # Cores are on the executor event and the cpus a task wants are here. Both are needed
    # before the word slot means anything, and a profiler that reads only the first one
    # answers the right number for the common case and the wrong one for the case that
    # matters.
    requests = event.get("Task Resource Requests", {})
    state.cpus_per_task = requests.get("cpus", {}).get("Amount", 0.0)


@handles("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
         "the physical plan, which is the only place a join or an exchange is named")
def _sql_execution_start(state, event):
    info = event.get("sparkPlanInfo")
    if info is not None:
        state.plans.append(info)


@handles("org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
         "metric values the driver reported, which never reach a stage")
def _driver_accum_updates(state, event):
    for pair in event.get("accumUpdates", []):
        # Written as a two element list rather than an object. The partition count of
        # every exchange in this repo arrives only here.
        state.driver_accums[pair[0]] = pair[1]


@handles("SparkListenerJobStart", "which stages belong to which job")
def _job_start(state, event):
    state.jobs.append({
        "job_id": event["Job ID"],
        "stage_ids": tuple(event.get("Stage IDs", [])),
        "submission_time": event.get("Submission Time"),
        "completion_time": None,
        "result": None,
    })


@handles("SparkListenerJobEnd", "when the job ended and whether it succeeded")
def _job_end(state, event):
    for job in state.jobs:
        if job["job_id"] == event["Job ID"]:
            job["completion_time"] = event.get("Completion Time")
            job["result"] = event.get("Job Result", {}).get("Result")


@handles("SparkListenerStageSubmitted", "the task count Spark planned and the stages it waited on")
def _stage_submitted(state, event):
    info = event["Stage Info"]
    state.submitted[(info["Stage ID"], info["Stage Attempt ID"])] = info


@handles("SparkListenerStageCompleted", "the stage timings and its accumulable totals")
def _stage_completed(state, event):
    info = event["Stage Info"]
    state.completed[(info["Stage ID"], info["Stage Attempt ID"])] = info


@handles("SparkListenerTaskEnd", "everything measured per task")
def _task_end(state, event):
    info = event["Task Info"]
    metrics = event["Task Metrics"]
    read = metrics["Shuffle Read Metrics"]
    written = metrics["Shuffle Write Metrics"]
    key = (event["Stage ID"], event["Stage Attempt ID"])
    state.tasks.setdefault(key, []).append(Task(
        stage_id=event["Stage ID"],
        stage_attempt=event["Stage Attempt ID"],
        index=info["Index"],
        partition=info.get("Partition ID", info["Index"]),
        executor_id=info["Executor ID"],
        launch_time=info["Launch Time"],
        finish_time=info["Finish Time"],
        executor_run_time=metrics["Executor Run Time"],
        deserialize_time=metrics["Executor Deserialize Time"],
        serialize_time=metrics["Result Serialization Time"],
        gc_time=metrics["JVM GC Time"],
        peak_memory=metrics["Peak Execution Memory"],
        memory_spilled=metrics["Memory Bytes Spilled"],
        disk_spilled=metrics["Disk Bytes Spilled"],
        records_read=read["Total Records Read"],
        local_bytes_read=read["Local Bytes Read"],
        remote_bytes_read=read["Remote Bytes Read"],
        records_written=written["Shuffle Records Written"],
        bytes_written=written["Shuffle Bytes Written"],
        failed=bool(info.get("Failed", False)),
    ))


def _totals_of(info):
    return tuple(Total(acc_id=entry.get("ID"), name=entry["Name"], value=entry.get("Value"))
                 for entry in info.get("Accumulables", []))


def _stage_from(key, submitted, completed, tasks):
    """Build one stage out of whichever of its three sources the log actually has.

    The completed event is preferred because it carries the timings and the totals. A log
    truncated before a stage finished still has the submitted one. A log holding only task
    events for a stage is the case that turned up while checking this, and it builds a
    stage carrying its tasks and nothing else rather than raising.
    """
    info = completed if completed is not None else submitted
    info = {} if info is None else info
    return Stage(
        stage_id=key[0],
        attempt=key[1],
        name=info.get("Stage Name", ""),
        declared_tasks=info.get("Number of Tasks", 0),
        parent_ids=tuple(info.get("Parent IDs", [])),
        submission_time=info.get("Submission Time"),
        completion_time=info.get("Completion Time"),
        failure=info.get("Failure Reason"),
        totals=_totals_of(info),
        tasks=tuple(sorted(tasks, key=lambda task: task.index)),
    )


def build(events):
    """Turn a stream of decoded event log objects into one `Application`.

    An event this module has no handler for is skipped rather than refused. A Spark
    version writing something new should not stop the profiler reading the rest.
    """
    state = _Build()
    seen = set()
    for event in events:
        name = event.get("Event")
        handler = HANDLERS.get(name)
        if handler is None:
            continue
        handler(state, event)
        seen.add(name)

    if "start_time" not in state.app:
        raise UnexpectedLog("no application start event, so there is no application here")

    keys = sorted(set(state.submitted) | set(state.completed) | set(state.tasks))
    stages = tuple(_stage_from(key, state.submitted.get(key), state.completed.get(key),
                               state.tasks.get(key, []))
                   for key in keys)
    if not stages:
        raise UnexpectedLog("{} ran no stages".format(state.app.get("app_id")))

    jobs = tuple(Job(**job) for job in state.jobs)
    # The driver values go in first so a stage reporting the same id wins. A stage
    # accumulable is the value at the end of the stage and a driver update is written
    # once, and where both exist the stage one is the later reading.
    accumulators = dict(state.driver_accums)
    for stage in stages:
        for total in stage.totals:
            accumulators[total.acc_id] = total.value
    return Application(
        app_id=state.app.get("app_id"),
        name=state.app.get("name"),
        user=state.app.get("user"),
        start_time=state.app.get("start_time"),
        end_time=state.app.get("end_time"),
        properties=state.properties,
        cores=state.cores,
        cpus_per_task=state.cpus_per_task,
        jobs=jobs,
        stages=stages,
        plans=tuple(state.plans),
        accumulators=accumulators,
        events_used=frozenset(seen),
    )
