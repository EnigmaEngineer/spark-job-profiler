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
from dataclasses import dataclass

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


@dataclass(frozen=True)
class Task:
    stage_id: int
    stage_attempt: int
    index: int
    partition: int
    executor_id: str
    launch_time: int
    finish_time: int
    executor_run_time: int
    deserialize_time: int
    serialize_time: int
    gc_time: int
    peak_memory: int
    memory_spilled: int
    disk_spilled: int
    records_read: int
    local_bytes_read: int
    remote_bytes_read: int
    records_written: int
    bytes_written: int
    failed: bool

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
        """The stage's own total for an internal metric.

        Absent means zero, because Spark drops an accumulable that never moved. Raises
        when the name appears more than once, since a caller asking for one number should
        be told the question was ambiguous rather than handed whichever came first.
        """
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
    jobs: tuple
    stages: tuple
    events_used: frozenset

    @property
    def wall_time(self):
        return self.end_time - self.start_time

    def stage(self, stage_id):
        for stage in self.stages:
            if stage.stage_id == stage_id:
                return stage
        raise UnexpectedLog("no stage {} in {}".format(stage_id, self.app_id))


# Accumulable name to the task field holding the same quantity. Every one of these was
# checked against the sum over the tasks before it went on the list.
STAGE_TOTAL_FIELDS = {
    "internal.metrics.diskBytesSpilled": "disk_spilled",
    "internal.metrics.executorDeserializeTime": "deserialize_time",
    "internal.metrics.executorRunTime": "executor_run_time",
    "internal.metrics.jvmGCTime": "gc_time",
    "internal.metrics.memoryBytesSpilled": "memory_spilled",
    "internal.metrics.peakExecutionMemory": "peak_memory",
    "internal.metrics.resultSerializationTime": "serialize_time",
    "internal.metrics.shuffle.read.localBytesRead": "local_bytes_read",
    "internal.metrics.shuffle.read.recordsRead": "records_read",
    "internal.metrics.shuffle.read.remoteBytesRead": "remote_bytes_read",
    "internal.metrics.shuffle.write.bytesWritten": "bytes_written",
    "internal.metrics.shuffle.write.recordsWritten": "records_written",
}


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
    return Application(
        app_id=state.app.get("app_id"),
        name=state.app.get("name"),
        user=state.app.get("user"),
        start_time=state.app.get("start_time"),
        end_time=state.app.get("end_time"),
        properties=state.properties,
        cores=state.cores,
        jobs=jobs,
        stages=stages,
        events_used=frozenset(seen),
    )
