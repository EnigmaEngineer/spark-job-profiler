"""Spill, and why the memory pressure behind it is mostly not in the event log.

A stage spills when an operator needs more execution memory than it was given. The log
records what was spilled. It does not record what was given, and the gap between those two
sentences is most of this module.

Four things were measured off the committed logs before any of this was written, and three
of them removed a metric this module was going to have.

**A spill is two measurements of one event.** `Memory Bytes Spilled` is the size of the
records in memory before they went out. `Disk Bytes Spilled` is the size of the same
records serialized and compressed on the way to disk. They are the same bytes counted
twice, so a total that adds them is a number describing nothing. On the skewed log's
grouping stage that total would read 708,844,603 against 620,755,808 of real data.

**The factor between them is not a constant, so there is nothing to convert with.** It is
1.9157 on the first stage of both logs and 7.0469 on the skewed grouping stage. A tool
holding one number to turn disk spill into memory spill would be wrong by 3.7 times inside
one file.

**Peak execution memory is absent exactly where the pressure was highest.** The first stage
of both logs spills 117,440,288 bytes to memory and reports a peak execution memory of zero
on every task. A pressure metric built as peak over budget reads no pressure at all on the
one stage that ran out of memory. So peak is evidence when it is there and it is not the
signal, and `pressure` says which of those it had.

**Peak execution memory does not order two jobs by whether they spill.** The balanced log's
grouping stage peaks at 167,771,904 bytes on one task and spills nothing. The skewed log's
grouping stage has seven tasks peaking between 20,971,488 and 33,554,384 that spill nothing
and one peaking at 377,486,768 that spills. The balanced job's peak is five times the
skewed job's ordinary task and it is the one that stays inside memory.

What is left is honest and smaller than the heading promises. Spill is a fact, its
concentration across tasks is a fact, and the budget it was measured against is a question
the log cannot answer.
"""
from dataclasses import dataclass

from sjp import model

# The two fields that measure one spill. Held as a pair so that anything tempted to sum
# them has to walk past the reason not to.
MEMORY = "memory_spilled"
DISK = "disk_spilled"

SPILLING = "spilling"
CLEAN = "clean"
OUTCOMES = (SPILLING, CLEAN)

# Properties that decide how much execution memory a task had. Spark's environment update
# records what was set and not what was defaulted, so an absent name here is a number the
# log does not contain rather than a number equal to Spark's default.
BUDGET_PROPERTIES = (
    "spark.executor.memory",
    "spark.executor.cores",
    "spark.memory.fraction",
    "spark.memory.storageFraction",
)


@dataclass(frozen=True)
class Spill:
    """What one stage spilled, and how few of its tasks did it.

    `inflation` is memory bytes over disk bytes and it is not a ratio anybody should act
    on. It is here so that a reader who was about to add the two numbers sees that they
    move together and that the factor is different on every stage.
    """
    stage_id: int
    outcome: str
    tasks: int
    tasks_spilling: int
    memory_bytes: int
    disk_bytes: int
    inflation: float
    concentration: float
    why: str

    @property
    def spilling(self):
        return self.outcome == SPILLING


@dataclass(frozen=True)
class Budget:
    """Execution memory per task, when the log holds enough to work it out.

    `known` is false far more often than it looks like it should be. Neither committed log
    sets any of the four properties, because both were captured in local mode where the
    driver is the executor. A caller gets the list of what is missing rather than a number
    assembled out of Spark's documented defaults, since a default is a fact about Spark and
    not a fact about the run.
    """
    known: bool
    absent: tuple
    recorded: dict


def budget(properties):
    """What the log says about the memory a task had to work in.

    Returns `known` false and names every absent property. There is deliberately no
    fallback. A pressure figure computed against a budget nobody recorded would be a
    measurement of Spark's defaults dressed as a measurement of the job.
    """
    absent = tuple(name for name in BUDGET_PROPERTIES if name not in properties)
    recorded = {name: properties[name] for name in BUDGET_PROPERTIES
                if name in properties}
    return Budget(known=not absent, absent=absent, recorded=recorded)


def inflation(stage):
    """Memory spilled over disk spilled, or None when nothing reached disk.

    None rather than zero for the same reason `Stage.spread` answers None on a zero median.
    A stage that spilled nothing has no factor, and a factor of zero would read as one that
    compressed infinitely well.
    """
    disk = stage.total(DISK)
    return None if disk == 0 else stage.total(MEMORY) / disk


def concentration(stage):
    """The share of the stage's memory spill that its largest spilling task did.

    1.0 means one task spilled everything and the rest spilled nothing, which is the shape
    the skewed log's grouping stage has. An even spill across eight tasks reads 0.125. None
    when the stage did not spill, because a share of zero bytes is not a share.
    """
    total = stage.total(MEMORY)
    return None if total == 0 else stage.largest(MEMORY) / total


def examine(stage):
    """One stage's spill as a record, with the reason spelled out either way."""
    values = stage.values(MEMORY)
    spilling = [value for value in values if value]
    memory_bytes = stage.total(MEMORY)
    disk_bytes = stage.total(DISK)

    if not memory_bytes and not disk_bytes:
        why = "no task spilled"
        outcome = CLEAN
    elif len(spilling) == 1 and len(values) > 1:
        why = "1 of {} tasks spilled all {} bytes of it".format(len(values), memory_bytes)
        outcome = SPILLING
    else:
        why = "{} of {} tasks spilled".format(len(spilling), len(values))
        outcome = SPILLING

    return Spill(stage_id=stage.stage_id, outcome=outcome, tasks=len(values),
                 tasks_spilling=len(spilling), memory_bytes=memory_bytes,
                 disk_bytes=disk_bytes, inflation=inflation(stage),
                 concentration=concentration(stage), why=why)


def scan(app):
    """Every stage's spill, in stage order."""
    return [examine(stage) for stage in app.stages]


def peak_reported(stage):
    """Whether any task of the stage reported a peak execution memory above zero.

    The first stage of both committed logs spills and reports zero on every task, so this
    is the question that decides whether peak can be read at all on a given stage. It is a
    separate function because a caller checking it is making a claim about the log and not
    about the job.
    """
    return stage.largest("peak_memory") > 0


def pressure_lines(app):
    """What can and cannot be said about memory pressure for one application.

    The budget goes first because every number under it is relative to something the log
    may not hold, and a reader who learns that at the bottom has already read the numbers
    as absolute.
    """
    money = budget(app.properties)
    lines = []
    if money.known:
        lines.append("execution memory budget readable from {}".format(
            ", ".join(sorted(money.recorded))))
    else:
        lines.append("no execution memory budget in this log. absent: {}".format(
            ", ".join(money.absent)))
        lines.append("  so spill is reported as measured and not as a share of a limit")

    for spill in scan(app):
        stage = app.stage(spill.stage_id)
        lines.append("  stage {}  {}  {}".format(spill.stage_id, spill.outcome, spill.why))
        if spill.spilling:
            lines.append("      {:<13} {} in memory  {} on disk  inflation {:.4f}".format(
                "same bytes", spill.memory_bytes, spill.disk_bytes, spill.inflation))
            lines.append("      {:<13} {:.4f} of the spill in one task".format(
                "concentration", spill.concentration))
        if peak_reported(stage):
            lines.append("      {:<13} {} largest task".format(
                "peak memory", stage.peak_memory))
        else:
            lines.append("      {:<13} zero on every task, so it says nothing here".format(
                "peak memory"))
    return lines
