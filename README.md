# spark-job-profiler

Reads a Spark event log and says why the job was slow. The Spark UI shows the numbers.
This reads the same file and names the stage that skewed, the spill that mattered and the
spill that did not.

```
python -m sjp stages tests/fixtures/eventlogs/skewed/*
```

That needs nothing but the standard library. Capturing a fresh log needs pyspark.

## Where it is

The parser, the stage and task model, the skew detector and the spill analysis are built
against two committed logs. The partition and broadcast recommendations are not. `sjp stages`
still prints measurements and no verdict about any of them, because the verdicts live in
`sjp skew` and `sjp spill`.

## One entry point, and every command says what it does to state

```
python -m sjp commands
{
  "capture": "write",
  "inventory": "read",
  "skew": "read",
  "spill": "read",
  "stages": "read"
}
```

Every command declares one of three effects when it registers. `read` changes nothing.
`write` changes state and belongs behind review when an agent is driving. `emergency`
changes state and has to run unattended because it is the recovery path. There is no
emergency command yet, because nothing here has anything to recover from.

A flag never changes the effect. A read that needs a writing variant becomes a second
command with its own name. The reason is that a guard keyed on the command name cannot see
a flag, so a flag that flips the effect is invisible to the thing meant to be watching.

The declared effect is a claim, so something tests it. Every `read` command is run against
a snapshot of a log directory and the directory has to come back with the same files
holding the same bytes.

```
python scripts/contract_probe.py
mapping, which is what a guard reads:
{
  "capture": "write",
  "inventory": "read",
  "skew": "read",
  "spill": "read",
  "stages": "read"
}
reads that changed the store: none
control, lying read caught: ['liar']
control, bad effect refused: oops declares effect 'readonly'. Expected one of ('read', 'write', 'emergency')
```

The two controls are the point. A clean first line means nothing on its own, because it is
also what a check that ran zero commands prints. The second line registers a command that
claims to be a read and writes a file, and it has to be caught. The third registers a
misspelt effect and it has to be refused at registration rather than at run time.

A `read` with no way to drive it is refused rather than skipped. A check that skips what it
cannot inspect will skip the case it was written for.

## Layout

```
sjp/cli.py          the entry point, the effect registry and the mapping
sjp/contract.py     snapshot a directory, run the reads, diff it
sjp/eventlog.py     get a log off disk, count events, say what is missing
sjp/model.py        the stage and task model, and the events it declares it reads
sjp/plan.py         the physical plan, where a join and an exchange are named
sjp/layout.py       partition counts and broadcast candidates
sjp/commands.py     the commands that exist
jobs/sample.py      five jobs, four pathologies and one shape the reader got wrong
scripts/            drivers and controls, no judgements
tests/fixtures/     six real event logs, unedited
```

## The sample jobs

Five jobs over 8,000,000 rows. Same payload in all of them. Adaptive execution off. Two more
were added in cycle 2 and they are described further down, because neither of them carries a
pathology in the query.

The first two differ in one expression. One puts 85 percent of the rows on a single key.
The other spreads them over 199.

The third joins the spread one to a 199 row relation with broadcasting switched off, so
Spark plans a sort merge join and shuffles both sides. It exists because neither of the
other two contains a join of any kind, and a tool that recommends a broadcast needs a log
where a join it could have replaced actually happened.

The fourth sends the skewed distribution through that same join. The first three name
their own partition count, and a count written into the query is not a count a config
change can move, so the profiler correctly refuses to advise on any of them. This one lets
the join demand the partitioning and Spark takes the count from the session. It is the log
the recommendation can actually be applied to. See what happened when it was.

The fifth is not a pathology. It repartitions the spread distribution on the key with no
count, which is a third origin the profiler was reading as a typed number. It is committed
twice, once at the default session count and once at 5, because that pair is what shows the
count belongs to the config rather than to the query.

The point is not that they are realistic. It is that the pathology is known before the
profiler is pointed at the log, so the profiler can be graded instead of believed.

Measured on this machine under pyspark 3.5.6 and OpenJDK 11.0.32.1. The three later
logs were captured under OpenJDK 21.0.10 and the key exchange reproduced to the byte.

```
skewed    stage 0  tasks 2 of 2
    records   median 0.0  max 0  spread no median to divide by
    duration  median 3886.0 ms  max 3895 ms  spread 1.00
    spilled   117440288 memory  61304287 disk
    peak      0 largest task  0 summed by the stage
skewed    stage 1  tasks 8 of 8
    records   median 1000000.0  max 1000000  spread 1.00
    duration  median 492.0 ms  max 855 ms  spread 1.74
    spilled   0 memory  0 disk
    peak      0 largest task  0 summed by the stage
skewed    stage 2  tasks 8 of 8
    records   median 156784.0  max 6932663  spread 44.22
    duration  median 207.0 ms  max 3048 ms  spread 14.72
    spilled   620755808 memory  88088795 disk
    peak      377486768 largest task  562035856 summed by the stage
balanced  stage 0  tasks 2 of 2
    records   median 0.0  max 0  spread no median to divide by
    duration  median 3671.5 ms  max 3686 ms  spread 1.00
    spilled   117440288 memory  61304287 disk
    peak      0 largest task  0 summed by the stage
balanced  stage 1  tasks 8 of 8
    records   median 1000000.0  max 1000000  spread 1.00
    duration  median 542.0 ms  max 849 ms  spread 1.57
    spilled   0 memory  0 disk
    peak      0 largest task  0 summed by the stage
balanced  stage 2  tasks 8 of 8
    records   median 984924.5  max 1206030  spread 1.22
    duration  median 938.0 ms  max 1258 ms  spread 1.34
    spilled   0 memory  0 disk
    peak      167771904 largest task  1166014800 summed by the stage
```

The first stage is the row worth stopping on. It is the same expression in both jobs and
it spills the same bytes in both. That does not make it harmless. Two tasks spilling 117 MB
on a range scan is a real cost and a tuned job would not pay it. What it is not is evidence
of skew, and a detector that sums spill over an application cannot tell the two apart. It
reports both jobs as spilling and says the same thing about the one with a 44x hot key and
the one without.

So spill has to be attributed to a stage, and a stage has to be compared against something,
before a number about it means anything.

Those two totals being equal to the byte is also the evidence that the two jobs really do
differ in one expression. That was not built as a control and it works as one.

Regenerate either log with

```
python -m sjp capture --job skewed   --out <dir> --rows 8000000
python -m sjp capture --job balanced --out <dir> --rows 8000000
```

Figures above come out of `tests/run_all.py`, which asserts them against the committed
logs. `docs/adr-0001-what-an-event-log-actually-holds.md` has where each number lives in
the file.

## The stage and task model

One application, its jobs, its stages and every task in them. A task duration is
`Finish Time` minus `Launch Time`, which is not a field in the log. `Executor Run Time` is
the compute part and it is a smaller number, so both are kept and the difference between
them is its own number.

Run it with `python -m sjp stages tests/fixtures/eventlogs/skewed/*` and the skewed
fixture comes back like this.

```
local-1790267120237  sjp-skewed  2 cores  14063 ms  1 job  3 stages
  shuffle partitions 8
  stage 0  2 of 2 tasks  4044 ms wall
      duration ms   median 3886.0  max 3895  spread 1.00
      records read  median 0.0  max 0  spread no median to divide by
      outside run   median 101.5  max 103  spread 1.01
      spilled       117440288 memory  61304287 disk
      peak memory   0 largest task  0 summed by the stage
      stage totals  12 mapped, 4 absent, 0 disagree with the tasks
      accumulables  19 rows, 8 kept, 5 read past, 6 plan, 0 unruled
  stage 1  8 of 8 tasks  2278 ms wall
      duration ms   median 492.0  max 855  spread 1.74
      records read  median 1000000.0  max 1000000  spread 1.00
      outside run   median 11.5  max 16  spread 1.39
      spilled       0 memory  0 disk
      peak memory   0 largest task  0 summed by the stage
      stage totals  12 mapped, 5 absent, 0 disagree with the tasks
      accumulables  35 rows, 8 kept, 18 read past, 9 plan, 0 unruled
  stage 2  8 of 8 tasks  3879 ms wall
      duration ms   median 207.0  max 3048  spread 14.72
      records read  median 156784.0  max 6932663  spread 44.22
      outside run   median 16.0  max 38  spread 2.38
      spilled       620755808 memory  88088795 disk
      peak memory   377486768 largest task  562035856 summed by the stage
      stage totals  12 mapped, 3 absent, 0 disagree with the tasks
      accumulables  37 rows, 10 kept, 17 read past, 9 plan, 0 unruled
        number of output rows appears more than once, so the id is the key
```

Nothing there is a verdict. A spread of 44.22 is a measurement beside the thing it should
be compared against, and deciding that it is a problem belongs to `sjp skew`.

Stage 0 reads `no median to divide by` rather than a number. That is the subject of the
next section and it is the only line here that is not a measurement.

### The events it reads are the events it registers

Each handler registers itself against one event name and says what the model takes from
it, so the set of names the profiler needs is read out of the code rather than kept in a
list beside it. `sjp inventory` reports a log as complete against that set. A check walks
the module and refuses any Spark event name written into it that no handler claimed,
because an inline comparison against an unregistered name is how the list and the parser
would come apart again.

### The stage totals are right, and that is why they are not enough

Spark reports the same metrics twice. Every stage carries accumulable totals, and every
task carries its own copy. Twelve metrics over six stages, and every total the log
reported equals the sum over that stage's tasks exactly.

```
skewed    stage totals: 24 present, 12 absent, 0 disagree with the task sums
balanced  stage totals: 22 present, 14 absent, 0 disagree with the task sums
control, a stage missing one task disagrees: yes
control, a repeated total name is refused: yes
controls: 0 of 2 failed
```

A sum cannot hold a spread, which is the whole question. The one that would have caused
damage is `internal.metrics.peakExecutionMemory`, because it is a sum of per task peaks
and the balanced job's stage total is more than twice the skewed job's while the balanced
job spilled nothing. `docs/adr-0002-what-a-stage-total-can-and-cannot-say.md` has the
numbers and the two other traps in that list.

### Twelve of how many

Twelve is the size of the map. It is not a fraction of anything until the denominator is
printed, and for a long time it was not. The `accumulables` line is that denominator and
`scripts/accumulable_probe.py` is where it comes from.

```
the map, read off the fields
  12 task fields carry a stage total, 8 do not
  19 task metrics are read past, with a reason recorded for each
every accumulable name over all 8 logs
  kept        12
  read past   19
  plan metric 15
  unruled     0
  46 distinct names in all
what the model reads past, and whether it has ever moved here
  7 of 19 nonzero somewhere, 12 zero on every stage of every log
```

Every name an accumulable list carries is one of four things. A task metric the model keeps
per task. A task metric it reads past, with the reason written down. A metric belonging to
one plan node rather than to the stage. Or a task metric nobody here has ruled on, which no
committed log holds and which a log from a newer Spark would.

That last class is reported rather than refused. A profiler that will not read a log because
it met an unfamiliar metric is worse than one that names what it skipped.

The twelve read past that have never moved are the nine push based shuffle names and three
remote fetch names. None of them can move on one machine, so their zeros say nothing about
whether keeping them would be worth it. The seven that have moved are the honest gap.

### The name is not an address for a plan metric

Spark writes a plan node's own metrics into the same list, under names like
`local bytes read` and `peak memory`. Strip the namespace and the capitals off and three of
them are byte identical to a task metric leaf, and two of those reach a metric the model
keeps.

```
plan metric names that spell the same thing as a task metric
  fetch wait time    answered on  36 stages, and spells
      read past   internal.metrics.shuffle.read.fetchWaitTime
  local bytes read   answered on  36 stages, and spells
      kept        internal.metrics.shuffle.read.localBytesRead
  records read       answered on  36 stages, and spells
      read past   internal.metrics.input.recordsRead
      kept        internal.metrics.shuffle.read.recordsRead
  task metric leaf spellings carried by more than one name: 1
      recordsread is the leaf of internal.metrics.input.recordsRead, internal.metrics.shuffle.read.recordsRead
```

The stage total reader used to answer those names with a plan node's number. `peak memory`
is the one that would have cost the most, because the stage total a word away from it is
the metric this README already says ranks the wrong job as the memory problem. It is
refused now, and a plan metric is read by accumulator id instead, which is the address a
plan node actually hands out.

## Naming the stage that skewed

```
python -m sjp skew tests/fixtures/eventlogs/skewed/* \
      --metric records_read --metric duration --metric memory_spilled --metric serialize_time
local-1790267120237  sjp-skewed  threshold 4.0  12 verdicts
  worst count   stage 2 on records_read at 44.2179
  worst bytes   stage 2 on memory_spilled at unbounded
  worst millis  stage 2 on duration at 14.7246
  3 skewed, 2 even, 7 undecided
  sjp 0.2.0  exit 1 over metric set selected 4:0d5a3c78
  skewed    stage 2  records_read         44.2179  largest task 6932663 against a median of 156784
  skewed    stage 2  memory_spilled     unbounded  more than half the tasks read zero and one reads 620755808
  skewed    stage 2  duration             14.7246  largest task 3048 against a median of 207
  even      stage 1  records_read          1.0000  largest task 1000000 against a median of 1000000
  even      stage 1  duration              1.7378  largest task 855 against a median of 492
  undecided stage 0  records_read        no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 0  memory_spilled      no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 1  memory_spilled      no ratio  every task reads zero, so there is no spread
  undecided stage 0  duration            no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 0  serialize_time      no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 1  serialize_time      no ratio  every task reads zero, so there is no spread
  undecided stage 2  serialize_time      no ratio  largest task 3 is under the millis floor of 50
```

Four metrics are named there to keep the block readable. The default is every quantity a
task carries, which is fourteen of them, and on this log that is 42 verdicts reading
`8 skewed, 6 even, 28 undecided`. The default used to be three names and
`### The metric set was three names I chose` below is about what widening it cost.

The same command on the balanced log, which differs from the skewed one in one expression.

```
  nothing skewed at this threshold
  0 skewed, 11 even, 31 undecided
  sjp 0.2.0  exit 0 over metric set selected 4:0d5a3c78
```

It exits 1 when something skewed, so a shell can act on the answer. The line above says
which set that status was counted over, which is the part a shell cannot work out for
itself. `## The exit status, and the set it is counted over` below is about why.

### There is a third verdict and it carried the day

A verdict is `skewed` or `even` or `undecided`. The third one is the addition.

Twenty eight of the 42 verdicts on the skewed log are `undecided` and none is a gap in the
tool. Fourteen are the two task stage. With two tasks the largest value is one of the two the
median averages, so the ratio is `2b / (a + b)` and it cannot reach 2 however extreme the
pair. That was searched rather than assumed.

```
n=1   values [1]*0 + [1e6] -> spread 1.0000
n=2   values [1]*1 + [1e6] -> spread 2.0000
n=3   values [1]*2 + [1e6] -> spread 1000000.0000
```

So a stage below three tasks gets no verdict rather than a reassuring one. The rest are a
metric no task moved, which is a different answer from a metric spread evenly, and a metric
whose largest value is under the floor described below.

### The zero median was answering 0.0 and that was the worst available answer

`Stage.spread` returned 0.0 when the median was zero, on the grounds that dividing by zero
would raise. That reads as perfectly even. On the skewed job's grouping stage it was
describing this.

```
memory_spilled   sorted=[0, 0, 0, 0, 0, 0, 0, 620755808]  median=0.0  max=620755808  1 of 8 tasks non zero
disk_spilled     sorted=[0, 0, 0, 0, 0, 0, 0, 88088795]  median=0.0  max=88088795  1 of 8 tasks non zero
gc_time          sorted=[0, 0, 0, 0, 0, 0, 0, 71]  median=0.0  max=71  1 of 8 tasks non zero
```

One task of eight spilled and the other seven spilled nothing. A profiler calling that
stage even on its spill would be wrong about the only thing the file was captured to show.

`Stage.spread` returns None there now. None raises when it is compared against a threshold,
which is why it was picked over 0.0 and over an exception. A caller that forgets gets a
stack trace instead of a wrong answer. When the median is zero and the maximum is not,
`sjp skew` answers `skewed` and records the ratio as unbounded.

This was expected to need a constructed fixture. It fires on captured data instead.

### Where the threshold came from

Over both logs, on every stage with enough tasks to answer, across three metrics.

```
largest healthy: ['1.7407 skewed stage 1 executor_run_time', '1.7378 skewed stage 1 duration', '1.5747 balanced stage 1 executor_run_time']
smallest guilty: ['14.7246 skewed stage 2 duration', '16.0422 skewed stage 2 executor_run_time', '44.2179 skewed stage 2 records_read']
default threshold 4.0 sits between 1.7407 and 14.7246
```

Any value between 1.7407 and 14.7246 behaves identically on every stage here. Two logs
bound the threshold and do not determine it, so the default is 4.0 and the range is
published next to it. A check asserts the default stays inside the measured range, so a
fixture that narrows it fails the suite rather than quietly invalidating the constant.

`docs/adr-0003-what-a-median-relative-threshold-cannot-say.md` has the rejected options.

### The metric set was three names I chose

`records_read`, `duration`, `memory_spilled`. The three a person reaches for, which is why
they were the wrong answer. A stage skewed only on disk spill or on remote bytes read got no
verdict at all, and the command's exit status is computed over whatever was judged, so a real
skew outside the three read as a clean run.

The set now comes from the task record. Every field on `model.Task` declares what kind of
thing it is, and a check refuses a field that declares nothing.

```
count    records_read  records_written
bytes    peak_memory  memory_spilled  disk_spilled  local_bytes_read  remote_bytes_read  bytes_written
millis   executor_run_time  deserialize_time  serialize_time  gc_time  duration  outside_run_time
```

An executor id and a launch time are declared too, as `identity` and `instant`, and neither
is judged. A ratio over a launch time is arithmetic on a clock reading.

That sentence was not true until 2026-10-06. The set the tool chooses never held either of
them, and `--metric launch_time` was accepted and scanned for eleven days. It is refused
now and the section on the exit status below is where that is written down.

### Widening it broke the control, and that is what the floor is for

The balanced log is the fixture whose job is to have nothing wrong with it. Over fourteen
metrics it reported a skewed stage.

```
python -m sjp skew tests/fixtures/eventlogs/balanced/* --metric serialize_time
local-1790267152313  sjp-balanced  threshold 4.0  3 verdicts
  nothing skewed at this threshold
  0 skewed, 0 even, 3 undecided
  sjp 0.2.0  exit 0 over metric set selected 1:4d4de2bc
  undecided stage 0  serialize_time      no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 1  serialize_time      no ratio  every task reads zero, so there is no spread
  undecided stage 2  serialize_time      no ratio  largest task 8 is under the millis floor of 50
```

The last of those three was `skewed` with an unbounded ratio before the floor existed. Seven tasks
serialized their result in zero milliseconds and the eighth took 8. The zero median rule that
the section above calls the most extreme skew a stage can reach cannot tell 8 milliseconds
from 620,755,808 bytes, because a ratio carries no unit.

So a verdict now needs the largest value to clear a floor, and the floor is per unit.
Milliseconds are bounded on both sides by the two logs.

```
millisecond values that produce a skewed verdict with no floor: [3, 8, 24, 29, 71, 3040, 3048]
the floor of 50 sits in the gap between 29 and 71
skewed verdicts on the skewed log by floor: {0: 11, 30: 8, 50: 8, 100: 7, 3041: 6}
```

Every floor from 30 to 71 gives the same answer, which is what a gap means. Where in that gap
the number goes is a judgement and not a measurement, so the sweep is published beside it.

Bytes and counts are bracketed by two fixtures rather than by one, because every log here
measured bytes in the tens of millions until a 600 row capture populated the small end. Both
floors are derived from their bracket rather than typed. The derivation and what it does not
settle are in `docs/adr-0006`.

```
bytes  noise       7449  real   59191004  span   7946x
count  noise        518  real    6932663  span  13383x
bytes  floor     700000  which is   93x the noise end and a 84th of the real
count  floor      60000  which is  115x the noise end and a 115th of the real
```

The 600 row log also falsified the millisecond bracket above, which it was not captured to
test. One of its tasks runs 556 milliseconds on 600 rows, so the largest millisecond value
that is noise is eight times 71, the smallest anything here calls real. That bracket is
inverted and the derivation refuses it, so the millisecond floor stays the hand placed 50.

### One worst stage was the metric order in disguise

`worst` used to return a single verdict, ranking an unbounded ratio above every finite one.
Over three metrics that was nearly harmless. Over fourteen the skewed grouping stage carries
three unbounded verdicts, on garbage collection, on memory spill and on disk spill. They tie
on the ratio because all three are infinite, they tie on concentration because one task did
all of each, and the single answer was decided by which metric the scan reached first. It was
naming a 71 millisecond pause ahead of a 620,755,808 byte spill.

There is no conversion between a millisecond and a byte, so the report gives one worst per
unit and stops pretending otherwise. Those lines sit at the top of the output as of
2026-10-03 and the same argument is why the ranked body groups by unit before it sorts.

## What spilled, and what the log will not say about why

```
python -m sjp spill tests/fixtures/eventlogs/skewed/*
local-1790267120237  sjp-skewed
no execution memory budget in this log. absent: spark.executor.memory, spark.executor.cores, spark.memory.fraction, spark.memory.storageFraction
  so spill is reported as measured and not as a share of a limit
  stage 0  spilling  2 of 2 tasks spilled
      same bytes    117440288 in memory  61304287 on disk  inflation 1.9157
      concentration 0.5000 of the spill in one task
      peak memory   zero on every task, so it says nothing here
  stage 1  clean  no task spilled
      peak memory   zero on every task, so it says nothing here
  stage 2  spilling  1 of 8 tasks spilled all 620755808 bytes of it
      same bytes    620755808 in memory  88088795 on disk  inflation 7.0469
      concentration 1.0000 of the spill in one task
      peak memory   377486768 largest task
```

It exits 1 when anything spilled. That status means the job spilled and it does not mean the
job has a problem, because both committed logs exit 1. Which spill is pathological is the
`memory_spilled` line in `sjp skew`, where the skewed log answers unbounded and the balanced
log answers even.

### The healthy job spills the same bytes as the broken one

Stage 0 of the balanced log is identical to stage 0 of the skewed log, to the byte, in both
spill columns. The two jobs differ in one expression and it is not in that stage. So a tool
reporting that a stage spilled is reporting something both jobs do.

What separates them is how few tasks did it. Stage 0 spreads 117,440,288 bytes over both of
its tasks and reads a concentration of 0.5. The skewed grouping stage puts all 620,755,808
bytes in one task of eight and reads 1.0.

### A spill is two measurements of one event

`Memory Bytes Spilled` is the size of the records in memory. `Disk Bytes Spilled` is the size
of the same records serialized and compressed on the way out. Adding them counts the same
bytes twice. On the skewed grouping stage the sum reads 708,844,603 against 620,755,808 of
real data.

The factor between them is not a constant either, so there is nothing to convert one into the
other with. It is 1.9157 on stage 0 of both logs and 7.0469 on the skewed grouping stage,
which is 3.68 times apart inside one file. `inflation` is printed to make that visible rather
than to be acted on.

### Peak execution memory is missing exactly where the pressure was

Stage 0 of both logs spills 117,440,288 bytes and reports a peak execution memory of zero on
every task. A pressure metric built as peak over a budget reads no pressure at all on the one
stage in the file that ran out of room.

It does not order two jobs by whether they spill either. The balanced grouping stage peaks at
167,771,904 bytes on one task and spills nothing. Seven of the skewed grouping stage's eight
tasks peak at 33,554,384 or below and also spill nothing. The eighth peaks at 377,486,768 and
spills all of it. The higher peak belongs to the job that stayed inside memory.

So peak is evidence where it is present, and the report says which of those it had on every
stage rather than printing a zero that reads like a measurement.

### There is no memory budget in either log

`spark.executor.memory`, `spark.executor.cores`, `spark.memory.fraction` and
`spark.memory.storageFraction` decide how much execution memory a task gets. None of the four
is in either log. Spark's environment update records what was set and not what was defaulted,
and both logs were captured in local mode where the driver is the executor.

Both logs do set `spark.driver.memory` to `1g` and that is deliberately not read as the
budget. It would be right for these two logs and wrong for every log captured on a cluster.

So there is no headroom figure here. A percentage computed against Spark's documented
defaults would be a measurement of the defaults, and `budget` returns the list of what is
missing instead.

`docs/adr-0004-what-the-log-cannot-say-about-memory-pressure.md` has the metrics this
removed.

## Partition counts and broadcast candidates

`sjp layout` reads the physical plan, which is the only record of what Spark decided to do.
A stage boundary looks the same whether it feeds an aggregate or a join, so nothing in the
stage model can tell a shuffle a broadcast would remove from one that nothing would.

```
python -m sjp layout tests/fixtures/eventlogs/join/local-*
local-1790609373067  sjp-join  2 cores  1.0 cpus a task  2 slots
  hashpartitioning into 8 partitions  chosen by ENSURE_REQUIREMENTS
      exchanges     2 into stage 3, and one count decides them
      bytes         163272682 measured  832000000 estimated
      bytes         2401 measured  4776 estimated
      bytes         163275083 measured in total, which is what the count divides
      plan text     agrees at 8
      per partition 20409385 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    4 rather than 8. 163275083 bytes at an advisory 67108864 wants 3, and the 2 slots round it to 4
      schedule after 2 rounds rather than 4 rounds
  RoundRobinPartitioning into 8 partitions  chosen by REPARTITION_BY_NUM
      bytes         42048255 measured  128000000 estimated
      plan text     agrees at 8
      per partition 5256032 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. REPARTITION_BY_NUM is an argument in the query, so the config does not decide it
  large side of SortMergeJoin  leave it shuffled
      why           832000000 estimated, which is over the 10485760 threshold
  small side of SortMergeJoin  broadcast it
      why           4776 estimated against a 10485760 threshold, and the other side shuffled 163272682
  2 worth changing
```

### There are two sizes and they answer different questions

Every exchange reports a `data size` and a `shuffle bytes written`. Both are bytes, both
are about the same operator, and using the wrong one is not a rounding error.

`data size` is an estimate. It is the row count multiplied by a width taken from the
schema, and it divides to a whole number of bytes every time.

```
832000000 / 8000000 = 104 bytes a row
128000000 / 8000000 = 16 bytes a row
     4776 /     199 = 24 bytes a row
```

`shuffle bytes written` is measured, serialized and compressed, and it does not divide
evenly. 130788590 over the same 8000000 rows is 16.34857375.

The two jobs that differ in one expression are what settled this. The estimate reads
832000000 on the key exchange of both of them. The measurement reads 130788590 on the
skewed job and 163272682 on the balanced one.

The estimate cannot see the data. A recommender reading it hands the sick job the advice
for the healthy one. There is no conversion available either. The factor between the two
runs 1.9892, 3.0441, 5.0958 and 6.3614 across the four exchanges here.

Partition sizing therefore reads the measured number, which is what adaptive execution
compares its advisory size against. A broadcast decision reads the estimate, which is what
Spark's own planner compares against the broadcast threshold.

### The measured number is smaller on the job with the problem

This is the reading that makes a volume only recommendation wrong rather than imprecise.

```
skewed    hashpartitioning  130788590 bytes written
balanced  hashpartitioning  163272682 bytes written
```

The hot key compresses. Eighty five percent of the rows carry the same value, so the job
with the pathology writes 32484092 fewer bytes than the healthy one. Sized on volume alone
the sick job gets fewer partitions than the job that is fine, which is the opposite of the
advice anybody wants.

### Most partition counts are not the config's to change

An exchange records who chose its count. `ENSURE_REQUIREMENTS` means Spark took it from
`spark.sql.shuffle.partitions`. `REPARTITION_BY_NUM` means it is an argument in the query.

Both aggregate logs are the second kind throughout, so `sjp layout` prints no target for
either of them and names the origin instead. The recommendation this command exists for
correctly has nothing to say on two of the three logs in this repo.

### Slots are cores only while a task asks for one cpu

The cores an executor brought are on one event and the cpus a task asks for are on another.
Both committed jobs run two cores at one cpu a task, so slots and cores are the same number
here and a profiler reading cores alone would look correct on every log in this repo.

### What the log will not say

A partition count cannot split a single key. Hash partitioning sends one key to one
partition however many partitions there are, so a target computed from volume is not advice
for a stage skewed on its key, and the log carries no key distribution to tell the two
cases apart.

Every derived table in this README and in the decision records is printed by
`scripts/figures.py`, so a figure here and the log it came from cannot drift apart without
the comparison failing.

`docs/adr-0005-two-sizes-and-which-question-each-one-answers.md` has the rest, including
what happened to the stage ids between two captures of the same job.

## Applying the recommendation, and what it did

The three jobs above all write their own partition count into the query, so `sjp layout`
correctly refuses to advise on any of them. A recommendation nothing can act on cannot be
benchmarked. The fourth job exists to close that gap. It sends the skewed distribution
through a sort merge join, so the join is what demands the partitioning and Spark takes
the count from `spark.sql.shuffle.partitions`. On that log the command has something to
say, and the advice can be applied and timed.

Putting the hot key through a join rather than a grouped aggregate is not decoration.
Spark puts a partial aggregate in front of the shuffle whenever the query allows it, and a
partial aggregate over two hundred keys empties the shuffle of the rows the skew is made
of. The join carries every row across the boundary.

On its log the sizing arithmetic in `sjp layout` reads the key exchange at 88,788,038
measured bytes and wants 2 partitions rather than the 8 the session used.

```
python scripts/benchmark.py --job skewed_join --rows 8000000 --arm current=8 --arm advised=2 --arm volume=3 --arm rounded=4 --passes 6 --out <dir>
python scripts/benchmark.py --report <dir>/manifest.json
```

Six passes an arm. Each run is its own process, so no arm inherits a warm session from
another. The arm order rotates, so each one runs first exactly once. The first pass is
discarded. Three against three cannot return anything under 0.05 however the numbers land,
and `sjp.bench` refuses to report a p for a comparison that size rather than printing one
beside a note that it could never have mattered.

The schedule was four until 2026-10-08 and four was the wrong default. Four against four
is 70 splits, and the only p under 0.05 that 70 splits can produce is the floor itself at
0.0286. So a four pass schedule has exactly one way to say separated, and it says it at
the weakest point on the scale. Six against six is 924 splits and 23 reachable values
below 0.05, with a floor of 0.0022.

The stage below is the one that reads every row. It is addressed by its row count and not
by its number, because a join submits both sides at once and whichever the scheduler takes
first gets the lower stage id.

```
the stage that reads every row, which is the one the job is about
  current   8 tasks  wall 5394 5289 5261 5173 5148 4995  largest task 4223 4117 4050 3910 3921 3842  median task 320 366 318 372 412 282
  advised   2 tasks  wall 5835 5454 5583 5276 6108 5234  largest task 5821 5439 5575 5265 6097 5220  median task 3856 3458 3623 3301 4032 3356
  volume    3 tasks  wall 5487 5325 5467 5251 5173 5378  largest task 5475 5312 5455 5242 5162 5367  median task 1221 1283 1305 1081 1039 1063
  rounded   4 tasks  wall 5665 5222 5805 5371 5460 5728  largest task 4616 4395 4841 4521 4542 4844  median task 1014 792 946 802 878 870
  wall advised against current  1.0713  separated  p 0.0238 against a floor of 0.0022
  wall volume against current  1.0263  undecided  p 0.0996 against a floor of 0.0022, so undecided rather than equal
  wall rounded against current  1.0637  separated  p 0.0173 against a floor of 0.0022
  largest task advised against current  1.3887  separated  p 0.0022 is the floor for 6 and 6 passes, so the arms do not interleave and no run of this size could report less
  largest task volume against current  1.3304  separated  p 0.0022 is the floor for 6 and 6 passes, so the arms do not interleave and no run of this size could report less
  largest task rounded against current  1.1536  separated  p 0.0022 is the floor for 6 and 6 passes, so the arms do not interleave and no run of this size could report less
```

### Taking the advice made the job no faster and its worst task 39 percent slower

The stage barely moved. 5,210 ms against 5,582 ms on the means is a ratio of 1.0713, and at
six passes that does separate, at a p of 0.0238. It is a 7 percent change on the stage
wall. None of the three stage wall totals separated in either direction.

The largest task moved and it moved the wrong way. 4,010 ms became 5,570 ms. All three cut
counts separated against the current one on this metric, and all three landed on the floor.
Landing there means no reading of one arm sits inside the other, which is the strongest
ordering available at any pass count rather than a strong p.

The reason is the thing this README already said before any of it was run. Hash partitioning
sends one key to one partition however many partitions there are. Eighty five percent of the
rows are on a single key, so that key's partition is the critical path at any count. Cutting
from 8 to 2 does not move the hot key. It moves the other fifteen percent onto fewer
partitions, so the hot task carries more and finishes later.

The advisory size the recommendation is built on is a total divided by a target. A total
cannot see a distribution. That was an argument on an earlier read of this file and it is a
measurement now.

### At four passes I ran the same schedule twice and three of its four separations did not come back

The first schedule reported four comparisons as separated at the floor. The second reported
one.

| largest task comparison | first | second | what changed |
| --- | --- | --- | --- |
| advised against current | 1.3979 | 1.5170 | separated both times |
| volume against current | 1.4089 | 1.3236 | separated, then undecided at p 0.0571 |
| rounded against current | 1.1933 | 1.2047 | separated, then undecided at p 0.0571 |
| rounded against volume | 0.8470 | 0.9102 | separated, then undecided at p 0.3143 |

This is a table rather than a fenced block because no command prints it. Both columns are
four pass schedules and neither of them is the output block above any more, which now
carries a six pass schedule. Neither manifest survived either. Both were written to a
scratch directory and lost. That is the reason the manifest exists at all and it was thrown
away anyway, twice.

None of the three reversed. Every one stayed on the same side of 1 and lost its separation,
and two came back at 0.0571, which is one reading out of order.

That is what a floor of 0.0286 buys. Four passes an arm gives seventy orderings and the
lowest reachable p is two of them, so a separated verdict means the two arms do not
interleave at all. One reading moving by a few hundred milliseconds ends it. Four of those
were being read here as results. One of them is a result.

### Six passes an arm, run twice on 2026-10-08, and what came back

The fix for the section above was supposed to be more passes so p stops sitting on the
floor. That is not what more passes do, and the first schedule said so immediately.

| largest task against current | first | second | outcome |
| --- | --- | --- | --- |
| advised | 1.3537 | 1.3887 | separated at the floor both times |
| volume | 1.3338 | 1.3304 | separated at the floor both times |
| rounded | 1.1728 | 1.1536 | separated at the floor both times |

Three for three, twice, on the claim this project actually makes. At four passes the volume
and rounded comparisons were the two that kept collapsing to 0.0571. At six they hold.

They still land on the floor, and they always will. A pair of arms that does not interleave
produces the two extreme splits and nothing more extreme, so its p is 2 over the split count
at every schedule size. The floor is not a symptom of too few passes. What six passes change
is where the floor sits, from 0.0286 and barely inside 0.05 to 0.0022 and a factor of 23
clear of it. The reading is the same shape and it is worth more.

The honest half is that nothing else reproduced. Stage wall total came back undecided on
both schedules and flipped direction between them. On the first schedule its three ratios
sat within 3 percent of the current count in both directions. On the second all three sat
below it by 4 to 10 percent. Application wall separated twice on the second schedule at p
0.0433 and p 0.0087, having separated nowhere on the first. The first schedule's manifest
was lost when the scratch directory it was written to was reclaimed mid run, which is the
third time that has cost this file a manifest, so the second schedule is the one published
above and its logs are kept outside the repo.

So six passes bought reproducibility on the metric that is about the hot task and bought
nothing on the two wall clock totals. Those two are measuring the machine.

The application wall is the other figure that did not survive. On the first schedule it sat
between 32,401 ms and 32,750 ms on every arm, which reads like a quantity nothing touches.
On the second it runs from 21,932 ms to 56,897 ms, because two runs took roughly twice as
long as their neighbours for reasons outside the job. The tight band was luck and it had
been written down as a property.

What did survive is worth naming too. The cut against the current count on the largest task
separated both times and grew. The key exchange reproduced at 88,788,038 bytes to the byte
across both schedules and across two major Java versions. The spread collapse below
reproduced in kind on every pass. The guard in `sjp layout` rests on the one comparison that
came back twice, and that is the only reason to trust it.

### The skew detector goes blind at the count the layout command recommends

Worth reading beside the table above. The spread on the carrying stage runs 7.58 to 9.91 at
8 partitions and 2.01 to 2.76 at 2. A detector ranking stages by a median relative ratio
stops naming this stage at all on the advised count, and on all four of those passes it
names a stage that reads no rows.

```
the stage a spread ranking would name, which is not always that one
  current   stage 3  8 tasks  spread 9.78
  current   stage 3  8 tasks  spread 9.91
  current   stage 3  8 tasks  spread 7.58
  current   stage 3  8 tasks  spread 9.37
  advised   stage 1  8 tasks  spread 2.24  not the carrying stage
  advised   stage 2  8 tasks  spread 2.76  not the carrying stage
  advised   stage 2  8 tasks  spread 2.17  not the carrying stage
  advised   stage 2  8 tasks  spread 2.01  not the carrying stage
  volume    stage 3  3 tasks  spread 3.90
  volume    stage 3  3 tasks  spread 4.55
  volume    stage 3  3 tasks  spread 2.91
  volume    stage 2  8 tasks  spread 35.71  not the carrying stage
  rounded   stage 3  4 tasks  spread 5.51
  rounded   stage 3  4 tasks  spread 4.95
  rounded   stage 3  4 tasks  spread 3.60
  rounded   stage 3  4 tasks  spread 5.31
```

The skew did not go away. The largest task got slower. What went away is the contrast,
because with two partitions the hot partition and the median partition are the same
partition. A median relative ratio is a statement about the partition count as much as about
the data, and it is least informative exactly where the partitions are fewest.

One pass on the volume arm is worse than going quiet. It ranks stage 2 at a spread of 35.71
and stage 2 reads no rows. So the failure is not only that the ratio shrinks on the stage
that matters. It is that the ratio can grow on a stage that does not, and a reader sorting by
it has no way to tell those two apart from the number alone. That is the reason
`bench.carrying_stage` exists and the reason the report leads with it.

So `sjp skew` and `sjp layout` can be pointed at one log and return advice that makes the
other one quieter without making the job better. Neither command is wrong. Reading either
alone is.

### Rounding the target to whole rounds is a direction rather than a result

The count the volume asks for here is 3 and the rounding takes it to 4. The argument for the
rounding was that a part filled last round pays a whole task's wall time for a fraction of
the slots. On two slots both 3 and 4 take two rounds, so the rounding removed no round at
all.

The replacement argument was that one more partition splits the cold keys one more way and
cuts the largest task. The first schedule measured that at 0.8470 and separated. The second
measured 0.9102 and undecided at a p of 0.3143. Same direction, no separation.

The two lines below are from a four pass schedule and the floor of 0.0286 dates them. The
rounding question was not re-run at six passes, so it is the one comparison in this file
that has no 2026-10-08 reading behind it.

```
python scripts/benchmark.py --report <dir>/manifest.json --against volume
  wall rounded against volume  1.1073  undecided  p 0.1429 against a floor of 0.0286, so undecided rather than equal
  largest task rounded against volume  0.9102  undecided  p 0.3143 against a floor of 0.0286, so undecided rather than equal
```

The rounding is kept, and the reason is now that nothing measured argues for removing it.
That is a weaker reason than the one this file used to give and it is the one the numbers
support.

### What a reader should take from the table

A partition count is the wrong instrument for skew, and this repo can now show that rather
than assert it. The honest recommendation on a stage like this one is a different key or a
salt, and neither is something an event log can propose, because the log carries no key
distribution.

There is a real limit on the numbers above. Two slots and one machine. A count that leaves
cores idle on a two slot local session is not the same mistake it is on a real cluster,
and nothing here has been run on one.

## The tool now carries what the benchmark measured

The section above ends on a cross reading a reader has to do themselves. `sjp skew` says
one stage is carrying a hot key and `sjp layout` says to cut that stage's partition count,
and the benchmark says the cut costs 40 percent on the worst task. A profiler that
publishes all three and still gives the advice is making the reader be the tool.

So the sizing is arithmetic and the recommendation is not, any more.

An exchange's measured bytes and the sum of its stage's per task write bytes are two
readings of one event, so the number names the stage. The count on an exchange decides how
many pieces the next stage runs in, so the stage a recommendation is about is that stage's
child. That gives every exchange a reading stage, and `records_read` on the reading stage
is the key distribution with nothing else mixed into it. A duration spread would answer
the same question through a slow executor too.

Three things have to hold before a cut is withheld. The scheme is a hash, the target is
below the current count, and the reading stage is skewed on rows. The arithmetic is printed
either way.

```
python -m sjp layout tests/fixtures/eventlogs/skewed_join/local-*
local-1790780305543  sjp-skewed_join  2 cores  1.0 cpus a task  2 slots
  hashpartitioning into 8 partitions  chosen by ENSURE_REQUIREMENTS
      exchanges     2 into stage 3, and one count decides them
      bytes         88788038 measured  768000000 estimated
      bytes         2401 measured  4776 estimated
      bytes         88790439 measured in total, which is what the count divides
      plan text     agrees at 8
      per partition 11098805 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. the volume asks for 2 rather than 8, and taking that cut was measured here to leave the stage wall alone and make the largest task 40 to 52 percent slower
      why           stage 3 reads 44.2 on records_read, so one key is most of the rows and a hash keeps it on one partition at any count
  RoundRobinPartitioning into 8 partitions  chosen by REPARTITION_BY_NUM
      bytes         42048255 measured  128000000 estimated
      plan text     agrees at 8
      per partition 5256032 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. REPARTITION_BY_NUM is an argument in the query, so the config does not decide it
  large side of SortMergeJoin  leave it shuffled
      why           768000000 estimated, which is over the 10485760 threshold
  small side of SortMergeJoin  broadcast it
      why           4776 estimated against a 10485760 threshold, and the other side shuffled 88788038
  1 worth changing
```

Withheld and not reversed. Nothing here measures what the right count is on a stage like
that, and the honest answer is a different key or a salt, which is not something an event
log can propose because it carries no key distribution. A raise is never withheld, because
the same benchmark measured eight partitions beating three and four on the worst task.

The control is the join log, and it is the reason to believe any of this. Same query shape.
Same two hash exchanges, both `ENSURE_REQUIREMENTS`, both cut by the same arithmetic. Its
reading stage reads 1.22 on rows against the skewed join's 44.21, so the guard stays quiet
and the join log reports 2 worth changing while the skewed join reports 1. The cut that
went is the one the guard withheld and what is left on the skewed join is the broadcast
candidate.

## One count, two exchanges, and the two answers it used to give

A sort merge join partitions both sides by the same key into the same number of pieces.
`spark.sql.shuffle.partitions` is one number, so the two hash exchanges under the join are
one decision. Sizing them apart is what this used to do, and on the join log it did not
just count one thing twice. It gave two different answers to one question.

The large side wrote 163,272,682 bytes and asked for 4 partitions. The small side wrote
2,401 and asked for 2. Both numbers came out of the same config key. Taking the second
would have unset the first, and the report printed `schedule after 1 round rather than 4
rounds` beside a count nobody could have set on its own.

Grouping them fixes the arithmetic as well as the count. The volume a partition count
divides is what lands in the stage it feeds, which is both sides, so the sum is what gets
sized. 163,275,083 bytes still asks for 4 on this log, because the small side is four
orders of magnitude down and does not move the ceiling. The answer did not change here.
What changed is that there is one of it.

On the skewed join the duplication was louder. Both exchanges were withheld and both
printed the same reason, and that reason names a hot key on stage 3, which is the stage
both sides feed. So a 2,401 byte exchange carried a sentence about 44.2 times the median
row count, measured on the other side of the join. One finding, stated twice, attached
once to something it was not about.

Each exchange keeps its own byte line. Those are two measurements and the report is a
reading of the log as well as a recommendation. What prints once is the count, the
schedule and the advice, because there is one of each.

## The third origin, and the advice it was throwing away

An exchange records who chose its partition count. Two origins were enough to read the first
three logs and the rule was written against them. `REPARTITION_BY_NUM` is a number somebody
typed into a `repartition` call and no config change moves it. `ENSURE_REQUIREMENTS` is a
number Spark took from `spark.sql.shuffle.partitions`. So `changeable` compared the origin to
one name.

There is a third. A `repartition` call naming a column and no count asks for a hash and says
nothing about how many ways, and the count then comes from the session exactly as it does
when a join demands the partitioning. Spark records that as `REPARTITION_BY_COL`. Under a
one name rule it fell through to not changeable, so the tool refused to advise on an exchange
it could advise on and printed that the config does not decide a count the config decides.
Wrong in the quiet direction, because a refusal reads as caution.

### A fourth origin now says it is a fourth origin

Adding the third name to the list fixed that one exchange and left the shape of the rule
alone. Membership of a two name list was still the whole test, so anything Spark writes
that is not on it came back as a count the config does not decide. That sentence is a
claim about somebody's query. It is not what a missing name means.

There are two lists now and three answers. One holds the origins the config decides, one
holds the origins the query names, and an origin on neither is reported as an origin this
tool has no reading for. The refusal is the same, which is the right default. The reason
printed beside it is no longer a guess dressed as a reading.

What it used to print, measured by handing the old code an origin Spark really does write
and this repo has never captured:

| origin handed to it | what it printed before | what it prints now |
| --- | --- | --- |
| `REBALANCE_PARTITIONS_BY_NONE` | is an argument in the query, so the config does not decide it | is not an origin this tool has a reading for, so whether the config decides this count is unknown |
| nothing, an exchange whose plan text carries one field | is an argument in the query, so the config does not decide it | the plan text names no origin, so whether the config decides this count is unknown |
| `ENSURE_REQUIREMENTS`, flagged as not the config's | is an argument in the query, so the config does not decide it | is an origin the config decides and this exchange is flagged otherwise, so the two readings of one fact disagree |

The third row is the one worth looking at twice. `changeable` is a flag copied onto the
exchange and `origin` is what it was copied from, and the message was built from the origin
while the branch was taken on the flag. So the one shape where the two disagree printed a
sentence contradicting the name in its own first word.

There is still no check that the list is complete, and there cannot be one off a log.
Nothing an event log holds says what origins Spark can write. What there is instead is a
check that every origin the eight committed logs carry is a name this repo reads, with the
count of each published rather than implied.

Nothing in the first three logs reached it, which is why no check caught it. The fifth sample
job exists to reach it.

```
python -m sjp layout tests/fixtures/eventlogs/by_column/local-*
local-1790873320137  sjp-by_column  2 cores  1.0 cpus a task  2 slots
  hashpartitioning into 8 partitions  chosen by REPARTITION_BY_COL
      bytes         119303250 measured  768000000 estimated
      plan text     agrees at 8
      per partition 14912906 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    2 rather than 8. 119303250 bytes at an advisory 67108864 wants 2, and the 2 slots round it to 2
      schedule after 1 round rather than 4 rounds
  RoundRobinPartitioning into 8 partitions  chosen by REPARTITION_BY_NUM
      bytes         42048255 measured  128000000 estimated
      plan text     agrees at 8
      per partition 5256032 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. REPARTITION_BY_NUM is an argument in the query, so the config does not decide it
  no join in this plan, so no broadcast has anything to replace
  1 worth changing
```

The origin's name is Spark's word for what happened and a name is not a measurement. The
measurement is two logs running one query at two session counts.

```
python -m sjp capture --job by_column --out <dir> --rows 8000000
python -m sjp capture --job by_column --out <dir> --rows 8000000 --partitions 5
```

```
python -m sjp layout tests/fixtures/eventlogs/by_column_at_5/local-*
local-1790873361041  sjp-by_column  2 cores  1.0 cpus a task  2 slots
  hashpartitioning into 5 partitions  chosen by REPARTITION_BY_COL
      bytes         123477926 measured  768000000 estimated
      plan text     agrees at 5
      per partition 24695585 measured bytes each
      schedule      3 rounds of 2 slots, 1 in the last
      partitions    2 rather than 5. 123477926 bytes at an advisory 67108864 wants 2, and the 2 slots round it to 2
      schedule after 1 round rather than 3 rounds
  RoundRobinPartitioning into 8 partitions  chosen by REPARTITION_BY_NUM
      bytes         42048255 measured  128000000 estimated
      plan text     agrees at 8
      per partition 5256032 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. REPARTITION_BY_NUM is an argument in the query, so the config does not decide it
  no join in this plan, so no broadcast has anything to replace
  1 worth changing
```

Read the second one against the first. The hash exchange went from 8 partitions to 5 because
the session did. The round robin exchange above it was handed 8 by the query and stayed at 8
in both. One session count, two exchanges in one log, and only the one the query left open
moved. That is the whole claim and it does not rest on the origin's name at all.

This log is also the positive control for the guard in the section above. It is a hash
exchange, the volume asks for a cut, and the stage the count feeds is even on rows rather
than carrying a hot key. The cut is recommended rather than withheld. Without a log of this
shape the guard could be withholding on every hash it sees and every check would still pass.

## Two logs the tool could not read well, and what they cost it

Added 2026-10-02. The five jobs above all carry something wrong with the query. These two
carry something wrong with this tool.

### 676 lines to say nothing is wrong

`sjp skew` printed one line per metric per stage, then the tally, then the worst line per
unit. On every log in this repo until the `wide` capture that was fine.

```
$ python -m sjp skew tests/fixtures/eventlogs/skewed/*   | wc -l
48
$ python -m sjp skew tests/fixtures/eventlogs/wide/*     | wc -l
676
```

Both figures are one higher than they were when this section was written, because day 5
added the line naming the metric set under the tally. The heading moved with them.

The `wide` job runs 24 small aggregates in one application instead of one large one, which
gives the application 48 stages. 48 stages times the 14 metric default set is 672 verdicts.
The job is not skewed anywhere.

```
python -m sjp skew tests/fixtures/eventlogs/wide/* | tail -2
  undecided stage 47  outside_run_time    no ratio  largest task 24 is under the millis floor of 50
  undecided stage 47  serialize_time      no ratio  every task reads zero, so there is no spread
```

Those used to be the two lines carrying the answer, which is to say the answer was the last
two lines of 675 and everything above them was a stage that is fine on a metric nobody asked
about. The section below moved them.

This was not a guess. The default metric set was widened from three to fourteen on
2026-09-27 and the cost of that was recorded at the time as an open question, against logs
of three stages where it did not show. The arithmetic is the whole story, which is why it
took a log rather than an argument to make it land. At 900 stages the same rule prints
12,600 lines.

Nothing about the output changed on the day this was captured. The fixture was the
deliverable, and four checks pinned the line count so that a change to the output had to
account for it. One of those four asserted the tally was second from last. It was right,
and the next section is what made it wrong.

### A floor cannot be placed from one end of a range

`skew.FLOORS` carried a millisecond floor of 50 and left bytes and counts at `None`.
`None` means no floor, so a byte difference of any size was judged on its ratio alone.

The `small` job runs the skewed distribution over 600 rows. Asked for its skewed verdicts
on that tree it answered like this. The lines are real output and the tree is gone, so the
fence carries no command. What the same command prints today is the second half of the
before and after pair further down.

```
local-1790970475322  sjp-small  threshold 4.0  42 verdicts
  worst count   stage 2 on records_read at 43.1667
  worst bytes   stage 2 on local_bytes_read at 9.3934
  worst millis  stage 2 on executor_run_time at 4.8559
  4 skewed, 10 even, 28 undecided
  showing 4 of 42, skewed only
  skewed    stage 2  records_read         43.1667  largest task 518 against a median of 12
  skewed    stage 2  local_bytes_read      9.3934  largest task 7449 against a median of 793
  skewed    stage 2  executor_run_time     4.8559  largest task 556 against a median of 114.5
  skewed    stage 1  executor_run_time     4.5714  largest task 224 against a median of 49
```

The two at the top were correct and useless. 7,449 bytes really is 9.4 times 793 bytes, and
518 records really is 43 times 12. Neither is a thing anyone would act on.

The millisecond floor was placed on evidence. The sweep `{0: 11, 30: 8, 50: 8, 100: 7,
3041: 6}` is published further up this file, and 50 sits inside a gap where every value
gives the same answer. The byte and count floors had no equivalent, because until this
capture every log here measured bytes in the tens of millions. The skewed join's key
exchange writes 88,788,038 of them. This stage reads 7,449. Four orders of magnitude apart
and judged by the same rule.

The byte and record figures came off the deterministic half of the log and reproduced
across two separate captures to the record and to the byte. The two `executor_run_time`
lines did not, because they are timings, so nothing pins them.

The floors are not set here. Setting a floor on the first log that ever populated the small
end, on the same day that log was captured, is how the millisecond floor would have been
placed badly. They are set in the section below, three days later.

### The floors, and what a bracket four orders of magnitude wide cannot decide

`scripts/floor_probe.py` reads both ends of both brackets off the committed logs and runs
four controls over what the floors change.

```
brackets re-derived from the logs
  bytes  noise       7449  real   59191004  span   7946x  agrees with skew.BRACKETS
  count  noise        518  real    6932663  span  13383x  agrees with skew.BRACKETS
floors derived from them
  bytes  floor     700000  which is   93x the noise end and a 84th of the real
  count  floor      60000  which is  115x the noise end and a 115th of the real
the millisecond floor is not derived
  millis measured bracket is (556, 71), a noise end above a real end
  so the floor stays at 50, placed by hand
```

The floor is the geometric mean of its bracket to one significant figure. A geometric mean
because these are quantities people compare by ratio, and the arithmetic middle of the byte
bracket sits within a factor of two of the real end. One significant figure because the
precision is not there.

The rule returns 50 for the millisecond bracket of 29 to 71, which is the floor that shipped
weeks before the rule existed. That is one agreement rather than a law.

What makes the magnitude the only thing worth reading is that the ratio is not. The same job
captured at five row counts moves the record ratio by one percent across four orders of
magnitude of input, because the ratio is a property of the key distribution and that is
held. No command prints this table, so it is a table.

| rows | records_read ratio | largest records | local_bytes_read ratio | largest bytes |
|---|---|---|---|---|
| 600 | 43.17 | 518 | 9.39 | 7,449 |
| 6,000 | 43.94 | 5,185 | 22.10 | 68,790 |
| 60,000 | 44.19 | 51,995 | 31.89 | 727,776 |
| 600,000 | 44.19 | 519,941 | 34.41 | 7,764,485 |
| 8,000,000 | 44.22 | 6,932,663 | 34.99 | 109,685,777 |

Rebuild it with

```
bash -c 'for n in 600 6000 60000 600000 8000000; do python -m sjp capture --job skewed --rows $n --out /tmp/ladder/$n; done'
```

The sweep is where the honesty is. Every candidate from 10,000 to 59,191,004 gives an
identical answer on all eight committed logs.

```
bytes verdicts surviving each candidate floor
  log                        0       10000      100000      700000    10000000    59191004    60000000
  balanced                   0           0           0           0           0           0           0
  by_column                  0           0           0           0           0           0           0
  by_column_at_5             0           0           0           0           0           0           0
  join                       0           0           0           0           0           0           0
  skewed                     4           4           4           4           4           4           4
  skewed_join                4           4           4           4           4           4           3
  small                      1           0           0           0           0           0           0
  wide                       0           0           0           0           0           0           0
```

The millisecond gap was a factor of 2.4 and it pinned a number. This one is a factor of
5,919 and the ladder above shows its interior is reachable by choosing a row count, so the
position inside it is a policy about the smallest job worth commenting on. A 60,000 row job
spilling 727,776 bytes loses its byte verdict and a 600,000 row job keeps it. At 60,000,000
the floor starts removing real evidence, which is the only hard edge either bracket has.

The small log before and after.

```
  4 skewed, 10 even, 28 undecided
  skewed    stage 2  records_read         43.1667  largest task 518 against a median of 12
  skewed    stage 2  local_bytes_read      9.3934  largest task 7449 against a median of 793
  skewed    stage 2  executor_run_time     4.8559  largest task 556 against a median of 114.5
  skewed    stage 1  executor_run_time     4.5714  largest task 224 against a median of 49
```

```
  2 skewed, 6 even, 34 undecided
  skewed    stage 2  executor_run_time     4.8559  largest task 556 against a median of 114.5
  skewed    stage 1  executor_run_time     4.5714  largest task 224 against a median of 49
```

Two verdicts remain and neither is worth acting on. Both are a duration on a 600 row job,
and that is the finding this capture produced without being asked. Bytes and records scale
with the data and a task's wall time does not, because a 600 row task still pays for a JVM,
a launch and a serialisation. So the millisecond floor under a trivial job is set by fixed
cost rather than by work, and no magnitude floor separates it from a real pause.

## The answer goes first now

Added 2026-10-03. Three changes to the same command and not one of them is about length.

**The summary moved to the top.** The worst line per unit, then the tally, then the
verdicts. The wide log still prints every verdict it did before. Trimming was never the fix.

The figure was 675 when this section was written and it is 676 now, because day 5 added one
line under the tally on every run.

**`--only` filters the body**, one outcome at a time.

```
$ python -m sjp skew tests/fixtures/eventlogs/wide/* --only skewed
local-1790970447319  sjp-wide  threshold 4.0  672 verdicts
  nothing skewed at this threshold
  0 skewed, 55 even, 617 undecided
  sjp 0.2.0  exit 0 over metric set default 14:e575855f
  showing 0 of 672, skewed only
```

676 lines to 5. That last line is there because a reader who filters still has to be told
what was left out.

The tally in that block read `0 skewed, 103 even, 569 undecided` until 2026-10-06, which was
this log before the byte and count floors landed the day before. The transcript was a day
stale and nothing caught it, because the block gate reads figures against captured logs and
a tally is not a figure in a log. Re-derived here by running the command.

**The body is ranked, and the ranking stays inside a unit.** Skewed verdicts come first and
undecided ones last. Inside an outcome the verdicts group by unit and sort by ratio with the
worst first, and an unbounded ratio leads its own group.

That last part is the one worth arguing about. Sorting the whole body by ratio is the
comparison `worst` refuses to make for the summary line one function above it. The skewed
log is where it shows, because three of its eight skewed verdicts divide by zero.

```
$ python -m sjp skew tests/fixtures/eventlogs/skewed/* --only skewed | grep unbounded
  skewed    stage 2  disk_spilled       unbounded  more than half the tasks read zero and one reads 88088795
  skewed    stage 2  memory_spilled     unbounded  more than half the tasks read zero and one reads 620755808
  skewed    stage 2  gc_time            unbounded  more than half the tasks read zero and one reads 71
```

`gc_time` has a largest task of 71 milliseconds. `memory_spilled` has 620,755,808 bytes.
Both ratios are infinite, so a flat sort orders those two by nothing at all and whichever
lands on top is a fact about the metric order rather than about the job. Grouping by unit
first keeps the sort where it means something. The cost is that the body cannot say which
single verdict is worst, and there is no cross unit answer to that question to give.

### The flag does not change the answer

`sjp.cli` says a flag must never move a command's declared effect, because a guard keyed on
a command name cannot see a flag. The same argument covers a verdict. `--only` narrows what
gets printed and nothing else, so the tally and the exit status still come off the whole
scan.

```
$ python -m sjp skew tests/fixtures/eventlogs/skewed/* --only undecided | grep skewed,
  8 skewed, 6 even, 28 undecided
$ python -m sjp skew tests/fixtures/eventlogs/skewed/* --only undecided > /dev/null
$ echo $?
1
```

A tally computed over the filtered list would read zero skewed on that command, which is a
false statement about a job that skewed on eight.

### Piping it to head is no longer a traceback

676 lines is a thing a reader pipes into `head`, and that used to print a `BrokenPipeError`
over the top of the output. The entry point catches it now and returns 141. The flush
happens inside that handler rather than at interpreter shutdown, because a short command
fits in the buffer and the write that fails is the final flush.

141 and not 0. A shell reads 0 from this command as nothing skewed, and a run cut off part
way through never finished answering.

The status is not deterministic and this file says so rather than a check pinning a number
it cannot reproduce. Measured over 20 runs each on 2026-10-03, `sjp skew` on the wide log
returns 141 every time and `sjp stages` on 6 of 20. `stages` writes little enough that the
buffer sometimes drains before the reader goes away. What held on all 40 runs is that
stderr stayed empty.

## The exit status, and the set it is counted over

Added 2026-10-06. `sjp skew` exits 1 when a stage skewed. That status is counted over
whatever the scan judged, and until today nothing in the output said what that was.

The set is not fixed. It is derived from the kind declared on every field of `model.Task`,
so a field added there widens it with nobody editing `sjp/skew.py`. It has already widened
once, from three names to fourteen, and the same log answered differently afterwards.

```
$ python -m sjp version
sjp 0.2.0
metric set default 14:e575855f
  executor_run_time  millis
  deserialize_time   millis
  serialize_time     millis
  gc_time            millis
  peak_memory        bytes
  memory_spilled     bytes
  disk_spilled       bytes
  records_read       count
  local_bytes_read   bytes
  remote_bytes_read  bytes
  records_written    count
  bytes_written      bytes
  duration           millis
  outside_run_time   millis
```

The id is the count of metrics and eight hex characters of a digest over their sorted
names. Sorted, because asking for the same fourteen in a different order is the same
question. Derived, because a hand written version is exactly the thing that cannot track a
set the code derives. A field added to the task record changes the id without anyone
remembering to.

`skew.METRIC_SET_HISTORY` holds both sets this tool has shipped against the version that
shipped them. `3:5f2a4f77` is the three names, read out of commit 4a5d5c0 rather than out
of memory. `14:e575855f` is today's. A check asserts the live set is the last entry and
that `sjp.__version__` is its version, so widening the set without recording what it became
fails the suite.

The version promises nothing about compatibility. Nothing outside this repo imports `sjp`.
Its minor digit moves when the metric set moves and that is all it is for.

### A metric a ratio cannot measure is refused now

`--metric launch_time` was accepted and scanned for eleven days. `launch_time` is declared
an `instant`. A median relative ratio over two wall clock readings is close to 1.0 by
construction, so it reported `even` on every stage of every log and meant nothing.

```
$ python -m sjp skew tests/fixtures/eventlogs/small/* --metric launch_time
refused launch_time. a declared instant rather than a measured quantity
$ echo $?
2
```

The status is the half of this that matters. Measured on 2026-10-06 against the tree before
the change, `--metric launch_time` printed two verdicts at `1.0000` off values near
1790970484000 and exited **0**, which a shell reads as a job with nothing wrong.
`--metric executor_id` was worse. It reached `statistics.median` and raised a `TypeError`
on `'str' and 'int'`. The interpreter then exited **1**, which a shell reads as a stage that
skewed. A typo in a metric name did the same thing.

So the refusal needed a status that is neither. 2 is the entry point's `REFUSED` and no
command here returns it as an answer.

Every refused name is reported rather than the first one, because a caller who fixes one
and runs again has been told half the answer twice.

```
$ python -m sjp skew tests/fixtures/eventlogs/small/* --metric launch_time --metric failed --metric nope
refused launch_time. a declared instant rather than a measured quantity
refused failed. a declared flag rather than a measured quantity
refused nope. not a task quantity
```

The refusal lives in `sjp/skew.py` rather than in the command, and `judge` repeats it for a
caller holding one stage. `scan` calls `judge` 672 times on the wide log, so the repeat is
paid for. Measured over 20 runs on 2026-10-06, the scan goes from a median of 2.9 ms to
4.2 ms.

### A crash used to be worth the same as a finding

Every command here answers its own question with 0 and 1. An unhandled exception left the
interpreter to exit 1, so a crash and a stage that skewed were the same status to a shell.

The entry point catches it now, prints the traceback to stderr and returns 3. `SystemExit`
and `KeyboardInterrupt` are not caught, because argparse raises the first to refuse an
argument and that is already a usage error with a status of its own.

## Running the checks

```
python tests/run_all.py
458 passed, 0 failed, 458 checks
```

Every check is graded by a mutation pass rather than counted. `sjp/bench.py` is from
today and it is the only row that moved. `sjp/model.py` and `sjp/commands.py` and
`scripts/accumulable_probe.py` are from 2026-10-07. `sjp/skew.py` and `sjp/cli.py` were
re-run on 2026-10-06 and `jobs/sample.py` on 2026-10-05. The rest are earlier passes.

```
sjp/layout.py: 114 mutation sites, running 0 to 114
114 killed, 0 survived, 0 ungraded, 114 graded
sjp/bench.py: 43 mutation sites, running 0 to 43
43 killed, 0 survived, 0 ungraded, 43 graded
sjp/model.py: 91 mutation sites, running 0 to 91
91 killed, 0 survived, 0 ungraded, 91 graded
sjp/plan.py: 33 mutation sites, running 0 to 33
33 killed, 0 survived, 0 ungraded, 33 graded
sjp/commands.py: 30 mutation sites, running 0 to 30
30 killed, 0 survived, 0 ungraded, 30 graded
sjp/skew.py: 45 mutation sites, running 0 to 45
45 killed, 0 survived, 0 ungraded, 45 graded
sjp/memory.py: 22 mutation sites, running 0 to 22
22 killed, 0 survived, 0 ungraded, 22 graded
sjp/cli.py: 23 mutation sites, running 0 to 23
23 killed, 0 survived, 0 ungraded, 23 graded
sjp/eventlog.py: 9 mutation sites, running 0 to 9
9 killed, 0 survived, 0 ungraded, 9 graded
sjp/contract.py: 6 mutation sites, running 0 to 6
6 killed, 0 survived, 0 ungraded, 6 graded
scripts/fixture_probe.py: 20 mutation sites, running 0 to 20
19 killed, 1 survived, 0 ungraded, 20 graded
scripts/accumulable_probe.py: 81 mutation sites, running 0 to 81
55 killed, 26 survived, 0 ungraded, 81 graded
jobs/sample.py: 35 mutation sites, running 0 to 35
14 killed, 21 survived, 0 ungraded, 35 graded
```

The new row is 55 of 81 and the 26 that survive are in the function that prints. Saying so
is how the row below it got explained away for eleven days, so the rest of that sentence
matters. The figures that function prints are graded by a separate gate. It compares every
numeric line in this file against captured output. The two subtractions that function used
to do inline are computed by a function the suite drives now, and what is left unguarded is
formatting.

`jobs/sample.py` went from 2 of 35 to 14 of 35 and the reason is the one thing in this repo
I got most wrong.

For eleven days, which is every day this repo has existed, that row was explained away. The
explanation was that every surviving mutant
in `jobs/sample.py` sits in code that only runs with a Spark session, that a check cannot
have one, and that what grades the module is the logs it produced. The first half of that
is true of every other function in the file, all six of which need a driver. It was never
true of `run`, which chooses
a branch, and a branch choice needs no driver at all.

So 33 survivors read as a property of the module rather than as a missing test, and one of
them was not a mutant in a copy. It was a mutant that had been committed. A killed mutation
pass left `job == "by_column"` as `job != "by_column"` in the working tree and the commit
that added the two cycle 2 jobs took it in. Four job names then went down one branch, two
crashed on a frame they were never handed, and `python -m sjp capture --job small` could not
produce the fixture this file publishes figures from. The suite stayed green for three days
because nothing had ever called `run` past its first line.

`tests/test_sample_jobs.py` now drives the dispatch against recorded helpers with pyspark
stubbed out. It asserts the branch each job name reaches and what `_keyed` was called
with. It also asserts whether a frame was handed over and which jobs switch broadcasting
off. It is controlled against the arm order that shipped, so a recorder that could not tell
the two apart fails.
Every mutant in `run` and in the dispatch now dies.

The 21 that remain are sites 0 to 21, which are the constants and the six functions that
need a driver.
A mutant on any of them changes what a capture would produce rather than what the code
answers, and killing one means running Spark inside a check. That is the part of the old
explanation that holds, and it is worth half as much now that it is not covering for the
rest.

Two mutants here used to die against checks that read the module's own constants back out
of the module, which is transcription and would have gone stale the moment somebody changed
the constant and the check together. Those were replaced by checks that read the same
settings out of the committed logs instead. The score fell and the question being asked got
better.

`sjp/bench.py` had four survivors until 2026-10-08 and has none now. All four were
boundary comparisons and the two halves needed different answers.

One widened a float tolerance that already absorbed the difference, so the two forms were
the same function on any input where the values differ by more than a nanosecond. That one
is gone rather than killed. The tolerance existed so the observed split could match itself,
and the comparison now runs on fractions instead, cross multiplied by the two arm sizes so
no division happens. The observed split matches itself by being the same number. Every p in
this file reproduced to the last digit across that rewrite, which was the point of it.

The other three separated a p value or a floor from the threshold at exactly equal. On the
default alpha that case is unreachable, and more cleanly than the old note here claimed. A
split and its complement are both enumerated and both score the same gap, so the extreme
count is always even when the arms are the same size. An exact p of 0.05 needs the split
total divided by twenty to be an even whole number, and it is odd at the only two pass
counts from 3 to 12 where it is whole at all. What is reachable is alpha itself, because
alpha is an argument. Three checks now pass one and the mutants die on the sentence the
verdict prints rather than on its outcome.

The first pass at this module killed 24 of 38. Six survivors were guards checked against a
value they must refuse and never against the smallest value they must accept, and four were
records nothing asserted frozen.

The one survivor in `scripts/fixture_probe.py` moves a `sys.path.insert` index from 0 to 1,
which changes nothing about what gets imported here. It is left alone rather than tested.

`tests/runner.py` is not in the table. Mutating the collection loop while using it as the
oracle grades it against itself, so whatever number came out would not mean anything.

`sjp/layout.py` went into that table at 30 of 47 and `sjp/plan.py` at 25 of 30, and every
one of the twenty two survivors was real. Six were Spark's two documented defaults, which
nothing pinned because every check spelled them as their own names. Four were records
nothing asserted frozen. Nine were the whole of the command's exit arithmetic, which lived
inside the command and therefore sat where a mutation pass pointed at the library could
never reach it. That rule is older than this module and it was broken again anyway. The
counting moved into `layout.actionable`, the count is printed rather than left as a status,
and the three remaining boundaries got a fixture sitting exactly on them.

`sjp/memory.py` went into that table at 17 of 22 and five of the survivors were real. A
budget was never asserted frozen. A stage that spilled to memory and reached no disk was
never built, so the rule that clean means both columns are zero could become either one. A
two task stage with one task spilling was never built either, so the wording that names a
concentrated spill could have slid to three tasks or down to one. And a peak of exactly one
byte was never tried against the zero test. Five checks closed all five.

## Known limitations

The row count decides which pathologies land in the log. At 2,000,000 rows the same
skewed job still skews and nothing spills anywhere. A fixture is a choice about what a
later check can see.

The read safety check only exercises the arguments it passes. A flag that turned a read
into a write would slip past it, which is why the no flag rule is a rule rather than
something a test can promise.

An event log discloses the driver's working directory, the operating system user who ran
the job and the full classpath. The committed logs are unedited and carry all three.
Editing them would make them something other than real logs.

Closed in v2. The map onto task fields was twelve entries kept beside the record, and this
section used to say the grouping stage carries thirty seven and the other twenty five are
not read. That subtraction mixed the size of the map with the contents of one stage. Ten of
the twelve are on that stage and two never moved, and of the twenty six names it does not
keep, seventeen are task metrics it reads past and nine belong to a plan node and were never
candidates. The accumulable is declared on the task field now, so the map is read off the
record and a field cannot arrive without answering the question.

What is still open is which of the seven task metrics that have moved here are worth
keeping. `resultSize` and `shuffle.write.writeTime` are the two a recommendation would reach
for first.

Closed in v2. `sjp skew` used to print every verdict with no ranking and no filter, with the
summary last, so the wider the log the further a reader scrolled to reach it. The summary is
first now and `--only` filters the body. The wide log goes from 676 lines to 5 when asked
for the skewed verdicts alone, and it still prints 676 unfiltered because the length was
never the part that mattered.

What the ranking does not do is compare two units. The body sorts by ratio inside a unit and
groups the units in declared order, so a reader cannot ask which single verdict in a log is
worst. That question has no answer a ratio can give.

Closed in v2. `sjp skew` used to judge any quantity a caller named with `--metric`,
including `launch_time`, which is a point on the clock rather than an amount of anything.
It is refused now and the status is 2, which this command never returns as an answer about
a job. `--metric executor_id` used to crash and exit 1, which a shell reads as a stage that
skewed.

What that leaves open is the version. `sjp 0.2.0` makes no compatibility promise and its
minor digit tracks the metric set alone. A check holds the live set against the last entry
of `skew.METRIC_SET_HISTORY`, so widening the set without recording it fails. Rewriting that
last entry in place rather than appending to it would pass, and nothing here would notice.

The metric set id is eight hex characters of a digest. That separates the handful of sets
this tool will have and it is an identity rather than a defence against anyone constructing
a collision.

The exit status of a command whose output was cut off by a closed pipe is 141 on `sjp skew`
and is not deterministic on the shorter commands. Measured on 2026-10-03 at 20 of 20 for
`skew` and 6 of 20 for `stages`, because a short output can drain from the buffer before the
reader goes away. stderr stayed empty on all 40 runs.

The byte and count floors are derived from a bracket whose two ends come from two logs, and
every candidate across 3.7 orders of magnitude of that bracket gives the same answer on all
eight. So the floors are bracketed and not located, and where they sit inside the bracket is
a policy about the smallest job this tool will comment on.

The millisecond floor is not derived and cannot be, because the 600 row log reports a noise
duration eight times the smallest real one. The small log still produces two
`executor_run_time` verdicts nobody would act on. A fix is a different mechanism rather than
a different number and none has been measured.

The model keeps every task of every stage. Both committed logs hold eighteen tasks. What
this costs on a log from a job with a million tasks has not been measured, so nothing here
claims it is fine.

A stage id is a property of one run when the plan is not a straight line. The join job was
captured twice and every stage moved, because the two sides are submitted together and
whichever is scheduled first takes the lower id. Nothing in `sjp/layout.py` or `sjp/plan.py`
addresses a stage by id. The earlier documents here do, and they are safe only because both
aggregate plans are linear.

The partition target is arithmetic over one shuffle and the slot count. It does not know
what the stage after the exchange will do with a partition, and a count that sizes the bytes
well can still be the count that makes something spill.

The broadcast verdict is about the plan Spark produced and not about the query. A join that
could be rewritten to avoid the shuffle entirely is outside what a log can see.

One ratio threshold covers every metric. The magnitude floor is per unit now and the ratio is
not, so a spill ratio and a duration ratio are still judged against the same 4.0. Nothing
measured here says what the difference should be, so per metric thresholds wait for a log that
argues for one.

Six passes an arm puts the floor at 0.0022 and gives 23 reachable p values under 0.05. A
separated verdict that lands on the floor still means only that the two arms do not
interleave, and that is the ordinary shape of a real separation rather than a weak one.
`Verdict.on_floor` reports which case a caller is holding. The schedule was four until
2026-10-08, where 70 splits left the floor as the only reachable p under 0.05, so every
separation it could report was a separation at the floor.

Two six pass schedules were run on 2026-10-08. The three largest task comparisons separated
on both. Nothing else did, and an undecided verdict here still carries almost no
information.

Closed in v2. The exchanges one partition count controls are grouped and counted once, and
an origin this repo has never seen is reported as unrecognised rather than as a number
somebody typed. Two things about that are worth keeping in view.

Grouping rests on two exchanges feeding one stage, and the stage is found by matching an
exchange's measured bytes against a stage total. An exchange whose bytes match no stage has
no reading stage. Neither does one whose bytes match two. Either way it is left on its own
and sized alone, which is a refusal rather than an error and it is silent. A log where it
happened would be counted the old way with nothing saying so.

The origin list still has no completeness check and cannot have one, because nothing in an
event log states what origins Spark can write. What is checked is that every origin the
eight committed logs carry is one this repo reads. A ninth log could carry a tenth name and
the only thing that would say so is the report itself.

The millisecond floor of 50 sits in a gap the two eight million row logs leave and its
position inside that gap is a judgement. The sweep from 0 to 3041 is published above so the
headline count reads as a fact about the floor. That gap is also the thing the 600 row log
falsified, so the floor is now a number with no live bracket behind it rather than a derived
one.

The spill analysis reports what spilled and cannot say what it spilled against. Both logs
were captured in local mode and neither records the four properties that decide a task's
execution memory. A log from a cluster would carry them and nothing here has been run against
one.

`undecided` is five of nine verdicts on the healthy job. That is a high proportion for a
detector and it is a property of these fixtures rather than of the rule. A real job with two
hundred partitions per stage would clear the task floor everywhere.

The detector reads task fields and never a stage total, so which accumulables the map covers
changes no verdict today. A recommendation that reads a stage total will change that.

`sjp stages` reports a stage that was submitted and never completed with whatever the log
holds for it. A job that died mid stage is a real thing to be handed, and the timings for
that stage are missing rather than zero.

A stage seen only through its task events declares no tasks, because the planned count
lives on the submitted event. Nothing in either committed log exercises that and the check
for it is built rather than captured.

The benchmark ran on two slots on one machine. A partition count that leaves cores idle on
a two slot local session is not the same mistake it is on a cluster, and the wave arithmetic
in the partition target has only ever been tested where a wave is two tasks wide.

Six passes an arm is the default and four was the default until 2026-10-08. An effect real
but smaller than these arms can separate reads as undecided, and undecided is not a finding
of no difference.

The benchmark logs are not committed. Four arms at six passes is twenty five event logs and
tens of megabytes, and the numbers above are reproducible from the command beside them
rather than from a file in the tree. Two schedules were run on 2026-10-08 and the first
one's manifest was lost when its scratch directory was reclaimed mid run, so the second
one's logs were written outside the repo instead.
