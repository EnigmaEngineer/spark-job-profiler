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

Five jobs over 8,000,000 rows. Same payload in all of them. Adaptive execution off.

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
  stage 1  8 of 8 tasks  2278 ms wall
      duration ms   median 492.0  max 855  spread 1.74
      records read  median 1000000.0  max 1000000  spread 1.00
      outside run   median 11.5  max 16  spread 1.39
      spilled       0 memory  0 disk
      peak memory   0 largest task  0 summed by the stage
      stage totals  12 mapped, 5 absent, 0 disagree with the tasks
  stage 2  8 of 8 tasks  3879 ms wall
      duration ms   median 207.0  max 3048  spread 14.72
      records read  median 156784.0  max 6932663  spread 44.22
      outside run   median 16.0  max 38  spread 2.38
      spilled       620755808 memory  88088795 disk
      peak memory   377486768 largest task  562035856 summed by the stage
      stage totals  12 mapped, 3 absent, 0 disagree with the tasks
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

## Naming the stage that skewed

```
python -m sjp skew tests/fixtures/eventlogs/skewed/* \
      --metric records_read --metric duration --metric memory_spilled --metric serialize_time
local-1790267120237  sjp-skewed  threshold 4.0  12 verdicts
  undecided stage 0  records_read        no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 0  duration            no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 0  memory_spilled      no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 0  serialize_time      no ratio  2 tasks, and below 3 the ratio cannot pass 2
  even      stage 1  records_read          1.0000  largest task 1000000 against a median of 1000000
  even      stage 1  duration              1.7378  largest task 855 against a median of 492
  undecided stage 1  memory_spilled      no ratio  every task reads zero, so there is no spread
  undecided stage 1  serialize_time      no ratio  every task reads zero, so there is no spread
  skewed    stage 2  records_read         44.2179  largest task 6932663 against a median of 156784
  skewed    stage 2  duration             14.7246  largest task 3048 against a median of 207
  skewed    stage 2  memory_spilled     unbounded  more than half the tasks read zero and one reads 620755808
  undecided stage 2  serialize_time      no ratio  largest task 3 is under the millis floor of 50
  3 skewed, 2 even, 7 undecided
  worst count   stage 2 on records_read at 44.2179
  worst bytes   stage 2 on memory_spilled at unbounded
  worst millis  stage 2 on duration at 14.7246
```

Four metrics are named there to keep the block readable. The default is every quantity a
task carries, which is fourteen of them, and on this log that is 42 verdicts reading
`8 skewed, 6 even, 28 undecided`. The default used to be three names and
`### The metric set was three names I chose` below is about what widening it cost.

The same command on the balanced log, which differs from the skewed one in one expression.

```
  0 skewed, 11 even, 31 undecided
  nothing skewed at this threshold
```

It exits 1 when something skewed, so a shell can act on the answer.

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

### Widening it broke the control, and that is what the floor is for

The balanced log is the fixture whose job is to have nothing wrong with it. Over fourteen
metrics it reported a skewed stage.

```
python -m sjp skew tests/fixtures/eventlogs/balanced/* --metric serialize_time
local-1790267152313  sjp-balanced  threshold 4.0  3 verdicts
  undecided stage 0  serialize_time      no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 1  serialize_time      no ratio  every task reads zero, so there is no spread
  undecided stage 2  serialize_time      no ratio  largest task 8 is under the millis floor of 50
  0 skewed, 0 even, 3 undecided
  nothing skewed at this threshold
```

That third line was `skewed` with an unbounded ratio before the floor existed. Seven tasks
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

Bytes and counts have no floor. Neither log produces a byte or a record verdict small enough
to be noise, so nothing here measures where that floor belongs, and `None` means no floor
rather than a number invented to fill the row.

### One worst stage was the metric order in disguise

`worst` used to return a single verdict, ranking an unbounded ratio above every finite one.
Over three metrics that was nearly harmless. Over fourteen the skewed grouping stage carries
three unbounded verdicts, on garbage collection, on memory spill and on disk spill. They tie
on the ratio because all three are infinite, they tie on concentration because one task did
all of each, and the single answer was decided by which metric the scan reached first. It was
naming a 71 millisecond pause ahead of a 620,755,808 byte spill.

There is no conversion between a millisecond and a byte, so the report gives one worst per
unit and stops pretending otherwise.

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
      bytes         163272682 measured  832000000 estimated
      plan text     agrees at 8
      per partition 20409085 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    4 rather than 8. 163272682 bytes at an advisory 67108864 wants 3, and the 2 slots round it to 4
      schedule after 2 rounds rather than 4 rounds
  RoundRobinPartitioning into 8 partitions  chosen by REPARTITION_BY_NUM
      bytes         42048255 measured  128000000 estimated
      plan text     agrees at 8
      per partition 5256032 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. REPARTITION_BY_NUM is an argument in the query, so the config does not decide it
  hashpartitioning into 8 partitions  chosen by ENSURE_REQUIREMENTS
      bytes         2401 measured  4776 estimated
      plan text     agrees at 8
      per partition 300 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    2 rather than 8. 2401 bytes at an advisory 67108864 wants 1, and the 2 slots round it to 2
      schedule after 1 round rather than 4 rounds
  large side of SortMergeJoin  leave it shuffled
      why           832000000 estimated, which is over the 10485760 threshold
  small side of SortMergeJoin  broadcast it
      why           4776 estimated against a 10485760 threshold, and the other side shuffled 163272682
  3 worth changing
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
python scripts/benchmark.py --job skewed_join --rows 8000000 --arm current=8 --arm advised=2 --arm volume=3 --arm rounded=4 --passes 4 --out <dir>
python scripts/benchmark.py --report <dir>/manifest.json
```

Four passes an arm. Each run is its own process, so no arm inherits a warm session from
another. The arm order rotates, so each one runs first exactly once. The first pass is
discarded. Four against four puts the smallest reachable p at 0.0286, which is the reason
the schedule is four and not three. Three against three cannot return anything under 0.05
however the numbers land, and `sjp.bench` refuses to report a p for a comparison that
size rather than printing one beside a note that it could never have mattered.

The stage below is the one that reads every row. It is addressed by its row count and not
by its number, because a join submits both sides at once and whichever the scheduler takes
first gets the lower stage id.

```
the stage that reads every row, which is the one the job is about
  current   8 tasks  wall 8236 7143 7430 7480  largest task 5953 4990 5263 5272  median task 608 504 694 562
  advised   2 tasks  wall 7185 7793 7067 10657  largest task 7157 7766 7033 10626  median task 4806 5214 4500 6840
  volume    3 tasks  wall 7252 7378 8087 5802  largest task 7229 7356 8063 5780  median task 1854 1616 2774 1153
  rounded   4 tasks  wall 8216 7480 7539 8345  largest task 6924 6167 5807 6977  median task 1258 1246 1615 1313
  wall advised against current  1.0797  undecided  p 0.7429 against a floor of 0.0286, so undecided rather than equal
  wall volume against current  0.9416  undecided  p 0.4571 against a floor of 0.0286, so undecided rather than equal
  wall rounded against current  1.0426  undecided  p 0.2571 against a floor of 0.0286, so undecided rather than equal
  largest task advised against current  1.5170  separated  p 0.0286 against a floor of 0.0286
  largest task volume against current  1.3236  undecided  p 0.0571 against a floor of 0.0286, so undecided rather than equal
  largest task rounded against current  1.2047  undecided  p 0.0571 against a floor of 0.0286, so undecided rather than equal
```

### Taking the advice made the job no faster and its worst task 52 percent slower

The stage did not move. 7,572 ms against 8,176 ms on the means is a ratio of 1.0797 at a p
of 0.7429, which is undecided. None of the three stage wall comparisons separated and none
of the three application wall comparisons did either.

The largest task did move and it moved the wrong way. 5,370 ms became 8,146 ms. That one
separated at the floor, which means every reading of one arm sits above every reading of the
other. On four passes an arm there is nothing stronger available.

The reason is the thing this README already said before any of it was run. Hash partitioning
sends one key to one partition however many partitions there are. Eighty five percent of the
rows are on a single key, so that key's partition is the critical path at any count. Cutting
from 8 to 2 does not move the hot key. It moves the other fifteen percent onto fewer
partitions, so the hot task carries more and finishes later.

The advisory size the recommendation is built on is a total divided by a target. A total
cannot see a distribution. That was an argument on an earlier read of this file and it is a
measurement now.

### I ran the same schedule twice and three of its four separations did not come back

The first schedule reported four comparisons as separated at the floor. The second reported
one.

| largest task comparison | first | second | what changed |
| --- | --- | --- | --- |
| advised against current | 1.3979 | 1.5170 | separated both times |
| volume against current | 1.4089 | 1.3236 | separated, then undecided at p 0.0571 |
| rounded against current | 1.1933 | 1.2047 | separated, then undecided at p 0.0571 |
| rounded against volume | 0.8470 | 0.9102 | separated, then undecided at p 0.3143 |

This is a table rather than a fenced block because no command prints it. The second column is
in the output above. The first column cannot be re-derived from anything here, because that
schedule's manifest was written to a scratch directory and not kept. That is the reason the
manifest exists at all and it was thrown away anyway.

None of the three reversed. Every one stayed on the same side of 1 and lost its separation,
and two came back at 0.0571, which is one reading out of order.

That is what a floor of 0.0286 buys. Four passes an arm gives seventy orderings and the
lowest reachable p is two of them, so a separated verdict means the two arms do not
interleave at all. One reading moving by a few hundred milliseconds ends it. Four of those
were being read here as results. One of them is a result.

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
      bytes         88788038 measured  768000000 estimated
      plan text     agrees at 8
      per partition 11098505 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. the volume asks for 2 rather than 8, and taking that cut was measured here to leave the stage wall alone and make the largest task 40 to 52 percent slower
      why           stage 3 reads 44.2 on records_read, so one key is most of the rows and a hash keeps it on one partition at any count
  RoundRobinPartitioning into 8 partitions  chosen by REPARTITION_BY_NUM
      bytes         42048255 measured  128000000 estimated
      plan text     agrees at 8
      per partition 5256032 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. REPARTITION_BY_NUM is an argument in the query, so the config does not decide it
  hashpartitioning into 8 partitions  chosen by ENSURE_REQUIREMENTS
      bytes         2401 measured  4776 estimated
      plan text     agrees at 8
      per partition 300 measured bytes each
      schedule      4 rounds of 2 slots, 2 in the last
      partitions    none. the volume asks for 2 rather than 8, and taking that cut was measured here to leave the stage wall alone and make the largest task 40 to 52 percent slower
      why           stage 3 reads 44.2 on records_read, so one key is most of the rows and a hash keeps it on one partition at any count
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
and the join log still reports 3 worth changing while the skewed join reports 1. The two
that went are the cuts and what is left is the broadcast candidate.

One thing this surfaced that was not the point of it. Both hash exchanges on the skewed
join are withheld, and the report was treating them as two independent recommendations.
They are not. A sort merge join partitions both sides by the same key into the same number
of pieces, so the two exchanges are one knob and `spark.sql.shuffle.partitions` is the
knob. The report still prints a line per exchange, which is right for reading the log and
wrong for counting the advice.

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

## Running the checks

```
python tests/run_all.py
341 passed, 0 failed, 341 checks
```

Every check is graded by a mutation pass rather than counted.

```
sjp/layout.py: 87 mutation sites, running 0 to 87
87 killed, 0 survived, 0 ungraded, 87 graded
sjp/bench.py: 38 mutation sites, running 0 to 38
34 killed, 4 survived, 0 ungraded, 38 graded
sjp/model.py: 44 mutation sites, running 0 to 44
44 killed, 0 survived, 0 ungraded, 44 graded
sjp/plan.py: 30 mutation sites, running 0 to 30
30 killed, 0 survived, 0 ungraded, 30 graded
sjp/commands.py: 29 mutation sites, running 0 to 29
29 killed, 0 survived, 0 ungraded, 29 graded
sjp/skew.py: 28 mutation sites, running 0 to 28
28 killed, 0 survived, 0 ungraded, 28 graded
sjp/memory.py: 22 mutation sites, running 0 to 22
22 killed, 0 survived, 0 ungraded, 22 graded
sjp/cli.py: 18 mutation sites, running 0 to 18
18 killed, 0 survived, 0 ungraded, 18 graded
sjp/eventlog.py: 9 mutation sites, running 0 to 9
9 killed, 0 survived, 0 ungraded, 9 graded
sjp/contract.py: 6 mutation sites, running 0 to 6
6 killed, 0 survived, 0 ungraded, 6 graded
scripts/fixture_probe.py: 20 mutation sites, running 0 to 20
19 killed, 1 survived, 0 ungraded, 20 graded
jobs/sample.py: 23 mutation sites, running 0 to 23
2 killed, 21 survived, 0 ungraded, 23 graded
```

The last row is the honest one. Every surviving mutant in `jobs/sample.py` sits in code
that only runs with a Spark session, and a check cannot have one. What grades that module
is the three logs it produced, which is weaker than a mutant and is not nothing.

That row has now got worse twice for the same reason. The join job added three sites and
the skewed join added two more, all of them in the same unreachable place, so the
denominator moves and the numerator does not. Saying so is the point. A score that only
ever gets quoted when it improves is not a measurement.

That row got worse on purpose. Two of its mutants used to die against checks that read the
module's own constants back out of the module, which is transcription and would have gone
stale the moment somebody changed the constant and the check together. Those were replaced
by checks that read the same settings out of the committed logs instead. The score fell and
the question being asked got better.

The four survivors in `sjp/bench.py` are all boundary comparisons and all four were checked
rather than waved through. One widens a float tolerance that already absorbs the
difference, so the two forms give the same answer on every input tried and on every input
where the values differ by more than a nanosecond. The other three separate a p value or a
floor from the threshold at exactly equal, and a p value is a count over the number of
splits. Four passes an arm puts that denominator at 70 and 0.05 of 70 is 3.5, so the case
is not reachable on the schedule this repo runs. It is reachable at 2 passes against 14,
which is not a benchmark anyone would run. The first pass at this module killed 24 of 38.
Six survivors were guards checked against a value they must refuse and never against the
smallest value they must accept, and four were records nothing asserted frozen.

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

The model maps twelve accumulables onto a field it keeps per task. The grouping stage of
the skewed job carries thirty seven. The other twenty five are not read, and nothing here
argues that they are uninteresting.

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

Four passes an arm puts the lowest reachable p at 0.0286, so a separated verdict means the
two arms do not interleave at all and one reading decides it. Running the schedule a second
time kept one of its four separations. Read a separated verdict here as a direction worth
re-running rather than as a settled number, and read an undecided one as carrying almost no
information at this sample size.

The guard that withholds a cut counts the exchanges it withholds on separately, and on a sort
merge join they are not separate. Both sides are partitioned by the same key into the same
number of pieces, so the two exchanges are one knob and `spark.sql.shuffle.partitions` is the
knob. The report prints a line for each, which is right for reading the log and wrong for
counting the advice.

The origins that read as config decided are a list of two names. A fourth origin would fall
through to not changeable and be refused in silence, which is exactly how the third one was
missed. The list is the whole test and there is no check that it is complete, because nothing
in a log says what origins Spark can write.

The millisecond floor of 50 sits in a gap the two logs leave and its position inside that gap
is a judgement. The sweep from 0 to 3041 is published above so the headline count reads as a
fact about the floor. Bytes and counts have no floor at all, because neither log produces a
case small enough to bound one, so a tiny byte skew is reported rather than suppressed.

The spill analysis reports what spilled and cannot say what it spilled against. Both logs
were captured in local mode and neither records the four properties that decide a task's
execution memory. A log from a cluster would carry them and nothing here has been run against
one.

`undecided` is five of nine verdicts on the healthy job. That is a high proportion for a
detector and it is a property of these fixtures rather than of the rule. A real job with two
hundred partitions per stage would clear the task floor everywhere.

The detector reads task fields and never a stage total, so the twelve of thirty seven above
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

Four passes an arm is the smallest schedule that can return a p under 0.05, so every
comparison here either lands on the floor or lands nowhere. An effect real but smaller than
these arms can separate reads as undecided, and undecided is not a finding of no difference.

The benchmark logs are not committed. Four arms at four passes is about seventeen event logs
and tens of megabytes, and the numbers above are reproducible from the command beside them
rather than from a file in the tree.
