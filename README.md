# spark-job-profiler

Reads a Spark event log and says why the job was slow. The Spark UI shows the numbers.
This reads the same file and names the stage that skewed, the spill that mattered and the
spill that did not.

```
python -m sjp stages tests/fixtures/eventlogs/skewed/*
```

That needs nothing but the standard library. Capturing a fresh log needs pyspark.

## Where it is

The parser, the stage and task model and the skew detector are built, against two committed
logs. The spill analysis and the recommendations are not. `sjp stages` still prints
measurements and no verdict about any of them, because the verdicts live in `sjp skew`.

## One entry point, and every command says what it does to state

```
$ python -m sjp commands
{
  "capture": "write",
  "inventory": "read",
  "skew": "read",
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
$ python scripts/contract_probe.py
mapping, which is what a guard reads:
{
  "capture": "write",
  "inventory": "read",
  "skew": "read",
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
sjp/commands.py     the commands that exist
jobs/sample.py      two jobs that differ in one expression
scripts/            drivers and controls, no judgements
tests/fixtures/     two real event logs, unedited
```

## The sample jobs

Two jobs over 8,000,000 rows. Same shuffle partition count and same payload in both.
Adaptive execution off. One puts 85 percent of the rows on a single key. The other
spreads them over 199. That is the only difference.

The point is not that they are realistic. It is that the pathology is known before the
profiler is pointed at the log, so the profiler can be graded instead of believed.

Measured on this machine under pyspark 3.5.6 and OpenJDK 11.0.32.1.

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
$ python -m sjp skew tests/fixtures/eventlogs/skewed/*
local-1790267120237  sjp-skewed  threshold 4.0  9 verdicts
  undecided stage 0  records_read       no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 0  duration           no ratio  2 tasks, and below 3 the ratio cannot pass 2
  undecided stage 0  memory_spilled     no ratio  2 tasks, and below 3 the ratio cannot pass 2
  even      stage 1  records_read         1.0000  largest task 1000000 against a median of 1000000
  even      stage 1  duration             1.7378  largest task 855 against a median of 492
  undecided stage 1  memory_spilled     no ratio  every task reads zero, so there is no spread
  skewed    stage 2  records_read        44.2179  largest task 6932663 against a median of 156784
  skewed    stage 2  duration            14.7246  largest task 3048 against a median of 207
  skewed    stage 2  memory_spilled    unbounded  more than half the tasks read zero and one reads 620755808
  3 skewed, 2 even, 4 undecided
  worst  stage 2 on memory_spilled at unbounded
```

The same command on the balanced log, which differs from the skewed one in one expression.

```
  0 skewed, 4 even, 5 undecided
  nothing skewed at this threshold
```

It exits 1 when something skewed, so a shell can act on the answer.

### There is a third verdict and it carried the day

A verdict is `skewed` or `even` or `undecided`. The third one is the addition.

Four of the nine verdicts on the skewed log are `undecided` and none is a gap in the tool.
Three are the two task stage. With two tasks the largest value is one of the two the median
averages, so the ratio is `2b / (a + b)` and it cannot reach 2 however extreme the pair.
That was searched rather than assumed.

```
n=1   values [1]*0 + [1e6] -> spread 1.0000
n=2   values [1]*1 + [1e6] -> spread 2.0000
n=3   values [1]*2 + [1e6] -> spread 1000000.0000
```

So a stage below three tasks gets no verdict rather than a reassuring one. The fourth
`undecided` is a spill metric no task moved, which is a different answer from a spill that
was spread evenly.

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

## Running the checks

```
$ python tests/run_all.py
148 passed, 0 failed, 148 checks
```

Every check is graded by a mutation pass rather than counted.

```
sjp/skew.py: 20 mutation sites, running 0 to 20
20 killed, 0 survived, 0 ungraded, 20 graded
sjp/model.py: 35 mutation sites, running 0 to 35
35 killed, 0 survived, 0 ungraded, 35 graded
sjp/commands.py: 24 mutation sites, running 0 to 24
24 killed, 0 survived, 0 ungraded, 24 graded
sjp/eventlog.py: 9 mutation sites, running 0 to 9
9 killed, 0 survived, 0 ungraded, 9 graded
sjp/cli.py: 18 mutation sites, running 0 to 18
18 killed, 0 survived, 0 ungraded, 18 graded
sjp/contract.py: 6 mutation sites, running 0 to 6
6 killed, 0 survived, 0 ungraded, 6 graded
scripts/fixture_probe.py: 20 mutation sites, running 0 to 20
19 killed, 1 survived, 0 ungraded, 20 graded
jobs/sample.py: 17 mutation sites, running 0 to 17
2 killed, 15 survived, 0 ungraded, 17 graded
```

The last row is the honest one. Every surviving mutant in `jobs/sample.py` sits in code
that only runs with a Spark session, and a check cannot have one. What grades that module
is the pair of logs it produced, which is weaker than a mutant and is not nothing.

That row got worse on purpose. Two of its mutants used to die against checks that read the
module's own constants back out of the module, which is transcription and would have gone
stale the moment somebody changed the constant and the check together. Those were replaced
by checks that read the same settings out of the committed logs instead. The score fell and
the question being asked got better.

The one survivor in `scripts/fixture_probe.py` moves a `sys.path.insert` index from 0 to 1,
which changes nothing about what gets imported here. It is left alone rather than tested.

`tests/runner.py` is not in the table. Mutating the collection loop while using it as the
oracle grades it against itself, so whatever number came out would not mean anything.

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

One threshold covers every metric. A spill ratio and a duration ratio almost certainly do
not deserve the same constant. Nothing measured here says what the difference should be, so
per metric thresholds wait for a log that argues for one.

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
