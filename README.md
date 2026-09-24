# spark-job-profiler

Reads a Spark event log and says why the job was slow. The Spark UI shows the numbers.
This reads the same file and names the stage that skewed, the spill that mattered and the
spill that did not.

```
python -m sjp inventory tests/fixtures/eventlogs/skewed
```

That needs nothing but the standard library. Capturing a fresh log needs pyspark.

## Where it is

The event log format is measured and written down, the sample jobs exist and produce known
pathologies, and there are committed logs to build against. The parser, the skew detector
and the recommendations are not built yet.

## One entry point, and every command says what it does to state

```
$ python -m sjp commands
{
  "capture": "write",
  "inventory": "read"
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
  "inventory": "read"
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
sjp/eventlog.py     read a log, count events, say what is missing
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
skewed    stage 0  tasks 2
    records   median 0.0  max 0  ratio 0.00
    duration  median 3886.0 ms  max 3895 ms  ratio 1.00
    spilled   117440288 memory  61304287 disk
skewed    stage 1  tasks 8
    records   median 1000000.0  max 1000000  ratio 1.00
    duration  median 492.0 ms  max 855 ms  ratio 1.74
    spilled   0 memory  0 disk
skewed    stage 2  tasks 8
    records   median 156784.0  max 6932663  ratio 44.22
    duration  median 207.0 ms  max 3048 ms  ratio 14.72
    spilled   620755808 memory  88088795 disk
balanced  stage 0  tasks 2
    records   median 0.0  max 0  ratio 0.00
    duration  median 3671.5 ms  max 3686 ms  ratio 1.00
    spilled   117440288 memory  61304287 disk
balanced  stage 1  tasks 8
    records   median 1000000.0  max 1000000  ratio 1.00
    duration  median 542.0 ms  max 849 ms  ratio 1.57
    spilled   0 memory  0 disk
balanced  stage 2  tasks 8
    records   median 984924.5  max 1206030  ratio 1.22
    duration  median 938.0 ms  max 1258 ms  ratio 1.34
    spilled   0 memory  0 disk
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

## Running the checks

```
$ python tests/run_all.py
65 passed, 0 failed, 65 checks
```

Every check is graded by a mutation pass rather than counted.

```
sjp/cli.py: 18 mutation sites, running 0 to 18
18 killed, 0 survived, 0 ungraded, 18 graded
sjp/commands.py: 14 mutation sites, running 0 to 14
14 killed, 0 survived, 0 ungraded, 14 graded
sjp/contract.py: 6 mutation sites, running 0 to 6
6 killed, 0 survived, 0 ungraded, 6 graded
sjp/eventlog.py: 9 mutation sites, running 0 to 9
9 killed, 0 survived, 0 ungraded, 9 graded
scripts/fixture_probe.py: 17 mutation sites, running 0 to 17
16 killed, 1 survived, 0 ungraded, 17 graded
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

The arithmetic that grades the fixtures lives in the test module rather than in the
library. That is deliberate for now, because the real detector does not exist yet and a
median in two places would be a second implementation to keep in step. It moves into the
library the day the detector lands.
