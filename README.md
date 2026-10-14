# spark-job-profiler

Reads a Spark event log and says why the job was slow. The Spark UI shows the numbers.
This reads the same file and names the stage that skewed, the spill that mattered and the
spill that did not.

```
python -m sjp stages tests/fixtures/eventlogs/skewed/*
```

That needs nothing but the standard library. Capturing a fresh log needs pyspark.

```
python tests/run_all.py
458 passed, 0 failed, 458 checks
```

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

## Results

Five sample jobs over 8,000,000 rows with adaptive execution off, plus two later ones that
exist to stress the profiler's own output. Six real event logs are committed unedited under
`tests/fixtures/`. The benchmark arms ran on two slots on one machine.

**The healthy job spills the same bytes as the broken one.** Stage 0 of the balanced log
matches stage 0 of the skewed log to the byte. What separates them is how few tasks did it.
The skewed grouping stage puts all 620,755,808 bytes in one task of eight. A tool that only
reports that a stage spilled is reporting something both jobs do.

**A spill is two measurements of one event.** `Memory Bytes Spilled` and `Disk Bytes
Spilled` describe the same records before and after serialization. Adding them reads
708,844,603 bytes against 620,755,808 of real data, and the factor between the two is not a
constant you could convert with.

**Taking the profiler's own partition advice made the job no faster and its worst task 39
percent slower.** The largest task went from 4,010 ms to 5,570 ms. Eighty five percent of
the rows sit on one key, and hash partitioning sends one key to one partition at any count.
Cutting the count moves the other fifteen percent onto fewer partitions. The advice is a
total divided by a target, and a total cannot see a distribution.

**At the recommended count the skew detector stops naming the stage that carries the
skew.** On all four passes it named a stage that reads no rows instead.

**Four passes an arm was not enough.** I ran the same four pass schedule twice and three of
its four separated comparisons did not come back. None reversed. They all lost their
separation. Six passes an arm is the default now.

## Known limitations

- The benchmark ran on two slots on one machine.
- The spill analysis reports what spilled and cannot say what it spilled against.
- The partition target is arithmetic over one shuffle and the slot count, so it cannot see
  a hot key.
- The millisecond floor is not derived and cannot be from these logs.
- `undecided` is five of nine verdicts on the healthy job.
- The benchmark logs are not committed. Four arms at six passes is twenty five event logs.
- An event log discloses the driver's working directory and the user who ran it.

The full list is in [docs/limitations.md](docs/limitations.md).

## More detail

**[Design notes](docs/design-notes.md)**

- [Where it is](docs/design-notes.md#where-it-is)
- [One entry point, and every command says what it does to state](docs/design-notes.md#one-entry-point-and-every-command-says-what-it-does-to-state)
- [The sample jobs](docs/design-notes.md#the-sample-jobs)
- [The stage and task model](docs/design-notes.md#the-stage-and-task-model)
  - [The events it reads are the events it registers](docs/design-notes.md#the-events-it-reads-are-the-events-it-registers)
  - [The stage totals are right, and that is why they are not enough](docs/design-notes.md#the-stage-totals-are-right-and-that-is-why-they-are-not-enough)
  - [Twelve of how many](docs/design-notes.md#twelve-of-how-many)
  - [The name is not an address for a plan metric](docs/design-notes.md#the-name-is-not-an-address-for-a-plan-metric)
- [Naming the stage that skewed](docs/design-notes.md#naming-the-stage-that-skewed)
  - [There is a third verdict and it carried the day](docs/design-notes.md#there-is-a-third-verdict-and-it-carried-the-day)
  - [The zero median was answering 0.0 and that was the worst available answer](docs/design-notes.md#the-zero-median-was-answering-00-and-that-was-the-worst-available-answer)
  - [Where the threshold came from](docs/design-notes.md#where-the-threshold-came-from)
  - [The metric set was three names I chose](docs/design-notes.md#the-metric-set-was-three-names-i-chose)
  - [Widening it broke the control, and that is what the floor is for](docs/design-notes.md#widening-it-broke-the-control-and-that-is-what-the-floor-is-for)
  - [One worst stage was the metric order in disguise](docs/design-notes.md#one-worst-stage-was-the-metric-order-in-disguise)
- [What spilled, and what the log will not say about why](docs/design-notes.md#what-spilled-and-what-the-log-will-not-say-about-why)
  - [The healthy job spills the same bytes as the broken one](docs/design-notes.md#the-healthy-job-spills-the-same-bytes-as-the-broken-one)
  - [A spill is two measurements of one event](docs/design-notes.md#a-spill-is-two-measurements-of-one-event)
  - [Peak execution memory is missing exactly where the pressure was](docs/design-notes.md#peak-execution-memory-is-missing-exactly-where-the-pressure-was)
  - [There is no memory budget in either log](docs/design-notes.md#there-is-no-memory-budget-in-either-log)
- [Partition counts and broadcast candidates](docs/design-notes.md#partition-counts-and-broadcast-candidates)
  - [There are two sizes and they answer different questions](docs/design-notes.md#there-are-two-sizes-and-they-answer-different-questions)
  - [The measured number is smaller on the job with the problem](docs/design-notes.md#the-measured-number-is-smaller-on-the-job-with-the-problem)
  - [Most partition counts are not the config's to change](docs/design-notes.md#most-partition-counts-are-not-the-configs-to-change)
  - [Slots are cores only while a task asks for one cpu](docs/design-notes.md#slots-are-cores-only-while-a-task-asks-for-one-cpu)
  - [What the log will not say](docs/design-notes.md#what-the-log-will-not-say)
- [Applying the recommendation, and what it did](docs/design-notes.md#applying-the-recommendation-and-what-it-did)
  - [Taking the advice made the job no faster and its worst task 39 percent slower](docs/design-notes.md#taking-the-advice-made-the-job-no-faster-and-its-worst-task-39-percent-slower)
  - [At four passes I ran the same schedule twice and three of its four separations did not come back](docs/design-notes.md#at-four-passes-i-ran-the-same-schedule-twice-and-three-of-its-four-separations-did-not-come-back)
  - [Six passes an arm, run twice on 2026-10-08, and what came back](docs/design-notes.md#six-passes-an-arm-run-twice-on-2026-10-08-and-what-came-back)
  - [The skew detector goes blind at the count the layout command recommends](docs/design-notes.md#the-skew-detector-goes-blind-at-the-count-the-layout-command-recommends)
  - [Rounding the target to whole rounds is a direction rather than a result](docs/design-notes.md#rounding-the-target-to-whole-rounds-is-a-direction-rather-than-a-result)
  - [What a reader should take from the table](docs/design-notes.md#what-a-reader-should-take-from-the-table)
- [The tool now carries what the benchmark measured](docs/design-notes.md#the-tool-now-carries-what-the-benchmark-measured)
- [One count, two exchanges, and the two answers it used to give](docs/design-notes.md#one-count-two-exchanges-and-the-two-answers-it-used-to-give)
- [The third origin, and the advice it was throwing away](docs/design-notes.md#the-third-origin-and-the-advice-it-was-throwing-away)
- [Two logs the tool could not read well, and what they cost it](docs/design-notes.md#two-logs-the-tool-could-not-read-well-and-what-they-cost-it)
- [The answer goes first now](docs/design-notes.md#the-answer-goes-first-now)
- [The exit status, and the set it is counted over](docs/design-notes.md#the-exit-status-and-the-set-it-is-counted-over)

**[Running the checks](docs/running-the-checks.md)**

**Other docs**

- [adr-0001-what-an-event-log-actually-holds](docs/adr-0001-what-an-event-log-actually-holds.md)
- [adr-0002-what-a-stage-total-can-and-cannot-say](docs/adr-0002-what-a-stage-total-can-and-cannot-say.md)
- [adr-0003-what-a-median-relative-threshold-cannot-say](docs/adr-0003-what-a-median-relative-threshold-cannot-say.md)
- [adr-0004-what-the-log-cannot-say-about-memory-pressure](docs/adr-0004-what-the-log-cannot-say-about-memory-pressure.md)
- [adr-0005-two-sizes-and-which-question-each-one-answers](docs/adr-0005-two-sizes-and-which-question-each-one-answers.md)
- [adr-0006-why-two-of-three-magnitude-floors-can-be-derived](docs/adr-0006-why-two-of-three-magnitude-floors-can-be-derived.md)
