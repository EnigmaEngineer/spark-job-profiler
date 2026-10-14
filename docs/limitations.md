# Limitations

Moved out of the README so the front page stays short. Back to the [README](../README.md).

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
