# Running the checks

Moved out of the README so the front page stays short. Back to the [README](../README.md).

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
that added the two later jobs took it in. Four job names then went down one branch, two
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
