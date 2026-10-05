# Why two of three magnitude floors can be derived and the third cannot

## Context

A skew verdict is a ratio, and a ratio carries no unit. A task reading 7,449 bytes against
a median of 793 and a task spilling 620,755,808 bytes against a median of zero are both
reported as skewed, and the first one is nothing. So a verdict needs the largest value to
clear a magnitude floor, and the floor has to be per unit.

The millisecond floor was placed first, at 50, inside a gap of 29 to 71 measured on the two
logs captured at eight million rows. The byte and count floors were left at `None` for five
weeks, and `None` means no floor. Nothing in the repo measured where they belonged, because
every log here produced byte verdicts in the tens of millions and record verdicts in the
millions. A bracket needs two ends and only one of them was populated.

## What was measured

The 600 row capture of the same skewed distribution gives the small end. The hot key still
takes 85 percent of the rows, so the pathology is identical and only the scale moves.

`scripts/floor_probe.py` reads both ends off the committed logs.

```
bytes  noise       7449  real   59191004  span   7946x
count  noise        518  real    6932663  span  13383x
```

The noise end is the largest value the 600 row log reaches. The real end is the smallest
value any other committed log reaches, taken across all seven rather than off the one that
happened to be open.

A second measurement decided how to read those brackets. The same job was captured at 600
and 6,000 and 60,000 and 600,000 and 8,000,000 rows, and the ratio barely moves.

| rows | records_read ratio | largest records | local_bytes_read ratio | largest bytes |
|---|---|---|---|---|
| 600 | 43.17 | 518 | 9.39 | 7,449 |
| 6,000 | 43.94 | 5,185 | 22.10 | 68,790 |
| 60,000 | 44.19 | 51,995 | 31.89 | 727,776 |
| 600,000 | 44.19 | 519,941 | 34.41 | 7,764,485 |
| 8,000,000 | 44.22 | 6,932,663 | 34.99 | 109,685,777 |

Reproduce it with

```
bash -c 'for n in 600 6000 60000 600000 8000000; do python -m sjp capture --job skewed --rows $n --out /tmp/ladder/$n; done'
```

The record ratio moves by one percent across four orders of magnitude of input. That is the
whole argument for a magnitude floor. The ratio is a property of the key distribution, which
is held, so it cannot tell a toy from a production job and the magnitude is the only thing
that can.

## The decision

**The floor is the geometric mean of its bracket, to one significant figure.**

```
bytes  floor     700000  which is   93x the noise end and a 84th of the real
count  floor      60000  which is  115x the noise end and a 115th of the real
```

A geometric mean rather than an arithmetic one, because these are quantities people compare
by ratio. The arithmetic middle of the byte bracket is 29,599,226, which sits within a
factor of two of the real end. That is a floor that starts deciding against real evidence
the first time a job arrives slightly smaller than the fixture.

One significant figure because the precision is not there. Rounding to it leaves roughly
equal multiplicative slack on each side, which is the property worth having when the
bracket spans four orders of magnitude.

The rule returns 50 for the millisecond bracket of 29 to 71, which is the number that
shipped weeks before the rule was written. That is one independent agreement and it is not
evidence the rule is right. It is evidence the rule was not reverse engineered to produce
the two new numbers.

## What the brackets do not settle

The committed logs bracket the byte floor and they do not locate it. Every candidate from
10,000 to 59,191,004 gives an identical answer on all eight.

```
bytes verdicts surviving each candidate floor
  log                        0       10000      100000      700000    10000000    59191004    60000000
  skewed                     4           4           4           4           4           4           4
  skewed_join                4           4           4           4           4           4           3
  small                      1           0           0           0           0           0           0
```

The millisecond floor had a gap of 29 to 71, a factor of 2.4, and every value inside it gave
the same answer. That is a narrow gap and it pins a number. This one spans a factor of
5,919 and the ladder above shows the interior is reachable by choosing a row count. At
60,000,000 the floor starts removing real evidence, which is the only hard edge either
bracket has.

So the position inside the bracket is a policy about the smallest job this tool will comment
on rather than a separation the data hands over. A 60,000 row job spilling 727,776 bytes
loses its byte verdict under this floor and a 600,000 row job keeps it. That is the line and
it is a choice.

## The part that was not expected

**The 600 row log falsifies the millisecond bracket it was not captured to test.**

Its grouping stage runs one task for 556 milliseconds against a median of 114.5, on 600
rows. 556 is a noise value and it is eight times 71, which is the smallest millisecond value
anything here calls real. Read over all eight logs the millisecond bracket is `(556, 71)`, a
noise end above a real end, and `floor_from` refuses it.

The reason the method works for two kinds and not the third is that bytes and records scale
with the data and a task's wall time does not. A 600 row task still pays for a JVM, a task
launch and a serialisation. The floor under a trivial job's duration is set by fixed cost
rather than by the work, so a magnitude floor cannot separate it from a real pause.

The millisecond floor therefore stays at 50, placed by hand, and the small log still reports
two `executor_run_time` verdicts nobody would act on. It is not fixed here. A fix is a
different mechanism rather than a different number, and the candidates are a floor on the
stage's total work rather than on one task's duration, or an absolute delta beside the
ratio. Neither has been measured.

## What this rules out

Picking the byte floor as a round number in the right region. 1 MB would have been
defensible and unexamined, and the sweep above is what says why that is not the same thing
as a derived number with its slack reported.

Deriving the millisecond floor the same way, which the measurement refused.
