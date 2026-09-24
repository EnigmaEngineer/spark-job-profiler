# Where these came from

Two real event logs, unedited. Both were produced on this machine by

```
python -m sjp capture --job skewed   --out <dir> --rows 8000000
python -m sjp capture --job balanced --out <dir> --rows 8000000
```

running pyspark 3.5.6 on Java 11. The session was `local[2]` with a 1g driver and 8
shuffle partitions. Adaptive execution was off. The two jobs differ in one expression,
which is the key. Everything else is held.

They are committed rather than regenerated because a fixture that has to be rebuilt by
anyone who clones the repo is a fixture most people never run.
