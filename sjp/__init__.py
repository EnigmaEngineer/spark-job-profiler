"""Read a Spark event log and say why the job was slow."""

# Not a release number. Nothing outside this repo imports sjp and this promises no
# compatibility to anyone.
#
# It is here because `sjp skew` exits 1 when a stage skewed and that status is computed
# over whatever the scan judged. A shell holding a stored status needs to know what was
# judged to know what the status meant, so the command prints this beside the id of the
# metric set it used.
#
# The minor digit moves when the metric set changes and not when anything else does.
# `skew.METRIC_SET_HISTORY` is the list, and a check refuses a set whose id is not the
# last entry there. A field added to `model.Task` widens the set without anybody editing
# `sjp.skew`, so that check is what stops the version from quietly going stale.
__version__ = "0.2.0"
