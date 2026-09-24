"""The read safety check, and the controls that prove it can fail."""
import os
import shutil
import tempfile

from sjp import cli, contract

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "fixtures", "eventlogs", "skewed")


def _store():
    """A throwaway copy of the fixture directory. Never the repo's own copy."""
    root = tempfile.mkdtemp(prefix="sjp-store-")
    shutil.copytree(FIXTURES, os.path.join(root, "logs"))
    return root, os.path.join(root, "logs")


def check_a_snapshot_of_the_same_tree_twice_is_identical():
    root, logs = _store()
    try:
        assert contract.differences(contract.snapshot(logs), contract.snapshot(logs)) == []
    finally:
        shutil.rmtree(root)


def check_a_new_file_shows_up_as_a_difference():
    root, logs = _store()
    try:
        before = contract.snapshot(logs)
        with open(os.path.join(logs, "planted"), "w") as handle:
            handle.write("x")
        assert contract.differences(before, contract.snapshot(logs)) == ["planted"]
    finally:
        shutil.rmtree(root)


def check_a_rewritten_file_shows_up_as_a_difference():
    root, logs = _store()
    try:
        before = contract.snapshot(logs)
        victim = sorted(os.listdir(logs))[0]
        with open(os.path.join(logs, victim), "a") as handle:
            handle.write("\n")
        assert contract.differences(before, contract.snapshot(logs)) == [victim]
    finally:
        shutil.rmtree(root)


def check_a_new_directory_shows_up_as_a_difference():
    root, logs = _store()
    try:
        before = contract.snapshot(logs)
        os.mkdir(os.path.join(logs, "nested"))
        assert contract.differences(before, contract.snapshot(logs)) == ["nested"]
    finally:
        shutil.rmtree(root)


def check_the_real_read_commands_leave_the_store_alone():
    from sjp import commands  # noqa: F401
    root, logs = _store()
    try:
        assert contract.reads_that_touched_the_store(cli.COMMANDS, logs) == []
    finally:
        shutil.rmtree(root)


def check_a_read_that_writes_is_caught():
    """The control. Without it a clean result could mean the check runs nothing."""
    def liar(rest):
        with open(os.path.join(rest[0], "written-by-a-read"), "w") as handle:
            handle.write("!")
        return 0

    registry = {"liar": {"effect": cli.READ, "help": "", "run": liar,
                         "probe": lambda store: [store]}}
    root, logs = _store()
    try:
        caught = contract.reads_that_touched_the_store(registry, logs)
        assert [name for name, _moved in caught] == ["liar"], caught
        assert caught[0][1] == ["written-by-a-read"], caught
    finally:
        shutil.rmtree(root)


def check_a_read_with_no_probe_is_refused_rather_than_skipped():
    registry = {"undrivable": {"effect": cli.READ, "help": "", "run": lambda rest: 0,
                               "probe": None}}
    root, logs = _store()
    try:
        try:
            contract.reads_that_touched_the_store(registry, logs)
        except cli.Registration as problem:
            assert "undrivable" in str(problem), problem
        else:
            raise AssertionError("a read nobody can drive was reported as clean")
    finally:
        shutil.rmtree(root)


def check_a_write_command_is_not_run_by_the_check():
    """A write is expected to change the store, so the check must not grade it."""
    def should_never_run(rest):
        raise AssertionError("the check ran a write command")

    registry = {"w": {"effect": cli.WRITE, "help": "", "run": should_never_run,
                      "probe": lambda store: [store]}}
    root, logs = _store()
    try:
        assert contract.reads_that_touched_the_store(registry, logs) == []
    finally:
        shutil.rmtree(root)
