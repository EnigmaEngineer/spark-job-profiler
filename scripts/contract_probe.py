"""Print the command to effect map, then test the claim each command makes.

    python scripts/contract_probe.py

Exits 1 if a read touched the store, or if either control fails. Every judgement is made
by sjp.contract. What lives here is the driving, the printing and the controls.
"""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sjp import cli, contract  # noqa: E402
from sjp import commands  # noqa: E402,F401  registers the real commands

FIXTURES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "tests", "fixtures", "eventlogs", "skewed")


def main():
    print("mapping, which is what a guard reads:")
    print(json.dumps(cli.mapping(), indent=2))

    root = tempfile.mkdtemp(prefix="sjp-probe-")
    store = os.path.join(root, "logs")
    shutil.copytree(FIXTURES, store)
    failures = []
    try:
        touched = contract.reads_that_touched_the_store(cli.COMMANDS, store)
        print("reads that changed the store:", touched or "none")
        if touched:
            failures.append("a read changed the store")

        # Control one. A read that writes has to be named, or a clean result above means
        # nothing more than that the loop ran zero times.
        def liar(rest):
            with open(os.path.join(rest[0], "written-by-a-read"), "w") as handle:
                handle.write("!")
            return 0

        lying = dict(cli.COMMANDS)
        lying["liar"] = {"effect": cli.READ, "help": "", "run": liar,
                         "probe": lambda where: [where]}
        caught = [name for name, _moved in contract.reads_that_touched_the_store(lying, store)]
        print("control, lying read caught:", caught)
        if caught != ["liar"]:
            failures.append("the lying read was not caught")

        # Control two. A misspelt effect refuses at registration rather than at run time.
        try:
            cli.command("oops", "readonly", "")(lambda rest: 0)
            print("control, bad effect: NOT REFUSED")
            failures.append("a misspelt effect registered")
        except cli.Registration as problem:
            print("control, bad effect refused:", problem)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    for line in failures:
        print("FAILED:", line)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
