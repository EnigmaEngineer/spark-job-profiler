"""Run the suite. Exit 1 on any failure.

    python tests/run_all.py

The imports happen inside `main` on purpose. A module that calls `sys.exit` while being
imported would otherwise end this process with whatever code it chose, printing nothing,
and a suite that exits 0 having run no checks is indistinguishable from a suite that
passed. Anything that stops the collection is a failure here, including `SystemExit`.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EXPECTED_MODULES = 8


def collect():
    from tests import test_cli_contract, test_commands, test_contract_checks
    from tests import test_eventlog, test_fixture_pathologies, test_model
    from tests import test_sample_jobs, test_skew
    return [test_cli_contract, test_commands, test_contract_checks,
            test_eventlog, test_fixture_pathologies, test_model, test_sample_jobs,
            test_skew]


def main():
    from tests import runner
    try:
        modules = collect()
    except BaseException as problem:
        print("FAIL could not collect the suite: {!r}".format(problem))
        return 1

    if len(modules) != EXPECTED_MODULES:
        print("FAIL collected {} modules, expected {}".format(len(modules), EXPECTED_MODULES))
        return 1

    passed, failures = runner.run_checks(modules)
    for name, detail in failures:
        print("FAIL {}\n{}".format(name, detail))
    print("{} passed, {} failed, {} checks".format(passed, len(failures),
                                                   passed + len(failures)))
    if passed + len(failures) == 0:
        print("FAIL the suite ran no checks at all")
        return 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
