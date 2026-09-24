"""The collection loop, on its own so a second runner can stand in for the first.

A runner that imports the runner it is standing in for cannot replace it, so the loop
lives here and both entry points call it.
"""
import traceback


def checks_in(module):
    return [(name, getattr(module, name)) for name in sorted(dir(module))
            if name.startswith("check_") and callable(getattr(module, name))]


def run_checks(modules):
    """Run every check_ function in every module. Return passed and failed counts."""
    passed = 0
    failures = []
    for module in modules:
        found = checks_in(module)
        if not found:
            failures.append((module.__name__, "holds no checks at all"))
            continue
        for name, fn in found:
            try:
                fn()
                passed += 1
            except Exception:
                failures.append((module.__name__ + "." + name, traceback.format_exc()))
    return passed, failures
