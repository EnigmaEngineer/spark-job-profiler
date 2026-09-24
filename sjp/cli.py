"""One entry point, and every command says up front what it does to state.

    python -m sjp commands           the name to effect map, as JSON
    python -m sjp <command> [args]

A command declares one of three effects when it registers.

    read       changes nothing on disk
    write      changes state and belongs behind review when an agent is driving
    emergency  changes state and has to run unattended because it is the recovery path

A flag never changes the effect. If a read needs a writing variant then that is a second
command with its own name. The reason is that a guard keyed on the command name cannot see
a flag, so a flag that flips the effect is invisible to the thing meant to be watching.

The declared effect is a claim. `sjp.contract` is what tests the claim.
"""
import argparse
import json
import sys

READ = "read"
WRITE = "write"
EMERGENCY = "emergency"
EFFECTS = (READ, WRITE, EMERGENCY)

# Reserved because `commands` is handled before the parser is built, so a command of that
# name would register fine and then never run.
RESERVED = ("commands",)

COMMANDS = {}


class Registration(Exception):
    """Raised at import time when a command is registered wrongly."""


def command(name, effect, help, probe=None):
    """Register a command under `name`.

    `probe` is only meaningful on a read. It takes a store directory and returns the argv
    that exercises the command against it. sjp.contract refuses a read without one rather
    than skipping it, because a check that skips what it cannot inspect will skip the case
    it was written for.
    """
    if effect not in EFFECTS:
        raise Registration(
            "{} declares effect {!r}. Expected one of {}".format(name, effect, EFFECTS))
    if name in RESERVED:
        raise Registration("{} is reserved by the entry point".format(name))
    if name in COMMANDS:
        raise Registration("{} is registered twice".format(name))

    def register(fn):
        COMMANDS[name] = {"effect": effect, "help": help, "run": fn, "probe": probe}
        return fn
    return register


def mapping(registry=None):
    """The name to effect map a guard reads instead of a README."""
    registry = COMMANDS if registry is None else registry
    return {name: entry["effect"] for name, entry in sorted(registry.items())}


def build_parser(registry=None):
    registry = COMMANDS if registry is None else registry
    parser = argparse.ArgumentParser(prog="sjp", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="name", required=True)
    for name, entry in sorted(registry.items()):
        sub.add_parser(name, add_help=False,
                       help="[{}] {}".format(entry["effect"], entry["help"]))
    return parser


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["commands"]:
        # Refused rather than ignored. A guard that pipes this into a parser should find
        # out that it passed something the entry point did not understand.
        if len(argv) > 1:
            print("commands takes no arguments, got {}".format(argv[1:]), file=sys.stderr)
            return 2
        print(json.dumps(mapping(), indent=2))
        return 0
    parser = build_parser()
    args, rest = parser.parse_known_args(argv)
    return COMMANDS[args.name]["run"](rest)
