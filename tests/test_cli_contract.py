"""Registration refuses the things a guard would not survive."""
import json

from sjp import cli


def _fresh():
    """A registry of our own, so a check never depends on what the real one holds."""
    saved = dict(cli.COMMANDS)
    cli.COMMANDS.clear()
    return saved


def _restore(saved):
    cli.COMMANDS.clear()
    cli.COMMANDS.update(saved)


def check_a_misspelt_effect_is_refused_at_registration():
    saved = _fresh()
    try:
        try:
            cli.command("oops", "readonly", "")(lambda rest: 0)
        except cli.Registration as problem:
            assert "readonly" in str(problem), problem
        else:
            raise AssertionError("readonly was accepted as an effect")
    finally:
        _restore(saved)


def check_every_listed_effect_is_accepted():
    saved = _fresh()
    try:
        for index, effect in enumerate(cli.EFFECTS):
            cli.command("c{}".format(index), effect, "", probe=lambda store: [])(lambda rest: 0)
        assert sorted(cli.mapping().values()) == sorted(cli.EFFECTS), cli.mapping()
    finally:
        _restore(saved)


def check_a_name_registered_twice_is_refused():
    saved = _fresh()
    try:
        cli.command("twice", cli.WRITE, "")(lambda rest: 0)
        try:
            cli.command("twice", cli.WRITE, "")(lambda rest: 0)
        except cli.Registration:
            pass
        else:
            raise AssertionError("the same name registered twice")
    finally:
        _restore(saved)


def check_the_reserved_name_is_refused():
    saved = _fresh()
    try:
        try:
            cli.command("commands", cli.READ, "", probe=lambda store: [])(lambda rest: 0)
        except cli.Registration:
            pass
        else:
            raise AssertionError("a command called commands registered and would never run")
    finally:
        _restore(saved)


def check_the_status_numbers_are_the_ones_the_readme_published():
    """Pinned as literals, and that is the point of the check.

    A mutation pass on 2026-10-06 moved `OK` to 1, `REFUSED` to 3 and `FAILED` to 4 and
    every check survived, because they all compared a returned status against the constant
    and the mutant moved both sides together. `FOUND` died only because other checks say
    `== 1` in full.

    These four numbers are a contract with a shell rather than an internal label, so one
    check holds them as digits and everything else can go on reading the names.
    """
    assert (cli.OK, cli.FOUND, cli.REFUSED, cli.FAILED) == (0, 1, 2, 3), (
        cli.OK, cli.FOUND, cli.REFUSED, cli.FAILED)
    assert len({cli.OK, cli.FOUND, cli.REFUSED, cli.FAILED}) == 4


def check_the_mapping_is_json_and_covers_every_command():
    from sjp import commands  # noqa: F401  registers the real ones
    text = json.dumps(cli.mapping())
    back = json.loads(text)
    assert set(back) == set(cli.COMMANDS), (sorted(back), sorted(cli.COMMANDS))
    assert set(back.values()) <= set(cli.EFFECTS), back


def check_the_parser_lists_every_command_with_its_effect():
    from sjp import commands  # noqa: F401
    help_text = cli.build_parser().format_help()
    for name, entry in cli.COMMANDS.items():
        assert name in help_text, name
        assert "[{}]".format(entry["effect"]) in help_text, entry
