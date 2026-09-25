"""Drive the entry point the way an operator would.

Nothing else in the suite reaches `cli.main`, the argument parsers or the lines the
inventory command actually prints. A mutation pass said so before this module existed.
"""
import contextlib
import io
import json
import os
import shutil
import sys
import tempfile

from sjp import cli, commands, eventlog, model

HERE = os.path.dirname(os.path.abspath(__file__))
SKEWED = os.path.join(HERE, "fixtures", "eventlogs", "skewed")


@contextlib.contextmanager
def _quiet():
    """argparse writes its refusals to stderr. A check that expects one should not make
    the suite's own output unreadable."""
    saved = sys.stderr
    caught = io.StringIO()
    sys.stderr = caught
    try:
        yield caught
    finally:
        sys.stderr = saved


def _run(argv):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cli.main(argv)
    return code, out.getvalue()


def check_the_commands_word_prints_the_mapping_as_json():
    code, text = _run(["commands"])
    assert code == 0, code
    assert json.loads(text) == cli.mapping(), text


def check_the_mapping_is_printed_indented_rather_than_on_one_line():
    """A guard reads this. A one line dump is still valid JSON and is not what ships."""
    _code, text = _run(["commands"])
    assert text.count("\n") >= len(cli.mapping()), text


def check_main_with_no_argv_reads_the_real_command_line():
    saved = sys.argv
    sys.argv = ["sjp", "commands"]
    try:
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli.main()
        assert code == 0, code
        assert json.loads(out.getvalue()) == cli.mapping(), out.getvalue()
    finally:
        sys.argv = saved


def check_inventory_runs_from_the_entry_point_and_reports_a_clean_log():
    code, text = _run(["inventory", SKEWED])
    assert code == 0, (code, text)
    assert "every event the profiler needs is present" in text, text
    assert "SparkListenerTaskEnd" in text, text


def check_inventory_exits_nonzero_when_a_file_could_not_be_read():
    root = tempfile.mkdtemp(prefix="sjp-cmd-")
    try:
        with open(os.path.join(root, "junk"), "w") as handle:
            handle.write("not a log\n")
        code, text = _run(["inventory", root])
        assert code == 1, (code, text)
        assert "UNREADABLE" in text, text
    finally:
        shutil.rmtree(root)


def check_the_counts_are_listed_commonest_first():
    _code, text = _run(["inventory", SKEWED])
    counts = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0].isdigit():
            counts.append(int(parts[0]))
    assert counts == sorted(counts, reverse=True), counts
    assert counts[0] == 18, counts


def check_a_missing_event_is_named_with_the_reason_it_is_wanted():
    root = tempfile.mkdtemp(prefix="sjp-thin-")
    try:
        with open(os.path.join(root, "thin"), "w") as handle:
            handle.write('{"Event":"SparkListenerJobStart"}\n')
        _code, text = _run(["inventory", root])
        assert "MISSING" in text, text
        assert "SparkListenerTaskEnd" in text, text
        assert model.CONSUMES["SparkListenerTaskEnd"] in text, text
    finally:
        shutil.rmtree(root)


def check_render_names_an_unreadable_file_and_says_nothing_else_about_it():
    lines = commands.render({"path": "p", "error": "broke"})
    assert lines == ["p  UNREADABLE  broke"], lines


def check_the_capture_parser_defaults_to_two_million_rows():
    args = commands.capture_parser().parse_args(["--job", "skewed", "--out", "somewhere"])
    assert args.rows == 2000000, args.rows
    assert args.job == "skewed", args.job


def check_the_capture_parser_refuses_a_job_it_does_not_have():
    try:
        with _quiet():
            commands.capture_parser().parse_args(["--job", "nonsense", "--out", "x"])
    except SystemExit:
        pass
    else:
        raise AssertionError("an unknown job name was accepted")


def check_the_capture_parser_requires_an_output_directory():
    try:
        with _quiet():
            commands.capture_parser().parse_args(["--job", "skewed"])
    except SystemExit:
        pass
    else:
        raise AssertionError("capture accepted no output directory")


def check_the_inventory_parser_takes_exactly_one_path():
    args = commands.inventory_parser().parse_args(["somewhere"])
    assert args.path == "somewhere", args
    try:
        with _quiet():
            commands.inventory_parser().parse_args([])
    except SystemExit:
        pass
    else:
        raise AssertionError("inventory accepted no path")


def check_the_help_line_says_what_the_tool_is():
    assert cli.build_parser().description == cli.__doc__.splitlines()[0]


def check_no_command_at_all_is_a_usage_error_rather_than_a_crash():
    try:
        with _quiet():
            _run([])
    except SystemExit as leaving:
        assert leaving.code != 0, leaving.code
    else:
        raise AssertionError("running with no command did not refuse")


def check_help_after_a_command_reaches_that_command_own_parser():
    """The subparser entries are stubs. Help has to fall through to the real parser."""
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out), _quiet():
            cli.main(["inventory", "--help"])
    except SystemExit:
        pass
    printed = out.getvalue()
    assert "usage: sjp inventory" in printed, printed
    # The stub subparser prints a usage line too. Only the real one knows about `path`.
    assert "an event log file, or a directory of them" in printed, printed


def check_the_commands_word_refuses_trailing_arguments_and_names_them():
    with _quiet() as complaint:
        code, text = _run(["commands", "extra", "more"])
    assert code == 2, (code, text)
    assert text == "", text
    assert "['extra', 'more']" in complaint.getvalue(), complaint.getvalue()


def check_the_mapping_is_printed_at_the_indent_that_ships():
    _code, text = _run(["commands"])
    expected = ('{\n  "capture": "write",\n  "inventory": "read",'
                '\n  "stages": "read"\n}\n')
    assert text == expected, repr(text)


def check_the_capture_parser_requires_a_job():
    try:
        with _quiet():
            commands.capture_parser().parse_args(["--out", "somewhere"])
    except SystemExit:
        pass
    else:
        raise AssertionError("capture accepted no job name")


def check_events_with_the_same_count_are_listed_alphabetically():
    _code, text = _run(["inventory", SKEWED])
    ones = [line.split()[1] for line in text.splitlines()
            if len(line.split()) == 2 and line.split()[0] == "1"]
    assert len(ones) > 2, ones
    assert ones == sorted(ones), ones


def check_the_number_of_unreadable_files_is_reported_and_not_only_the_fact():
    root = tempfile.mkdtemp(prefix="sjp-two-")
    try:
        for name in ("junk-a", "junk-b"):
            with open(os.path.join(root, name), "w") as handle:
                handle.write("not a log\n")
        with open(os.path.join(root, "real"), "w") as handle:
            handle.write('{"Event":"SparkListenerJobStart"}\n')
        code, text = _run(["inventory", root])
        assert code == 1, code
        assert "2 of 3 files could not be read" in text, text
    finally:
        shutil.rmtree(root)


def check_capture_hands_the_parsed_arguments_straight_to_the_job():
    """capture needs a Spark session and a check cannot have one, so the job is stubbed.

    An entry point that needs a real dependency is the function least likely to be tested,
    and it is the one that decides what the dependency is asked for.
    """
    import types

    seen = {}

    def run(job, out_dir, rows):
        seen.update(job=job, out_dir=out_dir, rows=rows)
        return "/somewhere/local-1"

    stub = types.ModuleType("jobs.sample")
    stub.run = run
    parent = types.ModuleType("jobs")
    parent.sample = stub
    saved = {name: sys.modules.get(name) for name in ("jobs", "jobs.sample")}
    sys.modules["jobs"] = parent
    sys.modules["jobs.sample"] = stub
    try:
        code, text = _run(["capture", "--job", "balanced", "--out", "here", "--rows", "7"])
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
    assert code == 0, code
    assert seen == {"job": "balanced", "out_dir": "here", "rows": 7}, seen
    assert "wrote /somewhere/local-1" in text, text


def check_a_single_trailing_argument_is_refused_too():
    """The smallest rejected case, beside the largest accepted one above."""
    with _quiet() as complaint:
        code, text = _run(["commands", "extra"])
    assert code == 2, (code, text)
    assert "['extra']" in complaint.getvalue(), complaint.getvalue()


def check_the_stages_command_prints_the_model_and_changes_nothing():
    log = os.path.join(SKEWED, sorted(os.listdir(SKEWED))[0])
    code, text = _run(["stages", log])
    assert code == 0, (code, text)
    assert "sjp-skewed" in text, text
    assert "44.22" in text, text
    assert "12 mapped" in text, text


def check_the_stage_lines_count_the_totals_the_log_left_out():
    """Absent is the interesting half. Zero absent would mean the stage carried them all."""
    app = eventlog.profile(commands._first_log(SKEWED))
    lines = commands.stage_lines(app)
    absent = [line.split("mapped, ")[1].split(" absent")[0]
              for line in lines if "mapped," in line]
    assert absent == ["4", "5", "3"], absent


def check_the_stages_command_counts_one_job_as_a_job():
    assert commands.counted(1, "job") == "1 job"
    assert commands.counted(0, "job") == "0 jobs"
    assert commands.counted(3, "stage") == "3 stages"


def check_the_read_probe_finds_a_log_under_a_directory_and_skips_the_notes():
    path = commands._first_log(SKEWED)
    assert path.endswith(sorted(os.listdir(SKEWED))[0]), path
    assert not path.endswith(".md"), path


def check_the_read_probe_refuses_a_directory_holding_no_log():
    root = tempfile.mkdtemp(prefix="sjp-nologs-")
    try:
        open(os.path.join(root, "notes.md"), "w").close()
        try:
            commands._first_log(root)
        except eventlog.NotAnEventLog:
            pass
        else:
            raise AssertionError("a directory of notes offered a log")
    finally:
        shutil.rmtree(root)


def check_a_stage_line_names_a_total_that_disagrees_with_its_tasks():
    """The printed disagreement path is unreachable on a healthy log, so it is built."""
    import dataclasses

    app = eventlog.profile(commands._first_log(SKEWED))
    stage = app.stage(2)
    damaged = dataclasses.replace(app, stages=(dataclasses.replace(
        stage, tasks=stage.tasks[:-1]),))
    lines = commands.stage_lines(damaged)
    assert any("disagree with the tasks" in line for line in lines), lines
    assert any("memoryBytesSpilled reports" in line for line in lines), lines
