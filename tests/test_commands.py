"""Drive the entry point the way an operator would.

Nothing else in the suite reaches `cli.main`, the argument parsers or the lines the
inventory command actually prints. A mutation pass said so before this module existed.
"""
import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile

from sjp import cli, commands, eventlog, model, skew

HERE = os.path.dirname(os.path.abspath(__file__))
SKEWED = os.path.join(HERE, "fixtures", "eventlogs", "skewed")
BALANCED = os.path.join(HERE, "fixtures", "eventlogs", "balanced")
JOIN = os.path.join(HERE, "fixtures", "eventlogs", "join")
WIDE = os.path.join(HERE, "fixtures", "eventlogs", "wide")
ROOT = os.path.dirname(HERE)


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
                '\n  "layout": "read",\n  "skew": "read",\n  "spill": "read",'
                '\n  "stages": "read"\n}\n')
    assert text == expected, repr(text)


def _only_log(directory):
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])


def check_skew_exits_one_on_the_skewed_log_and_zero_on_the_balanced_one():
    """The status is the part a shell reads, so it is the part worth pinning.

    A command that prints a finding and exits 0 is a command nothing downstream can act
    on, and the printed lines would still look right.
    """
    guilty, _text = _run(["skew", _only_log(SKEWED)])
    clean, _text = _run(["skew", _only_log(BALANCED)])
    assert guilty == 1, guilty
    assert clean == 0, clean


def check_skew_names_the_worst_stage_or_says_nothing_skewed():
    """The worst lines sit directly under the header as of day 2.

    They used to be last. This check asserted that, correctly, for as long as it was true.
    What it is pinning now is the position and not just the presence, because the whole
    point of moving them is that a reader should not have to reach the bottom of 672 lines
    to find out whether anything is wrong.
    """
    _code, guilty = _run(["skew", _only_log(SKEWED)])
    _code, clean = _run(["skew", _only_log(BALANCED)])
    head = guilty.strip().splitlines()
    assert head[1].strip().startswith("worst count"), head[:4]
    assert head[3].strip().startswith("worst millis"), head[:4]
    assert "worst bytes   stage 2 on memory_spilled" in guilty, guilty
    assert clean.strip().splitlines()[1].strip() == "nothing skewed at this threshold", clean


def check_only_skewed_prints_the_problems_and_nothing_else():
    _code, text = _run(["skew", _only_log(SKEWED), "--only", "skewed"])
    body = [line for line in text.strip().splitlines() if line.startswith("  skewed")
            or line.startswith("  even") or line.startswith("  undecided")]
    assert len(body) == 8, body
    assert all(line.startswith("  skewed") for line in body), body
    assert "showing 8 of 42, skewed only" in text, text


def check_only_does_not_change_the_exit_status_or_the_tally():
    """A flag that changed either would be a flag that changes the answer.

    `sjp.cli` declares an effect per command and says a flag must never move it. The same
    argument applies to a verdict. The exit status is the whole scan's and so is the tally,
    and `--only undecided` on the skewed log is the case that would hide all eight.
    """
    full_code, full = _run(["skew", _only_log(SKEWED)])
    for outcome in ("skewed", "even", "undecided"):
        code, text = _run(["skew", _only_log(SKEWED), "--only", outcome])
        assert code == full_code == 1, (outcome, code, full_code)
        assert "8 skewed, 6 even, 28 undecided" in text, (outcome, text)
    clean_code, clean = _run(["skew", _only_log(BALANCED), "--only", "skewed"])
    assert clean_code == 0, clean_code
    assert "showing 0 of" in clean, clean


def check_skew_prints_one_line_per_stage_and_metric_plus_three():
    """The header, the tally and the worst line. A dropped row would otherwise be invisible."""
    app = eventlog.profile(_only_log(SKEWED))
    _code, text = _run(["skew", _only_log(SKEWED)])
    kinds = len(skew.worst_by_kind(skew.scan(app)))
    assert kinds == 3, kinds
    expected = len(app.stages) * len(skew.DEFAULT_METRICS) + 2 + kinds
    assert len(text.strip().splitlines()) == expected, text


def check_the_skew_threshold_can_be_overridden_from_the_command_line():
    _code, wide = _run(["skew", _only_log(SKEWED), "--threshold", "100"])
    assert "nothing skewed" not in wide, wide
    assert "0 skewed" not in wide, wide


def check_skew_takes_one_metric_when_asked_for_one():
    stages = len(eventlog.profile(_only_log(SKEWED)).stages)
    _code, text = _run(["skew", _only_log(SKEWED), "--metric", "duration"])
    assert "records_read" not in text, text
    assert len(text.strip().splitlines()) == stages + 3, text


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

    def run(job, out_dir, rows, partitions=None):
        seen.update(job=job, out_dir=out_dir, rows=rows, partitions=partitions)
        return "/somewhere/local-1"

    # Built from the real module with one function replaced, rather than from the two
    # attributes this check happens to use. A hand built double covers what its author
    # remembered, and this one stopped covering the module the day the module grew an
    # argument the command reads. Importing it costs no pyspark, because every pyspark
    # import in it is inside a function.
    from jobs import sample as real

    stub = types.ModuleType("jobs.sample")
    for attribute in dir(real):
        if not attribute.startswith("__"):
            setattr(stub, attribute, getattr(real, attribute))
    stub.run = run
    parent = types.ModuleType("jobs")
    parent.sample = stub
    saved = {name: sys.modules.get(name) for name in ("jobs", "jobs.sample")}
    sys.modules["jobs"] = parent
    sys.modules["jobs.sample"] = stub
    try:
        code, text = _run(["capture", "--job", "balanced", "--out", "here", "--rows", "7"])
        omitted = dict(seen)
        seen.clear()
        at_five, _ = _run(["capture", "--job", "by_column", "--out", "here",
                           "--rows", "7", "--partitions", "5"])
        given = dict(seen)
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
    assert code == 0, code
    assert omitted == {"job": "balanced", "out_dir": "here", "rows": 7,
                       "partitions": None}, omitted
    assert "wrote /somewhere/local-1" in text, text
    # Omitting the flag has to arrive as None rather than as the default count. The job
    # decides what no answer means, and a command that picks 8 here would be deciding it
    # twice in two places.
    assert at_five == 0, at_five
    assert given == {"job": "by_column", "out_dir": "here", "rows": 7,
                     "partitions": 5}, given


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


def _log_without_spill(directory, into):
    """The balanced log with every task's spill zeroed, written to `into`.

    Derived from a real log rather than hand built, because the format is the thing a hand
    built fixture would get wrong. Neither committed log has a stage that spills nothing on
    every stage, so this is the only way to reach the exit zero branch of `sjp spill`.
    """
    source = _only_log(directory)
    with open(source, "r", encoding="utf-8") as handle:
        lines = handle.readlines()
    written = 0
    with open(into, "w", encoding="utf-8") as handle:
        for line in lines:
            event = json.loads(line)
            if event.get("Event") == "SparkListenerTaskEnd":
                event["Task Metrics"]["Memory Bytes Spilled"] = 0
                event["Task Metrics"]["Disk Bytes Spilled"] = 0
                written += 1
            handle.write(json.dumps(event) + "\n")
    assert written == 18, written
    return into


def check_spill_exits_one_on_both_logs_because_both_of_them_spill():
    """The status is the part a shell reads and both fixtures give it the same answer.

    That is the finding rather than a gap. A spill is not a pathology, so the balanced log
    exits 1 too and `sjp skew` on memory_spilled is what separates them.
    """
    for directory in (SKEWED, BALANCED):
        code, text = _run(["spill", _only_log(directory)])
        assert code == 1, (directory, text)


def check_spill_exits_zero_when_nothing_spilled():
    """The other side of the status, on the balanced log with its spill zeroed."""
    holder = tempfile.mkdtemp()
    try:
        path = _log_without_spill(BALANCED, os.path.join(holder, "local-quiet"))
        code, text = _run(["spill", path])
        assert code == 0, text
        assert "clean" in text, text
        assert "spilling" not in text, text
    finally:
        shutil.rmtree(holder)


def check_spill_names_the_missing_budget_before_any_stage():
    code, text = _run(["spill", _only_log(SKEWED)])
    lines = text.strip().splitlines()
    assert lines[1].startswith("no execution memory budget"), lines[1]
    assert "spark.memory.fraction" in lines[1], lines[1]
    assert lines[3].strip().startswith("stage 0"), lines[3]


def check_layout_exits_zero_when_nothing_in_the_log_is_the_config_to_change():
    for directory in (SKEWED, BALANCED):
        code, text = _run(["layout", _only_log(directory)])
        assert code == 0, (directory, code)
        assert "0 worth changing" in text, text


def check_layout_exits_one_when_something_is_worth_changing():
    """Two, not three. The join log's two hash exchanges are one count, so the partition
    target they share is counted once and the broadcast candidate is the other."""
    code, text = _run(["layout", _only_log(JOIN)])
    assert code == 1, code
    assert "2 worth changing" in text, text


def check_layout_passes_the_advisory_size_through_to_the_count():
    """A flag nothing reads looks exactly like a flag that works.

    The total is asserted and so is the arithmetic, because the total alone is a weak
    witness. Several advisory sizes move the target without moving the count, so a check
    reading only the count would pass on a flag that reached the report and not the sum.
    21000000 bytes over the log's eight partitions is 168000000, which is above the
    163275083 the pair really wrote, so the count stops being advice.
    """
    code, text = _run(["layout", "--advisory", "21000000", _only_log(JOIN)])
    assert code == 1, code
    assert "1 worth changing" in text, text
    assert "163275083 bytes over 8 partitions is already inside" in text, text


def check_layout_passes_the_broadcast_threshold_through_to_the_count():
    """A one byte threshold takes the broadcast candidate out and leaves the one partition
    target the join's two exchanges share."""
    code, text = _run(["layout", "--broadcast", "1", _only_log(JOIN)])
    assert code == 1, code
    assert "1 worth changing" in text, text
    assert "broadcast it" not in text, text


def check_an_unknown_only_value_is_refused_by_the_parser():
    with _quiet() as caught:
        try:
            commands.skew_parser().parse_args([_only_log(SKEWED), "--only", "broken"])
        except SystemExit as exit_code:
            assert exit_code.code == 2, exit_code.code
        else:
            raise AssertionError("accepted an outcome that does not exist")
    # "invalid choice" and not just the word. argparse refuses an unknown value and an
    # option it has never heard of with the same status and a different message, so
    # checking for the value alone passes against a tree where --only does not exist.
    # Caught by running this check against HEAD before committing.
    message = caught.getvalue()
    assert "invalid choice" in message, message
    assert "skewed" in message, message


def check_a_closed_pipe_does_not_print_a_traceback():
    """`sjp skew` on the wide log is 675 lines, so a reader pipes it to `head`.

    This runs the entry point as a process, because a BrokenPipeError needs a real pipe
    and nothing else in the suite leaves this interpreter.

    Measured 2026-10-03 over 20 runs each. `skew` returns 141 on all 20 and `stages` on 6
    of 20, because `stages` writes little enough that the buffer sometimes drains before
    the reader goes away. So what is asserted is the part that holds. stderr stays empty
    and the status is either the command's own or 141. The racy half is in the README
    rather than pinned here, because pinning a number this check cannot reproduce is how a
    flaky test gets written.
    """
    wide = _only_log(WIDE)
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    for name, own in (("skew", (0, 1)), ("stages", (0,))):
        proc = subprocess.Popen([sys.executable, "-m", "sjp", name, wide], cwd=ROOT,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        proc.stdout.readline()
        proc.stdout.close()
        stderr = proc.stderr.read()
        proc.stderr.close()
        proc.wait()
        assert stderr == b"", (name, stderr[-300:])
        assert proc.returncode in own + (141,), (name, proc.returncode)
