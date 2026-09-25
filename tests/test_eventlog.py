"""Reading a log, and refusing one that is not a log."""
import os
import shutil
import tempfile

from sjp import eventlog, model

HERE = os.path.dirname(os.path.abspath(__file__))
SKEWED = os.path.join(HERE, "fixtures", "eventlogs", "skewed")
BALANCED = os.path.join(HERE, "fixtures", "eventlogs", "balanced")


def _only_log(directory):
    names = [n for n in sorted(os.listdir(directory)) if not n.endswith(".md")]
    assert len(names) == 1, names
    return os.path.join(directory, names[0])


def check_every_line_of_the_fixture_decodes():
    count = sum(1 for _ in eventlog.read_events(_only_log(SKEWED)))
    assert count > 0, count


def check_counts_sum_to_the_line_count():
    path = _only_log(SKEWED)
    counts = eventlog.event_counts(path)
    lines = sum(1 for line in open(path, encoding="utf-8") if line.strip())
    assert sum(counts.values()) == lines, (sum(counts.values()), lines)


def check_a_line_that_is_not_json_raises():
    root = tempfile.mkdtemp(prefix="sjp-bad-")
    try:
        path = os.path.join(root, "broken")
        with open(path, "w") as handle:
            handle.write('{"Event":"SparkListenerJobEnd"}\nnot json at all\n')
        try:
            list(eventlog.read_events(path))
        except eventlog.NotAnEventLog as problem:
            assert "line 2" in str(problem), problem
        else:
            raise AssertionError("a file holding junk was read as a log")
    finally:
        shutil.rmtree(root)


def check_an_empty_file_is_refused():
    root = tempfile.mkdtemp(prefix="sjp-empty-")
    try:
        path = os.path.join(root, "nothing")
        open(path, "w").close()
        try:
            eventlog.event_counts(path)
        except eventlog.NotAnEventLog:
            pass
        else:
            raise AssertionError("an empty file reported as a log")
    finally:
        shutil.rmtree(root)


def check_a_line_with_no_event_key_is_counted_rather_than_dropped():
    root = tempfile.mkdtemp(prefix="sjp-noevent-")
    try:
        path = os.path.join(root, "odd")
        with open(path, "w") as handle:
            handle.write('{"Event":"SparkListenerJobEnd"}\n{"something":"else"}\n')
        counts = eventlog.event_counts(path)
        assert counts[""] == 1, counts
        assert sum(counts.values()) == 2, counts
    finally:
        shutil.rmtree(root)


def check_logs_under_takes_a_file_as_well_as_a_directory():
    path = _only_log(SKEWED)
    assert eventlog.logs_under(path) == [path]
    assert path in eventlog.logs_under(SKEWED)


def check_inventory_reports_nothing_missing_for_both_fixtures():
    for directory in (SKEWED, BALANCED):
        for report in eventlog.inventory(_only_log(directory)):
            assert report["missing"] == [], (directory, report["missing"])


def check_inventory_names_what_is_missing():
    """The forward direction alone would pass on a log with everything in it."""
    root = tempfile.mkdtemp(prefix="sjp-thin-")
    try:
        path = os.path.join(root, "thin")
        with open(path, "w") as handle:
            handle.write('{"Event":"SparkListenerJobStart"}\n')
        report = eventlog.inventory(path)[0]
        assert "SparkListenerTaskEnd" in report["missing"], report
        assert "SparkListenerJobStart" not in report["missing"], report
        assert len(report["missing"]) == len(model.CONSUMES) - 1, report
    finally:
        shutil.rmtree(root)


def check_an_empty_directory_is_refused():
    root = tempfile.mkdtemp(prefix="sjp-bare-")
    try:
        try:
            eventlog.inventory(root)
        except eventlog.NotAnEventLog:
            pass
        else:
            raise AssertionError("an empty directory reported as an inventory")
    finally:
        shutil.rmtree(root)


def check_one_unreadable_file_does_not_hide_the_logs_beside_it():
    root = tempfile.mkdtemp(prefix="sjp-mixed-")
    try:
        with open(os.path.join(root, "a-real-one"), "w") as handle:
            handle.write('{"Event":"SparkListenerJobStart"}\n')
        with open(os.path.join(root, "notes.md"), "w") as handle:
            handle.write("# this is not an event log\n")
        reports = eventlog.inventory(root)
        assert len(reports) == 2, reports
        by_name = {os.path.basename(r["path"]): r for r in reports}
        assert "error" in by_name["notes.md"], by_name
        assert by_name["a-real-one"]["lines"] == 1, by_name
    finally:
        shutil.rmtree(root)


def check_an_unreadable_file_is_reported_rather_than_skipped():
    root = tempfile.mkdtemp(prefix="sjp-junk-")
    try:
        with open(os.path.join(root, "junk"), "w") as handle:
            handle.write("nope\n")
        reports = eventlog.inventory(root)
        assert len(reports) == 1 and "error" in reports[0], reports
        assert "counts" not in reports[0], reports
    finally:
        shutil.rmtree(root)


def check_the_needed_list_is_the_model_and_not_a_copy_of_it():
    """There is one list. A second one would be the thing that drifts."""
    report = eventlog.inventory(_only_log(SKEWED))[0]
    assert report["missing"] == [], report
    for name in model.CONSUMES:
        assert name in report["counts"], (name, sorted(report["counts"]))


def check_profile_reads_a_path_and_builds_the_model():
    app = eventlog.profile(_only_log(BALANCED))
    assert app.name == "sjp-balanced", app.name
    assert len(app.stages) == 3, app.stages
