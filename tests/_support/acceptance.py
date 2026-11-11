"""Opt-in acceptance cannot succeed on skipped or unexecuted comparisons."""
from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--require-redis", action="store_true",
                     help="Require all selected differential tests to pass without skips/xfails")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "differential: mandatory official Redis comparison")
    if config.getoption("--require-redis"):
        config.pluginmanager.register(AcceptanceGate(), "redis-acceptance-gate")


class AcceptanceGate:
    def __init__(self) -> None:
        self.selected: set[str] = set()
        self.passed: set[str] = set()
        self.unaccepted: set[str] = set()
        self.deselected: set[str] = set()
        self.problem = ""

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        self.selected = {item.nodeid for item in items if item.get_closest_marker("differential")}

    def pytest_deselected(self, items: list[pytest.Item]) -> None:
        self.deselected.update(item.nodeid for item in items
                               if item.get_closest_marker("differential"))

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.nodeid not in self.selected:
            return
        if report.skipped or hasattr(report, "wasxfail"):
            self.unaccepted.add(report.nodeid)
        if report.when == "call" and report.passed:
            self.passed.add(report.nodeid)

    def pytest_sessionfinish(self, session: pytest.Session) -> None:
        if (not self.selected or self.selected != self.passed or self.unaccepted
                or self.deselected or session.config.getoption("collectonly")):
            self.problem = (
                "Redis acceptance incomplete: "
                f"{len(self.selected)} selected, {len(self.passed)} passed, "
                f"{len(self.unaccepted)} skipped/xfail, {len(self.deselected)} deselected. "
                "All mandatory comparisons must execute and pass."
            )
            if session.exitstatus == pytest.ExitCode.OK:
                session.exitstatus = pytest.ExitCode.TESTS_FAILED

    def pytest_terminal_summary(self, terminalreporter: object) -> None:
        if self.problem:
            terminalreporter.write_sep("=", self.problem, red=True)  # type: ignore[attr-defined]
