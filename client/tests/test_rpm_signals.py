"""
Section 10 of the test plan: SIGINT / SIGTERM / SIGHUP / SIGQUIT / SIGBREAK handling.

The e2e variant (Ctrl-C mid-job) is inherently flaky because the timing
depends on how fast the cluster returns results. We cover both the unit
contract (handler installation, cleanup, double-SIGINT guard) and a
single e2e happy-path.
"""

from __future__ import annotations

import os
import signal
import threading
import pytest


pytestmark = pytest.mark.unit


def test_SIGNALS_TO_HANDLE_is_platform_specific():
    from burla._helpers import (
        SIGNALS_TO_HANDLE,
        POSIX_SIGNALS_TO_HANDLE,
        NT_SIGNALS_TO_HANDLE,
    )

    if os.name == "posix":
        expected = [getattr(signal, s) for s in POSIX_SIGNALS_TO_HANDLE]
    else:
        expected = [getattr(signal, s) for s in NT_SIGNALS_TO_HANDLE]

    assert set(SIGNALS_TO_HANDLE) == set(expected)


def test_POSIX_signals_contain_SIGINT_SIGTERM_SIGHUP_SIGQUIT():
    from burla._helpers import POSIX_SIGNALS_TO_HANDLE

    assert "SIGINT" in POSIX_SIGNALS_TO_HANDLE
    assert "SIGTERM" in POSIX_SIGNALS_TO_HANDLE
    assert "SIGHUP" in POSIX_SIGNALS_TO_HANDLE
    assert "SIGQUIT" in POSIX_SIGNALS_TO_HANDLE


def test_NT_signals_contain_SIGINT_SIGBREAK():
    from burla._helpers import NT_SIGNALS_TO_HANDLE

    assert "SIGINT" in NT_SIGNALS_TO_HANDLE
    assert "SIGBREAK" in NT_SIGNALS_TO_HANDLE


def test_install_signal_handlers_returns_original_handlers(monkeypatch):
    from burla._helpers import SIGNALS_TO_HANDLE, install_signal_handlers, restore_signal_handlers

    # Patch out ClusterClient so signal handler body never tries to hit the network.
    from burla import _cluster_client

    class _StubClient:
        @staticmethod
        def patch_job_sync(*a, **kw):
            return None

    monkeypatch.setattr(_cluster_client, "ClusterClient", _StubClient)

    class _StubSpinner:
        text = ""

        def ok(self, *a): pass
        def fail(self, *a): pass
        def write(self, msg): pass

    terminal_cancel_event = threading.Event()
    inputs_done_event = threading.Event()

    originals = install_signal_handlers(
        "job-xyz", False, _StubSpinner(), terminal_cancel_event, inputs_done_event
    )
    try:
        assert set(originals.keys()) == set(SIGNALS_TO_HANDLE)
    finally:
        restore_signal_handlers(originals)


def test_install_signal_handlers_restores_on_exit(monkeypatch):
    from burla import _cluster_client
    from burla._helpers import SIGNALS_TO_HANDLE, install_signal_handlers, restore_signal_handlers

    class _StubClient:
        @staticmethod
        def patch_job_sync(*a, **kw):
            pass

    monkeypatch.setattr(_cluster_client, "ClusterClient", _StubClient)

    class _StubSpinner:
        text = ""
        def ok(self, *a): pass
        def fail(self, *a): pass
        def write(self, msg): pass

    before = {s: signal.getsignal(s) for s in SIGNALS_TO_HANDLE}
    originals = install_signal_handlers("j", False, _StubSpinner(), threading.Event(), threading.Event())
    # Handlers are mutated during install.
    during = {s: signal.getsignal(s) for s in SIGNALS_TO_HANDLE}
    restore_signal_handlers(originals)
    after = {s: signal.getsignal(s) for s in SIGNALS_TO_HANDLE}

    assert before == after


def test_double_ctrl_c_is_idempotent(monkeypatch):
    """Signal handler short-circuits if terminal_cancel_event is already set."""
    from burla import _cluster_client
    from burla._helpers import install_signal_handlers, restore_signal_handlers

    patched_count = [0]

    class _StubClient:
        @staticmethod
        def patch_job_sync(*a, **kw):
            patched_count[0] += 1

    monkeypatch.setattr(_cluster_client, "ClusterClient", _StubClient)

    class _StubSpinner:
        text = ""
        def ok(self, *a): pass
        def fail(self, *a): pass
        def write(self, msg): pass

    terminal_cancel_event = threading.Event()
    inputs_done_event = threading.Event()
    originals = install_signal_handlers(
        "j", False, _StubSpinner(), terminal_cancel_event, inputs_done_event
    )
    try:
        handler = signal.getsignal(signal.SIGINT)
        handler(signal.SIGINT, None)
        handler(signal.SIGINT, None)
        assert terminal_cancel_event.is_set()
        # Second invocation should return immediately, so patch_job_sync should be called once total.
        assert patched_count[0] == 1
    finally:
        restore_signal_handlers(originals)


def test_detach_inputs_done_no_patch_job_sync(monkeypatch):
    """When `detach=True` and inputs finished uploading, signal handler does NOT
    patch the job status — it lets the job keep running on the cluster."""
    from burla import _cluster_client
    from burla._helpers import install_signal_handlers, restore_signal_handlers

    patched_count = [0]

    class _StubClient:
        @staticmethod
        def patch_job_sync(*a, **kw):
            patched_count[0] += 1

    monkeypatch.setattr(_cluster_client, "ClusterClient", _StubClient)

    class _StubSpinner:
        text = ""
        def ok(self, *a): pass
        def fail(self, *a): pass
        def write(self, msg): pass

    terminal_cancel_event = threading.Event()
    inputs_done_event = threading.Event()
    inputs_done_event.set()  # inputs done uploading

    originals = install_signal_handlers(
        "j", True, _StubSpinner(), terminal_cancel_event, inputs_done_event
    )
    try:
        handler = signal.getsignal(signal.SIGINT)
        handler(signal.SIGINT, None)
        assert terminal_cancel_event.is_set()
        assert patched_count[0] == 0  # detach + inputs done => no cancel write
    finally:
        restore_signal_handlers(originals)


def test_detach_inputs_uploading_patches_status_CANCELED(monkeypatch):
    from burla import _cluster_client
    from burla._helpers import install_signal_handlers, restore_signal_handlers

    captured = []

    class _StubClient:
        @staticmethod
        def patch_job_sync(job_id, updates=None, append_fail_reason=None):
            captured.append((job_id, updates, append_fail_reason))

    monkeypatch.setattr(_cluster_client, "ClusterClient", _StubClient)

    class _StubSpinner:
        text = ""
        def ok(self, *a): pass
        def fail(self, *a): pass
        def write(self, msg): pass

    originals = install_signal_handlers(
        "j", True, _StubSpinner(), threading.Event(), threading.Event()
    )
    try:
        handler = signal.getsignal(signal.SIGINT)
        handler(signal.SIGINT, None)
        assert captured
        assert captured[0][1] == {"status": "CANCELED"}
        assert "background" in captured[0][2].lower()
    finally:
        restore_signal_handlers(originals)


def test_foreground_ctrl_c_patches_CANCELED(monkeypatch):
    from burla import _cluster_client
    from burla._helpers import install_signal_handlers, restore_signal_handlers

    captured = []

    class _StubClient:
        @staticmethod
        def patch_job_sync(job_id, updates=None, append_fail_reason=None):
            captured.append((job_id, updates, append_fail_reason))

    monkeypatch.setattr(_cluster_client, "ClusterClient", _StubClient)

    class _StubSpinner:
        text = ""
        def ok(self, *a): pass
        def fail(self, *a): pass
        def write(self, msg): pass

    originals = install_signal_handlers(
        "j-123", False, _StubSpinner(), threading.Event(), threading.Event()
    )
    try:
        handler = signal.getsignal(signal.SIGINT)
        handler(signal.SIGINT, None)
        assert captured
        assert captured[0][0] == "j-123"
        assert captured[0][1] == {"status": "CANCELED"}
    finally:
        restore_signal_handlers(originals)
