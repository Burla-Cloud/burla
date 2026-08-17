"""
Dashboard-UI tier: real Chromium driving the dashboard a person actually
uses, against the live local-dev cluster. This is where dashboard behavior
is verified end to end (the pages, their data fetches, and what the user
sees); the service tier keeps only endpoint contracts a browser can't reach.

Chromium comes from `playwright install chromium`; `make test-dashboard`
runs that automatically.
"""

from __future__ import annotations

import re
import uuid

import pytest

pytestmark = pytest.mark.dashboard

playwright_api = pytest.importorskip("playwright.sync_api")
expect = playwright_api.expect


@pytest.fixture(scope="session")
def browser():
    with playwright_api.sync_playwright() as playwright:
        try:
            browser = playwright.chromium.launch()
        except playwright_api.Error as error:
            pytest.skip(f"chromium not installed ({error}); run `make test-dashboard`")
        yield browser
        browser.close()


@pytest.fixture
def page(browser, dashboard_url):
    context = browser.new_context(base_url=dashboard_url)
    browser_page = context.new_page()
    browser_page.set_default_timeout(15_000)
    yield browser_page
    context.close()


def test_jobs_page_lists_completed_job_and_shows_its_logs(
    page, rpm_subprocess, local_dev_cluster
):
    marker = f"dashboard-marker-{uuid.uuid4().hex[:8]}"
    source = f"def test_function(x):\n    print('{marker}')\n    return x\n"
    result = rpm_subprocess(source, [1], timeout_seconds=60, grow=False)
    assert result["ok"], result.get("traceback")

    page.goto("/jobs")
    newest_row = page.get_by_role("row").nth(1)  # row 0 is the header
    expect(newest_row).to_contain_text("test_function")
    expect(newest_row).to_contain_text("Completed")
    expect(newest_row).to_contain_text("1 / 1")

    newest_row.click()
    expect(page).to_have_url(re.compile(r"/jobs/test_function-"))
    expect(page.get_by_role("heading", name="test_function")).to_be_visible()
    expect(page.get_by_text("Completed").first).to_be_visible()
    # The UDF's printed output must reach the person reading the dashboard.
    page.get_by_role("cell", name="0", exact=True).click()
    expect(page.get_by_text(marker)).to_be_visible()


def test_cluster_page_shows_ready_nodes_live(page, local_dev_cluster):
    ready_names = {
        node["instance_name"] for node in local_dev_cluster["state"]["ready_nodes"]
    }
    assert ready_names, "readiness gate guarantees at least one READY node"

    page.goto("/")
    for name in ready_names:
        expect(page.get_by_role("cell", name=name)).to_be_visible()
    expect(page.get_by_role("row", name=re.compile("Ready")).first).to_be_visible()

    # "Show deleted nodes" pulls node history on demand.
    with page.expect_response(
        lambda response: "/v1/cluster/deleted_recent_paginated" in response.url
        and response.status == 200
    ):
        page.get_by_role("switch").click()


def test_settings_cluster_roundtrip(page, local_dev_cluster):
    def timeout_input():
        # The settings inputs carry no ids or labels; the field wrapper div
        # holding the label text is the innermost `div` match, so `.last`.
        field = page.locator("div").filter(
            has=page.get_by_text("Inactivity timeout (minutes)", exact=True)
        )
        return field.last.get_by_role("textbox")

    page.goto("/settings")
    original = timeout_input().input_value()
    changed = "12" if original != "12" else "11"

    try:
        timeout_input().fill(changed)
        page.get_by_role("button", name="Save").click()
        expect(page.get_by_role("button", name="Save")).not_to_be_visible()

        page.reload()
        expect(timeout_input()).to_have_value(changed)
    finally:
        page.goto("/settings")
        if timeout_input().input_value() != original:
            timeout_input().fill(original)
            page.get_by_role("button", name="Save").click()
            expect(page.get_by_role("button", name="Save")).not_to_be_visible()


def test_settings_usage_section_loads_live_usage_data(page, local_dev_cluster):
    with page.expect_response(
        lambda response: "/v1/nodes/daily_hours" in response.url
        and response.status == 200
    ):
        with page.expect_response(
            lambda response: "/v1/nodes/month_nodes" in response.url
            and response.status == 200
        ):
            page.goto("/settings?section=usage")

    expect(page.get_by_role("button", name="Usage")).to_have_attribute(
        "aria-pressed", "true"
    )
    expect(page.get_by_text("Daily spend")).to_be_visible()
    expect(page.get_by_text("Compute types")).to_be_visible()
