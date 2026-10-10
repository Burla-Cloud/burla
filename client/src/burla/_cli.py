import argparse
import json
import sys
from datetime import datetime, timezone
from uuid import uuid4

from burla import __version__
from burla._management_client import (
    ManagementClient,
    ManagementError,
    resolve_management_context,
)


MANAGEMENT_GROUPS = {"auth", "cluster", "nodes", "jobs", "settings", "usage"}

DESCRIPTION = """\
Burla runs Python functions on many cloud VMs at once.

Start jobs from Python, not from this CLI:

    from burla import remote_parallel_map
    results = remote_parallel_map(my_function, my_inputs)

Use this CLI to log in, open the dashboard, and inspect or control the
cluster and its jobs. Docs: https://burla.dev/docs"""

# Rendered as grouped panels in `burla --help`, modeled on modal's CLI.
COMMAND_PANELS = (
    (
        "Getting started",
        (
            ("login", "Log in to Burla and save credentials on this machine."),
            ("dashboard", "Open the cluster dashboard in a web browser."),
            ("deploy", "Deploy a shared, always-on cluster into your cloud account."),
        ),
    ),
    (
        "Cluster",
        (
            ("cluster", "Show cluster status, or start, restart, stop, and watch it."),
            ("nodes", "List the cluster's VMs, inspect one, or read its logs."),
            ("settings", "Show or change what nodes the cluster boots, and where."),
        ),
    ),
    (
        "Jobs",
        (
            ("jobs", "List, inspect, watch, or cancel jobs and their function calls."),
            ("usage", "Show compute hours and estimated spend by month."),
        ),
    ),
    (
        "Configuration",
        (
            ("config", "Show or set local client settings, like which cloud to use."),
            ("auth", "Show which cluster this CLI talks to and how it authenticates."),
        ),
    ),
)


def _epilog():
    lines = []
    for title, commands in COMMAND_PANELS:
        lines.append(f"{title}:")
        for name, one_liner in commands:
            lines.append(f"  {name:<9}  {one_liner}")
        lines.append("")
    lines.append(
        "Management commands (auth, cluster, nodes, jobs, settings, usage) are\n"
        "non-interactive and print JSON: one document per command, NDJSON for\n"
        "streams, and on failure one JSON error with a remediation hint.\n"
        "\n"
        "Run `burla <command> --help` to see any command's subcommands, options,\n"
        "and examples."
    )
    return "\n".join(lines)


class ArgumentError(Exception):
    pass


class _HelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Modal-style help: usage lines collapse flags into `[options]`, and
    subcommands render as a name/description table instead of argparse's
    default `{a,b,c}` blob."""

    def _format_usage(self, usage, actions, groups, prefix):
        if usage is None:
            parts = [self._prog]
            # argparse derives subparser prog prefixes through this method with
            # positionals only; adding "[options]" then would repeat it at
            # every nesting level of the final usage line.
            if any(action.option_strings for action in actions):
                parts.append("[options]")
            for action in actions:
                if action.option_strings or action.help is argparse.SUPPRESS:
                    continue
                parts.append(self._format_args(action, action.dest))
            usage = " ".join(parts)
        return super()._format_usage(usage, (), (), prefix)

    def _format_action(self, action):
        text = super()._format_action(action)
        if action.nargs == argparse.PARSER:
            text = "\n".join(text.split("\n")[1:])
        return text

    def _iter_indented_subactions(self, action):
        # Without the extra indent, subcommand rows line up with option rows.
        if action.nargs == argparse.PARSER:
            yield from action._get_subactions()


class StrictArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("formatter_class", _HelpFormatter)
        super().__init__(*args, **kwargs)
        self._positionals.title = "Arguments"
        self._optionals.title = "Options"
        if self.add_help:
            self._actions[0].help = "Show this help message and exit."

    def error(self, message):
        raise ArgumentError(message)


def _bool_argument(value):
    value = value.lower()
    if value in {"true", "1"}:
        return True
    if value in {"false", "0"}:
        return False
    raise argparse.ArgumentTypeError("expected true or false")


def _add_list_arguments(parser, sorts=()):
    if sorts:
        parser.add_argument("--sort", choices=sorts, help="Sort results by this field.")
        parser.add_argument(
            "--order",
            choices=("asc", "desc"),
            default="desc",
            help="Sort direction (default: desc).",
        )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        metavar="N",
        help="Maximum items to return (default: 100).",
    )
    parser.add_argument(
        "--cursor",
        metavar="CURSOR",
        help="Return the next page; use `next_cursor` from the previous response.",
    )


def _add_log_arguments(parser, follow):
    cursors = parser.add_mutually_exclusive_group()
    cursors.add_argument(
        "--before",
        metavar="CURSOR",
        help="Only lines before this log cursor (page backward).",
    )
    cursors.add_argument(
        "--after",
        metavar="CURSOR",
        help="Only lines after this log cursor (page forward).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        metavar="N",
        help="Maximum lines to return (default: 500).",
    )
    if follow:
        parser.add_argument(
            "--follow",
            action="store_true",
            help="Keep streaming new lines as NDJSON until interrupted.",
        )


def _legacy_parser(root):
    login = root.add_parser(
        "login",
        allow_abbrev=False,
        description=(
            "Log in to Burla in a web browser and save credentials on this\n"
            "machine. Run this once per machine; every other command and\n"
            "`remote_parallel_map` then use the saved credentials."
        ),
    )
    login.add_argument(
        "positional_no_browser", nargs="?", type=_bool_argument, help=argparse.SUPPRESS
    )
    login.add_argument(
        "-n",
        "--no-browser",
        "--no_browser",
        dest="flag_no_browser",
        action="store_true",
        help="Print the login URL instead of opening a browser.",
    )
    login.set_defaults(handler=_legacy_login)

    dashboard = root.add_parser(
        "dashboard",
        allow_abbrev=False,
        description=(
            "Open the cluster dashboard in a web browser.\n\n"
            "If no coordinator is running, one is started on this machine first\n"
            "and runs until Ctrl-C. The dashboard shows nodes, jobs, logs, metrics,\n"
            "settings, and users."
        ),
    )
    dashboard.add_argument(
        "positional_port", nargs="?", type=int, help=argparse.SUPPRESS
    )
    dashboard.add_argument(
        "-p",
        "--port",
        dest="flag_port",
        type=int,
        metavar="N",
        help=(
            "Serve the coordinator and dashboard on this port, open to any "
            "machine that can reach it with no login. Implies --local."
        ),
    )
    dashboard.add_argument(
        "--local",
        dest="flag_local",
        action="store_true",
        help=(
            "Run a coordinator and dashboard on this machine even if this cloud "
            "account has a deployed one."
        ),
    )
    dashboard.set_defaults(handler=_legacy_dashboard)

    deploy = root.add_parser(
        "deploy",
        allow_abbrev=False,
        description=(
            "Deploy (or update) a shared, always-on Burla cluster in your cloud\n"
            "account.\n\n"
            "Burla needs no deployment to work: `remote_parallel_map` and\n"
            "`burla dashboard` run the cluster's coordinator on this machine.\n"
            "Deploy when the cluster should stay up and be shared by a team."
        ),
    )
    deploy.add_argument("positional_cloud", nargs="?", help=argparse.SUPPRESS)
    deploy.add_argument(
        "-c",
        "--cloud",
        dest="flag_cloud",
        metavar="CLOUD",
        help=(
            "Cloud to deploy into: aws, gcp, or azure "
            "(default: `burla config get cloud`)."
        ),
    )
    deploy.set_defaults(handler=_legacy_deploy)

    config = root.add_parser(
        "config",
        allow_abbrev=False,
        description=(
            "Show or set local client settings.\n"
            "The only setting is `cloud`: which cloud provider Burla uses."
        ),
    )
    config.set_defaults(handler=_legacy_group_help, group_parser=config)
    commands = config.add_subparsers(
        dest="config_command", title="Commands", metavar="<command>"
    )
    get = commands.add_parser(
        "get",
        allow_abbrev=False,
        help="Print local client settings.",
        description=(
            "Print local client settings: cloud, environment, and backend URL.\n"
            "Pass KEY to print one value."
        ),
    )
    get.add_argument(
        "positional_key",
        nargs="?",
        metavar="KEY",
        help="Setting to print: cloud, environment, or backend.",
    )
    get.add_argument("-k", "--key", dest="flag_key", help=argparse.SUPPRESS)
    get.set_defaults(handler=_legacy_config_get)
    set_ = commands.add_parser(
        "set",
        allow_abbrev=False,
        usage="burla config set [options] KEY VALUE",
        help="Set a local client setting.",
        description="Set a local client setting. The only settable key is `cloud`.",
        epilog="Examples:\n  burla config set cloud aws",
    )
    set_.add_argument(
        "positional_key",
        nargs="?",
        metavar="KEY",
        help="Setting to change; only `cloud` is supported.",
    )
    set_.add_argument(
        "positional_value",
        nargs="?",
        metavar="VALUE",
        help="New value: aws, gcp, or azure.",
    )
    set_.add_argument("-k", "--key", dest="flag_key", help=argparse.SUPPRESS)
    set_.add_argument("-v", "--value", dest="flag_value", help=argparse.SUPPRESS)
    set_.set_defaults(handler=_legacy_config_set)


def _cluster_parser(root):
    cluster = root.add_parser(
        "cluster",
        allow_abbrev=False,
        description=(
            "Show cluster status, or start, restart, stop, and watch it.\n"
            "All output is JSON."
        ),
    )
    commands = cluster.add_subparsers(
        dest="cluster_command", required=True, title="Commands", metavar="<command>"
    )
    status = commands.add_parser(
        "status",
        allow_abbrev=False,
        help="Print one JSON snapshot of the cluster.",
        description=(
            "Print one JSON snapshot of the cluster: status (off, booting, ready,\n"
            "or running), node counts, total vCPUs and memory, and how many jobs\n"
            "are running."
        ),
    )
    status.set_defaults(
        handler=_request_command,
        command_name="cluster.status",
        method="GET",
        path="/v1/management/cluster",
    )
    watch = commands.add_parser(
        "watch",
        allow_abbrev=False,
        help="Stream cluster state changes as NDJSON until interrupted.",
        description=(
            "Stream cluster state as NDJSON: a full snapshot first, then one line\n"
            "per change. Runs until interrupted (Ctrl-C)."
        ),
    )
    watch.set_defaults(
        handler=_sse_command,
        command_name="cluster.watch",
        path="/v1/management/cluster/watch",
    )
    lifecycle = (
        (
            "start",
            "Boot the configured nodes and wait until they are ready.",
            "Boot node VMs using the current `burla settings`, then wait until\n"
            "they are ready to run jobs. Prints one JSON result when done.",
        ),
        (
            "restart",
            "Replace all nodes with fresh ones (cancels running jobs).",
            "Cancel running jobs, delete all node VMs, boot fresh ones, and wait\n"
            "until they are ready. Prints one JSON result when done.",
        ),
        (
            "stop",
            "Delete all nodes (cancels running jobs).",
            "Cancel running jobs, delete all node VMs, and wait until the cluster\n"
            "is fully off. Prints one JSON result when done.",
        ),
    )
    for action, help_text, description in lifecycle:
        action_parser = commands.add_parser(
            action, allow_abbrev=False, help=help_text, description=description
        )
        action_parser.set_defaults(
            handler=_request_command,
            command_name=f"cluster.{action}",
            method="POST",
            path=f"/v1/management/cluster/{action}",
            long_running=True,
        )


def _nodes_parser(root):
    nodes = root.add_parser(
        "nodes",
        allow_abbrev=False,
        description=(
            "List the cluster's VMs (nodes), inspect one, or read its logs.\n"
            "All output is JSON."
        ),
    )
    commands = nodes.add_subparsers(
        dest="nodes_command", required=True, title="Commands", metavar="<command>"
    )
    list_ = commands.add_parser(
        "list",
        allow_abbrev=False,
        help="List nodes (active ones by default).",
        description=(
            "List nodes. Shows active nodes (booting, ready, or running) by\n"
            "default; pass `--status all` to include deleted and failed nodes."
        ),
        epilog=(
            "Examples:\n"
            "  burla nodes list\n"
            "  burla nodes list --status all --limit 20\n"
            "  burla nodes list --job JOB_ID"
        ),
    )
    list_.add_argument(
        "--status",
        choices=("active", "booting", "ready", "running", "failed", "deleted", "all"),
        default="active",
        help="Only include nodes with this status (default: active).",
    )
    list_.add_argument(
        "--region", metavar="REGION", help="Only include nodes in this cloud region."
    )
    list_.add_argument(
        "--job", metavar="JOB_ID", help="Only include nodes that worked on this job."
    )
    list_.add_argument(
        "--started-after",
        metavar="TIME",
        help="Only include nodes started after this ISO-8601 UTC time.",
    )
    list_.add_argument(
        "--ended-after",
        metavar="TIME",
        help="Only include nodes that ended after this ISO-8601 UTC time.",
    )
    _add_list_arguments(
        list_, ("started_at", "ended_at", "status", "machine_type")
    )
    list_.set_defaults(handler=_nodes_list, command_name="nodes.list")
    show = commands.add_parser(
        "show",
        allow_abbrev=False,
        help="Print one node's full record.",
        description=(
            "Print one node's full record: status, machine type, region, timing,\n"
            "current job, and any error."
        ),
    )
    show.add_argument(
        "node_id", metavar="NODE_ID", help="Node ID, from `burla nodes list`."
    )
    show.set_defaults(handler=_node_show, command_name="nodes.show")
    logs = commands.add_parser(
        "logs",
        allow_abbrev=False,
        help="Print a node's logs, or stream them with --follow.",
        description="Print a node's logs, or stream them live with --follow.",
        epilog=(
            "Examples:\n"
            "  burla nodes logs NODE_ID\n"
            "  burla nodes logs NODE_ID --follow"
        ),
    )
    logs.add_argument(
        "node_id", metavar="NODE_ID", help="Node ID, from `burla nodes list`."
    )
    _add_log_arguments(logs, follow=True)
    logs.set_defaults(handler=_node_logs, command_name="nodes.logs")


def _jobs_parser(root):
    jobs = root.add_parser(
        "jobs",
        allow_abbrev=False,
        description=(
            "List, inspect, watch, or cancel jobs. Every `remote_parallel_map`\n"
            "call is one job; each input it maps over is one function call\n"
            "(see `burla jobs calls --help`). All output is JSON."
        ),
    )
    commands = jobs.add_subparsers(
        dest="jobs_command", required=True, title="Commands", metavar="<command>"
    )
    list_ = commands.add_parser(
        "list",
        allow_abbrev=False,
        help="List jobs, most recent first.",
        description="List jobs, most recent first.",
        epilog=(
            "Examples:\n"
            "  burla jobs list --status running\n"
            "  burla jobs list --function train_model --limit 10"
        ),
    )
    list_.add_argument(
        "--status",
        choices=("running", "completed", "failed", "canceled"),
        help="Only include jobs with this status.",
    )
    list_.add_argument(
        "--user", metavar="EMAIL", help="Only include jobs started by this user."
    )
    list_.add_argument(
        "--function",
        dest="function_name",
        metavar="NAME",
        help="Only include jobs whose function has this name.",
    )
    list_.add_argument(
        "--started-after",
        metavar="TIME",
        help="Only include jobs started after this ISO-8601 UTC time.",
    )
    list_.add_argument(
        "--started-before",
        metavar="TIME",
        help="Only include jobs started before this ISO-8601 UTC time.",
    )
    _add_list_arguments(
        list_,
        (
            "started_at",
            "ended_at",
            "duration",
            "status",
            "input_count",
            "result_count",
            "failed_count",
        ),
    )
    list_.set_defaults(handler=_jobs_list, command_name="jobs.list")
    show = commands.add_parser(
        "show",
        allow_abbrev=False,
        help="Print one job's full record.",
        description=(
            "Print one job's full record: status, function name, user, timing,\n"
            "node count, and input/result/failure counts."
        ),
    )
    show.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    show.set_defaults(handler=_job_show, command_name="jobs.show")
    watch = commands.add_parser(
        "watch",
        allow_abbrev=False,
        help="Stream a job's state as NDJSON until it finishes.",
        description=(
            "Stream a job's state as NDJSON: a snapshot first, then one line per\n"
            "change. Exits when the job finishes: 0 completed, 7 failed, 8\n"
            "canceled."
        ),
    )
    watch.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    watch.set_defaults(handler=_job_watch, command_name="jobs.watch")
    cancel = commands.add_parser(
        "cancel",
        allow_abbrev=False,
        help="Cancel a running job.",
        description="Cancel a running job and wait until the cancellation is complete.",
    )
    cancel.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    cancel.set_defaults(handler=_job_cancel, command_name="jobs.cancel")
    errors = commands.add_parser(
        "errors",
        allow_abbrev=False,
        help="Print a job's failures, grouped by traceback.",
        description=(
            "Print a job's failures grouped by traceback, each with a count, a\n"
            "representative traceback, and sample input indexes. Inspect one\n"
            "failure with `burla jobs calls show` or `burla jobs calls logs`."
        ),
    )
    errors.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    _add_list_arguments(errors)
    errors.set_defaults(handler=_job_errors, command_name="jobs.errors")
    metrics = commands.add_parser(
        "metrics",
        allow_abbrev=False,
        help="Print a job's utilization time series.",
        description=(
            "Print a job's utilization time series: node count, CPU, memory,\n"
            "network, disk, and GPU when present. Same data as the dashboard\n"
            "charts."
        ),
    )
    metrics.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    metrics.add_argument(
        "--raw",
        action="store_true",
        help="Stream every stored sample as NDJSON instead of the bounded series.",
    )
    metrics.add_argument(
        "--limit",
        type=int,
        default=10_000,
        metavar="N",
        help="With --raw: maximum samples to return (default: 10000).",
    )
    metrics.add_argument(
        "--cursor",
        metavar="CURSOR",
        help="With --raw: resume from a cursor emitted by a previous stream.",
    )
    metrics.set_defaults(handler=_job_metrics, command_name="jobs.metrics")

    calls = commands.add_parser(
        "calls",
        allow_abbrev=False,
        help="Inspect a job's individual function calls (one per input).",
        description=(
            "Inspect a job's individual function calls. Each input to\n"
            "`remote_parallel_map` becomes one call, identified by its input\n"
            "index. All output is JSON."
        ),
    )
    call_commands = calls.add_subparsers(
        dest="calls_command", required=True, title="Commands", metavar="<command>"
    )
    call_list = call_commands.add_parser(
        "list",
        allow_abbrev=False,
        help="List a job's function calls.",
        description="List a job's function calls, one per input.",
        epilog=(
            "Examples:\n"
            "  burla jobs calls list JOB_ID --failed-only\n"
            "  burla jobs calls list JOB_ID --sort duration --order desc"
        ),
    )
    call_list.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    call_list.add_argument(
        "--input-index",
        type=int,
        metavar="N",
        help="Only the call for this input index.",
    )
    call_list.add_argument(
        "--status",
        choices=(
            "pending",
            "running",
            "succeeded",
            "failed",
            "canceled",
            "not_run",
            "unknown",
        ),
        help="Only include calls with this status.",
    )
    call_list.add_argument(
        "--failed-only", action="store_true", help="Only include failed calls."
    )
    call_list.add_argument(
        "--logs-only",
        action="store_true",
        help="Only include calls that printed logs.",
    )
    call_list.add_argument(
        "--has-metrics",
        action="store_true",
        help="Only include calls that have utilization samples.",
    )
    _add_list_arguments(
        call_list,
        (
            "input_index",
            "started_at",
            "ended_at",
            "duration",
            "attempts",
            "status",
            "peak_cpu",
            "peak_memory",
        ),
    )
    call_list.set_defaults(handler=_calls_list, command_name="jobs.calls.list")
    call_show = call_commands.add_parser(
        "show",
        allow_abbrev=False,
        help="Print one call's full record.",
        description=(
            "Print one call's full record: status, attempts, timing, and peak\n"
            "CPU and memory."
        ),
    )
    call_show.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    call_show.add_argument(
        "input_index",
        type=int,
        metavar="INPUT_INDEX",
        help="Index of the input the call ran on (0-based).",
    )
    call_show.set_defaults(handler=_call_show, command_name="jobs.calls.show")
    call_logs = call_commands.add_parser(
        "logs",
        allow_abbrev=False,
        help="Print everything one call logged.",
        description="Print everything one call logged: its prints and tracebacks.",
    )
    call_logs.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    call_logs.add_argument(
        "input_index",
        type=int,
        metavar="INPUT_INDEX",
        help="Index of the input the call ran on (0-based).",
    )
    call_logs.add_argument(
        "--errors-only", action="store_true", help="Only include error output."
    )
    _add_log_arguments(call_logs, follow=False)
    call_logs.set_defaults(handler=_call_logs, command_name="jobs.calls.logs")
    call_metrics = call_commands.add_parser(
        "metrics",
        allow_abbrev=False,
        help="Print one call's utilization time series.",
        description=(
            "Print one call's utilization time series: CPU cores, memory bytes,\n"
            "network and disk rates, and GPU when present."
        ),
    )
    call_metrics.add_argument(
        "job_id", metavar="JOB_ID", help="Job ID, from `burla jobs list`."
    )
    call_metrics.add_argument(
        "input_index",
        type=int,
        metavar="INPUT_INDEX",
        help="Index of the input the call ran on (0-based).",
    )
    call_metrics.add_argument(
        "--raw",
        action="store_true",
        help="Stream every stored sample as NDJSON instead of the bounded series.",
    )
    call_metrics.add_argument(
        "--limit",
        type=int,
        default=10_000,
        metavar="N",
        help="With --raw: maximum samples to return (default: 10000).",
    )
    call_metrics.add_argument(
        "--cursor",
        metavar="CURSOR",
        help="With --raw: resume from a cursor emitted by a previous stream.",
    )
    call_metrics.set_defaults(handler=_call_metrics, command_name="jobs.calls.metrics")


def _settings_usage_parsers(root):
    settings = root.add_parser(
        "settings",
        allow_abbrev=False,
        description=(
            "Show or change cluster settings: the Docker image and machine type\n"
            "nodes run, how many boot, in which region, their disk size, and the\n"
            "idle shutdown timeout. All output is JSON."
        ),
    )
    commands = settings.add_subparsers(
        dest="settings_command", required=True, title="Commands", metavar="<command>"
    )
    show = commands.add_parser(
        "show",
        allow_abbrev=False,
        help="Print current cluster settings and the valid options for each.",
        description="Print current cluster settings and the valid options for each.",
    )
    show.set_defaults(
        handler=_request_command,
        command_name="settings.show",
        method="GET",
        path="/v1/management/settings",
    )
    update = commands.add_parser(
        "update",
        allow_abbrev=False,
        help="Change one or more cluster settings.",
        description=(
            "Change one or more cluster settings. Changes apply to nodes booted\n"
            "after the update; restart the cluster to apply them everywhere."
        ),
        epilog=(
            "Examples:\n"
            "  burla settings update --quantity 10\n"
            "  burla settings update --machine-type n4-standard-8 --disk-gb 100"
        ),
    )
    update.add_argument(
        "--image",
        metavar="IMAGE",
        help="Docker image user code runs in on every node.",
    )
    update.add_argument(
        "--machine-type",
        metavar="TYPE",
        help="VM type for every node; `burla settings show` lists valid types.",
    )
    update.add_argument(
        "--quantity", type=int, metavar="N", help="Number of nodes the cluster boots."
    )
    update.add_argument(
        "--region", metavar="REGION", help="Cloud region nodes boot in."
    )
    update.add_argument(
        "--disk-gb", type=int, metavar="N", help="Disk size of every node, in GB."
    )
    update.add_argument(
        "--inactivity-timeout-seconds",
        type=int,
        metavar="N",
        help="Delete idle nodes after this many seconds (default: 600).",
    )
    update.add_argument(
        "--max-vcpus",
        type=int,
        metavar="N",
        help="Global ceiling on vCPUs across all CPU nodes; grow never exceeds it.",
    )
    update.add_argument(
        "--max-gpus",
        action="append",
        metavar="MODEL=N",
        help=(
            "Global ceiling on GPUs of one model, e.g. --max-gpus A100=8. "
            "Repeatable; models not mentioned keep their current cap."
        ),
    )
    update.set_defaults(handler=_settings_update, command_name="settings.update")

    usage = root.add_parser(
        "usage",
        allow_abbrev=False,
        description="Show compute hours and estimated spend. All output is JSON.",
    )
    usage_commands = usage.add_subparsers(
        dest="usage_command", required=True, title="Commands", metavar="<command>"
    )
    usage_show = usage_commands.add_parser(
        "show",
        allow_abbrev=False,
        help="Print one month's compute hours and estimated spend.",
        description=(
            "Print one month's node hours, compute hours, and estimated spend in\n"
            "USD, broken down by day and machine type."
        ),
    )
    usage_show.add_argument(
        "--month",
        metavar="YYYY-MM",
        help="Month to report (default: the current month).",
    )
    usage_show.set_defaults(handler=_usage_show, command_name="usage.show")


def build_parser():
    parser = StrictArgumentParser(
        prog="burla",
        allow_abbrev=False,
        description=DESCRIPTION,
        epilog=_epilog(),
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=__version__,
        help="Print the installed burla version and exit.",
    )
    parser.add_argument(
        "--head",
        metavar="URL",
        help=(
            "Send management commands to the cluster at this URL instead of the "
            "saved one (also overrides BURLA_CLUSTER_DASHBOARD_URL)."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Also raise errors as Python tracebacks, for debugging burla itself.",
    )
    root = parser.add_subparsers(dest="root_command", metavar="<command>")
    _legacy_parser(root)
    auth = root.add_parser(
        "auth",
        allow_abbrev=False,
        description=(
            "Show which cluster this CLI talks to and how it authenticates.\n"
            "All output is JSON."
        ),
    )
    auth_commands = auth.add_subparsers(
        dest="auth_command", required=True, title="Commands", metavar="<command>"
    )
    auth_status = auth_commands.add_parser(
        "status",
        allow_abbrev=False,
        help="Print the selected cluster and whether credentials work.",
        description=(
            "Print which cluster URL is selected and where it came from, which\n"
            "credentials are in use, and whether the cluster is reachable and\n"
            "accepts them. Read-only; run this first when a management command\n"
            "fails."
        ),
    )
    auth_status.set_defaults(handler=_auth_status, command_name="auth.status")
    _cluster_parser(root)
    _nodes_parser(root)
    _jobs_parser(root)
    _settings_usage_parsers(root)
    return parser


def _legacy_value(positional, flag, label):
    if positional is not None and flag is not None and positional != flag:
        raise ArgumentError(f"{label} was provided twice with different values")
    return flag if flag is not None else positional


def _print_legacy_result(result):
    if result is None:
        return
    if isinstance(result, dict):
        width = max(len(key) for key in result)
        for key, value in result.items():
            print(f"{key + ':':<{width + 2}} {value}")
        return
    print(result)


def _legacy_login(args):
    from burla._auth import login

    no_browser = bool(args.flag_no_browser)
    if args.positional_no_browser is not None:
        no_browser = args.positional_no_browser
    return login(no_browser=no_browser)


def _legacy_group_help(args):
    args.group_parser.print_help()


def _legacy_dashboard(args):
    from burla import dashboard

    port = _legacy_value(args.positional_port, args.flag_port, "port")
    return dashboard(port=port, local=args.flag_local)


def _legacy_deploy(args):
    from burla._deploy import deploy

    cloud = _legacy_value(args.positional_cloud, args.flag_cloud, "cloud")
    return deploy(cloud=cloud)


def _legacy_config_get(args):
    from burla import get_config

    key = _legacy_value(args.positional_key, args.flag_key, "key")
    return get_config(key)


def _legacy_config_set(args):
    from burla import set_config

    key = _legacy_value(args.positional_key, args.flag_key, "key")
    value = _legacy_value(args.positional_value, args.flag_value, "value")
    if key is None or value is None:
        raise ArgumentError("config set requires KEY and VALUE")
    return set_config(key, value)


def _compact(values):
    return {key: value for key, value in values.items() if value is not None}


def _client(args, allow_missing=False):
    context = resolve_management_context(args.head, allow_missing=allow_missing)
    client = ManagementClient(context) if context.head_url else None
    return context, client


def _emit(document):
    print(json.dumps(document, separators=(",", ":")), flush=True)


def _emitted_at():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _success(command, data, request_id):
    return {
        "schema_version": 1,
        "ok": True,
        "command": command,
        "request_id": request_id,
        "data": data,
    }


def _emit_error(command, request_id, error):
    _emit(
        {
            "schema_version": 1,
            "ok": False,
            "command": command,
            "request_id": error.request_id or request_id,
            "error": error.as_dict(),
        }
    )


def _stream_line(command, request_id, sequence, event, data, cursor=None):
    line = {
        "schema_version": 1,
        "ok": True,
        "command": command,
        "request_id": request_id,
        "event": event,
        "sequence": sequence,
        "emitted_at": _emitted_at(),
        "cursor": cursor,
        "data": data,
    }
    _emit(line)


def _stream_error(command, request_id, sequence, error):
    _emit(
        {
            "schema_version": 1,
            "ok": False,
            "command": command,
            "request_id": error.request_id or request_id,
            "event": "error",
            "sequence": sequence,
            "emitted_at": _emitted_at(),
            "cursor": None,
            "error": error.as_dict(),
        }
    )


def _auth_status(args):
    context, client = _client(args, allow_missing=True)
    data = {
        "head_url": context.head_url,
        "head_source": context.head_source,
        "project_id": context.project_id,
        "auth_source": context.auth_source,
        "principal": context.principal,
        "reachable": False,
        "authenticated": False,
        "head": None,
    }
    if client is None:
        return data
    try:
        data["head"] = client.request("GET", "/version")
    except ManagementError as error:
        if error.code in {"AUTH_REQUIRED", "FORBIDDEN"}:
            data["reachable"] = True
            return data
        if error.code == "HEAD_UNREACHABLE":
            return data
        raise
    data["reachable"] = True
    data["authenticated"] = True
    data["project_id"] = data["head"].get("project")
    return data


def _request_command(args):
    _, client = _client(args)
    return client.request(
        args.method,
        args.path,
        long_running=getattr(args, "long_running", False),
    )


def _sse_command(args):
    _, client = _client(args)
    return _run_sse(args, client, args.path)


def _run_sse(args, client, path, params=None):
    request_id = uuid4().hex
    sequence = 1
    _stream_line(
        args.command_name,
        request_id,
        sequence,
        "stream_start",
        {"resume_supported": False, "query": params or {}},
    )
    sequence += 1
    try:
        for item in client.stream_sse(path, params=params):
            data = item["data"]
            event = item["event"]
            _stream_line(
                args.command_name,
                request_id,
                sequence,
                event,
                data,
                item.get("cursor"),
            )
            sequence += 1
            if args.command_name == "jobs.watch":
                status = str(data.get("status", "")).lower()
                if status in {"completed", "failed", "canceled"}:
                    _stream_line(
                        args.command_name,
                        request_id,
                        sequence,
                        "stream_end",
                        {"status": status},
                    )
                    if status == "failed":
                        return 7
                    if status == "canceled":
                        return 8
                    return 0
    except ManagementError as error:
        _stream_error(
            args.command_name,
            request_id,
            sequence,
            error,
        )
        return error.exit_code
    _stream_line(args.command_name, request_id, sequence, "stream_end", {})
    return 0


def _run_ndjson(args, client, path, params):
    request_id = uuid4().hex
    sequence = 1
    _stream_line(
        args.command_name,
        request_id,
        sequence,
        "stream_start",
        {"resume_supported": True, "query": params},
    )
    sequence += 1
    try:
        for item in client.stream_ndjson(path, params=params):
            cursor = item.pop("cursor", None)
            _stream_line(
                args.command_name,
                request_id,
                sequence,
                "metric",
                item,
                cursor,
            )
            sequence += 1
    except ManagementError as error:
        _stream_error(
            args.command_name,
            request_id,
            sequence,
            error,
        )
        return error.exit_code
    _stream_line(args.command_name, request_id, sequence, "stream_end", {})
    return 0


def _nodes_list(args):
    _, client = _client(args)
    params = _compact(
        {
            "status": args.status,
            "region": args.region,
            "job_id": args.job,
            "started_after": args.started_after,
            "ended_after": args.ended_after,
            "sort": args.sort,
            "order": args.order,
            "limit": args.limit,
            "cursor": args.cursor,
        }
    )
    return client.request("GET", "/v1/management/nodes", params=params)


def _node_show(args):
    _, client = _client(args)
    return client.request("GET", f"/v1/management/nodes/{args.node_id}")


def _node_logs(args):
    _, client = _client(args)
    params = _compact(
        {"before": args.before, "after": args.after, "limit": args.limit}
    )
    path = f"/v1/management/nodes/{args.node_id}/logs"
    if args.follow:
        if args.before:
            raise ManagementError(
                "INVALID_ARGUMENT", "--before cannot be used with --follow."
            )
        return _run_sse(
            args, client, f"{path}/stream", params=_compact({"after": args.after})
        )
    return client.request("GET", path, params=params)


def _jobs_list(args):
    _, client = _client(args)
    params = _compact(
        {
            "status": args.status,
            "user": args.user,
            "function_name": args.function_name,
            "started_after": args.started_after,
            "started_before": args.started_before,
            "sort": args.sort,
            "order": args.order,
            "limit": args.limit,
            "cursor": args.cursor,
        }
    )
    return client.request("GET", "/v1/management/jobs", params=params)


def _job_show(args):
    _, client = _client(args)
    return client.request("GET", f"/v1/management/jobs/{args.job_id}")


def _job_watch(args):
    _, client = _client(args)
    return _run_sse(
        args,
        client,
        f"/v1/management/jobs/{args.job_id}/watch",
    )


def _job_cancel(args):
    _, client = _client(args)
    return client.request(
        "POST",
        f"/v1/management/jobs/{args.job_id}/cancel",
    )


def _job_errors(args):
    _, client = _client(args)
    params = _compact({"limit": args.limit, "cursor": args.cursor})
    return client.request(
        "GET",
        f"/v1/management/jobs/{args.job_id}/errors",
        params=params,
    )


def _job_metrics(args):
    _, client = _client(args)
    params = _compact({"limit": args.limit, "cursor": args.cursor})
    path = f"/v1/management/jobs/{args.job_id}/metrics"
    if args.raw:
        return _run_ndjson(args, client, f"{path}/raw", params)
    return client.request("GET", path)


def _calls_list(args):
    _, client = _client(args)
    params = _compact(
        {
            "input_index": args.input_index,
            "status": args.status,
            "failed_only": args.failed_only or None,
            "logs_only": args.logs_only or None,
            "has_metrics": args.has_metrics or None,
            "sort": args.sort,
            "order": args.order,
            "limit": args.limit,
            "cursor": args.cursor,
        }
    )
    return client.request(
        "GET",
        f"/v1/management/jobs/{args.job_id}/calls",
        params=params,
    )


def _call_show(args):
    _, client = _client(args)
    return client.request(
        "GET",
        f"/v1/management/jobs/{args.job_id}/calls/{args.input_index}",
    )


def _call_logs(args):
    _, client = _client(args)
    params = _compact(
        {
            "errors_only": args.errors_only or None,
            "before": args.before,
            "after": args.after,
            "limit": args.limit,
        }
    )
    return client.request(
        "GET",
        f"/v1/management/jobs/{args.job_id}/calls/{args.input_index}/logs",
        params=params,
    )


def _call_metrics(args):
    _, client = _client(args)
    params = _compact({"limit": args.limit, "cursor": args.cursor})
    path = f"/v1/management/jobs/{args.job_id}/calls/{args.input_index}/metrics"
    if args.raw:
        return _run_ndjson(args, client, f"{path}/raw", params)
    return client.request("GET", path)


def _parse_max_gpus(entries):
    if not entries:
        return None
    max_gpus = {}
    for entry in entries:
        model, sep, count = entry.partition("=")
        if not sep or not model or not count.isdigit():
            raise ManagementError(
                "INVALID_ARGUMENT",
                f"--max-gpus expects MODEL=N (e.g. A100=8), got {entry!r}.",
            )
        max_gpus[model] = int(count)
    return max_gpus


def _settings_update(args):
    _, client = _client(args)
    body = _compact(
        {
            "image": args.image,
            "machine_type": args.machine_type,
            "quantity": args.quantity,
            "region": args.region,
            "disk_gb": args.disk_gb,
            "inactivity_timeout_seconds": args.inactivity_timeout_seconds,
            "max_vcpus": args.max_vcpus,
            "max_gpus": _parse_max_gpus(args.max_gpus),
        }
    )
    if not body:
        raise ManagementError(
            "INVALID_ARGUMENT",
            "settings update requires at least one setting flag.",
        )
    return client.request(
        "PATCH",
        "/v1/management/settings",
        body=body,
    )


def _usage_show(args):
    _, client = _client(args)
    return client.request(
        "GET",
        "/v1/management/usage",
        params=_compact({"month": args.month}),
    )


def _management_argv(argv):
    return any(value in MANAGEMENT_GROUPS for value in argv)


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if not argv:
        parser.print_help()
        return 0

    command_name = "cli"
    request_id = uuid4().hex
    try:
        args = parser.parse_args(argv)
        if args.root_command is None:
            parser.print_help()
            return 0
        command_name = getattr(args, "command_name", args.root_command)
        result = args.handler(args)
        if args.root_command not in MANAGEMENT_GROUPS:
            _print_legacy_result(result)
            return 0
        if isinstance(result, int):
            return result
        _emit(_success(command_name, result, request_id))
        return 0
    except ArgumentError as error:
        if _management_argv(argv):
            management_error = ManagementError("INVALID_ARGUMENT", str(error))
            _emit_error(command_name, request_id, management_error)
        else:
            parser.print_usage(sys.stderr)
            print(f"burla: error: {error}", file=sys.stderr)
        return 2
    except ManagementError as error:
        _emit_error(command_name, request_id, error)
        if "--debug" in argv:
            raise
        return error.exit_code
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
