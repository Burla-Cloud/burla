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


class ArgumentError(Exception):
    pass


class StrictArgumentParser(argparse.ArgumentParser):
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
        parser.add_argument("--sort", choices=sorts)
        parser.add_argument("--order", choices=("asc", "desc"), default="desc")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--cursor")


def _add_log_arguments(parser, follow):
    cursors = parser.add_mutually_exclusive_group()
    cursors.add_argument("--before")
    cursors.add_argument("--after")
    parser.add_argument("--limit", type=int, default=500)
    if follow:
        parser.add_argument("--follow", action="store_true")


def _legacy_parser(root):
    login = root.add_parser("login", allow_abbrev=False)
    login.add_argument("positional_no_browser", nargs="?", type=_bool_argument)
    login.add_argument(
        "-n", "--no-browser", "--no_browser", dest="flag_no_browser", action="store_true"
    )
    login.set_defaults(handler=_legacy_login)

    dashboard = root.add_parser("dashboard", allow_abbrev=False)
    dashboard.add_argument("positional_port", nargs="?", type=int)
    dashboard.add_argument("-p", "--port", dest="flag_port", type=int)
    dashboard.set_defaults(handler=_legacy_dashboard)

    deploy = root.add_parser("deploy", allow_abbrev=False)
    deploy.add_argument("positional_cloud", nargs="?")
    deploy.add_argument("-c", "--cloud", dest="flag_cloud")
    deploy.set_defaults(handler=_legacy_deploy)

    config = root.add_parser("config", allow_abbrev=False)
    config.set_defaults(handler=_legacy_group_help, group_parser=config)
    commands = config.add_subparsers(dest="config_command")
    get = commands.add_parser("get", allow_abbrev=False)
    get.add_argument("positional_key", nargs="?")
    get.add_argument("-k", "--key", dest="flag_key")
    get.set_defaults(handler=_legacy_config_get)
    set_ = commands.add_parser("set", allow_abbrev=False)
    set_.add_argument("positional_key", nargs="?")
    set_.add_argument("positional_value", nargs="?")
    set_.add_argument("-k", "--key", dest="flag_key")
    set_.add_argument("-v", "--value", dest="flag_value")
    set_.set_defaults(handler=_legacy_config_set)


def _cluster_parser(root):
    cluster = root.add_parser("cluster", allow_abbrev=False)
    commands = cluster.add_subparsers(dest="cluster_command", required=True)
    status = commands.add_parser("status", allow_abbrev=False)
    status.set_defaults(
        handler=_request_command,
        command_name="cluster.status",
        method="GET",
        path="/v1/management/cluster",
    )
    watch = commands.add_parser("watch", allow_abbrev=False)
    watch.set_defaults(
        handler=_sse_command,
        command_name="cluster.watch",
        path="/v1/management/cluster/watch",
    )
    for action in ("start", "restart", "stop"):
        action_parser = commands.add_parser(action, allow_abbrev=False)
        action_parser.set_defaults(
            handler=_request_command,
            command_name=f"cluster.{action}",
            method="POST",
            path=f"/v1/management/cluster/{action}",
            long_running=True,
        )


def _nodes_parser(root):
    nodes = root.add_parser("nodes", allow_abbrev=False)
    commands = nodes.add_subparsers(dest="nodes_command", required=True)
    list_ = commands.add_parser("list", allow_abbrev=False)
    list_.add_argument(
        "--status",
        choices=("active", "booting", "ready", "running", "failed", "deleted", "all"),
        default="active",
    )
    list_.add_argument("--region")
    list_.add_argument("--job")
    list_.add_argument("--started-after")
    list_.add_argument("--ended-after")
    _add_list_arguments(
        list_, ("started_at", "ended_at", "status", "machine_type")
    )
    list_.set_defaults(handler=_nodes_list, command_name="nodes.list")
    show = commands.add_parser("show", allow_abbrev=False)
    show.add_argument("node_id")
    show.set_defaults(handler=_node_show, command_name="nodes.show")
    logs = commands.add_parser("logs", allow_abbrev=False)
    logs.add_argument("node_id")
    _add_log_arguments(logs, follow=True)
    logs.set_defaults(handler=_node_logs, command_name="nodes.logs")


def _jobs_parser(root):
    jobs = root.add_parser("jobs", allow_abbrev=False)
    commands = jobs.add_subparsers(dest="jobs_command", required=True)
    list_ = commands.add_parser("list", allow_abbrev=False)
    list_.add_argument("--status", choices=("running", "completed", "failed", "canceled"))
    list_.add_argument("--user")
    list_.add_argument("--function", dest="function_name")
    list_.add_argument("--started-after")
    list_.add_argument("--started-before")
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
    show = commands.add_parser("show", allow_abbrev=False)
    show.add_argument("job_id")
    show.set_defaults(handler=_job_show, command_name="jobs.show")
    watch = commands.add_parser("watch", allow_abbrev=False)
    watch.add_argument("job_id")
    watch.set_defaults(handler=_job_watch, command_name="jobs.watch")
    cancel = commands.add_parser("cancel", allow_abbrev=False)
    cancel.add_argument("job_id")
    cancel.set_defaults(handler=_job_cancel, command_name="jobs.cancel")
    errors = commands.add_parser("errors", allow_abbrev=False)
    errors.add_argument("job_id")
    _add_list_arguments(errors)
    errors.set_defaults(handler=_job_errors, command_name="jobs.errors")
    metrics = commands.add_parser("metrics", allow_abbrev=False)
    metrics.add_argument("job_id")
    metrics.add_argument("--raw", action="store_true")
    metrics.add_argument("--limit", type=int, default=10_000)
    metrics.add_argument("--cursor")
    metrics.set_defaults(handler=_job_metrics, command_name="jobs.metrics")

    calls = commands.add_parser("calls", allow_abbrev=False)
    call_commands = calls.add_subparsers(dest="calls_command", required=True)
    call_list = call_commands.add_parser("list", allow_abbrev=False)
    call_list.add_argument("job_id")
    call_list.add_argument("--input-index", type=int)
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
    )
    call_list.add_argument("--failed-only", action="store_true")
    call_list.add_argument("--logs-only", action="store_true")
    call_list.add_argument("--has-metrics", action="store_true")
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
    call_show = call_commands.add_parser("show", allow_abbrev=False)
    call_show.add_argument("job_id")
    call_show.add_argument("input_index", type=int)
    call_show.set_defaults(handler=_call_show, command_name="jobs.calls.show")
    call_logs = call_commands.add_parser("logs", allow_abbrev=False)
    call_logs.add_argument("job_id")
    call_logs.add_argument("input_index", type=int)
    call_logs.add_argument("--errors-only", action="store_true")
    _add_log_arguments(call_logs, follow=False)
    call_logs.set_defaults(handler=_call_logs, command_name="jobs.calls.logs")
    call_metrics = call_commands.add_parser("metrics", allow_abbrev=False)
    call_metrics.add_argument("job_id")
    call_metrics.add_argument("input_index", type=int)
    call_metrics.add_argument("--raw", action="store_true")
    call_metrics.add_argument("--limit", type=int, default=10_000)
    call_metrics.add_argument("--cursor")
    call_metrics.set_defaults(handler=_call_metrics, command_name="jobs.calls.metrics")


def _settings_usage_parsers(root):
    settings = root.add_parser("settings", allow_abbrev=False)
    commands = settings.add_subparsers(dest="settings_command", required=True)
    show = commands.add_parser("show", allow_abbrev=False)
    show.set_defaults(
        handler=_request_command,
        command_name="settings.show",
        method="GET",
        path="/v1/management/settings",
    )
    update = commands.add_parser("update", allow_abbrev=False)
    update.add_argument("--image")
    update.add_argument("--machine-type")
    update.add_argument("--quantity", type=int)
    update.add_argument("--region")
    update.add_argument("--disk-gb", type=int)
    update.add_argument("--inactivity-timeout-seconds", type=int)
    update.set_defaults(handler=_settings_update, command_name="settings.update")

    usage = root.add_parser("usage", allow_abbrev=False)
    usage_commands = usage.add_subparsers(dest="usage_command", required=True)
    usage_show = usage_commands.add_parser("show", allow_abbrev=False)
    usage_show.add_argument("--month")
    usage_show.set_defaults(handler=_usage_show, command_name="usage.show")


def build_parser():
    parser = StrictArgumentParser(prog="burla", allow_abbrev=False)
    parser.add_argument("-v", "--version", action="version", version=__version__)
    parser.add_argument("--head")
    parser.add_argument("--debug", action="store_true")
    root = parser.add_subparsers(dest="root_command")
    _legacy_parser(root)
    auth = root.add_parser("auth", allow_abbrev=False)
    auth_commands = auth.add_subparsers(dest="auth_command", required=True)
    auth_status = auth_commands.add_parser("status", allow_abbrev=False)
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
    return dashboard(port=port)


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
