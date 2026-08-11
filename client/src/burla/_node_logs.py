"""Print node logs as the head recorded them (`make node-logs`).

`docker logs node_*` only exists for local-dev nodes on this machine, and the
container is replaced between jobs, so it cannot show what a node did earlier in
a job's life. The head keeps every node log line in its history db, which covers
remote-dev's EC2 nodes too.
"""

import argparse
import sqlite3
import time
from pathlib import Path

from burla._local_head import STATE_ROOT


def _history_db(local_dev_db: Path, namespace: str) -> Path:
    """This checkout's history db, most specific location first.

    Ordered rather than newest-first on purpose: many clusters share this
    machine and the account-wide db belongs to ad hoc client-hosted heads, so
    choosing by write time can silently show a different cluster's nodes.
    """
    candidates = [local_dev_db]
    candidates += sorted(STATE_ROOT.glob(f"*/cluster-{namespace}/history.db"))
    candidates += sorted(STATE_ROOT.glob("*/history.db"))
    for path in candidates:
        if path.exists():
            return path
    raise SystemExit(f"no history db under {STATE_ROOT}, has this cluster run?")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", default="", help="only nodes matching this substring")
    parser.add_argument("--job", default="", help="only lines mentioning this job id")
    parser.add_argument("-n", "--lines", type=int, default=200)
    parser.add_argument("--local-dev-db", default="", help="local-dev history db path")
    parser.add_argument("--namespace", default="", help="this checkout's cluster name")
    args = parser.parse_args()

    db = _history_db(Path(args.local_dev_db), args.namespace)

    query = "SELECT instance_name, ts, msg FROM node_logs"
    params = []
    if args.node:
        query += " WHERE instance_name LIKE ?"
        params.append(f"%{args.node}%")
    query += " ORDER BY id DESC LIMIT ?"
    params.append(args.lines)

    print(f"# {db}")
    rows = sqlite3.connect(db).execute(query, params).fetchall()
    for instance_name, ts, msg in reversed(rows):
        if args.job and args.job not in msg:
            continue
        stamp = time.strftime("%H:%M:%S", time.localtime(ts))
        for line in msg.splitlines() or [""]:
            print(f"{stamp}  {instance_name[-8:]}  {line}")


if __name__ == "__main__":
    main()
